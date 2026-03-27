use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
};

use duckdb::ffi::{duckdb_string_t, duckdb_string_t_data, duckdb_string_t_length};
use simdxml::bloom::TagBloom;
use simdxml::quick::QuickScanner;
use simdxml::CompiledXPath;
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Thread-local reusable parse buffer — zero alloc after first document
// ---------------------------------------------------------------------------

thread_local! {
    static SCRATCH: RefCell<simdxml::scratch::ParseScratch> =
        RefCell::new(simdxml::scratch::ParseScratch::new());
}

// ---------------------------------------------------------------------------
// Shared XPath state: compiled once, reused across ALL chunks
// ---------------------------------------------------------------------------

pub struct XPathState {
    compiled: OnceLock<(String, CompiledXPath)>,
    interesting: OnceLock<Option<HashSet<String>>>,
    bloom_targets: OnceLock<Vec<Vec<u8>>>,
    scanner: OnceLock<Option<QuickScanner>>,
}

impl Default for XPathState {
    fn default() -> Self {
        Self {
            compiled: OnceLock::new(),
            interesting: OnceLock::new(),
            bloom_targets: OnceLock::new(),
            scanner: OnceLock::new(),
        }
    }
}

impl XPathState {
    fn get_compiled(&self, expr: &str) -> Option<&CompiledXPath> {
        let (cached_expr, compiled) = self.compiled.get_or_init(|| {
            let compiled = CompiledXPath::compile(expr)
                .expect("XPath compilation should succeed");
            (expr.to_string(), compiled)
        });
        if cached_expr == expr { Some(compiled) } else { None }
    }

    fn get_interesting(&self, compiled: &CompiledXPath) -> &Option<HashSet<String>> {
        self.interesting.get_or_init(|| compiled.interesting_names())
    }

    fn get_bloom_targets(&self, interesting: &Option<HashSet<String>>) -> &Vec<Vec<u8>> {
        self.bloom_targets.get_or_init(|| {
            interesting.as_ref()
                .map(|names| names.iter().map(|n| n.as_bytes().to_vec()).collect())
                .unwrap_or_default()
        })
    }

    fn get_scanner(&self, compiled: &CompiledXPath) -> &Option<QuickScanner> {
        self.scanner.get_or_init(|| {
            compiled.simple_tag_name().map(QuickScanner::new)
        })
    }
}

// ---------------------------------------------------------------------------
// Zero-copy input reading
// ---------------------------------------------------------------------------

unsafe fn read_constant_expr(input: &mut DataChunkHandle) -> String {
    let expr_vec = input.flat_vector(1);
    let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(input.len());
    String::from_utf8_lossy(str_bytes(expr_data, 0)).into_owned()
}

#[inline(always)]
unsafe fn str_bytes<'a>(data: &'a [duckdb_string_t], i: usize) -> &'a [u8] {
    let ptr = &data[i] as *const duckdb_string_t as *mut duckdb_string_t;
    let len = duckdb_string_t_length(*ptr);
    let data_ptr = duckdb_string_t_data(ptr);
    std::slice::from_raw_parts(data_ptr as *const u8, len as usize)
}

// ---------------------------------------------------------------------------
// xpath_text(xml VARCHAR, expr VARCHAR) → VARCHAR
// ---------------------------------------------------------------------------

pub struct XPathText;

impl VScalar for XPathText {
    type State = XPathState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        if len == 0 { return Ok(()); }

        let out = output.flat_vector();
        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let expr_string = read_constant_expr(input);

        if let Some(compiled) = state.get_compiled(&expr_string) {
            let scanner = state.get_scanner(compiled);
            let interesting = state.get_interesting(compiled);
            let bloom_targets = state.get_bloom_targets(interesting);
            let use_bloom = !bloom_targets.is_empty();
            let target_refs: Vec<&[u8]> = bloom_targets.iter().map(|n| n.as_slice()).collect();

            for i in 0..len {
                let doc = str_bytes(xml_data, i);

                // Tier 1: Pre-built scanner — zero alloc
                if let Some(ref s) = *scanner {
                    match s.extract_first(doc) {
                        Some(text) => { out.insert(i, text); continue; }
                        None => {}
                    }
                }

                // Tier 2: Bloom reject
                if use_bloom {
                    let bloom = TagBloom::from_prescan(doc);
                    if !bloom.may_contain_any(&target_refs) {
                        out.insert(i, "");
                        continue;
                    }
                }

                // Tier 3: Parse with reused buffers
                SCRATCH.with_borrow_mut(|scratch| {
                    scratch.with_parsed(doc, |index| {
                        let texts = compiled.eval_text(index)?;
                        out.insert(i, texts.first().copied().unwrap_or(""));
                        Ok(())
                    })
                })?;
            }
        } else {
            let expr_vec = input.flat_vector(1);
            let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);
            for i in 0..len {
                let doc = str_bytes(xml_data, i);
                let expr = std::str::from_utf8(str_bytes(expr_data, i))
                    .map_err(|e| simdxml::SimdXmlError::InvalidXml(format!("invalid XPath expression: {}", e)))?;
                SCRATCH.with_borrow_mut(|scratch| {
                    scratch.with_parsed(doc, |index| {
                        let texts = index.xpath_text(expr)?;
                        out.insert(i, texts.first().copied().unwrap_or(""));
                        Ok(())
                    })
                })?;
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

// ---------------------------------------------------------------------------
// xpath_list(xml VARCHAR, expr VARCHAR) → VARCHAR (JSON array)
// ---------------------------------------------------------------------------

pub struct XPathList;

impl VScalar for XPathList {
    type State = XPathState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        if len == 0 { return Ok(()); }

        let out = output.flat_vector();
        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let expr_string = read_constant_expr(input);

        let compiled = state.get_compiled(&expr_string);
        let bloom_targets = compiled.map(|c| {
            let int = state.get_interesting(c);
            state.get_bloom_targets(int)
        });
        let use_bloom = bloom_targets.map_or(false, |bt| !bt.is_empty());
        let target_refs: Vec<&[u8]> = bloom_targets
            .map(|bt| bt.iter().map(|n| n.as_slice()).collect())
            .unwrap_or_default();

        for i in 0..len {
            let doc = str_bytes(xml_data, i);

            if use_bloom {
                let bloom = TagBloom::from_prescan(doc);
                if !bloom.may_contain_any(&target_refs) {
                    out.insert(i, "[]");
                    continue;
                }
            }

            SCRATCH.with_borrow_mut(|scratch| {
                scratch.with_parsed(doc, |index| {
                    let texts = if let Some(c) = compiled {
                        c.eval_text(index)?
                    } else {
                        let expr_vec = input.flat_vector(1);
                        let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);
                        let expr = std::str::from_utf8(str_bytes(expr_data, i))
                            .map_err(|e| simdxml::SimdXmlError::InvalidXml(format!("invalid expression: {}", e)))?;
                        index.xpath_text(expr)?
                    };

                    let mut json = String::from("[");
                    for (j, t) in texts.iter().enumerate() {
                        if j > 0 { json.push(','); }
                        json.push('"');
                        for ch in t.chars() {
                            match ch {
                                '"' => json.push_str("\\\""),
                                '\\' => json.push_str("\\\\"),
                                '\n' => json.push_str("\\n"),
                                c => json.push(c),
                            }
                        }
                        json.push('"');
                    }
                    json.push(']');
                    out.insert(i, json.as_str());
                    Ok(())
                })
            })?;
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

// ---------------------------------------------------------------------------
// xpath_count(xml VARCHAR, expr VARCHAR) → BIGINT
// ---------------------------------------------------------------------------

pub struct XPathCount;

impl VScalar for XPathCount {
    type State = XPathState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        if len == 0 { return Ok(()); }

        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let mut out = output.flat_vector();
        let counts = out.as_mut_slice::<i64>();
        let expr_string = read_constant_expr(input);

        if let Some(compiled) = state.get_compiled(&expr_string) {
            let scanner = state.get_scanner(compiled);
            let interesting = state.get_interesting(compiled);
            let bloom_targets = state.get_bloom_targets(interesting);
            let use_bloom = !bloom_targets.is_empty();
            let target_refs: Vec<&[u8]> = bloom_targets.iter().map(|n| n.as_slice()).collect();

            for i in 0..len {
                let doc = str_bytes(xml_data, i);

                if let Some(ref s) = *scanner {
                    counts[i] = s.count(doc) as i64;
                    continue;
                }

                if use_bloom {
                    let bloom = TagBloom::from_prescan(doc);
                    if !bloom.may_contain_any(&target_refs) {
                        counts[i] = 0;
                        continue;
                    }
                }

                SCRATCH.with_borrow_mut(|scratch| {
                    scratch.with_parsed(doc, |index| {
                        counts[i] = compiled.eval_count(index)? as i64;
                        Ok(())
                    })
                })?;
            }
        } else {
            let expr_vec = input.flat_vector(1);
            let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);
            for i in 0..len {
                let doc = str_bytes(xml_data, i);
                let expr = std::str::from_utf8(str_bytes(expr_data, i))
                    .map_err(|e| simdxml::SimdXmlError::InvalidXml(format!("invalid XPath expression: {}", e)))?;
                SCRATCH.with_borrow_mut(|scratch| {
                    scratch.with_parsed(doc, |index| {
                        let nodes = index.xpath(expr)?;
                        counts[i] = nodes.len() as i64;
                        Ok(())
                    })
                })?;
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Varchar.into()],
            LogicalTypeId::Bigint.into(),
        )]
    }
}

// ---------------------------------------------------------------------------
// xpath_exists(xml VARCHAR, expr VARCHAR) → BOOLEAN
// ---------------------------------------------------------------------------

pub struct XPathExists;

impl VScalar for XPathExists {
    type State = XPathState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        if len == 0 { return Ok(()); }

        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let mut out = output.flat_vector();
        let bools = out.as_mut_slice::<bool>();
        let expr_string = read_constant_expr(input);

        if let Some(compiled) = state.get_compiled(&expr_string) {
            let scanner = state.get_scanner(compiled);
            let interesting = state.get_interesting(compiled);
            let bloom_targets = state.get_bloom_targets(interesting);
            let use_bloom = !bloom_targets.is_empty();
            let target_refs: Vec<&[u8]> = bloom_targets.iter().map(|n| n.as_slice()).collect();

            for i in 0..len {
                let doc = str_bytes(xml_data, i);

                if let Some(ref s) = *scanner {
                    bools[i] = s.exists(doc);
                    continue;
                }

                if use_bloom {
                    let bloom = TagBloom::from_prescan(doc);
                    if !bloom.may_contain_any(&target_refs) {
                        bools[i] = false;
                        continue;
                    }
                }

                SCRATCH.with_borrow_mut(|scratch| {
                    scratch.with_parsed(doc, |index| {
                        bools[i] = compiled.eval_exists(index)?;
                        Ok(())
                    })
                })?;
            }
        } else {
            let expr_vec = input.flat_vector(1);
            let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);
            for i in 0..len {
                let doc = str_bytes(xml_data, i);
                let expr = std::str::from_utf8(str_bytes(expr_data, i))
                    .map_err(|e| simdxml::SimdXmlError::InvalidXml(format!("invalid XPath expression: {}", e)))?;
                SCRATCH.with_borrow_mut(|scratch| {
                    scratch.with_parsed(doc, |index| {
                        let nodes = index.xpath(expr)?;
                        bools[i] = !nodes.is_empty();
                        Ok(())
                    })
                })?;
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Varchar.into()],
            LogicalTypeId::Boolean.into(),
        )]
    }
}

// ---------------------------------------------------------------------------
// xpath_eval(xml VARCHAR, expr VARCHAR) → VARCHAR
// ---------------------------------------------------------------------------

pub struct XPathEval;

impl VScalar for XPathEval {
    type State = XPathState;

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        if len == 0 { return Ok(()); }

        let out = output.flat_vector();
        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let expr_string = read_constant_expr(input);

        for i in 0..len {
            let doc = str_bytes(xml_data, i);
            SCRATCH.with_borrow_mut(|scratch| {
                scratch.with_parsed(doc, |index| {
                    let result = index.eval(&expr_string)?;
                    let display = result.to_display_string(index);
                    out.insert(i, display.as_str());
                    Ok(())
                })
            })?;
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}
