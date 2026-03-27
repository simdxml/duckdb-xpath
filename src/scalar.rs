use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
};

use duckdb::ffi::{duckdb_string_t, duckdb_string_t_data, duckdb_string_t_length};
use simdxml::bloom::TagBloom;
use simdxml::CompiledXPath;
use std::collections::HashSet;
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Shared XPath state: compiled once, reused across all chunks
// ---------------------------------------------------------------------------

/// Persistent state shared across all invocations of a scalar function.
/// Uses OnceLock for lock-free access after first initialization.
pub struct XPathState {
    /// Compiled XPath expression + the source string it was compiled from.
    compiled: OnceLock<(String, CompiledXPath)>,
    /// Tag names the query references (for bloom filtering and lazy parsing).
    interesting: OnceLock<Option<HashSet<String>>>,
    /// Pre-computed bloom target byte slices.
    bloom_targets: OnceLock<Vec<Vec<u8>>>,
    /// Simple tag name for grep-mode fast path (e.g., "claim" for `//claim`).
    simple_tag: OnceLock<Option<String>>,
}

impl Default for XPathState {
    fn default() -> Self {
        Self {
            compiled: OnceLock::new(),
            interesting: OnceLock::new(),
            bloom_targets: OnceLock::new(),
            simple_tag: OnceLock::new(),
        }
    }
}

impl XPathState {
    /// Get or compile the XPath expression. Returns None if expression varies per row.
    fn get_compiled(&self, expr: &str) -> Option<&CompiledXPath> {
        let (cached_expr, compiled) = self.compiled.get_or_init(|| {
            let compiled = CompiledXPath::compile(expr)
                .expect("XPath compilation should succeed");
            (expr.to_string(), compiled)
        });
        if cached_expr == expr {
            Some(compiled)
        } else {
            None
        }
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

    fn get_simple_tag(&self, compiled: &CompiledXPath) -> &Option<String> {
        self.simple_tag.get_or_init(|| {
            compiled.simple_tag_name().map(|s| s.to_string())
        })
    }
}

// ---------------------------------------------------------------------------
// Zero-copy input reading
// ---------------------------------------------------------------------------

/// Read the constant XPath expression from a chunk (all rows have the same value).
/// Returns the expression string. Panics if chunk is empty.
unsafe fn read_constant_expr(input: &mut DataChunkHandle) -> String {
    let expr_vec = input.flat_vector(1);
    let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(input.len());
    let bytes = str_bytes(expr_data, 0);
    String::from_utf8_lossy(bytes).into_owned()
}

/// Zero-copy access to a string in a DuckDB vector.
/// Uses raw FFI pointers to avoid copying the duckdb_string_t (which would
/// invalidate inline string pointers). The returned slice points into DuckDB's
/// vector memory, which is valid for the duration of the invoke() call.
#[inline(always)]
unsafe fn str_bytes<'a>(data: &'a [duckdb_string_t], i: usize) -> &'a [u8] {
    let ptr = &data[i] as *const duckdb_string_t as *mut duckdb_string_t;
    let len = duckdb_string_t_length(*ptr);
    let data_ptr = duckdb_string_t_data(ptr);
    std::slice::from_raw_parts(data_ptr as *const u8, len as usize)
}

// ---------------------------------------------------------------------------
// xpath_text(xml VARCHAR, expr VARCHAR) → VARCHAR
// Returns the text content of the first matching node.
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

        // Read the constant expression (same for all rows in the overwhelming common case)
        let expr_string = read_constant_expr(input);

        if let Some(compiled) = state.get_compiled(&expr_string) {
            let simple_tag = state.get_simple_tag(compiled);
            let interesting = state.get_interesting(compiled);
            let bloom_targets = state.get_bloom_targets(interesting);
            let use_bloom = !bloom_targets.is_empty();
            let target_refs: Vec<&[u8]> = bloom_targets.iter().map(|n| n.as_slice()).collect();

            for i in 0..len {
                let doc = str_bytes(xml_data, i);

                // Tier 1: Grep-mode fast path for simple //tagname queries
                if let Some(ref tag) = *simple_tag {
                    match simdxml::quick::quick_extract_first(doc, tag) {
                        Some(text) => {
                            out.insert(i, text);
                            continue;
                        }
                        None => {
                            // Nested elements detected — fall through to full parser
                        }
                    }
                }

                // Tier 2: Bloom pre-filter
                if use_bloom {
                    let bloom = TagBloom::from_prescan(doc);
                    if !bloom.may_contain_any(&target_refs) {
                        out.insert(i, "");
                        continue;
                    }
                }

                // Tier 3: Lazy parse + compiled XPath eval
                let index = match interesting {
                    Some(names) => simdxml::index::lazy::parse_for_query(doc, names)?,
                    None => simdxml::parse(doc)?,
                };
                let texts = compiled.eval_text(&index)?;
                out.insert(i, texts.first().copied().unwrap_or(""));
            }
        } else {
            // Varying expressions per row — fallback
            let expr_vec = input.flat_vector(1);
            let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);

            for i in 0..len {
                let doc = str_bytes(xml_data, i);
                let expr = std::str::from_utf8(str_bytes(expr_data, i))
                    .map_err(|e| format!("invalid XPath expression: {}", e))?;
                let index = simdxml::parse(doc)?;
                let texts = index.xpath_text(&expr)?;
                out.insert(i, texts.first().copied().unwrap_or(""));
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

        // Precompute bloom targets if we have a compiled expression
        let (interesting, bloom_targets) = if let Some(c) = compiled {
            let int = state.get_interesting(c);
            let bt = state.get_bloom_targets(int);
            (int, bt)
        } else {
            (&None as &Option<HashSet<String>>, &Vec::new() as &Vec<Vec<u8>>)
        };
        let use_bloom = !bloom_targets.is_empty();
        let target_refs: Vec<&[u8]> = bloom_targets.iter().map(|n| n.as_slice()).collect();

        for i in 0..len {
            let doc = str_bytes(xml_data, i);

            // Bloom pre-filter
            if use_bloom {
                let bloom = TagBloom::from_prescan(doc);
                if !bloom.may_contain_any(&target_refs) {
                    out.insert(i, "[]");
                    continue;
                }
            }

            // Lazy parse + eval
            let index = if compiled.is_some() {
                match interesting {
                    Some(names) => simdxml::index::lazy::parse_for_query(doc, names)?,
                    None => simdxml::parse(doc)?,
                }
            } else {
                simdxml::parse(doc)?
            };

            let texts = if let Some(c) = compiled {
                c.eval_text(&index)?
            } else {
                let expr_vec = input.flat_vector(1);
                let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);
                let expr = std::str::from_utf8(str_bytes(expr_data, i))
                    .map_err(|e| format!("invalid XPath expression: {}", e))?;
                index.xpath_text(&expr)?
            };

            // JSON array encoding
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
            let simple_tag = state.get_simple_tag(compiled);
            let interesting = state.get_interesting(compiled);
            let bloom_targets = state.get_bloom_targets(interesting);
            let use_bloom = !bloom_targets.is_empty();
            let target_refs: Vec<&[u8]> = bloom_targets.iter().map(|n| n.as_slice()).collect();

            for i in 0..len {
                let doc = str_bytes(xml_data, i);

                // Grep-mode count for simple //tagname
                if let Some(ref tag) = *simple_tag {
                    counts[i] = simdxml::quick::quick_count(doc, tag) as i64;
                    continue;
                }

                // Bloom pre-filter
                if use_bloom {
                    let bloom = TagBloom::from_prescan(doc);
                    if !bloom.may_contain_any(&target_refs) {
                        counts[i] = 0;
                        continue;
                    }
                }

                // Lazy parse + count
                let index = match interesting {
                    Some(names) => simdxml::index::lazy::parse_for_query(doc, names)?,
                    None => simdxml::parse(doc)?,
                };
                counts[i] = compiled.eval_count(&index)? as i64;
            }
        } else {
            let expr_vec = input.flat_vector(1);
            let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);

            for i in 0..len {
                let doc = str_bytes(xml_data, i);
                let expr = std::str::from_utf8(str_bytes(expr_data, i))
                    .map_err(|e| format!("invalid XPath expression: {}", e))?;
                let index = simdxml::parse(doc)?;
                let nodes = index.xpath(&expr)?;
                counts[i] = nodes.len() as i64;
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
            let simple_tag = state.get_simple_tag(compiled);
            let interesting = state.get_interesting(compiled);
            let bloom_targets = state.get_bloom_targets(interesting);
            let use_bloom = !bloom_targets.is_empty();
            let target_refs: Vec<&[u8]> = bloom_targets.iter().map(|n| n.as_slice()).collect();

            for i in 0..len {
                let doc = str_bytes(xml_data, i);

                // Grep-mode exists for simple //tagname
                if let Some(ref tag) = *simple_tag {
                    bools[i] = simdxml::quick::quick_exists(doc, tag);
                    continue;
                }

                // Bloom pre-filter: definite reject
                if use_bloom {
                    let bloom = TagBloom::from_prescan(doc);
                    if !bloom.may_contain_any(&target_refs) {
                        bools[i] = false;
                        continue;
                    }
                }

                // Lazy parse + short-circuit exists
                let index = match interesting {
                    Some(names) => simdxml::index::lazy::parse_for_query(doc, names)?,
                    None => simdxml::parse(doc)?,
                };
                bools[i] = compiled.eval_exists(&index)?;
            }
        } else {
            let expr_vec = input.flat_vector(1);
            let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);

            for i in 0..len {
                let doc = str_bytes(xml_data, i);
                let expr = std::str::from_utf8(str_bytes(expr_data, i))
                    .map_err(|e| format!("invalid XPath expression: {}", e))?;
                let index = simdxml::parse(doc)?;
                let nodes = index.xpath(&expr)?;
                bools[i] = !nodes.is_empty();
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
// Evaluates any XPath expression including scalar (count, string, boolean).
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

        // xpath_eval needs mutable index for eval(), so no bloom/lazy optimization
        // (eval() may call ensure_indices() internally)
        let expr_string = read_constant_expr(input);

        // Still cache the expression string to avoid re-reading per row
        for i in 0..len {
            let doc = str_bytes(xml_data, i);
            let mut index = simdxml::parse(doc)?;
            let result = index.eval(&expr_string)?;
            let display = result.to_display_string(&index);
            out.insert(i, display.as_str());
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
