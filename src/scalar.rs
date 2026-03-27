use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeId},
    types::DuckString,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
};

use duckdb::ffi::duckdb_string_t;
use simdxml::CompiledXPath;

// ---------------------------------------------------------------------------
// Shared vectorised evaluation helper
// ---------------------------------------------------------------------------

/// Read XML and expression strings from a DuckDB input chunk.
/// Returns (xml_strings, expr_strings).
unsafe fn read_inputs(input: &mut DataChunkHandle) -> (Vec<String>, Vec<String>) {
    let len = input.len();
    let xml_vec = input.flat_vector(0);
    let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
    let expr_vec = input.flat_vector(1);
    let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);

    let mut xmls = Vec::with_capacity(len);
    let mut exprs = Vec::with_capacity(len);
    for i in 0..len {
        let mut xml_raw = xml_data[i];
        xmls.push(DuckString::new(&mut xml_raw).as_str().to_string());
        let mut expr_raw = expr_data[i];
        exprs.push(DuckString::new(&mut expr_raw).as_str().to_string());
    }
    (xmls, exprs)
}

/// Check if all expressions in the chunk are the same (constant folding).
/// If so, compile once and return the compiled expression.
fn try_compile_constant(exprs: &[String]) -> Option<CompiledXPath> {
    if exprs.is_empty() { return None; }
    let first = &exprs[0];
    if exprs.iter().all(|e| e == first) {
        CompiledXPath::compile(first).ok()
    } else {
        None
    }
}

/// Parse XML and evaluate XPath, using compiled expression when available.
fn eval_xpath_text<'a>(
    xml: &'a [u8],
    expr: &str,
    compiled: Option<&CompiledXPath>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let index = simdxml::parse(xml)?;
    let texts = if let Some(compiled) = compiled {
        compiled.eval_text(&index)?
    } else {
        index.xpath_text(expr)?
    };
    Ok(texts.into_iter().map(|s| s.to_string()).collect())
}

fn eval_xpath_nodes(
    xml: &[u8],
    expr: &str,
    compiled: Option<&CompiledXPath>,
) -> Result<Vec<simdxml::xpath::XPathNode>, Box<dyn std::error::Error>> {
    let index = simdxml::parse(xml)?;
    let nodes = if let Some(compiled) = compiled {
        compiled.eval(&index)?
    } else {
        index.xpath(expr)?
    };
    Ok(nodes)
}

// ---------------------------------------------------------------------------
// xpath_text(xml VARCHAR, expr VARCHAR) → VARCHAR
// Returns the text content of the first matching node.
// ---------------------------------------------------------------------------

pub struct XPathText;

impl VScalar for XPathText {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (xmls, exprs) = read_inputs(input);
        let out = output.flat_vector();

        // Batch path: constant expression → compile once, bloom prescan + batch eval
        if let Some(compiled) = try_compile_constant(&exprs) {
            let doc_refs: Vec<&[u8]> = xmls.iter().map(|s| s.as_bytes()).collect();
            let batch = simdxml::batch::eval_batch_text(&doc_refs, &compiled)?;

            for i in 0..xmls.len() {
                let result = batch[i].first().map(|s| s.as_str()).unwrap_or("");
                out.insert(i, result);
            }
            return Ok(());
        }

        // Fallback: varying expressions per row
        for i in 0..xmls.len() {
            let texts = eval_xpath_text(xmls[i].as_bytes(), &exprs[i], None)?;
            let result = texts.first().map(|s| s.as_str()).unwrap_or("");
            out.insert(i, result);
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
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (xmls, exprs) = read_inputs(input);
        let compiled = try_compile_constant(&exprs);
        let out = output.flat_vector();

        for i in 0..xmls.len() {
            let texts = eval_xpath_text(xmls[i].as_bytes(), &exprs[i], compiled.as_ref())?;

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
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (xmls, exprs) = read_inputs(input);
        let mut out = output.flat_vector();
        let counts = out.as_mut_slice::<i64>();

        // Batch path: constant expression → count_batch
        if let Some(compiled) = try_compile_constant(&exprs) {
            let doc_refs: Vec<&[u8]> = xmls.iter().map(|s| s.as_bytes()).collect();
            let batch_counts = simdxml::batch::count_batch(&doc_refs, &compiled)?;
            for i in 0..xmls.len() {
                counts[i] = batch_counts[i] as i64;
            }
            return Ok(());
        }

        for i in 0..xmls.len() {
            let nodes = eval_xpath_nodes(xmls[i].as_bytes(), &exprs[i], None)?;
            counts[i] = nodes.len() as i64;
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
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (xmls, exprs) = read_inputs(input);
        let compiled = try_compile_constant(&exprs);
        let mut out = output.flat_vector();
        let bools = out.as_mut_slice::<bool>();

        for i in 0..xmls.len() {
            let nodes = eval_xpath_nodes(xmls[i].as_bytes(), &exprs[i], compiled.as_ref())?;
            bools[i] = !nodes.is_empty();
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
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (xmls, exprs) = read_inputs(input);
        let out = output.flat_vector();

        for i in 0..xmls.len() {
            let mut index = simdxml::parse(xmls[i].as_bytes())?;
            let result = index.eval(&exprs[i])?;
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
