use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeId},
    types::DuckString,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
};

use duckdb::ffi::duckdb_string_t;

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
        let len = input.len();
        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let expr_vec = input.flat_vector(1);
        let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);

        let out = output.flat_vector();

        for i in 0..len {
            let mut xml_raw = xml_data[i];
            let xml_str = DuckString::new(&mut xml_raw).as_str().to_string();
            let mut expr_raw = expr_data[i];
            let expr_str = DuckString::new(&mut expr_raw).as_str().to_string();

            let result = match simdxml::parse(xml_str.as_bytes()) {
                Ok(index) => {
                    match index.xpath_text(&expr_str) {
                        Ok(texts) => texts.first().map(|s| s.to_string()).unwrap_or_default(),
                        Err(e) => return Err(format!("XPath error: {}", e).into()),
                    }
                }
                Err(e) => return Err(format!("XML parse error: {}", e).into()),
            };

            out.insert(i, result.as_str());
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
// xpath_list(xml VARCHAR, expr VARCHAR) → VARCHAR (JSON array for now)
// Returns all matching text values as a JSON array string.
// TODO: Return LIST(VARCHAR) when duckdb-rs list vector API is available.
// ---------------------------------------------------------------------------

pub struct XPathList;

impl VScalar for XPathList {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let expr_vec = input.flat_vector(1);
        let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);

        let out = output.flat_vector();

        for i in 0..len {
            let mut xml_raw = xml_data[i];
            let xml_str = DuckString::new(&mut xml_raw).as_str().to_string();
            let mut expr_raw = expr_data[i];
            let expr_str = DuckString::new(&mut expr_raw).as_str().to_string();

            let result = match simdxml::parse(xml_str.as_bytes()) {
                Ok(index) => {
                    match index.xpath_text(&expr_str) {
                        Ok(texts) => {
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
                            json
                        }
                        Err(e) => return Err(format!("XPath error: {}", e).into()),
                    }
                }
                Err(e) => return Err(format!("XML parse error: {}", e).into()),
            };

            out.insert(i, result.as_str());
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
        let len = input.len();
        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let expr_vec = input.flat_vector(1);
        let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);

        let mut out = output.flat_vector();
        let counts = out.as_mut_slice::<i64>();

        for i in 0..len {
            let mut xml_raw = xml_data[i];
            let xml_str = DuckString::new(&mut xml_raw).as_str().to_string();
            let mut expr_raw = expr_data[i];
            let expr_str = DuckString::new(&mut expr_raw).as_str().to_string();

            counts[i] = match simdxml::parse(xml_str.as_bytes()) {
                Ok(index) => {
                    match index.xpath(&expr_str) {
                        Ok(nodes) => nodes.len() as i64,
                        Err(e) => return Err(format!("XPath error: {}", e).into()),
                    }
                }
                Err(e) => return Err(format!("XML parse error: {}", e).into()),
            };
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
        let len = input.len();
        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let expr_vec = input.flat_vector(1);
        let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);

        let mut out = output.flat_vector();
        let bools = out.as_mut_slice::<bool>();

        for i in 0..len {
            let mut xml_raw = xml_data[i];
            let xml_str = DuckString::new(&mut xml_raw).as_str().to_string();
            let mut expr_raw = expr_data[i];
            let expr_str = DuckString::new(&mut expr_raw).as_str().to_string();

            bools[i] = match simdxml::parse(xml_str.as_bytes()) {
                Ok(index) => {
                    match index.xpath(&expr_str) {
                        Ok(nodes) => !nodes.is_empty(),
                        Err(e) => return Err(format!("XPath error: {}", e).into()),
                    }
                }
                Err(e) => return Err(format!("XML parse error: {}", e).into()),
            };
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
// Evaluates any XPath expression (including scalar: count(), string(), etc.)
// ---------------------------------------------------------------------------

pub struct XPathEval;

impl VScalar for XPathEval {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let xml_vec = input.flat_vector(0);
        let xml_data = xml_vec.as_slice_with_len::<duckdb_string_t>(len);
        let expr_vec = input.flat_vector(1);
        let expr_data = expr_vec.as_slice_with_len::<duckdb_string_t>(len);

        let out = output.flat_vector();

        for i in 0..len {
            let mut xml_raw = xml_data[i];
            let xml_str = DuckString::new(&mut xml_raw).as_str().to_string();
            let mut expr_raw = expr_data[i];
            let expr_str = DuckString::new(&mut expr_raw).as_str().to_string();

            let result = match simdxml::parse(xml_str.as_bytes()) {
                Ok(mut index) => {
                    match index.eval(&expr_str) {
                        Ok(r) => r.to_display_string(&index),
                        Err(e) => return Err(format!("XPath error: {}", e).into()),
                    }
                }
                Err(e) => return Err(format!("XML parse error: {}", e).into()),
            };

            out.insert(i, result.as_str());
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
