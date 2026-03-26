use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::ffi::CString;
use std::sync::Mutex;

/// read_xml(file VARCHAR, xpath VARCHAR) → (result VARCHAR)
///
/// Reads an XML file, evaluates the XPath expression, and returns
/// each matching text value as a row.
///
/// Example:
///   SELECT * FROM read_xml('patent.xml', '//claim/text()');

#[repr(C)]
pub struct ReadXmlBindData {
    file_path: String,
    xpath_expr: String,
}

pub struct ReadXmlState {
    results: Vec<String>,
    offset: usize,
    initialized: bool,
}

#[repr(C)]
pub struct ReadXmlInitData {
    state: Mutex<ReadXmlState>,
}

pub struct ReadXmlVTab;

impl VTab for ReadXmlVTab {
    type InitData = ReadXmlInitData;
    type BindData = ReadXmlBindData;

    fn bind(bind: &BindInfo) -> duckdb::Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("result", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let file_path = bind.get_parameter(0).to_string();
        let xpath_expr = bind.get_parameter(1).to_string();

        Ok(ReadXmlBindData {
            file_path,
            xpath_expr,
        })
    }

    fn init(_: &InitInfo) -> duckdb::Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(ReadXmlInitData {
            state: Mutex::new(ReadXmlState {
                results: Vec::new(),
                offset: 0,
                initialized: false,
            }),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> duckdb::Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();
        let mut state = init_data.state.lock().unwrap();

        if !state.initialized {
            state.initialized = true;

            let xml_path = std::path::Path::new(&bind_data.file_path);
            let sxi_path = xml_path.with_extension("sxi");

            // Try loading pre-built .sxi index first (instant, skip parsing)
            if sxi_path.exists() {
                match simdxml::load_or_parse(xml_path) {
                    Ok(owned_index) => {
                        let texts = owned_index.xpath_text(&bind_data.xpath_expr)?;
                        state.results = texts.into_iter().map(|s| s.to_string()).collect();
                        // Skip the parse path below
                        state.offset = 0;
                        // Return early from init
                    }
                    Err(_) => {
                        // Fall through to parse from scratch
                    }
                }
            }

            if state.results.is_empty() || !sxi_path.exists() {
                // Read and parse the XML file
                let data = std::fs::read(&bind_data.file_path)
                    .map_err(|e| format!("Failed to read '{}': {}", bind_data.file_path, e))?;

                // Use parallel parse for large files
                let index = if data.len() > 1_048_576 {
                    simdxml::parallel::parse_parallel(&data, num_cpus())?
                } else {
                    simdxml::parse(&data)?
                };

                let texts = index.xpath_text(&bind_data.xpath_expr)?;
                state.results = texts.into_iter().map(|s| s.to_string()).collect();
            }
        }

        let remaining = state.results.len() - state.offset;
        if remaining == 0 {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = remaining.min(2048);
        let vector = output.flat_vector(0);

        for i in 0..chunk_size {
            let text = &state.results[state.offset + i];
            let c_str = CString::new(text.as_str())?;
            vector.insert(i, c_str);
        }

        output.set_len(chunk_size);
        state.offset += chunk_size;

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // file path
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // xpath expression
        ])
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
