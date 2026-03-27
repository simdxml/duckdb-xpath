use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
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
    /// File data kept alive so byte-offset ranges remain valid.
    data: Vec<u8>,
    /// Results as byte-offset ranges (start, end) into `data`.
    /// Avoids materializing all results as owned Strings.
    result_ranges: Vec<(usize, usize)>,
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
                data: Vec::new(),
                result_ranges: Vec::new(),
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
            let mut loaded = false;
            if sxi_path.exists() {
                match simdxml::load_or_parse(xml_path) {
                    Ok(owned_index) => {
                        let texts = owned_index.xpath_text(&bind_data.xpath_expr)?;
                        // With .sxi we can't do byte-offset ranges (different memory layout),
                        // so collect as owned strings via ranges into a temporary buffer
                        let mut data = Vec::new();
                        let mut ranges = Vec::new();
                        for text in texts {
                            let start = data.len();
                            data.extend_from_slice(text.as_bytes());
                            ranges.push((start, data.len()));
                        }
                        state.data = data;
                        state.result_ranges = ranges;
                        state.offset = 0;
                        loaded = true;
                    }
                    Err(_) => {
                        // Fall through to parse from scratch
                    }
                }
            }

            if !loaded {
                // Read and parse the XML file
                state.data = std::fs::read(&bind_data.file_path)
                    .map_err(|e| format!("Failed to read '{}': {}", bind_data.file_path, e))?;

                // Use lazy parsing when possible, parallel for large files
                let index = if state.data.len() > 1_048_576 {
                    simdxml::parallel::parse_parallel(&state.data, num_cpus())?
                } else {
                    simdxml::parse_for_xpath(&state.data, &bind_data.xpath_expr)?
                };

                let texts = index.xpath_text(&bind_data.xpath_expr)?;

                // Store as byte-offset ranges into the original file data
                state.result_ranges = texts.iter().map(|text| {
                    let text_ptr = text.as_ptr();
                    let data_ptr = state.data.as_ptr();
                    // Safety: xpath_text returns borrowed slices into the input data
                    let start = unsafe { text_ptr.offset_from(data_ptr) as usize };
                    (start, start + text.len())
                }).collect();
            }
        }

        let remaining = state.result_ranges.len() - state.offset;
        if remaining == 0 {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = remaining.min(2048);
        let vector = output.flat_vector(0);

        for i in 0..chunk_size {
            let (start, end) = state.result_ranges[state.offset + i];
            let bytes = &state.data[start..end];
            // Safety: text content from valid XML is valid UTF-8
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            vector.insert(i, text);
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
