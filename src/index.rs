use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::sync::atomic::{AtomicBool, Ordering};

// ---------------------------------------------------------------------------
// xpath_create_index(file VARCHAR) → (path VARCHAR, tags BIGINT, size BIGINT)
//
// Eagerly builds a .sxi sidecar index for an XML file.
// Subsequent read_xml / xpath_* calls can mmap the index and skip parsing.
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct CreateIndexBindData {
    file_path: String,
}

#[repr(C)]
pub struct CreateIndexInitData {
    done: AtomicBool,
}

pub struct CreateIndexVTab;

impl VTab for CreateIndexVTab {
    type InitData = CreateIndexInitData;
    type BindData = CreateIndexBindData;

    fn bind(bind: &BindInfo) -> duckdb::Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("sxi_path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("tags", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("size_bytes", LogicalTypeHandle::from(LogicalTypeId::Bigint));

        let file_path = bind.get_parameter(0).to_string();
        Ok(CreateIndexBindData { file_path })
    }

    fn init(_: &InitInfo) -> duckdb::Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(CreateIndexInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> duckdb::Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let xml_path = std::path::Path::new(&bind_data.file_path);
        let sxi_path = xml_path.with_extension("sxi");

        // Read and parse the XML
        let xml_bytes = std::fs::read(xml_path)
            .map_err(|e| format!("Failed to read '{}': {}", bind_data.file_path, e))?;

        // Use parallel parse for large files
        let mut index = if xml_bytes.len() > 1_048_576 {
            simdxml::parallel::parse_parallel(&xml_bytes, num_cpus())?
        } else {
            simdxml::parse(&xml_bytes)?
        };
        index.build_name_index();

        // Serialize the index
        simdxml::persist::serialize_index(&index, &xml_bytes, &sxi_path)
            .map_err(|e| format!("Failed to write '{}': {}", sxi_path.display(), e))?;

        let sxi_size = std::fs::metadata(&sxi_path)
            .map(|m| m.len() as i64)
            .unwrap_or(0);
        let tag_count = index.tag_count() as i64;

        // Output: sxi_path, tag_count, sxi_size
        let path_vec = output.flat_vector(0);
        path_vec.insert(0, sxi_path.to_string_lossy().as_ref());

        let tags_vec = output.flat_vector(1);
        unsafe { *(tags_vec.as_mut_ptr::<i64>()) = tag_count; }

        let size_vec = output.flat_vector(2);
        unsafe { *(size_vec.as_mut_ptr::<i64>()) = sxi_size; }

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ---------------------------------------------------------------------------
// xpath_drop_index(file VARCHAR) → (dropped BOOLEAN)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct DropIndexBindData {
    file_path: String,
}

#[repr(C)]
pub struct DropIndexInitData {
    done: AtomicBool,
}

pub struct DropIndexVTab;

impl VTab for DropIndexVTab {
    type InitData = DropIndexInitData;
    type BindData = DropIndexBindData;

    fn bind(bind: &BindInfo) -> duckdb::Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("dropped", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        let file_path = bind.get_parameter(0).to_string();
        Ok(DropIndexBindData { file_path })
    }

    fn init(_: &InitInfo) -> duckdb::Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(DropIndexInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> duckdb::Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let sxi_path = std::path::Path::new(&bind_data.file_path).with_extension("sxi");
        let dropped = if sxi_path.exists() {
            std::fs::remove_file(&sxi_path).is_ok()
        } else {
            false
        };

        let mut vec = output.flat_vector(0);
        let bools = vec.as_mut_slice::<bool>();
        bools[0] = dropped;
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
