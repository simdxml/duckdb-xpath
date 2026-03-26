mod scalar;

use duckdb::{duckdb_entrypoint_c_api, Connection, Result};
use std::error::Error;

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<scalar::XPathText>("xpath_text")?;
    con.register_scalar_function::<scalar::XPathList>("xpath_list")?;
    con.register_scalar_function::<scalar::XPathCount>("xpath_count")?;
    con.register_scalar_function::<scalar::XPathExists>("xpath_exists")?;
    con.register_scalar_function::<scalar::XPathEval>("xpath_eval")?;
    Ok(())
}
