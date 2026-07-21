pub mod config;
pub mod control;
pub mod model;
pub mod nbd;
pub mod report;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const COMMIT: &str = env!("FTW_SD_EMULATOR_COMMIT");
