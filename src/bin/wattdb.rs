use std::env;
use std::process::ExitCode;
use wattdb::Database;

fn main() -> ExitCode {
    let mut arguments = env::args();
    let program = arguments.next().unwrap_or_else(|| "wattdb".to_owned());
    let Some(command) = arguments.next() else {
        eprintln!("usage: {program} inspect <database-path>");
        return ExitCode::from(2);
    };
    let Some(path) = arguments.next() else {
        eprintln!("usage: {program} inspect <database-path>");
        return ExitCode::from(2);
    };

    if command != "inspect" || arguments.next().is_some() {
        eprintln!("usage: {program} inspect <database-path>");
        return ExitCode::from(2);
    }

    match Database::open(path).and_then(|database| database.stats()) {
        Ok(stats) => {
            println!("format: wattdb-v1");
            println!("points: {}", stats.points);
            println!("commits: {}", stats.commits);
            println!("series: {}", stats.series);
            println!("catalog_records: {}", stats.catalog_records);
            println!("file_bytes: {}", stats.file_bytes);
            println!("recovered_tail_bytes: {}", stats.recovered_tail_bytes);
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}
