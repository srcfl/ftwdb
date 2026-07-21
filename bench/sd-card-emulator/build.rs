use std::process::Command;

fn main() {
    println!("cargo::rerun-if-env-changed=FTW_SD_EMULATOR_COMMIT");

    if std::env::var_os("FTW_SD_EMULATOR_COMMIT").is_some() {
        return;
    }

    let commit = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo::rustc-env=FTW_SD_EMULATOR_COMMIT={commit}");
}
