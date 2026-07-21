use ftw_sd_emulator::config::Profile;
use ftw_sd_emulator::control::{ConnectionRegistry, send_command, start_control_server};
use ftw_sd_emulator::model::SdCard;
use ftw_sd_emulator::nbd;
use ftw_sd_emulator::report::{VerifyInput, append_report, verify};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ftw-sd-emulator: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = std::env::args().skip(1);
    let Some(command) = arguments.next() else {
        print_help();
        return Err("missing command".to_owned());
    };
    let rest: Vec<String> = arguments.collect();
    match command.as_str() {
        "serve" => serve(parse_serve_options(&rest)?),
        "ctl" => control(&rest),
        "validate" => validate(&rest),
        "verify" => verify_run(&rest),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        _ => Err(format!("unknown command {command:?}")),
    }
}

struct ServeOptions {
    config: PathBuf,
    backing: PathBuf,
    listen: String,
    control: String,
    seed: u64,
    metrics: Option<PathBuf>,
    power_loss_after_ops: Option<u64>,
}

fn parse_serve_options(arguments: &[String]) -> Result<ServeOptions, String> {
    let mut config = None;
    let mut backing = None;
    let mut listen = "127.0.0.1:10809".to_owned();
    let mut control = "127.0.0.1:10810".to_owned();
    let mut seed = 42;
    let mut metrics = None;
    let mut power_loss_after_ops = None;
    let mut index = 0;
    while index < arguments.len() {
        let flag = &arguments[index];
        index += 1;
        let value = arguments
            .get(index)
            .ok_or_else(|| format!("{flag} needs a value"))?;
        index += 1;
        match flag.as_str() {
            "--config" => config = Some(PathBuf::from(value)),
            "--backing" => backing = Some(PathBuf::from(value)),
            "--listen" => listen.clone_from(value),
            "--control" => control.clone_from(value),
            "--seed" => seed = parse_u64("seed", value)?,
            "--metrics" => metrics = Some(PathBuf::from(value)),
            "--power-loss-after-ops" => {
                power_loss_after_ops = Some(parse_u64("power-loss operation", value)?)
            }
            _ => return Err(format!("unknown serve option {flag:?}")),
        }
    }
    Ok(ServeOptions {
        config: config.ok_or_else(|| "serve needs --config".to_owned())?,
        backing: backing.ok_or_else(|| "serve needs --backing".to_owned())?,
        listen,
        control,
        seed,
        metrics,
        power_loss_after_ops,
    })
}

fn serve(options: ServeOptions) -> Result<(), String> {
    let mut profile = Profile::load(&options.config).map_err(|error| error.to_string())?;
    if let Some(operation) = options.power_loss_after_ops {
        if operation == 0 {
            return Err("power-loss operation must be positive".to_owned());
        }
        profile.faults.power_loss_after_ops = Some(operation);
    }
    profile.validate().map_err(|error| error.to_string())?;
    let card =
        SdCard::open(&options.backing, profile, options.seed).map_err(|error| error.to_string())?;
    let card = Arc::new(Mutex::new(card));
    let connections = Arc::new(ConnectionRegistry::default());
    let running = Arc::new(AtomicBool::new(true));
    let control_thread = start_control_server(
        options.control,
        Arc::clone(&card),
        Arc::clone(&connections),
        Arc::clone(&running),
        options.metrics,
    )
    .map_err(|error| format!("start control server: {error}"))?;
    let result = nbd::serve(
        &options.listen,
        Arc::clone(&card),
        Arc::clone(&connections),
        Arc::clone(&running),
    )
    .map_err(|error| format!("run NBD server: {error}"));
    running.store(false, Ordering::Release);
    connections.disconnect_all();
    control_thread
        .join()
        .map_err(|_| "control thread panicked".to_owned())?;
    let stats = card.lock().expect("SD-card lock poisoned").stats();
    println!(
        "{}",
        serde_json::to_string(&stats).map_err(|error| error.to_string())?
    );
    result
}

fn control(arguments: &[String]) -> Result<(), String> {
    let mut address = "127.0.0.1:10810";
    let command;
    match arguments {
        [value] => command = value.as_str(),
        [flag, value, requested] if flag == "--control" => {
            address = value;
            command = requested;
        }
        _ => {
            return Err("use: ftw-sd-emulator ctl [--control HOST:PORT] COMMAND".to_owned());
        }
    }
    let response = send_command(address, command)
        .map_err(|error| format!("send control command to {address}: {error}"))?;
    print!("{response}");
    let value: serde_json::Value =
        serde_json::from_str(response.trim()).map_err(|error| error.to_string())?;
    if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err("emulator rejected the control command".to_owned())
    }
}

fn validate(arguments: &[String]) -> Result<(), String> {
    let [path] = arguments else {
        return Err("use: ftw-sd-emulator validate PROFILE.json".to_owned());
    };
    let profile = Profile::load(path).map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&profile).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn verify_run(arguments: &[String]) -> Result<(), String> {
    let mut emulator = None;
    let mut check = None;
    let mut inspect = None;
    let mut output = None;
    let mut expected_points = None;
    let mut expected_commits = None;
    let mut writer_exit = None;
    let mut writer_signal = None;
    let mut checksum_ok = None;
    let mut index = 0;
    while index < arguments.len() {
        let flag = &arguments[index];
        index += 1;
        let value = arguments
            .get(index)
            .ok_or_else(|| format!("{flag} needs a value"))?;
        index += 1;
        match flag.as_str() {
            "--emulator" => emulator = Some(PathBuf::from(value)),
            "--check" => check = Some(PathBuf::from(value)),
            "--inspect" => inspect = Some(PathBuf::from(value)),
            "--output" => output = Some(PathBuf::from(value)),
            "--expected-points" => expected_points = Some(parse_u64("points", value)?),
            "--expected-commits" => expected_commits = Some(parse_u64("commits", value)?),
            "--writer-exit" => writer_exit = Some(parse_i32("writer exit", value)?),
            "--writer-signal" => writer_signal = Some(parse_i32("writer signal", value)?),
            "--checksum-ok" => checksum_ok = Some(parse_bool("checksum status", value)?),
            _ => return Err(format!("unknown verify option {flag:?}")),
        }
    }
    let emulator = emulator.ok_or_else(|| "verify needs --emulator".to_owned())?;
    let check = check.ok_or_else(|| "verify needs --check".to_owned())?;
    let inspect = inspect.ok_or_else(|| "verify needs --inspect".to_owned())?;
    let report = verify(&VerifyInput {
        emulator: &emulator,
        check: &check,
        inspect: &inspect,
        expected_points: expected_points
            .ok_or_else(|| "verify needs --expected-points".to_owned())?,
        expected_commits: expected_commits
            .ok_or_else(|| "verify needs --expected-commits".to_owned())?,
        writer_exit,
        writer_signal,
        checksum_ok,
    })
    .map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string(&report).map_err(|error| error.to_string())?
    );
    if let Some(output) = output {
        append_report(output, &report).map_err(|error| error.to_string())?;
    }
    if report.passed {
        Ok(())
    } else {
        Err("recovered FTWDB data does not match the acknowledged watermark".to_owned())
    }
}

fn parse_u64(name: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|error| format!("invalid {name} {value:?}: {error}"))
}

fn parse_i32(name: &str, value: &str) -> Result<i32, String> {
    value
        .parse()
        .map_err(|error| format!("invalid {name} {value:?}: {error}"))
}

fn parse_bool(name: &str, value: &str) -> Result<bool, String> {
    value
        .parse()
        .map_err(|error| format!("invalid {name} {value:?}: {error}"))
}

fn print_help() {
    println!(
        "ftw-sd-emulator\n\n\
         serve --config PROFILE.json --backing CARD.img [--listen HOST:PORT] \\\n+               [--control HOST:PORT] [--seed N] [--metrics FILE.jsonl] \\\n+               [--power-loss-after-ops N]\n\
         ctl [--control HOST:PORT] status|power-loss|reset|detach|read-only|read-write|flush|shutdown\n\
         validate PROFILE.json\n\
         verify --emulator METRICS.jsonl --check CHECK.json --inspect INSPECT.txt \\\n+                --expected-points N --expected-commits N [--checksum-ok BOOL] \\\n+                [--writer-exit N] [--writer-signal N] [--output RESULT.jsonl]"
    );
}
