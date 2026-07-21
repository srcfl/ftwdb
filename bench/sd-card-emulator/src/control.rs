use crate::model::{SdCard, Stats};
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub type SharedCard = Arc<Mutex<SdCard>>;

#[derive(Default)]
pub struct ConnectionRegistry {
    connections: Mutex<Vec<TcpStream>>,
}

impl ConnectionRegistry {
    pub fn register(&self, stream: &TcpStream) -> io::Result<()> {
        self.connections
            .lock()
            .expect("connection lock poisoned")
            .push(stream.try_clone()?);
        Ok(())
    }

    pub fn disconnect_all(&self) {
        let mut connections = self.connections.lock().expect("connection lock poisoned");
        for connection in connections.drain(..) {
            let _ = connection.shutdown(Shutdown::Both);
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ControlReply {
    pub ok: bool,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub stats: Stats,
}

pub fn start_control_server(
    address: String,
    card: SharedCard,
    connections: Arc<ConnectionRegistry>,
    running: Arc<AtomicBool>,
    metrics_path: Option<PathBuf>,
) -> io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(&address)?;
    listener.set_nonblocking(true)?;
    eprintln!("control server listening on {address}");
    Ok(thread::spawn(move || {
        while running.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    if let Err(error) =
                        handle_control(stream, &card, &connections, &running, metrics_path.as_ref())
                    {
                        eprintln!("control request failed: {error}");
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => {
                    eprintln!("control listener failed: {error}");
                    break;
                }
            }
        }
    }))
}

pub fn send_command(address: &str, command: &str) -> io::Result<String> {
    let mut stream = TcpStream::connect(address)?;
    stream.write_all(command.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.shutdown(Shutdown::Write)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

fn handle_control(
    mut stream: TcpStream,
    card: &SharedCard,
    connections: &ConnectionRegistry,
    running: &AtomicBool,
    metrics_path: Option<&PathBuf>,
) -> io::Result<()> {
    let mut command = String::new();
    BufReader::new(stream.try_clone()?)
        .take(4097)
        .read_line(&mut command)?;
    let command = command.trim();
    if command.len() > 4096 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "control command exceeds 4096 bytes",
        ));
    }

    let mut card = card.lock().expect("SD-card lock poisoned");
    let result = match command {
        "status" => Ok(()),
        "power-loss" => card.power_loss().map_err(|error| error.to_string()),
        "reset" => {
            card.reset();
            Ok(())
        }
        "detach" => card.detach().map_err(|error| error.to_string()),
        "read-only" => {
            card.set_read_only(true);
            Ok(())
        }
        "read-write" => {
            card.set_read_only(false);
            Ok(())
        }
        "flush" => card.flush().map_err(|error| error.to_string()),
        "shutdown" => Ok(()),
        _ => Err(format!(
            "unknown command {command:?}; use status, power-loss, reset, detach, read-only, read-write, flush, or shutdown"
        )),
    };
    let stats = card.stats();
    drop(card);

    if matches!(
        command,
        "power-loss" | "detach" | "read-only" | "read-write" | "shutdown"
    ) {
        connections.disconnect_all();
    }
    if command == "shutdown" && result.is_ok() {
        running.store(false, Ordering::Release);
    }
    let reply = ControlReply {
        ok: result.is_ok(),
        command: command.to_owned(),
        error: result.err(),
        stats,
    };
    let encoded = serde_json::to_vec(&reply).map_err(io::Error::other)?;
    stream.write_all(&encoded)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    if let Some(path) = metrics_path {
        append_metrics(path, &encoded)?;
    }
    Ok(())
}

fn append_metrics(path: &PathBuf, encoded: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(encoded)?;
    file.write_all(b"\n")?;
    file.sync_data()
}
