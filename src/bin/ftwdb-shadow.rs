use ftwdb::shadow_protocol::MAX_BATCH_POINTS;
use ftwdb::shadow_runtime::{ShadowRuntime, ShadowRuntimeConfig, ShadowStorageLimits};
use ftwdb::shadow_server::{ShadowServerConfig, ShadowStopToken, serve};
use ftwdb::{Config, Durability, Store};
use std::env;
use std::error::Error;
use std::fs::{self, DirBuilder};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

static TERMINATION_REQUESTED: AtomicBool = AtomicBool::new(false);

fn main() {
    if env::args_os().len() == 2 && env::args_os().nth(1).as_deref() == Some("--version".as_ref()) {
        println!("ftwdb-shadow {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if let Err(error) = run() {
        eprintln!("ftwdb-shadow: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let Some(store_path) = arguments.next() else {
        return Err(usage().into());
    };
    let Some(socket_path) = arguments.next() else {
        return Err(usage().into());
    };
    if arguments.next().is_some() {
        return Err(usage().into());
    }

    let limits = ShadowStorageLimits {
        max_store_bytes: limit_from_env("FTWDB_SHADOW_MAX_STORE_BYTES", 512 * 1024 * 1024)?,
        minimum_free_bytes: limit_from_env("FTWDB_SHADOW_MIN_FREE_BYTES", 512 * 1024 * 1024)?,
    };
    if env::var_os("FTWDB_SHADOW_MAINTAIN_SECS").is_some() {
        return Err("bounded shadow collection does not run background maintenance; remove FTWDB_SHADOW_MAINTAIN_SECS".into());
    }
    let stop = ShadowStopToken::new();
    let _signals = TerminationSignals::install(stop.clone())?;
    let store_path = PathBuf::from(store_path);
    prepare_private_store_root(&store_path)?;
    // Bound active-log replay before allocating its in-memory index. A lower
    // limit needs an operator decision about the existing evaluation store.
    match fs::symlink_metadata(store_path.join("active.wlog")) {
        Ok(metadata) if metadata.len() > limits.max_store_bytes => {
            return Err("existing active log exceeds FTWDB_SHADOW_MAX_STORE_BYTES".into());
        }
        Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error.into()),
        _ => {}
    }
    let store = Store::open_with(
        store_path,
        Config {
            durability: Durability::Always,
            max_batch_points: MAX_BATCH_POINTS,
            // The wire codec bounds input frames separately. Keep the storage
            // limit: its canonical encoding differs from the wire encoding.
            ..Config::default()
        },
    )?;
    let runtime = ShadowRuntime::start_store(
        store,
        ShadowRuntimeConfig {
            queue_capacity: 8,
            max_queued_points: 32_768,
            storage_limits: Some(limits),
            ..ShadowRuntimeConfig::default()
        },
    )?;
    let submitter = runtime.submitter();
    eprintln!(
        "ftwdb-shadow: version={} max_store_bytes={} minimum_free_bytes={} maintenance=off",
        env!("CARGO_PKG_VERSION"),
        limits.max_store_bytes,
        limits.minimum_free_bytes
    );
    let server_config = ShadowServerConfig::new(PathBuf::from(socket_path));
    let result = serve(&server_config, submitter, &stop);
    let shutdown = runtime.shutdown();
    let report = result?;
    shutdown?;
    eprintln!(
        "ftwdb-shadow: stopped accepted_clients={} peer_auth_failures={} client_errors={} overload_count={} protocol_error_count={} database_bytes={} database_points={} database_commits={} recovered_tail_bytes={} sync_policy={} last_ack_durable={}",
        report.accepted_clients,
        report.peer_auth_failures,
        report.client_errors,
        report.overload_count,
        report.protocol_error_count,
        report.database_bytes,
        report.database_points,
        report.database_commits,
        report.recovered_tail_bytes,
        report.sync_policy,
        report.last_ack_durable
    );
    Ok(())
}

extern "C" fn request_termination(_signal: libc::c_int) {
    // AtomicBool is lock-free on the supported Linux and macOS targets. The
    // handler does no allocation, locking, I/O, or cleanup work.
    TERMINATION_REQUESTED.store(true, Ordering::Relaxed);
}

struct TerminationSignals {
    previous_sigint: libc::sigaction,
    previous_sigterm: libc::sigaction,
    cancel_watcher: Arc<AtomicBool>,
    watcher: Option<thread::JoinHandle<()>>,
}

impl TerminationSignals {
    fn install(stop: ShadowStopToken) -> io::Result<Self> {
        TERMINATION_REQUESTED.store(false, Ordering::Relaxed);
        let previous_sigterm = install_signal(libc::SIGTERM)?;
        let previous_sigint = match install_signal(libc::SIGINT) {
            Ok(previous) => previous,
            Err(error) => {
                restore_signal(libc::SIGTERM, &previous_sigterm);
                return Err(error);
            }
        };

        let cancel_watcher = Arc::new(AtomicBool::new(false));
        let watcher_cancel = Arc::clone(&cancel_watcher);
        let watcher = match thread::Builder::new()
            .name("ftwdb-shadow-signals".to_owned())
            .spawn(move || {
                while !watcher_cancel.load(Ordering::Acquire) {
                    if TERMINATION_REQUESTED.load(Ordering::Relaxed) {
                        stop.stop();
                        return;
                    }
                    thread::park_timeout(Duration::from_millis(10));
                }
            }) {
            Ok(watcher) => watcher,
            Err(error) => {
                restore_signal(libc::SIGINT, &previous_sigint);
                restore_signal(libc::SIGTERM, &previous_sigterm);
                return Err(error);
            }
        };

        Ok(Self {
            previous_sigint,
            previous_sigterm,
            cancel_watcher,
            watcher: Some(watcher),
        })
    }
}

impl Drop for TerminationSignals {
    fn drop(&mut self) {
        // Restore first so a signal that arrives during teardown keeps the
        // launcher's prior behavior instead of getting lost.
        restore_signal(libc::SIGINT, &self.previous_sigint);
        restore_signal(libc::SIGTERM, &self.previous_sigterm);
        self.cancel_watcher.store(true, Ordering::Release);
        if let Some(watcher) = self.watcher.take() {
            watcher.thread().unpark();
            let _ = watcher.join();
        }
    }
}

fn install_signal(signal: libc::c_int) -> io::Result<libc::sigaction> {
    // SAFETY: zero is a valid starting state for sigaction on the supported
    // targets. sigemptyset initializes the mask before sigaction reads it.
    let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
    action.sa_sigaction = request_termination as *const () as libc::sighandler_t;
    action.sa_flags = 0;
    // SAFETY: action owns valid mask storage.
    if unsafe { libc::sigemptyset(&mut action.sa_mask) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both pointers refer to valid sigaction values for this call.
    let mut previous = unsafe { std::mem::zeroed::<libc::sigaction>() };
    if unsafe { libc::sigaction(signal, &action, &mut previous) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(previous)
}

fn restore_signal(signal: libc::c_int, previous: &libc::sigaction) {
    // SAFETY: previous came from a successful sigaction call in this process.
    let _ = unsafe { libc::sigaction(signal, previous, std::ptr::null_mut()) };
}

fn usage() -> &'static str {
    "usage: ftwdb-shadow <store-directory> <socket-path>"
}

fn limit_from_env(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| format!("{name} must be a positive byte count").into()),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn prepare_private_store_root(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => return check_private_store_root(path, &metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let mut builder = DirBuilder::new();
    builder.mode(0o700).create(path)?;
    // The process umask may only remove bits, but set the exact mode so the
    // service does not depend on its launcher configuration.
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    let metadata = fs::symlink_metadata(path)?;
    check_private_store_root(path, &metadata)
}

fn check_private_store_root(path: &Path, metadata: &fs::Metadata) -> io::Result<()> {
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("store root is not a real directory: {}", path.display()),
        ));
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("store root has another owner: {}", path.display()),
        ));
    }
    if metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("store root must have mode 0700: {}", path.display()),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::prepare_private_store_root;
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn creates_a_private_store_root() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("shadow-store");
        prepare_private_store_root(&root).unwrap();
        assert_eq!(
            fs::symlink_metadata(root).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn rejects_a_store_root_visible_to_other_users() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("shadow-store");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
        let error = prepare_private_store_root(&root).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn rejects_a_symlink_store_root() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        let root = directory.path().join("shadow-store");
        fs::create_dir(&target).unwrap();
        symlink(&target, &root).unwrap();
        let error = prepare_private_store_root(&root).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
}
