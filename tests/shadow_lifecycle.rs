#![cfg(unix)]

use ftwdb::Store;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn assert_clean_signal_shutdown(signal: libc::c_int) {
    let directory = tempfile::tempdir().unwrap();
    let store_path = directory.path().join("store");
    let socket_path = directory.path().join("run/shadow.sock");
    let child = Command::new(env!("CARGO_BIN_EXE_ftwdb-shadow"))
        .arg(&store_path)
        .arg(&socket_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);

    wait_for_socket(&mut child.0, &socket_path);
    // SAFETY: the child PID came from a live Child handle and the signal is
    // SIGINT or SIGTERM.
    let result = unsafe { libc::kill(child.0.id() as libc::pid_t, signal) };
    assert_eq!(
        result,
        0,
        "could not signal child: {}",
        std::io::Error::last_os_error()
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "shadow sidecar did not stop after signal"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "shadow sidecar exited with {status}");
    let mut stderr = String::new();
    child
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(
        stderr.contains(
            "ftwdb-shadow: stopped accepted_clients=0 peer_auth_failures=0 client_errors=0 overload_count=0 protocol_error_count=0"
        )
            && stderr.contains("sync_policy=always last_ack_durable=false"),
        "clean shutdown report was missing: {stderr}"
    );
    assert!(!socket_path.exists(), "shadow socket was not cleaned up");
    Store::open(&store_path).expect("a clean shutdown must release and leave a readable store");
}

fn wait_for_socket(child: &mut Child, socket_path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if socket_path.exists() {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("shadow sidecar exited before binding its socket: {status}: {stderr}");
        }
        assert!(
            Instant::now() < deadline,
            "shadow sidecar did not bind its socket"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn sigterm_causes_a_clean_shutdown() {
    assert_clean_signal_shutdown(libc::SIGTERM);
}

#[test]
fn sigint_causes_a_clean_shutdown() {
    assert_clean_signal_shutdown(libc::SIGINT);
}
