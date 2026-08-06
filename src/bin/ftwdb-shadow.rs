use ftwdb::shadow_runtime::{ShadowRuntime, ShadowRuntimeConfig};
use ftwdb::shadow_server::{ShadowServerConfig, ShadowStopToken, serve};
use ftwdb::{Config, Durability, Store};
use std::env;
use std::error::Error;
use std::fs::{self, DirBuilder};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

fn main() {
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

    let store_path = PathBuf::from(store_path);
    prepare_private_store_root(&store_path)?;
    let store = Store::open_with(
        store_path,
        Config {
            durability: Durability::Always,
            ..Config::default()
        },
    )?;
    let runtime = ShadowRuntime::start_store(
        store,
        ShadowRuntimeConfig {
            queue_capacity: 8,
            max_queued_points: 32_768,
        },
    )?;
    let submitter = runtime.submitter();
    let server_config = ShadowServerConfig::new(PathBuf::from(socket_path));
    let stop = ShadowStopToken::new();
    let result = serve(&server_config, submitter, &stop);
    let shutdown = runtime.shutdown();
    result?;
    shutdown?;
    Ok(())
}

fn usage() -> &'static str {
    "usage: ftwdb-shadow <store-directory> <socket-path>"
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
