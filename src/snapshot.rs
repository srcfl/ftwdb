use crate::storage::sync_directory;
use crate::{Error, Result};
use crc32fast::Hasher;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SNAPSHOT_CRC32_DOMAIN: &[u8] = b"FTWDB snapshot CRC32 v1\0";
const CHECKSUM_BUFFER_BYTES: usize = 64 * 1024;
const STAGE_ATTEMPTS: u64 = 64;

static STAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotDigest {
    pub(crate) files: usize,
    pub(crate) bytes: u64,
    pub(crate) crc32: u32,
}

/// Computes the FTWDB snapshot CRC32 over the selected files only.
///
/// Paths must be unique, safe, UTF-8 relative paths sorted by their UTF-8
/// bytes. The checksum domain is `FTWDB snapshot CRC32 v1\0`. Each file then
/// contributes its path length as little-endian `u64`, path bytes, file length
/// as little-endian `u64`, and exact file bytes. Directory entries, mtimes,
/// permissions, inactive manifests, and unselected rollups are excluded.
pub(crate) fn snapshot_digest(root: &Path, relative_paths: &[String]) -> Result<SnapshotDigest> {
    let root_metadata = std::fs::symlink_metadata(root)?;
    if !root_metadata.file_type().is_dir() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "snapshot root is not a directory",
        )));
    }
    let mut previous: Option<&str> = None;
    let mut hasher = Hasher::new();
    hasher.update(SNAPSHOT_CRC32_DOMAIN);
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; CHECKSUM_BUFFER_BYTES];

    for relative in relative_paths {
        validate_relative_path(relative)?;
        if previous.is_some_and(|previous| previous >= relative.as_str()) {
            return Err(Error::InvalidArgument(
                "snapshot paths must be unique and sorted",
            ));
        }
        previous = Some(relative);

        let path_bytes = relative.as_bytes();
        let path_len = u64::try_from(path_bytes.len())
            .map_err(|_| Error::Serialization("snapshot path is too long".to_owned()))?;
        let mut file = open_snapshot_file(root, relative)?;
        let file_metadata = file.metadata()?;
        let file_len = file_metadata.len();
        bytes = bytes
            .checked_add(file_len)
            .ok_or_else(|| Error::Serialization("snapshot byte count exceeds u64".to_owned()))?;

        hasher.update(&path_len.to_le_bytes());
        hasher.update(path_bytes);
        hasher.update(&file_len.to_le_bytes());

        let mut remaining = file_len;
        while remaining > 0 {
            let requested = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
            let read = file.read(&mut buffer[..requested])?;
            if read == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("snapshot file changed while reading {relative}"),
                )));
            }
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
        }
        if file.read(&mut buffer[..1])? != 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("snapshot file grew while reading {relative}"),
            )));
        }
    }

    Ok(SnapshotDigest {
        files: relative_paths.len(),
        bytes,
        crc32: hasher.finalize(),
    })
}

/// Computes the one-file snapshot digest for an exact prefix of an already
/// opened regular file. Salvage uses this to compare its checked source prefix
/// with the complete `active.wlog` written to the new store.
pub(crate) fn snapshot_file_prefix_digest(
    file: &mut File,
    relative: &str,
    prefix_bytes: u64,
) -> Result<SnapshotDigest> {
    validate_relative_path(relative)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || metadata.len() < prefix_bytes {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "snapshot prefix is no longer present in the source file",
        )));
    }
    let path_bytes = relative.as_bytes();
    let path_len = u64::try_from(path_bytes.len())
        .map_err(|_| Error::Serialization("snapshot path is too long".to_owned()))?;
    let mut hasher = Hasher::new();
    hasher.update(SNAPSHOT_CRC32_DOMAIN);
    hasher.update(&path_len.to_le_bytes());
    hasher.update(path_bytes);
    hasher.update(&prefix_bytes.to_le_bytes());
    file.seek(SeekFrom::Start(0))?;
    hash_exact_bytes(file, prefix_bytes, relative, &mut hasher)?;
    Ok(SnapshotDigest {
        files: 1,
        bytes: prefix_bytes,
        crc32: hasher.finalize(),
    })
}

/// Snapshot digest over `relative_paths`, hashing `prefix_bytes` of an already
/// open `prefix_relative` file and the full contents of every other selected
/// path. Salvage uses this so a torn live tail does not enter the checksum
/// while sealed segment bytes still do.
pub(crate) fn snapshot_digest_with_open_prefix(
    root: &Path,
    relative_paths: &[String],
    prefix_relative: &str,
    prefix_file: &mut File,
    prefix_bytes: u64,
) -> Result<SnapshotDigest> {
    if !relative_paths.iter().any(|path| path == prefix_relative) {
        return Err(Error::InvalidArgument(
            "snapshot prefix path must be one of the selected files",
        ));
    }
    validate_relative_path(prefix_relative)?;
    let metadata = prefix_file.metadata()?;
    if !metadata.file_type().is_file() || metadata.len() < prefix_bytes {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "snapshot prefix is no longer present in the source file",
        )));
    }

    let root_metadata = std::fs::symlink_metadata(root)?;
    if relative_paths.len() > 1 && !root_metadata.file_type().is_dir() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "snapshot root is not a directory",
        )));
    }

    let mut previous: Option<&str> = None;
    let mut hasher = Hasher::new();
    hasher.update(SNAPSHOT_CRC32_DOMAIN);
    let mut bytes = 0_u64;

    for relative in relative_paths {
        validate_relative_path(relative)?;
        if previous.is_some_and(|previous| previous >= relative.as_str()) {
            return Err(Error::InvalidArgument(
                "snapshot paths must be unique and sorted",
            ));
        }
        previous = Some(relative);

        let path_bytes = relative.as_bytes();
        let path_len = u64::try_from(path_bytes.len())
            .map_err(|_| Error::Serialization("snapshot path is too long".to_owned()))?;
        let mut selected = None;
        let file_len = if relative == prefix_relative {
            prefix_bytes
        } else {
            let file = open_snapshot_file(root, relative)?;
            let file_len = file.metadata()?.len();
            selected = Some(file);
            file_len
        };
        bytes = bytes
            .checked_add(file_len)
            .ok_or_else(|| Error::Serialization("snapshot byte count exceeds u64".to_owned()))?;

        hasher.update(&path_len.to_le_bytes());
        hasher.update(path_bytes);
        hasher.update(&file_len.to_le_bytes());
        if relative == prefix_relative {
            prefix_file.seek(SeekFrom::Start(0))?;
            hash_exact_bytes(prefix_file, prefix_bytes, relative, &mut hasher)?;
        } else {
            let mut file = selected.expect("opened selected snapshot file");
            hash_exact_bytes(&mut file, file_len, relative, &mut hasher)?;
            if file.read(&mut [0_u8; 1])? != 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("snapshot file grew while reading {relative}"),
                )));
            }
        }
    }

    Ok(SnapshotDigest {
        files: relative_paths.len(),
        bytes,
        crc32: hasher.finalize(),
    })
}

fn hash_exact_bytes(
    file: &mut File,
    mut remaining: u64,
    relative: &str,
    hasher: &mut Hasher,
) -> Result<()> {
    let mut buffer = vec![0_u8; CHECKSUM_BUFFER_BYTES];
    while remaining > 0 {
        let requested = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        let read = file.read(&mut buffer[..requested])?;
        if read == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("snapshot file changed while reading {relative}"),
            )));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(())
}

fn open_snapshot_file(root: &Path, relative: &str) -> Result<File> {
    use rustix::fs::{Mode, OFlags, open, openat};

    let components: Vec<_> = Path::new(relative).components().collect();
    let mut directory = open(
        root,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
        Mode::empty(),
    )
    .map_err(|error| Error::Io(error.into()))?;
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(component) = component else {
            return Err(Error::InvalidArgument(
                "snapshot path must be a safe relative path",
            ));
        };
        let last = index + 1 == components.len();
        let flags = if last {
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK
        } else {
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY
        };
        let opened = openat(&directory, *component, flags, Mode::empty())
            .map_err(|error| Error::Io(error.into()))?;
        if last {
            let file = File::from(opened);
            if file.metadata()?.file_type().is_file() {
                return Ok(file);
            }
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("selected snapshot path is not a regular file: {relative}"),
            )));
        }
        directory = opened;
    }
    Err(Error::InvalidArgument(
        "snapshot path must name a regular file",
    ))
}

fn validate_relative_path(relative: &str) -> Result<()> {
    let path = Path::new(relative);
    if relative.is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        || path.is_absolute()
    {
        return Err(Error::InvalidArgument(
            "snapshot path must be a safe relative path",
        ));
    }
    Ok(())
}

/// Owns a hidden sibling directory until an atomic no-clobber publish succeeds.
/// Any error before the rename removes the stage on drop.
pub(crate) struct StagedDirectory {
    path: PathBuf,
    published: bool,
}

impl StagedDirectory {
    pub(crate) fn create(destination: &Path, operation: &str) -> Result<Self> {
        reject_existing(destination)?;
        let parent = destination_parent(destination)?;
        std::fs::create_dir_all(parent)?;
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(Error::InvalidConfig(
                "snapshot destination must have a UTF-8 name",
            ))?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        for attempt in 0..STAGE_ATTEMPTS {
            let counter = STAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(
                ".{name}.{operation}-{}-{timestamp}-{counter}-{attempt}",
                std::process::id()
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        published: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(Error::Io(error)),
            }
        }

        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique snapshot staging directory",
        )))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn publish(mut self, destination: &Path) -> Result<PublishedDirectory> {
        publication_checkpoint(PublicationStep::Publish)?;
        let identity = FileIdentity::read(&self.path)?;
        rename_noreplace(&self.path, destination)?;
        self.published = true;
        let publication = PublishedDirectory {
            hidden_path: self.path.clone(),
            destination: destination.to_owned(),
            identity,
            at_destination: true,
            committed: false,
        };
        let parent = publication.parent()?;
        let synced = publication_checkpoint(PublicationStep::ParentSync)
            .and_then(|()| sync_directory(parent));
        match synced {
            Ok(()) => Ok(publication),
            Err(error) => Err(publication.rollback_after(error)),
        }
    }
}

impl Drop for StagedDirectory {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    fn read(path: &Path) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.file_type().is_dir() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "published snapshot path is not a directory",
            )));
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

/// Keeps a just-published directory reversible until its caller finishes all
/// post-publication checks. Rollback only touches the directory when its Unix
/// device/inode identity still matches the stage that this process renamed.
pub(crate) struct PublishedDirectory {
    hidden_path: PathBuf,
    destination: PathBuf,
    identity: FileIdentity,
    at_destination: bool,
    committed: bool,
}

impl PublishedDirectory {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }

    pub(crate) fn rollback_after(mut self, failure: Error) -> Error {
        match self.rollback() {
            Ok(()) => failure,
            Err(rollback) => Error::SnapshotPublication {
                path: self.destination.clone(),
                reason: format!("operation failed: {failure}; rollback failed: {rollback}"),
            },
        }
    }

    fn parent(&self) -> Result<&Path> {
        destination_parent(&self.destination)
    }

    fn rollback(&mut self) -> Result<()> {
        if !self.at_destination {
            return Ok(());
        }
        if FileIdentity::read(&self.destination)? != self.identity {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                "published destination identity changed before rollback",
            )));
        }
        rename_noreplace(&self.destination, &self.hidden_path)?;
        self.at_destination = false;
        let parent = self.parent()?.to_owned();
        sync_directory(&parent)?;
        std::fs::remove_dir_all(&self.hidden_path)?;
        sync_directory(&parent)?;
        Ok(())
    }
}

fn destination_parent(destination: &Path) -> Result<&Path> {
    match destination.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => Ok(parent),
        Some(_) if destination.file_name().is_some() => Ok(Path::new(".")),
        _ => Err(Error::InvalidConfig(
            "snapshot destination must have a parent directory",
        )),
    }
}

impl Drop for PublishedDirectory {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.rollback();
        }
    }
}

fn reject_existing(destination: &Path) -> Result<()> {
    match std::fs::symlink_metadata(destination) {
        Ok(_) => Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "snapshot destination already exists",
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::Io(error)),
    }
}

#[cfg(any(
    target_os = "android",
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
    target_os = "redox",
))]
fn rename_noreplace(source: &Path, destination: &Path) -> Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};

    renameat_with(CWD, source, CWD, destination, RenameFlags::NOREPLACE)
        .map_err(|error| Error::Io(error.into()))
}

#[cfg(not(any(
    target_os = "android",
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
    target_os = "redox",
)))]
fn rename_noreplace(_source: &Path, _destination: &Path) -> Result<()> {
    Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "this platform lacks atomic no-clobber directory publication",
    )))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationStep {
    Copy,
    Sync,
    #[cfg(test)]
    ChecksumMismatch,
    Publish,
    ParentSync,
    PostCheck,
}

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_PUBLICATION_STEP: std::cell::Cell<Option<PublicationStep>> = const {
        std::cell::Cell::new(None)
    };
}

#[cfg(test)]
pub(crate) fn fail_next_publication_step(step: PublicationStep) {
    FAIL_NEXT_PUBLICATION_STEP.with(|failure| failure.set(Some(step)));
}

pub(crate) fn publication_checkpoint(step: PublicationStep) -> Result<()> {
    #[cfg(test)]
    if FAIL_NEXT_PUBLICATION_STEP.with(|failure| failure.get()) == Some(step) {
        FAIL_NEXT_PUBLICATION_STEP.with(|failure| failure.set(None));
        return Err(Error::Io(std::io::Error::other(format!(
            "injected {step:?} failure"
        ))));
    }
    let _ = step;
    Ok(())
}

pub(crate) fn inject_checksum_mismatch(digest: SnapshotDigest) -> SnapshotDigest {
    #[cfg(test)]
    if FAIL_NEXT_PUBLICATION_STEP.with(|failure| failure.get())
        == Some(PublicationStep::ChecksumMismatch)
    {
        FAIL_NEXT_PUBLICATION_STEP.with(|failure| failure.set(None));
        return SnapshotDigest {
            crc32: digest.crc32 ^ 1,
            ..digest
        };
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::{StagedDirectory, snapshot_digest};
    use std::sync::{Arc, Barrier};
    use tempfile::tempdir;

    #[test]
    fn digest_ignores_unselected_files_and_changes_with_paths_lengths_or_bytes() {
        let directory = tempdir().unwrap();
        std::fs::create_dir(directory.path().join("nested")).unwrap();
        std::fs::write(directory.path().join("one"), b"first").unwrap();
        std::fs::write(directory.path().join("nested/two"), b"second").unwrap();
        std::fs::write(directory.path().join("stale"), b"ignored").unwrap();
        let selected = vec!["nested/two".to_owned(), "one".to_owned()];

        let first = snapshot_digest(directory.path(), &selected).unwrap();
        std::fs::write(directory.path().join("stale"), b"changed").unwrap();
        assert_eq!(snapshot_digest(directory.path(), &selected).unwrap(), first);

        std::fs::write(directory.path().join("one"), b"different").unwrap();
        assert_ne!(snapshot_digest(directory.path(), &selected).unwrap(), first);
    }

    #[test]
    fn digest_rejects_symlinked_parents_and_non_files() {
        let directory = tempdir().unwrap();
        let external = tempdir().unwrap();
        std::fs::write(external.path().join("outside"), b"external").unwrap();
        std::os::unix::fs::symlink(external.path(), directory.path().join("linked")).unwrap();
        let linked = vec!["linked/outside".to_owned()];
        assert!(snapshot_digest(directory.path(), &linked).is_err());

        std::fs::create_dir(directory.path().join("not-a-file")).unwrap();
        let not_a_file = vec!["not-a-file".to_owned()];
        assert!(snapshot_digest(directory.path(), &not_a_file).is_err());
    }

    #[test]
    fn digest_rejects_unsafe_unsorted_and_duplicate_paths() {
        let directory = tempdir().unwrap();
        std::fs::write(directory.path().join("one"), b"first").unwrap();
        std::fs::write(directory.path().join("two"), b"second").unwrap();

        for paths in [
            vec!["../one".to_owned()],
            vec!["/one".to_owned()],
            vec!["two".to_owned(), "one".to_owned()],
            vec!["one".to_owned(), "one".to_owned()],
        ] {
            assert!(snapshot_digest(directory.path(), &paths).is_err());
        }
    }

    #[test]
    fn simultaneous_no_clobber_publications_have_exactly_one_winner() {
        let directory = tempdir().unwrap();
        let destination = directory.path().join("target");
        let left = StagedDirectory::create(&destination, "race-left").unwrap();
        let right = StagedDirectory::create(&destination, "race-right").unwrap();
        std::fs::write(left.path().join("winner"), b"left").unwrap();
        std::fs::write(right.path().join("winner"), b"right").unwrap();
        let barrier = Arc::new(Barrier::new(3));

        let left_barrier = Arc::clone(&barrier);
        let left_destination = destination.clone();
        let left_thread = std::thread::spawn(move || {
            left_barrier.wait();
            left.publish(&left_destination).map(|publication| {
                publication.commit();
            })
        });
        let right_barrier = Arc::clone(&barrier);
        let right_destination = destination.clone();
        let right_thread = std::thread::spawn(move || {
            right_barrier.wait();
            right.publish(&right_destination).map(|publication| {
                publication.commit();
            })
        });
        barrier.wait();

        let results = [left_thread.join().unwrap(), right_thread.join().unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let loser = results
            .iter()
            .find_map(|result| result.as_ref().err())
            .unwrap();
        match loser {
            crate::Error::Io(error) => {
                assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
            }
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
        assert!(matches!(
            std::fs::read(destination.join("winner"))
                .unwrap()
                .as_slice(),
            b"left" | b"right"
        ));
        let leftovers: Vec<_> = std::fs::read_dir(directory.path())
            .unwrap()
            .filter_map(|entry| {
                let entry = entry.unwrap();
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(".target.race-"))
                    .then_some(entry.path())
            })
            .collect();
        assert!(leftovers.is_empty(), "stages survived: {leftovers:?}");
    }

    #[test]
    fn rollback_failure_reports_that_the_published_target_may_remain() {
        let directory = tempdir().unwrap();
        let destination = directory.path().join("target");
        let staged = StagedDirectory::create(&destination, "rollback").unwrap();
        std::fs::write(staged.path().join("data"), b"complete").unwrap();
        let publication = staged.publish(&destination).unwrap();
        std::fs::create_dir(&publication.hidden_path).unwrap();

        let error = publication
            .rollback_after(crate::Error::Io(std::io::Error::other("post-check failed")));
        match error {
            crate::Error::SnapshotPublication { path, reason } => {
                assert_eq!(path, destination);
                assert!(reason.contains("post-check failed"));
                assert!(reason.contains("rollback failed"));
            }
            other => panic!("expected an explicit rollback failure, got {other:?}"),
        }
        assert!(destination.join("data").exists());
    }
}
