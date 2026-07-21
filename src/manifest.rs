use crate::{Error, Result, RollupResolution};
use crc32fast::hash;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 8] = b"WMAN0001";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 24;
const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const PREFIX: &str = "MANIFEST.";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RollupDescriptor {
    pub file: String,
    pub series_id: u64,
    pub resolution: RollupResolution,
    pub start: i64,
    pub end: i64,
    pub source_commit: u64,
    pub source_points: u64,
    pub active: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Manifest {
    pub generation: u64,
    pub rollups: Vec<RollupDescriptor>,
}

impl Manifest {
    pub fn load(directory: &Path) -> Result<Self> {
        let mut candidates = Vec::new();
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(raw_generation) = name.strip_prefix(PREFIX) else {
                continue;
            };
            if let Ok(generation) = raw_generation.parse::<u64>() {
                candidates.push((generation, entry.path()));
            }
        }
        candidates.sort_by_key(|(generation, _)| std::cmp::Reverse(*generation));
        if candidates.is_empty() {
            return Ok(Self::default());
        }

        let mut last_error = None;
        for (generation, path) in candidates {
            match read_manifest(&path, generation) {
                Ok(manifest) => return Ok(manifest),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| Error::Corruption {
            offset: 0,
            reason: "no valid manifest generation".to_owned(),
        }))
    }

    pub fn publish(&self, directory: &Path) -> Result<()> {
        validate_manifest(self)?;
        let payload = postcard::to_stdvec(self)
            .map_err(|error| Error::Serialization(format!("manifest encode failed: {error}")))?;
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(Error::Serialization("manifest exceeds 64 MiB".to_owned()));
        }
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| Error::Serialization("manifest exceeds u32 length".to_owned()))?;
        let mut header = [0_u8; HEADER_BYTES];
        header[..8].copy_from_slice(MAGIC);
        header[8..10].copy_from_slice(&VERSION.to_le_bytes());
        header[12..16].copy_from_slice(&payload_len.to_le_bytes());
        header[16..20].copy_from_slice(&hash(&payload).to_le_bytes());
        let header_crc = hash(&header[..20]);
        header[20..24].copy_from_slice(&header_crc.to_le_bytes());

        let target = directory.join(format!("{PREFIX}{:020}", self.generation));
        if target.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "manifest generation already exists",
            )));
        }
        let temporary = temporary_path(&target)?;
        let write_result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&header)?;
            file.write_all(&payload)?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&temporary);
            return Err(error);
        }
        if let Err(error) = std::fs::hard_link(&temporary, &target) {
            let _ = std::fs::remove_file(&temporary);
            return Err(Error::Io(error));
        }
        File::open(directory)?.sync_all()?;
        std::fs::remove_file(&temporary)?;
        File::open(directory)?.sync_all()?;
        Ok(())
    }
}

fn read_manifest(path: &Path, expected_generation: u64) -> Result<Manifest> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let file_len = file.metadata()?.len();
    if file_len < HEADER_BYTES as u64 {
        return corruption("manifest is too small");
    }
    let mut header = [0_u8; HEADER_BYTES];
    file.read_exact(&mut header)?;
    if &header[..8] != MAGIC {
        return corruption("invalid manifest magic");
    }
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    if version != VERSION {
        return corruption("unsupported manifest version");
    }
    if hash(&header[..20]) != u32::from_le_bytes(header[20..24].try_into().unwrap()) {
        return corruption("manifest header checksum mismatch");
    }
    let payload_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
    if payload_len > MAX_PAYLOAD_BYTES || file_len != (HEADER_BYTES + payload_len) as u64 {
        return corruption("invalid manifest length");
    }
    let mut payload = vec![0_u8; payload_len];
    file.read_exact(&mut payload)?;
    if hash(&payload) != u32::from_le_bytes(header[16..20].try_into().unwrap()) {
        return corruption("manifest payload checksum mismatch");
    }
    let manifest: Manifest = postcard::from_bytes(&payload)
        .map_err(|error| Error::Serialization(format!("manifest decode failed: {error}")))?;
    if manifest.generation != expected_generation {
        return corruption("manifest generation does not match its filename");
    }
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_manifest(manifest: &Manifest) -> Result<()> {
    for rollup in &manifest.rollups {
        let path = Path::new(&rollup.file);
        if path.components().count() != 1
            || !matches!(path.components().next(), Some(Component::Normal(_)))
        {
            return Err(Error::InvalidModel(
                "manifest rollup filename must be a single safe path component".to_owned(),
            ));
        }
        if rollup.series_id == 0 || rollup.end <= rollup.start {
            return Err(Error::InvalidModel(
                "manifest rollup descriptor has invalid identity or bounds".to_owned(),
            ));
        }
    }
    Ok(())
}

fn temporary_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().ok_or(Error::InvalidConfig(
        "manifest path must have a parent directory",
    ))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(Error::InvalidConfig("manifest path must be UTF-8"))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    Ok(parent.join(format!(".{name}.tmp-{}-{nonce}", std::process::id())))
}

fn corruption<T>(reason: &str) -> Result<T> {
    Err(Error::Corruption {
        offset: 0,
        reason: reason.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::{Manifest, RollupDescriptor};
    use crate::RollupResolution;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempdir;

    fn manifest(generation: u64) -> Manifest {
        Manifest {
            generation,
            rollups: vec![RollupDescriptor {
                file: "one.rseg".to_owned(),
                series_id: 1,
                resolution: RollupResolution::FixedMicros(300_000_000),
                start: 0,
                end: 300_000_000,
                source_commit: 2,
                source_points: 10,
                active: true,
            }],
        }
    }

    #[test]
    fn loads_the_highest_valid_generation() {
        let directory = tempdir().unwrap();
        manifest(1).publish(directory.path()).unwrap();
        manifest(2).publish(directory.path()).unwrap();
        assert_eq!(Manifest::load(directory.path()).unwrap().generation, 2);
    }

    #[test]
    fn falls_back_when_the_newest_generation_is_torn() {
        let directory = tempdir().unwrap();
        manifest(1).publish(directory.path()).unwrap();
        manifest(2).publish(directory.path()).unwrap();
        let path = directory.path().join("MANIFEST.00000000000000000002");
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(30)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.sync_all().unwrap();

        assert_eq!(Manifest::load(directory.path()).unwrap().generation, 1);
    }
}
