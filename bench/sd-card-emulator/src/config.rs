use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

pub const PROBABILITY_SCALE: u32 = 1_000_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub schema_version: u32,
    pub name: String,
    pub device: DeviceProfile,
    pub read: IoProfile,
    pub write: IoProfile,
    pub cache: CacheProfile,
    pub wear: WearProfile,
    pub faults: FaultProfile,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceProfile {
    pub size_bytes: u64,
    pub logical_block_bytes: u64,
    pub erase_block_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IoProfile {
    pub bandwidth_bytes_per_second: u64,
    pub iops: u64,
    pub base_latency_us: u64,
    pub jitter_latency_us: u64,
    pub spike_probability_ppm: u32,
    pub spike_latency_us_min: u64,
    pub spike_latency_us_max: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CacheProfile {
    pub enabled: bool,
    pub max_bytes: u64,
    pub false_flush_probability_ppm: u32,
    pub power_loss_persist_probability_ppm: u32,
    pub power_loss_torn_probability_ppm: u32,
    pub power_loss_reorder_probability_ppm: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WearProfile {
    pub enabled: bool,
    pub accelerated_factor: u64,
    pub warning_cycles: u64,
    pub failure_cycles: u64,
    pub worn_eio_probability_ppm: u32,
    pub read_only_after_bad_blocks: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FaultProfile {
    pub eio_probability_ppm: u32,
    pub torn_write_probability_ppm: u32,
    pub silent_corruption_probability_ppm: u32,
    pub power_loss_after_ops: Option<u64>,
    pub read_only_after_ops: Option<u64>,
    pub disappear_after_ops: Option<u64>,
}

impl Profile {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let file = File::open(path).map_err(ConfigError::Io)?;
        let profile: Self =
            serde_json::from_reader(BufReader::new(file)).map_err(ConfigError::Json)?;
        profile.validate()?;
        Ok(profile)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.schema_version != 1 {
            return Err(ConfigError::Invalid(format!(
                "schema_version must be 1, got {}",
                self.schema_version
            )));
        }
        if self.name.trim().is_empty() {
            return Err(ConfigError::Invalid("profile name is empty".to_owned()));
        }
        if self.device.size_bytes == 0 {
            return Err(ConfigError::Invalid(
                "device size must be positive".to_owned(),
            ));
        }
        if self.device.logical_block_bytes < 512
            || !self.device.logical_block_bytes.is_power_of_two()
        {
            return Err(ConfigError::Invalid(
                "logical block size must be a power of two of at least 512 bytes".to_owned(),
            ));
        }
        if self.device.erase_block_bytes < self.device.logical_block_bytes
            || !self
                .device
                .erase_block_bytes
                .is_multiple_of(self.device.logical_block_bytes)
        {
            return Err(ConfigError::Invalid(
                "erase block size must be a multiple of the logical block size".to_owned(),
            ));
        }
        if !self
            .device
            .size_bytes
            .is_multiple_of(self.device.logical_block_bytes)
        {
            return Err(ConfigError::Invalid(
                "device size must be a multiple of the logical block size".to_owned(),
            ));
        }
        if self.cache.max_bytes > self.device.size_bytes {
            return Err(ConfigError::Invalid(
                "cache cannot be larger than the device".to_owned(),
            ));
        }
        validate_io("read", &self.read)?;
        validate_io("write", &self.write)?;
        for (name, value) in [
            (
                "cache.false_flush_probability_ppm",
                self.cache.false_flush_probability_ppm,
            ),
            (
                "cache.power_loss_persist_probability_ppm",
                self.cache.power_loss_persist_probability_ppm,
            ),
            (
                "cache.power_loss_torn_probability_ppm",
                self.cache.power_loss_torn_probability_ppm,
            ),
            (
                "cache.power_loss_reorder_probability_ppm",
                self.cache.power_loss_reorder_probability_ppm,
            ),
            (
                "wear.worn_eio_probability_ppm",
                self.wear.worn_eio_probability_ppm,
            ),
            (
                "faults.eio_probability_ppm",
                self.faults.eio_probability_ppm,
            ),
            (
                "faults.torn_write_probability_ppm",
                self.faults.torn_write_probability_ppm,
            ),
            (
                "faults.silent_corruption_probability_ppm",
                self.faults.silent_corruption_probability_ppm,
            ),
        ] {
            validate_probability(name, value)?;
        }
        let power_loss_total = self.cache.power_loss_persist_probability_ppm as u64
            + self.cache.power_loss_torn_probability_ppm as u64;
        if power_loss_total > PROBABILITY_SCALE as u64 {
            return Err(ConfigError::Invalid(
                "power-loss persist and torn probabilities exceed 1,000,000 ppm".to_owned(),
            ));
        }
        if self.wear.enabled {
            if self.wear.accelerated_factor == 0 {
                return Err(ConfigError::Invalid(
                    "wear accelerated_factor must be positive".to_owned(),
                ));
            }
            if self.wear.warning_cycles >= self.wear.failure_cycles {
                return Err(ConfigError::Invalid(
                    "wear warning_cycles must be below failure_cycles".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_io(name: &str, profile: &IoProfile) -> Result<(), ConfigError> {
    validate_probability(
        &format!("{name}.spike_probability_ppm"),
        profile.spike_probability_ppm,
    )?;
    if profile.spike_latency_us_min > profile.spike_latency_us_max {
        return Err(ConfigError::Invalid(format!(
            "{name} spike minimum exceeds maximum"
        )));
    }
    Ok(())
}

fn validate_probability(name: &str, value: u32) -> Result<(), ConfigError> {
    if value > PROBABILITY_SCALE {
        return Err(ConfigError::Invalid(format!(
            "{name} exceeds {PROBABILITY_SCALE} ppm"
        )));
    }
    Ok(())
}

#[derive(Debug)]
pub enum ConfigError {
    Io(io::Error),
    Json(serde_json::Error),
    Invalid(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "cannot read profile: {error}"),
            Self::Json(error) => write!(formatter, "invalid profile JSON: {error}"),
            Self::Invalid(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_profiles_are_valid() {
        for contents in [
            include_str!("../profiles/healthy.json"),
            include_str!("../profiles/cheap-consumer.json"),
            include_str!("../profiles/nearly-worn.json"),
            include_str!("../profiles/sudden-power-loss.json"),
            include_str!("../profiles/full-disk-64m.json"),
        ] {
            let profile: Profile = serde_json::from_str(contents).unwrap();
            profile.validate().unwrap();
        }
    }

    #[test]
    fn invalid_probability_is_rejected() {
        let mut profile: Profile =
            serde_json::from_str(include_str!("../profiles/healthy.json")).unwrap();
        profile.faults.eio_probability_ppm = PROBABILITY_SCALE + 1;
        assert!(profile.validate().is_err());
    }
}
