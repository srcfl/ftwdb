use flate2::read::GzDecoder;
use ftwdb::{Config, Durability, SalvageStatus, SalvageStopReason, Store, load_real_fixture};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

#[test]
fn committed_real_fixture_loads_with_stable_identity() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench/fixtures/ftw-real-v1/points.csv.gz");
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let backup = directory.path().join("backup");
    let restored = directory.path().join("restored");
    let salvaged = directory.path().join("salvaged");
    let mut store = Store::open_with(
        &source,
        Config {
            durability: Durability::Manual,
            ..Config::default()
        },
    )
    .unwrap();
    let reader = BufReader::new(GzDecoder::new(File::open(fixture).unwrap()));

    let report = load_real_fixture(reader, &mut store, 10_000).unwrap();
    assert_eq!(report.points, 889_978);
    assert_eq!(report.entities, 2);
    assert_eq!(report.series, 54);
    assert_eq!(report.commits, 89);
    assert_eq!(report.input_bytes, 18_189_885);
    assert_eq!(report.input_crc32, 0x130b_64f4);
    assert_eq!(report.points_crc32, 0x9ebd_e920);
    assert_eq!(report.first_offset_millis, 581);
    assert_eq!(report.last_offset_millis, 86_399_728);

    let integrity = store.check_integrity().unwrap();
    assert_eq!(integrity.raw_points, 889_978);
    assert_eq!(integrity.raw_commits, 89);

    store.backup_to(&backup).unwrap();
    let restore = Store::restore_from(&backup, &restored).unwrap();
    assert_eq!(restore.raw_points, 889_978);
    assert_eq!(restore.raw_commits, 89);
    assert_eq!(restore.source_snapshot_crc32, 0x9ff1_a95b);
    assert_eq!(
        restore.source_snapshot_crc32,
        restore.destination_snapshot_crc32
    );
    let restored_integrity = Store::open_read_only(&restored)
        .unwrap()
        .check_integrity()
        .unwrap();
    assert_eq!(restored_integrity.raw_points, 889_978);
    assert_eq!(restored_integrity.raw_commits, 89);

    let salvage = Store::salvage_from(&backup, &salvaged).unwrap();
    assert_eq!(salvage.status, SalvageStatus::Clean);
    assert_eq!(salvage.stop_reason, SalvageStopReason::CleanEof);
    assert_eq!(salvage.source_bytes, salvage.recovered_prefix_bytes);
    assert_eq!(salvage.discarded_bytes, 0);
    assert_eq!(salvage.recovered_points, 889_978);
    assert_eq!(salvage.recovered_commits, 89);
    assert_eq!(salvage.source_prefix_crc32, 0x9ff1_a95b);
    assert_eq!(
        salvage.source_prefix_crc32,
        salvage.destination_snapshot_crc32
    );
    let salvaged_integrity = Store::open_read_only(&salvaged)
        .unwrap()
        .check_integrity()
        .unwrap();
    assert_eq!(salvaged_integrity.raw_points, 889_978);
    assert_eq!(salvaged_integrity.raw_commits, 89);
}
