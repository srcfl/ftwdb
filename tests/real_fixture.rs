use flate2::read::GzDecoder;
use ftwdb::{Config, Durability, Store, load_real_fixture};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

#[test]
fn committed_real_fixture_loads_with_stable_identity() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench/fixtures/ftw-real-v1/points.csv.gz");
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open_with(
        directory.path(),
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
}
