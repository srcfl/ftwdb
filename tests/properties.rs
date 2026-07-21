use proptest::prelude::*;
use std::fs::OpenOptions;
use tempfile::tempdir;
use wattdb::{Config, Database, Durability, Point};

fn point(series_id: u64, index: usize, value: f64) -> Point {
    Point {
        series_id,
        valid_time: index as i64 * 1_000_000,
        valid_time_end: index as i64 * 1_000_000,
        knowledge_time: index as i64 * 1_000_000 + 10,
        change_time: index as i64 * 1_000_000 + 20,
        run_id: index as u128 + 1,
        value,
        quality: index as u32 % 4,
        flags: index as u32 % 8,
    }
}

proptest! {
    #[test]
    fn arbitrary_finite_points_round_trip(
        series_id in 1_u64..32,
        values in proptest::collection::vec(-1.0e12_f64..1.0e12_f64, 1..512),
    ) {
        let directory = tempdir().unwrap();
        let path = directory.path().join("property.wattdb");
        let expected: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(index, value)| point(series_id, index, *value))
            .collect();

        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::Manual,
                ..Config::default()
            },
        ).unwrap();
        for chunk in expected.chunks(37) {
            database.append(chunk).unwrap();
        }
        database.close().unwrap();

        let reopened = Database::open(&path).unwrap();
        prop_assert_eq!(
            reopened.query_history(series_id, i64::MIN, i64::MAX),
            expected,
        );
    }

    #[test]
    fn any_truncation_inside_last_frame_discards_the_entire_batch(
        tail_points in 1_usize..128,
        bytes_removed_seed in any::<usize>(),
    ) {
        let directory = tempdir().unwrap();
        let path = directory.path().join("truncation.wattdb");
        let first = point(1, 0, 1.0);
        let tail: Vec<_> = (0..tail_points)
            .map(|index| point(2, index, index as f64))
            .collect();

        let mut database = Database::open(&path).unwrap();
        database.append(&[first]).unwrap();
        let first_length = database.stats().unwrap().file_bytes;
        database.append(&tail).unwrap();
        database.close().unwrap();

        let full_length = std::fs::metadata(&path).unwrap().len();
        let tail_frame_bytes = (full_length - first_length) as usize;
        let bytes_removed = 1 + bytes_removed_seed % (tail_frame_bytes - 1);
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full_length - bytes_removed as u64).unwrap();

        let recovered = Database::open(&path).unwrap();
        prop_assert_eq!(recovered.query_latest(1, i64::MIN, i64::MAX), vec![first]);
        prop_assert!(recovered.query_latest(2, i64::MIN, i64::MAX).is_empty());
        prop_assert_eq!(recovered.stats().unwrap().file_bytes, first_length);
    }
}
