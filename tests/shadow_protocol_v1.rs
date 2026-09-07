use ftwdb::shadow_protocol::{
    self, Ack, AckKind, CommitBatchRequest, ErrorCode, ErrorResponse, FlushRequest, HealthRequest,
    HealthResponse, HealthStatus, HelloRequest, HelloResponse, Request, Response, SyncPolicy,
    WireMessage,
};
use ftwdb::{
    CalendarUnit, Entity, EntityId, Plan, PlanStatus, Point, PropertyValue, Relation, RelationId,
    RollupPolicy, RollupResolution, RollupTier, Run, RunId, RunKind, RunStatus, SeriesDefinition,
    SeriesSemantics,
};
use std::collections::BTreeMap;

fn fixture_messages() -> Vec<(&'static str, WireMessage)> {
    let source_id = 0x0011_2233_4455_6677_8899_aabb_ccdd_eeff;
    let commit_id = 0xffee_ddcc_bbaa_9988_7766_5544_3322_1100;
    let entity_id = EntityId(0x1020_3040_5060_7080_90a0_b0c0_d0e0_f001);
    let target_id = EntityId(0x1020_3040_5060_7080_90a0_b0c0_d0e0_f002);
    let relation_id = RelationId(0x2030_4050_6070_8090_a0b0_c0d0_e0f0_0102);
    let run_id = RunId(0x3040_5060_7080_90a0_b0c0_d0e0_f001_0203);

    let properties = BTreeMap::from([
        ("bool".into(), PropertyValue::Bool(true)),
        ("float".into(), PropertyValue::Float(-12.5)),
        ("int".into(), PropertyValue::Integer(-42)),
        ("null".into(), PropertyValue::Null),
        ("text".into(), PropertyValue::Text("grid import".into())),
    ]);
    let batch = CommitBatchRequest {
        source_id,
        sequence: 0x0102_0304_0506_0708,
        commit_id,
        entities: vec![Entity {
            id: entity_id,
            kind: "site".into(),
            name: "FTW test box".into(),
            parent: None,
            valid_from: 1_754_382_400_123_456,
            valid_to: Some(1_754_468_800_123_456),
            properties,
        }],
        relations: vec![Relation {
            id: relation_id,
            kind: "feeds".into(),
            source: entity_id,
            target: target_id,
            valid_from: 1_754_382_400_123_456,
            valid_to: None,
            properties: BTreeMap::from([("phase".into(), PropertyValue::Text("L1".into()))]),
        }],
        series: vec![SeriesDefinition {
            id: 0x1122_3344_5566_7788,
            owner_entity: Some(entity_id),
            owner_relation: None,
            name: "grid_power".into(),
            physical_quantity: "power".into(),
            canonical_unit: "W".into(),
            semantics: SeriesSemantics::Gauge,
            maximum_gap_micros: Some(5_000_000),
            rollup_policy: RollupPolicy {
                raw_retain_for_micros: Some(1_209_600_000_000),
                tiers: vec![
                    RollupTier {
                        resolution: RollupResolution::FixedMicros(300_000_000),
                        retain_for_micros: Some(31_536_000_000_000),
                    },
                    RollupTier {
                        resolution: RollupResolution::Calendar {
                            unit: CalendarUnit::Day,
                            iana_timezone: "Europe/Stockholm".into(),
                        },
                        retain_for_micros: None,
                    },
                ],
            },
        }],
        runs: vec![Run {
            id: run_id,
            kind: RunKind::Optimization,
            status: RunStatus::Succeeded,
            created_at: 1_754_382_300_000_000,
            knowledge_time: 1_754_382_350_000_000,
            workflow: "day-ahead".into(),
            model: "ftw-plan".into(),
            model_version: "2026.08".into(),
            parent_run: Some(RunId(1)),
            input_snapshot: Some(RunId(2)),
            attributes: BTreeMap::from([("tariff".into(), PropertyValue::Text("SE4".into()))]),
        }],
        plans: vec![Plan {
            id: 0x4050_6070_8090_a0b0_c0d0_e0f0_0102_0304,
            run_id,
            status: PlanStatus::Deployed,
            horizon_start: 1_754_382_400_000_000,
            horizon_end: 1_754_468_800_000_000,
            resolution_micros: 300_000_000,
            scenario: "base".into(),
            objective_terms: BTreeMap::from([
                ("cost_sek".into(), 12.25),
                ("peak_w".into(), 4_500.0),
            ]),
            objective_value: Some(12.25),
            supersedes: Some(3),
            attributes: BTreeMap::from([("mode".into(), PropertyValue::Text("auto".into()))]),
        }],
        points: vec![Point {
            series_id: 0x1122_3344_5566_7788,
            valid_time: 1_754_382_400_123_456,
            valid_time_end: 1_754_382_700_123_456,
            knowledge_time: 1_754_382_350_000_000,
            change_time: 1_754_382_351_000_000,
            run_id: run_id.0,
            value: -1_234.5,
            quality: 0x1020_3040,
            flags: 0x5060_7080,
        }],
    };

    vec![
        (
            "hello-request.hex",
            WireMessage::Request(Request::Hello(HelloRequest {
                source_id,
                node_id: "ftw-box-01".into(),
                client_version: "go-ftw/0.1.0".into(),
                capabilities: 0x0102_0304_0506_0708,
            })),
        ),
        (
            "commit-batch-request.hex",
            WireMessage::Request(Request::CommitBatch(batch)),
        ),
        (
            "flush-request.hex",
            WireMessage::Request(Request::Flush(FlushRequest {
                source_id,
                through_sequence: 0x0102_0304_0506_0708,
            })),
        ),
        (
            "health-request.hex",
            WireMessage::Request(Request::Health(HealthRequest {
                nonce: 0x1122_3344_5566_7788,
            })),
        ),
        (
            "hello-response.hex",
            WireMessage::Response(Response::Hello(HelloResponse {
                selected_version: 1,
                session_id: [
                    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc,
                    0xdd, 0xee, 0xff,
                ],
                server_time_micros: 1_754_382_400_123_456,
            })),
        ),
        (
            "commit-ack-response.hex",
            WireMessage::Response(Response::Ack(Ack {
                kind: AckKind::CommitBatch,
                source_id,
                sequence: 0x0102_0304_0506_0708,
                commit_id,
                accepted_through_sequence: Some(0x0102_0304_0506_0708),
                durable_through_sequence: Some(0x0102_0304_0506_0708),
                durable: true,
                deduplicated: false,
                frame_offset: 0x1112_1314_1516_1718,
                records: 6,
                points: 1,
                bytes_written: 0x2122_2324_2526_2728,
            })),
        ),
        (
            "flush-ack-response.hex",
            WireMessage::Response(Response::Ack(Ack {
                kind: AckKind::Flush,
                source_id,
                sequence: 0x0102_0304_0506_0708,
                commit_id: 0,
                accepted_through_sequence: Some(0x0102_0304_0506_0708),
                durable_through_sequence: Some(0x0102_0304_0506_0708),
                durable: true,
                deduplicated: false,
                frame_offset: 0,
                records: 0,
                points: 0,
                bytes_written: 0,
            })),
        ),
        (
            "health-response.hex",
            WireMessage::Response(Response::Health(HealthResponse {
                nonce: 0x1122_3344_5566_7788,
                source_id,
                status: HealthStatus::Degraded,
                queue_entries: 3,
                accepted_through_sequence: Some(0x0102_0304_0506_0708),
                durable_through_sequence: None,
                overload_count: 5,
                protocol_error_count: 7,
                database_bytes: 0x3132_3334_3536_3738,
                database_points: 9,
                database_commits: 4,
                recovered_tail_bytes: 0,
                sync_policy: SyncPolicy::Always,
                last_ack_durable: false,
            })),
        ),
        (
            "error-response.hex",
            WireMessage::Response(Response::Error(ErrorResponse {
                code: ErrorCode::IdempotencyConflict,
                retryable: false,
                message: "idempotency-conflict".into(),
            })),
        ),
    ]
}

fn decode_hex(text: &str) -> Vec<u8> {
    let text = text.trim();
    assert_eq!(text.len() % 2, 0);
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digits = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(digits, 16).unwrap()
        })
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn fixture_file(name: &str) -> &'static [u8] {
    match name {
        "hello-request.hex" => {
            include_bytes!("../testdata/shadow-protocol-v1/hello-request.hex")
        }
        "commit-batch-request.hex" => {
            include_bytes!("../testdata/shadow-protocol-v1/commit-batch-request.hex")
        }
        "flush-request.hex" => {
            include_bytes!("../testdata/shadow-protocol-v1/flush-request.hex")
        }
        "health-request.hex" => {
            include_bytes!("../testdata/shadow-protocol-v1/health-request.hex")
        }
        "hello-response.hex" => {
            include_bytes!("../testdata/shadow-protocol-v1/hello-response.hex")
        }
        "commit-ack-response.hex" => {
            include_bytes!("../testdata/shadow-protocol-v1/commit-ack-response.hex")
        }
        "flush-ack-response.hex" => {
            include_bytes!("../testdata/shadow-protocol-v1/flush-ack-response.hex")
        }
        "health-response.hex" => {
            include_bytes!("../testdata/shadow-protocol-v1/health-response.hex")
        }
        "error-response.hex" => {
            include_bytes!("../testdata/shadow-protocol-v1/error-response.hex")
        }
        _ => panic!("fixture list contains an unknown file: {name}"),
    }
}

fn sha256(input: &[u8]) -> [u8; 32] {
    const INITIAL: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    const ROUND: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let bit_length = u64::try_from(input.len()).unwrap().checked_mul(8).unwrap();
    let mut padded = input.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_length.to_be_bytes());

    let mut state = INITIAL;
    for chunk in padded.chunks_exact(64) {
        let mut schedule = [0_u32; 64];
        for (word, bytes) in schedule[..16].iter_mut().zip(chunk.chunks_exact(4)) {
            *word = u32::from_be_bytes(bytes.try_into().unwrap());
        }
        for index in 16..64 {
            let s0 = schedule[index - 15].rotate_right(7)
                ^ schedule[index - 15].rotate_right(18)
                ^ (schedule[index - 15] >> 3);
            let s1 = schedule[index - 2].rotate_right(17)
                ^ schedule[index - 2].rotate_right(19)
                ^ (schedule[index - 2] >> 10);
            schedule[index] = schedule[index - 16]
                .wrapping_add(s0)
                .wrapping_add(schedule[index - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for index in 0..64 {
            let sum1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let first = h
                .wrapping_add(sum1)
                .wrapping_add(choice)
                .wrapping_add(ROUND[index])
                .wrapping_add(schedule[index]);
            let sum0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let second = sum0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(first);
            d = c;
            c = b;
            b = a;
            a = first.wrapping_add(second);
        }
        for (value, addition) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *value = value.wrapping_add(addition);
        }
    }

    let mut digest = [0_u8; 32];
    for (output, value) in digest.chunks_exact_mut(4).zip(state) {
        output.copy_from_slice(&value.to_be_bytes());
    }
    digest
}

#[test]
fn every_v1_message_matches_the_shared_frozen_bytes() {
    for (name, message) in fixture_messages() {
        let fixture = match name {
            "hello-request.hex" => {
                include_str!("../testdata/shadow-protocol-v1/hello-request.hex")
            }
            "commit-batch-request.hex" => {
                include_str!("../testdata/shadow-protocol-v1/commit-batch-request.hex")
            }
            "flush-request.hex" => {
                include_str!("../testdata/shadow-protocol-v1/flush-request.hex")
            }
            "health-request.hex" => {
                include_str!("../testdata/shadow-protocol-v1/health-request.hex")
            }
            "hello-response.hex" => {
                include_str!("../testdata/shadow-protocol-v1/hello-response.hex")
            }
            "commit-ack-response.hex" => {
                include_str!("../testdata/shadow-protocol-v1/commit-ack-response.hex")
            }
            "flush-ack-response.hex" => {
                include_str!("../testdata/shadow-protocol-v1/flush-ack-response.hex")
            }
            "health-response.hex" => {
                include_str!("../testdata/shadow-protocol-v1/health-response.hex")
            }
            "error-response.hex" => {
                include_str!("../testdata/shadow-protocol-v1/error-response.hex")
            }
            _ => panic!("fixture list contains an unknown file: {name}"),
        };
        let frozen = decode_hex(fixture);
        assert_eq!(
            shadow_protocol::encode(&message).unwrap(),
            frozen,
            "encoder drifted for {name}"
        );
        assert_eq!(
            shadow_protocol::decode(&frozen).unwrap(),
            message,
            "decoder drifted for {name}"
        );
    }
}

#[test]
fn sha256_manifest_covers_every_fixture_file() {
    assert_eq!(
        encode_hex(&sha256(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    let manifest = include_str!("../testdata/shadow-protocol-v1/SHA256SUMS");
    let mut covered = BTreeMap::new();
    for line in manifest.lines() {
        let (expected, name) = line.split_once("  ").unwrap();
        assert!(covered.insert(name, expected).is_none());
        assert_eq!(encode_hex(&sha256(fixture_file(name))), expected, "{name}");
    }
    let names: BTreeMap<_, _> = fixture_messages()
        .into_iter()
        .map(|(name, _)| (name, ()))
        .collect();
    assert_eq!(
        covered.keys().copied().collect::<Vec<_>>(),
        names.keys().copied().collect::<Vec<_>>()
    );
}
