use ftwdb::shadow_protocol::{self, CommitBatchRequest, Request, WireMessage};
use ftwdb::{Entity, EntityId, IngressIdentity, Store, Transaction};
use std::collections::BTreeMap;
use std::process::Command;

#[test]
fn offline_command_reports_matching_content_without_claiming_durability() {
    let directory = tempfile::tempdir().unwrap();
    let store_path = directory.path().join("store");
    let frame_path = directory.path().join("0001.hex");
    let entity = Entity {
        id: EntityId(1),
        kind: "site".to_owned(),
        name: "test site".to_owned(),
        parent: None,
        valid_from: 1,
        valid_to: None,
        properties: BTreeMap::new(),
    };
    let batch = CommitBatchRequest {
        source_id: 7,
        sequence: 50,
        commit_id: 500,
        entities: vec![entity.clone()],
        relations: Vec::new(),
        series: Vec::new(),
        runs: Vec::new(),
        plans: Vec::new(),
        points: Vec::new(),
    };
    {
        let mut store = Store::open(&store_path).unwrap();
        let mut transaction = Transaction::new();
        transaction.upsert_entity(entity);
        store
            .commit_ingress(IngressIdentity::new(7, 50, 500), transaction)
            .unwrap();
        store.close().unwrap();
    }
    let frame =
        shadow_protocol::encode(&WireMessage::Request(Request::CommitBatch(batch))).unwrap();
    std::fs::write(&frame_path, encode_hex(&frame)).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_ftwdb-shadow-reconcile"))
        .arg(&store_path)
        .arg(&frame_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "reconcile failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"content_matches\":true"));
    assert!(stdout.contains("\"matching_receipts\":1"));
    assert!(stdout.contains("\"matching_catalog_objects\":1"));
    assert!(stdout.contains("\"receipt_payload_mismatches\":0"));
    assert!(stdout.contains("\"scanned_points\":0"));
    assert!(stdout.contains("\"read_only_durability_proof\":false"));
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2 + 1);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output.push('\n');
    output
}
