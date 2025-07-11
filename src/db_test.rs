use crate::db::FuguDB;
use crate::object::ObjectRecord;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::Value;
use std::path::PathBuf;
use tempfile::tempdir;

#[tokio::test]
async fn test_upsert_insert_and_update() {
    // Create a temporary directory for the index
    let dir = tempdir().expect("Failed to create temp dir");
    let db = FuguDB::new(PathBuf::from(dir.path()));

    // First upsert: insert record with id "X" and metadata {"a":1}
    let rec1 = ObjectRecord {
        id: "X".to_string(),
        text: "foo".to_string(),
        metadata: Some(serde_json::json!({"a":1})),
        ..Default::default()
    };
    let res = db.upsert(vec![rec1.clone()]).await;
    assert!(res.is_ok(), "Upsert insert failed: {:?}", res);

    // Retrieve and check metadata
    let docs = db.get("X").expect("Failed to get document");
    assert_eq!(docs.len(), 1, "Expected one document after upsert insert");
    let doc_json = docs[0].to_json(&db.schema());
    let v: Value = serde_json::from_str(&doc_json).expect("Invalid JSON");
    assert_eq!(v["metadata"]["a"], Value::from(1));

    // Second upsert: update record with same id and metadata {"a":2}
    let rec2 = ObjectRecord {
        id: "X".to_string(),
        text: "bar".to_string(),
        metadata: Some(serde_json::json!({"a":2})),
        ..Default::default()
    };
    let res2 = db.upsert(vec![rec2.clone()]).await;
    assert!(res2.is_ok(), "Upsert update failed: {:?}", res2);

    // Retrieve and check updated metadata and text
    let docs2 = db.get("X").expect("Failed to get document after update");
    assert_eq!(docs2.len(), 1, "Expected one document after upsert update");
    let doc_json2 = docs2[0].to_json(&db.schema());
    let v2: Value = serde_json::from_str(&doc_json2).expect("Invalid JSON after update");
    assert_eq!(v2["metadata"]["a"], Value::from(2));
    assert_eq!(v2["text"], Value::from("bar"));
}

