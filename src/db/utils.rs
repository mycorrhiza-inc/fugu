//! Utility functions for database operations

use serde_json::Value as SerdeValue;
use tantivy::TantivyDocument;
use tracing::info;

use crate::db::{FuguDB, FuguSearchResult};
use crate::object::ObjectRecord;

/// Helper function to create metadata facets
pub fn create_metadata_facets(value: &SerdeValue, mut prefix: Vec<String>) -> Vec<Vec<String>> {
    let mut facets = Vec::new();

    match value {
        SerdeValue::Object(map) => {
            for (key, val) in map {
                let mut new_prefix = prefix.clone();
                new_prefix.push(key.clone());
                let sub_facets = create_metadata_facets(val, new_prefix);
                facets.extend(sub_facets);
            }
        }
        SerdeValue::Array(arr) => {
            for item in arr {
                let sub_facets = create_metadata_facets(item, prefix.clone());
                facets.extend(sub_facets);
            }
        }
        _ => {
            if let Some(value_str) = value.as_str() {
                if !value_str.is_empty() {
                    prefix.push(value_str.to_string());
                    facets.push(prefix);
                }
            }
        }
    }

    facets
}

/// Create facet indexes from JSON value
pub fn create_facet_indexes(
    value: &SerdeValue,
    mut prefix: Vec<String>,
    _doc: &TantivyDocument,
) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = Vec::new();
    match value {
        SerdeValue::Object(map) => {
            for (key, val) in map {
                let mut p = prefix.clone();
                p.push(key.to_string());
                let temp = create_facet_indexes(val, p, _doc);
                out.extend(temp);
            }
        }
        SerdeValue::Array(arr) => {
            for item in arr.iter() {
                let temp = create_facet_indexes(item, prefix.clone(), _doc);
                out.extend(temp);
            }
        }
        _ => {
            let field_str = value.as_str().unwrap_or_default().to_string();
            prefix.push(field_str);
            info!("new facet: {}", prefix.join("/"));
            out.push(prefix);
        }
    }
    out
}

/// Process additional fields from ObjectRecord
pub fn process_additional_fields(object_record: &ObjectRecord) -> SerdeValue {
    let serialized = serde_json::to_string(object_record).unwrap_or_default();
    let mut value: SerdeValue = serde_json::from_str(&serialized).unwrap_or(SerdeValue::Null);

    if let SerdeValue::Object(ref mut map) = value {
        map.remove("id");
        map.remove("text");
        SerdeValue::Object(map.clone())
    } else {
        SerdeValue::Object(serde_json::Map::new())
    }
}

/// Check if a JSON value is considered empty
pub fn is_value_empty(value: &SerdeValue) -> bool {
    match value {
        SerdeValue::Null => true,
        SerdeValue::Bool(_) => false, // Both true and false are not empty
        SerdeValue::Number(n) => n.as_f64().map(|x| x == 0.0).unwrap_or(false),
        SerdeValue::String(s) => s.is_empty(),
        SerdeValue::Array(arr) => arr.is_empty(),
        SerdeValue::Object(obj) => obj.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_create_metadata_facets() {
        let metadata = json!({
            "category": "documents",
            "tags": ["important", "legal"],
            "details": {
                "department": "legal",
                "priority": "high"
            }
        });

        let facets = create_metadata_facets(&metadata, vec![]);
        assert!(!facets.is_empty());

        // Should create facets for leaf values
        assert!(
            facets
                .iter()
                .any(|f| f.last() == Some(&"documents".to_string()))
        );
        assert!(
            facets
                .iter()
                .any(|f| f.last() == Some(&"important".to_string()))
        );
        assert!(
            facets
                .iter()
                .any(|f| f.last() == Some(&"legal".to_string()))
        );
    }

    #[test]
    fn test_is_value_empty() {
        assert!(is_value_empty(&SerdeValue::Null));
        assert!(is_value_empty(&json!("")));
        assert!(is_value_empty(&json!([])));
        assert!(is_value_empty(&json!({})));

        assert!(!is_value_empty(&json!(false)));
        assert!(!is_value_empty(&json!(true)));
        assert!(!is_value_empty(&json!(0)));
        assert!(!is_value_empty(&json!("text")));
    }

    #[test]
    fn test_create_facet_indexes() {
        let test_doc = TantivyDocument::new();
        let value = json!({
            "status": "active",
            "tags": ["urgent", "review"]
        });

        let facets = create_facet_indexes(&value, vec![], &test_doc);
        assert!(!facets.is_empty());

        // Should create facet paths
        assert!(facets.iter().any(|f| f.contains(&"active".to_string())));
        assert!(facets.iter().any(|f| f.contains(&"urgent".to_string())));
    }

    #[test]
    fn test_process_additional_fields() {
        use crate::object::ObjectRecord;

        let record = ObjectRecord {
            id: "test_id".to_string(),
            text: "test text".to_string(),
            facets: None,
            namespace: Some("test_namespace".to_string()),
            organization: Some("test_org".to_string()),
            conversation_id: None,
            data_type: None,
            metadata: Some(json!({"key": "value"})),
            date_created: None,
            date_updated: None,
            date_published: None,
        };

        let additional = process_additional_fields(&record);

        // Should not contain id or text
        if let SerdeValue::Object(map) = additional {
            assert!(!map.contains_key("id"));
            assert!(!map.contains_key("text"));
            assert!(map.contains_key("namespace"));
            assert!(map.contains_key("organization"));
        } else {
            panic!("Expected object");
        }
    }
}
