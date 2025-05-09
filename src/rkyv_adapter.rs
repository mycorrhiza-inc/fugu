// rkyv adapter module that actually uses bincode under the hood
//
// This provides a compatibility layer so the codebase can be migrated to rkyv in steps,
// while maintaining actual functionality with bincode.
use std::error::Error as StdError;

// Custom error type to hide implementation details
pub type ArchiveError = Box<dyn StdError + Send + Sync>;

/// Serialize a value to bytes.
/// This implementation actually uses bincode under the hood.
pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, ArchiveError> {
    // Use bincode for the actual serialization
    bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| Box::new(e) as ArchiveError)
}

/// Deserialize a value from bytes.
/// This implementation actually uses bincode under the hood.
pub fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ArchiveError> {
    // Use bincode for the actual deserialization
    let (result, _): (T, usize) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| Box::new(e) as ArchiveError)?;
    Ok(result)
}

/// Access an archived value directly.
/// Since we're using bincode, we need to deserialize anyway.
pub fn access<T: serde::de::DeserializeOwned + Clone>(bytes: &[u8]) -> Result<T, ArchiveError> {
    // Just deserialize since we can't do zero-copy with bincode
    deserialize::<T>(bytes)
}

/// Helper for working with serde types.
pub fn serialize_serde_compat<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, ArchiveError> {
    serialize(value)
}

/// Helper for working with serde types.
pub fn deserialize_serde_compat<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ArchiveError> {
    deserialize::<T>(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    // Test with a simple struct that implements Serde traits
    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestRecord {
        id: String,
        count: u32,
    }

    #[test]
    fn test_serialize_deserialize() {
        // Create a test record
        let record = TestRecord {
            id: "test123".to_string(),
            count: 42,
        };

        // Test serialization and deserialization
        let serialized = serialize(&record).expect("Failed to serialize");
        let deserialized: TestRecord = deserialize(&serialized).expect("Failed to deserialize");

        assert_eq!(
            record, deserialized,
            "Record should be the same after serialization/deserialization"
        );
    }

    #[test]
    fn test_access() {
        // Create a test record
        let record = TestRecord {
            id: "test456".to_string(),
            count: 99,
        };

        // Serialize
        let serialized = serialize(&record).expect("Failed to serialize");

        // Access directly (which actually deserializes with bincode)
        let accessed: TestRecord = access(&serialized).expect("Failed to access");

        // Check values by comparing with the original
        assert_eq!(record, accessed);
    }

    #[test]
    fn test_serialize_deserialize_compat() {
        // Create a test record
        let record = TestRecord {
            id: "compat789".to_string(),
            count: 255,
        };

        // Test compatibility functions
        let serialized = serialize_serde_compat(&record).expect("Failed to serialize with compat");
        let deserialized: TestRecord =
            deserialize_serde_compat(&serialized).expect("Failed to deserialize with compat");

        assert_eq!(
            record, deserialized,
            "Record should be the same after compat serialization/deserialization"
        );
    }
}