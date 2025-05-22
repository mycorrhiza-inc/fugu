use serde::{Deserialize, Serialize};
use serde_json::Value;
use tantivy::schema::{FAST, STORED, SchemaBuilder, TEXT};

#[derive(Serialize, Deserialize, Clone)]
pub struct ObjectRecord {
    pub id: String,
    pub text: String,
    pub metadata: Value,
}

pub fn build_object_record_schema(mut schema_builder: SchemaBuilder) -> tantivy::schema::Schema {
    schema_builder.add_text_field("id", TEXT | STORED);
    schema_builder.add_text_field("text", TEXT | STORED);
    schema_builder.add_date_field("date_created", STORED | FAST);
    schema_builder.add_date_field("date_updated", STORED | FAST);
    schema_builder.add_json_field("metadata", STORED | FAST);
    let schema = schema_builder.build();
    return schema;
}
