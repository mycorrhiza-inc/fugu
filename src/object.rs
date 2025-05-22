use serde::{Deserialize, Serialize};
use serde_json::Value;
use tantivy::schema::{FAST, FacetOptions, STORED, STRING, SchemaBuilder, TEXT};

#[derive(Serialize, Deserialize, Clone)]
pub struct ObjectRecord {
    pub id: String,
    //pub name: String,
    pub text: String,
    // pub date_created: i64,
    // pub date_upated: i64,
    // pub date_published: Option<i64>,
    pub metadata: Value,
    //pub data: Option<Value>,
}

pub fn build_object_record_schema(mut schema_builder: SchemaBuilder) -> tantivy::schema::Schema {
    schema_builder.add_text_field("id", STRING | STORED);
    schema_builder.add_text_field("text", TEXT | STORED);
    // schema_builder.add_text_field("name", TEXT | STORED);
    // schema_builder.add_date_field("date_published", STORED | FAST);
    // schema_builder.add_date_field("date_created", STORED | FAST);
    // schema_builder.add_date_field("date_updated", STORED | FAST);
    schema_builder.add_facet_field("facet", FacetOptions::default().set_stored());
    schema_builder.add_json_field("metadata", STORED | FAST);
    let schema = schema_builder.build();
    return schema;
}
