use arrow_schema::{DataType, Field, Schema};

pub fn customer_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ])
}

pub fn address_schema() -> Schema {
    Schema::new(vec![
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("address_line_1", DataType::Utf8, false),
        Field::new("address_line_2", DataType::Utf8, true), // nullable
        Field::new("city", DataType::Utf8, false),
        Field::new("province", DataType::Utf8, false),
    ])
}

pub fn amount_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("data", DataType::Int32, false),
    ])
}

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // ====================== First RecordBatch: Person / Customer ======================
//     let person_schema =

//     let person_batch = RecordBatch::try_new(
//         Arc::new(person_schema),
//         vec![
//             Arc::new(StringArray::from(vec!["1"])) as ArrayRef, // id
//             Arc::new(StringArray::from(vec!["John"])) as ArrayRef, // first_name
//             Arc::new(StringArray::from(vec!["Doe"])) as ArrayRef, // last_name
//             Arc::new(Int32Array::from(vec![21])) as ArrayRef,   // age
//         ],
//     )?;

//     println!("Person RecordBatch (1 row):");
//     println!("{:?}\n", person_batch);

//     // ====================== Second RecordBatch: Address ======================
//     let address_schema =

//     let address_batch = RecordBatch::try_new(
//         Arc::new(address_schema),
//         vec![
//             Arc::new(StringArray::from(vec!["1"])) as ArrayRef, // customer_id
//             Arc::new(StringArray::from(vec!["12 Tennatn Avenut"])) as ArrayRef, // address_line_1
//             Arc::new(StringArray::from(vec![Some("Bonteheuwel")])) as ArrayRef, // address_line_2 (nullable)
//             Arc::new(StringArray::from(vec!["Cape Town"])) as ArrayRef,         // city
//             Arc::new(StringArray::from(vec!["Western Cape"])) as ArrayRef,      // province
//         ],
//     )?;

//     println!("Address RecordBatch (1 row):");
//     println!("{:?}", address_batch);

//     // Optional: Print column names and types
//     println!("\nPerson schema:");
//     for field in person_batch.schema().fields() {
//         println!("  {}: {:?}", field.name(), field.data_type());
//     }

//     Ok(())
// }
