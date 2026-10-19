use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// Numbers rows as they arrive, in a column after the others.
pub(crate) struct RowNumbers {
    schema: SchemaRef,
    next: u64,
}

impl RowNumbers {
    /// Numbers for rows of `schema`, in a column named `name` (which must be free).
    pub(crate) fn new(name: &str, schema: &Schema) -> Result<Self, String> {
        if schema.column_with_name(name).is_some() {
            return Err(format!("the result already has a column named {name:?}"));
        }
        let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(name, DataType::UInt64, false)));
        Ok(Self {
            schema: Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            next: 0,
        })
    }

    /// The schema of numbered batches.
    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// `batch` with its rows' numbers after its columns.
    pub(crate) fn add(&mut self, batch: &RecordBatch) -> Result<RecordBatch, String> {
        // Lossless: a batch holds fewer rows than u64 can count.
        let rows = batch.num_rows() as u64;
        let numbers = UInt64Array::from_iter_values(self.next..self.next + rows);
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(numbers));
        let numbered = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|error| format!("a batch unlike the result's schema: {error}"))?;
        self.next += rows;
        Ok(numbered)
    }
}
