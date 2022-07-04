use std::io;
use std::io::Write;

use grenad::{CompressionType, WriterBuilder};
use serde::de::Deserializer;
use serde_json::{Number, Value};

use crate::documents::document_visitor::DocumentVisitor;
use crate::documents::Error;
use crate::Object;
/// The `DocumentsBatchBuilder` provides a way to build a documents batch in the intermediary
/// format used by milli.
///
/// The writer used by the `DocumentsBatchBuilder` can be read using a `DocumentsBatchReader`
/// to iterate over the documents.
///
/// ## example:
/// ```
/// use serde_json::json;
/// use milli::documents::DocumentsBatchBuilder;
///
/// let json = json!({ "id": 1, "name": "foo" });
///
/// let mut builder = DocumentsBatchBuilder::new(Vec::new());
/// builder.append_json_object(json.as_object().unwrap()).unwrap();
/// let _vector = builder.into_inner().unwrap();
/// ```
pub struct DocumentsBatchBuilder<W> {
    /// The inner grenad writer
    writer: grenad::Writer<W>,
    /// The number of documents that were added to this builder,
    /// it doesn't take the primary key of the documents into account at this point.
    documents_count: u32,
    /// A buffer to serialize the values and avoid reallocating,
    /// serialized values are stored in an obkv.
    value_buffer: Vec<u8>,
}

#[derive(Default)]
struct StringAllocations {
    strings: Vec<String>,
}
impl StringAllocations {
    fn take_one(&mut self) -> String {
        self.strings.pop().unwrap_or_else(|| String::new())
    }
    fn reclaim_from_object(&mut self, object: &mut Object) {
        for value in object.values_mut() {
            match value.take() {
                Value::String(mut s) => {
                    s.clear();
                    self.strings.push(s);
                }
                _ => {}
            }
        }
    }
}

impl<W: Write> DocumentsBatchBuilder<W> {
    pub fn new(writer: W) -> DocumentsBatchBuilder<W> {
        DocumentsBatchBuilder {
            writer: WriterBuilder::new().compression_type(CompressionType::None).build(writer),
            documents_count: 0,
            value_buffer: Vec::new(),
        }
    }

    /// Returns the number of documents inserted into this builder.
    pub fn documents_count(&self) -> u32 {
        self.documents_count
    }

    /// Appends a new JSON object into the batch
    pub fn append_unparsed_json_object(&mut self, object: &str) -> Result<(), Error> {
        let internal_id = self.documents_count.to_be_bytes();
        self.writer.insert(internal_id, object.as_bytes())?;
        self.documents_count += 1;
        Ok(())
    }

    /// Appends a new JSON object into the batch
    pub fn append_json_object(&mut self, object: &Object) -> Result<(), Error> {
        self.value_buffer.clear();
        let internal_id = self.documents_count.to_be_bytes();
        serde_json::to_writer(&mut self.value_buffer, object)?;
        self.writer.insert(internal_id, &self.value_buffer)?;
        self.documents_count += 1;
        Ok(())
    }

    /// Appends new json documents from a reader. The reader may contain a Json object
    /// or an array of Json objects.
    pub fn append_json<R: io::Read>(&mut self, reader: R) -> Result<(), Error> {
        let mut de = serde_json::Deserializer::from_reader(reader);
        let mut visitor = DocumentVisitor { batch_builder: self };

        // The result of `deserialize_any` is StdResult<Result<(), Error>, serde_json::Error>
        // See the documentqtion of DocumentVisitor for an explanation
        de.deserialize_any(&mut visitor).map_err(Error::Json)?
    }

    /// Appends a new CSV file into the batch
    pub fn append_csv<R: io::Read>(&mut self, mut reader: csv::Reader<R>) -> Result<(), Error> {
        let fields: Vec<(String, AllowedType)> =
            reader.headers()?.into_iter().map(parse_csv_header).collect();

        let mut object = Object::new();
        for (key, _) in fields.iter() {
            let key = key.to_string();
            object.insert(key, Value::Null);
        }

        let mut string_allocs = StringAllocations::default();

        let mut record = csv::StringRecord::new();
        let mut line = 0;

        while reader.read_record(&mut record)? {
            // We increment here and not at the end of the while loop to take
            // the header offset into account.
            line += 1;
            for (i, ((_, type_), value_builder)) in
                fields.iter().zip(object.values_mut()).enumerate()
            {
                let value = &record[i];
                match type_ {
                    AllowedType::Number => {
                        let trimmed_value = value.trim();
                        if trimmed_value.is_empty() {
                            *value_builder = Value::Null;
                        } else {
                            match trimmed_value.parse::<f64>() {
                                Ok(float) => {
                                    if let Some(number) = Number::from_f64(float) {
                                        *value_builder = Value::Number(number);
                                    } else {
                                        let mut string = string_allocs.take_one();
                                        string.push_str(trimmed_value);
                                        *value_builder = Value::String(string);
                                    }
                                }
                                Err(error) => {
                                    return Err(Error::ParseFloat {
                                        error,
                                        line,
                                        value: value.to_string(),
                                    });
                                }
                            }
                        }
                    }
                    AllowedType::String => {
                        if value.is_empty() {
                            *value_builder = Value::Null;
                        } else {
                            let mut string = string_allocs.take_one();
                            string.push_str(value);
                            *value_builder = Value::String(string);
                        }
                    }
                }
            }

            let internal_id = self.documents_count.to_be_bytes();
            self.value_buffer.clear();
            serde_json::to_writer(&mut self.value_buffer, &object)?;
            self.writer.insert(internal_id, &self.value_buffer)?;
            string_allocs.reclaim_from_object(&mut object);
            self.documents_count += 1;
        }

        Ok(())
    }

    /// Flushes the content on disk
    pub fn into_inner(self) -> io::Result<W> {
        self.writer.into_inner()
    }
}

#[derive(Debug)]
enum AllowedType {
    String,
    Number,
}

fn parse_csv_header(header: &str) -> (String, AllowedType) {
    // if there are several separators we only split on the last one.
    match header.rsplit_once(':') {
        Some((field_name, field_type)) => match field_type {
            "string" => (field_name.to_string(), AllowedType::String),
            "number" => (field_name.to_string(), AllowedType::Number),
            // if the pattern isn't reconized, we keep the whole field.
            _otherwise => (header.to_string(), AllowedType::String),
        },
        None => (header.to_string(), AllowedType::String),
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use serde_json::json;

    use super::*;
    use crate::documents::DocumentsBatchReader;

    #[test]
    fn add_single_documents_json() {
        let json = serde_json::json!({
            "id": 1,
            "field": "hello!",
        });

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_json_object(json.as_object().unwrap()).unwrap();

        let json = serde_json::json!({
            "blabla": false,
            "field": "hello!",
            "id": 1,
        });

        builder.append_json_object(json.as_object().unwrap()).unwrap();

        assert_eq!(builder.documents_count(), 2);
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        let document = cursor.next_document().unwrap().unwrap();
        assert_eq!(document.iter().count(), 2);

        let document = cursor.next_document().unwrap().unwrap();
        assert_eq!(document.iter().count(), 3);

        assert!(cursor.next_document().unwrap().is_none());
    }

    #[test]
    fn add_documents_csv() {
        let csv_content = "id:number,field:string\n1,hello!\n2,blabla";
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        assert_eq!(builder.documents_count(), 2);
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        let document = cursor.next_document().unwrap().unwrap();
        assert_eq!(document.iter().count(), 2);

        let document = cursor.next_document().unwrap().unwrap();
        assert_eq!(document.iter().count(), 2);

        assert!(cursor.next_document().unwrap().is_none());
    }

    #[test]
    fn simple_csv_document() {
        let csv_content = r#"city,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();
        let doc = cursor.next_document().unwrap().unwrap();

        assert_eq!(
            &doc,
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
            .as_object()
            .unwrap(),
        );

        assert!(cursor.next_document().unwrap().is_none());
    }

    #[test]
    fn coma_in_field() {
        let csv_content = r#"city,country,pop
"Boston","United, States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        let doc = cursor.next_document().unwrap().unwrap();

        assert_eq!(
            &doc,
            json!({
                "city": "Boston",
                "country": "United, States",
                "pop": "4628910",
            })
            .as_object()
            .unwrap(),
        );
    }

    #[test]
    fn quote_in_field() {
        let csv_content = r#"city,country,pop
"Boston","United"" States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        let doc = cursor.next_document().unwrap().unwrap();

        assert_eq!(
            &doc,
            json!({
                "city": "Boston",
                "country": "United\" States",
                "pop": "4628910",
            })
            .as_object()
            .unwrap(),
        );
    }

    #[test]
    fn integer_in_field() {
        let csv_content = r#"city,country,pop:number
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();
        let doc = cursor.next_document().unwrap().unwrap();
        assert_eq!(
            &doc,
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": 4628910.0,
            })
            .as_object()
            .unwrap(),
        );
    }

    #[test]
    fn float_in_field() {
        let csv_content = r#"city,country,pop:number
"Boston","United States","4628910.01""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        let doc = cursor.next_document().unwrap().unwrap();

        assert_eq!(
            &doc,
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": 4628910.01,
            })
            .as_object()
            .unwrap(),
        );
    }

    #[test]
    fn several_colon_in_header() {
        let csv_content = r#"city:love:string,country:state,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        let doc = cursor.next_document().unwrap().unwrap();

        assert_eq!(
            &doc,
            json!({
                "city:love": "Boston",
                "country:state": "United States",
                "pop": "4628910",
            })
            .as_object()
            .unwrap(),
        );
    }

    #[test]
    fn ending_by_colon_in_header() {
        let csv_content = r#"city:,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        let doc = cursor.next_document().unwrap().unwrap();

        assert_eq!(
            &doc,
            json!({
                "city:": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
            .as_object()
            .unwrap(),
        );
    }

    #[test]
    fn starting_by_colon_in_header() {
        let csv_content = r#":city,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        let doc = cursor.next_document().unwrap().unwrap();

        assert_eq!(
            &doc,
            json!({
                ":city": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
            .as_object()
            .unwrap(),
        );
    }

    #[ignore]
    #[test]
    fn starting_by_colon_in_header2() {
        let csv_content = r#":string,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        assert!(cursor.next_document().is_err());
    }

    #[test]
    fn double_colon_in_header() {
        let csv_content = r#"city::string,country,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut cursor =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        let doc = cursor.next_document().unwrap().unwrap();

        assert_eq!(
            &doc,
            json!({
                "city:": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
            .as_object()
            .unwrap(),
        );
    }

    #[test]
    fn bad_type_in_header() {
        let csv_content = r#"city,country:number,pop
"Boston","United States","4628910""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        assert!(builder.append_csv(csv).is_err());
    }

    #[test]
    fn bad_column_count1() {
        let csv_content = r#"city,country,pop
"Boston","United States","4628910", "too much
        let csv = csv::Reader::from_reader(Cursor::new(csv_content"#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        assert!(builder.append_csv(csv).is_err());
    }

    #[test]
    fn bad_column_count2() {
        let csv_content = r#"city,country,pop
"Boston","United States""#;
        let csv = csv::Reader::from_reader(Cursor::new(csv_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        assert!(builder.append_csv(csv).is_err());
    }
}
