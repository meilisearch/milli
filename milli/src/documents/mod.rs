mod builder;
pub mod bumpalo_json;
pub mod document_formats;
mod document_visitor;
mod reader;

use std::fmt::{self, Debug};
use std::io;

pub use builder::DocumentsBatchBuilder;
pub use reader::{DocumentsBatchCursor, DocumentsBatchCursorError, DocumentsBatchReader};

#[derive(Debug)]
pub enum Error {
    ParseFloat { error: std::num::ParseFloatError, line: usize, value: String },
    Csv(csv::Error),
    Json(serde_json::Error),
    Bincode(bincode::Error),
    Grenad(grenad::Error),
    Io(io::Error),
}

impl From<csv::Error> for Error {
    fn from(e: csv::Error) -> Self {
        Self::Csv(e)
    }
}

impl From<io::Error> for Error {
    fn from(other: io::Error) -> Self {
        Self::Io(other)
    }
}

impl From<serde_json::Error> for Error {
    fn from(other: serde_json::Error) -> Self {
        Self::Json(other)
    }
}

impl From<grenad::Error> for Error {
    fn from(other: grenad::Error) -> Self {
        Self::Grenad(other)
    }
}

impl From<bincode::Error> for Error {
    fn from(other: bincode::Error) -> Self {
        Self::Bincode(other)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ParseFloat { error, line, value } => {
                write!(f, "Error parsing number {:?} at line {}: {}", value, line, error)
            }
            Error::Io(e) => write!(f, "{}", e),
            Error::Grenad(e) => write!(f, "{}", e),
            Error::Csv(e) => write!(f, "{}", e),
            Error::Json(e) => write!(f, "{}", e),
            Error::Bincode(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}

/// Macro used to generate documents, with the same syntax as `serde_json::json`
#[cfg(test)]
macro_rules! documents {
    ($data:tt) => {{
        let documents = serde_json::json!($data);
        let documents = match documents {
            object @ serde_json::Value::Object(_) => vec![object],
            serde_json::Value::Array(objects) => objects,
            invalid => {
                panic!("an array of objects must be specified, {:#?} is not an array", invalid)
            }
        };

        let mut builder = crate::documents::DocumentsBatchBuilder::new(Vec::new());
        for document in documents {
            let object = match document {
                serde_json::Value::Object(object) => object,
                invalid => panic!("an object must be specified, {:#?} is not an object", invalid),
            };
            builder.append_json_object(&object).unwrap();
        }

        let vector = builder.into_inner().unwrap();
        crate::documents::DocumentsBatchReader::from_reader(std::io::Cursor::new(vector)).unwrap()
    }};
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use bumpalo::Bump;
    use serde_json::json;

    use super::*;

    #[test]
    fn create_documents_no_errors() {
        let value = json!({
            "number": 1,
            "string": "this is a field",
            "array": ["an", "array"],
            "object": {
                "key": "value",
            },
            "bool": true
        });
        let bump = Bump::new();
        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_json_object(value.as_object().unwrap()).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut documents =
            DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap().into_cursor();

        // assert_eq!(documents.documents_batch_index().iter().count(), 5);
        let reader: &_ = bump.alloc(documents.next_bump_document(&bump).unwrap().unwrap());
        let reader: crate::Object = reader.into();
        assert_eq!(reader.iter().count(), 5);
        assert!(documents.next_bump_document(&bump).unwrap().is_none());
    }

    #[test]
    fn test_add_multiple_documents() {
        let doc1 = json!({
            "bool": true,
        });
        let doc2 = json!({
            "toto": false,
        });
        let bump = Bump::new();
        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_json_object(doc1.as_object().unwrap()).unwrap();
        builder.append_json_object(doc2.as_object().unwrap()).unwrap();
        let vector = builder.into_inner().unwrap();

        let mut documents =
            DocumentsBatchReader::from_reader(io::Cursor::new(vector)).unwrap().into_cursor();

        let reader: &_ = bump.alloc(documents.next_bump_document(&bump).unwrap().unwrap());
        let reader: crate::Object = reader.into();
        assert_eq!(reader.iter().count(), 1);
        assert!(documents.next_bump_document(&bump).unwrap().is_some());
        assert!(documents.next_bump_document(&bump).unwrap().is_none());
    }

    #[test]
    fn test_nested() {
        let docs_reader = documents!([{
            "hello": {
                "toto": ["hello"]
            }
        }]);
        let bump = Bump::new();
        let mut cursor = docs_reader.into_cursor();
        let doc: &_ = bump.alloc(cursor.next_bump_document(&bump).unwrap().unwrap());
        let doc: crate::Object = doc.into();
        let nested = doc.get("hello").unwrap();
        assert_eq!(nested, &json!({ "toto": ["hello"] }));
    }

    #[test]
    fn out_of_order_json_fields() {
        let _documents = documents!([
            {"id": 1,"b": 0},
            {"id": 2,"a": 0,"b": 0},
        ]);
    }

    #[test]
    fn out_of_order_csv_fields() {
        let csv1_content = "id:number,b\n1,0";
        let csv1 = csv::Reader::from_reader(Cursor::new(csv1_content));

        let csv2_content = "id:number,a,b\n2,0,0";
        let csv2 = csv::Reader::from_reader(Cursor::new(csv2_content));

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        builder.append_csv(csv1).unwrap();
        builder.append_csv(csv2).unwrap();
        let vector = builder.into_inner().unwrap();

        DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap();
    }
}
