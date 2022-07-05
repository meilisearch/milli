/*!
This module contains a custom implementation of serde's traits for serde_json::Value.

The [`DocumentsBatchBuilder`](crate::documents::DocumentsBatchBuilder) needs to read a
file containing an array of Json objects and write it to another file as NDJson quickly.
One way to do this would be to do it in two steps:

1. deserialize the whole Json array in memory
2. write the array as NDJson to the new file

But this would be wasteful, memory-wise. Instead, we perform this task incrementally:
as we read each Json object from the array, we immediately write it to the new file.
*/

use std::fmt;
use std::io::Write;

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::documents::{DocumentsBatchBuilder, Error};
use crate::Object;

/// A Visitor that passes each visited Json object to a `DocumentsBatchBuilder`
/// so that it is written to a file.
pub struct DocumentVisitor<'a, W> {
    pub batch_builder: &'a mut DocumentsBatchBuilder<W>,
}

impl<'a, 'de, W: Write> Visitor<'de> for &mut DocumentVisitor<'a, W> {
    /// The Visitor value is `Ok` if all file operations were successful, and
    /// `Err(crate::documents::Error)` if a visited Json object could not be written to the file.
    type Value = Result<(), Error>;

    // This is normally the first function that is called on our visitor, for the top-level
    // array containing all the Json documents. The exception is when we add a single Json document.
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        // We tell serde to deserialize each element of the sequence using the `DeserializeSeed`
        // implementation of our DocumentVisitor. This impl expects to find a Json object and nothing else.
        // It will call our visitor's `visit_map` method on each object.
        while let Some(v) = seq.next_element_seed(&mut *self)? {
            // This happens if the element was deserialized correctly,
            // but an error happened when we tried to insert it into the batch builder
            // In that case, we want to return early.
            if let Err(e) = v {
                return Ok(Err(e.into()));
            }
        }

        Ok(Ok(()))
    }

    // Called on each object inside the visited Json array
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = Object::new();
        // Note that here we call serde_json's normal `next_entry` method, which
        // does not use our visitor. So we deserialize each field of the object normally.
        // And we add each field to our object.
        while let Some((key, value)) = map.next_entry()? {
            object.insert(key, value);
        }
        // Now that we visited each field, we can pass our object to the batch builder.
        if let Err(e) = self.batch_builder.append_json_object(&object) {
            // and again return early if an error was encountered
            return Ok(Err(e.into()));
        }

        Ok(Ok(()))
    }

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a document, or a sequence of documents.")
    }
}

/// A Deserialize implementation which only accepts maps (ie Json object in our case)
/// and which uses DocumentVisitor as the map's visitor.
impl<'a, 'de, W> DeserializeSeed<'de> for &mut DocumentVisitor<'a, W>
where
    W: Write,
{
    type Value = Result<(), Error>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde::Deserializer;

    use super::*;

    fn deser(input: &str) -> Result<String, serde_json::Error> {
        let mut writer = Vec::new();
        let mut de = serde_json::Deserializer::from_reader(Cursor::new(input));
        let mut batch_builder = DocumentsBatchBuilder::new(&mut writer);
        let mut visitor = DocumentVisitor { batch_builder: &mut batch_builder };

        // The result of `deserialize_any` is StdResult<Result<(), Error>, serde_json::Error>
        // See the documentqtion of DocumentVisitor for an explanation
        de.deserialize_any(&mut visitor)?.unwrap();

        let reader = batch_builder.into_inner().unwrap();
        let reader = grenad::Reader::new(Cursor::new(reader)).unwrap();
        let mut cursor = reader.into_cursor().unwrap();
        let mut output = String::new();
        while let Some((_, value)) = cursor.move_on_next().unwrap() {
            output.push_str(&String::from_utf8_lossy(value));
        }
        Ok(output)
    }

    // TODO: tests with actual content

    #[test]
    fn one_object() {
        let result = deser("{}").unwrap();
        assert_eq!(result, "{}");

        let result = deser(r#"{"id":1}"#).unwrap();
        assert_eq!(result, r#"{"id":1}"#);

        let result = deser(r#"{"id": {}}"#).unwrap();
        assert_eq!(result, r#"{"id":{}}"#);

        let result = deser(r#"{"id": [1, 2]}"#).unwrap();
        assert_eq!(result, r#"{"id":[1,2]}"#);
    }
    #[test]
    fn sequence_objects() {
        let result = deser("[]").unwrap();
        assert_eq!(result, "");

        let result = deser(r#"[{"id":1}]"#).unwrap();
        assert_eq!(result, r#"{"id":1}"#);

        let result = deser(r#"[{"id": {}}, {}, {"hello": [null] }]"#).unwrap();
        assert_eq!(result, r#"{"id":{}}{}{"hello":[null]}"#);
    }
    #[test]
    fn invalid_documents() {
        let result = deser("");
        assert!(result.is_err());

        let result = deser("1");
        assert!(result.is_err());

        let result = deser(r#""""#);
        assert!(result.is_err());

        let result = deser(r#"null"#);
        assert!(result.is_err());

        let result = deser(r#"[1]"#);
        assert!(result.is_err());

        let result = deser(r#"[null]"#);
        assert!(result.is_err());

        let result = deser(r#"[""]"#);
        assert!(result.is_err());

        let result = deser(r#"[[{}]]"#);
        assert!(result.is_err());

        let result = deser(r#"[{}, 2]"#);
        assert!(result.is_err());
    }
}
