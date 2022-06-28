use crate::documents::{DocumentsBatchBuilder, Error};
use crate::Object;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::fmt;
use std::io::Write;

pub struct DocumentVisitor<'a, W> {
    pub batch_builder: &'a mut DocumentsBatchBuilder<W>,
    pub object_buffer: &'a mut Object,
}

impl<'a, 'de, W: Write> Visitor<'de> for &mut DocumentVisitor<'a, W> {
    /// This Visitor value is nothing, since it write the value to a file.
    type Value = Result<(), Error>;

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(v) = seq.next_element_seed(&mut *self)? {
            // This happens if the element was deserialized correctly,
            // but an IO error happened when we tried to insert it into
            // the batch builder
            if let Err(e) = v {
                return Ok(Err(e.into()));
            }
        }

        Ok(Ok(()))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some((key, value)) = map.next_entry()? {
            self.object_buffer.insert(key, value);
        }
        if let Err(e) = self.batch_builder.append_json_object(self.object_buffer) {
            return Ok(Err(e.into()));
        }
        self.object_buffer.clear();

        Ok(Ok(()))
    }

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a document, or a sequence of documents.")
    }
}

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
