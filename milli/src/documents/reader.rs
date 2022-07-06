use std::convert::TryInto;
use std::{error, fmt, io};

use bumpalo::Bump;

use super::bumpalo_json::{self, Map};
use super::Error;
use crate::Object;

/// The `DocumentsBatchReader` provides a way to iterate over documents that have been created with
/// a `DocumentsBatchWriter`.
///
/// The documents are returned in the form of `obkv::Reader` where each field is identified with a
/// `FieldId`. The mapping between the field ids and the field names is done thanks to the index.
pub struct DocumentsBatchReader<R> {
    cursor: grenad::ReaderCursor<R>,
}

impl<R: io::Read + io::Seek> DocumentsBatchReader<R> {
    /// Construct a `DocumentsReader` from a reader.
    ///
    /// It first retrieves the index, then moves to the first document. Use the `into_cursor`
    /// method to iterator over the documents, from the first to the last.
    pub fn from_reader(reader: R) -> Result<Self, Error> {
        let reader = grenad::Reader::new(reader)?;
        let cursor = reader.into_cursor()?;

        Ok(DocumentsBatchReader { cursor })
    }

    pub fn documents_count(&self) -> u32 {
        self.cursor.len().try_into().expect("Invalid number of documents")
    }

    pub fn is_empty(&self) -> bool {
        self.cursor.len() == 0
    }

    /// This method returns a forward cursor over the documents.
    pub fn into_cursor(self) -> DocumentsBatchCursor<R> {
        let DocumentsBatchReader { cursor } = self;
        let mut cursor = DocumentsBatchCursor { cursor };
        cursor.reset();
        cursor
    }
}

/// A forward cursor over the documents in a `DocumentsBatchReader`.
pub struct DocumentsBatchCursor<R> {
    cursor: grenad::ReaderCursor<R>,
}

impl<R> DocumentsBatchCursor<R> {
    pub fn into_reader(self) -> DocumentsBatchReader<R> {
        let DocumentsBatchCursor { cursor } = self;
        DocumentsBatchReader { cursor }
    }

    /// Resets the cursor to be able to read from the start again.
    pub fn reset(&mut self) {
        self.cursor.reset();
    }
}

impl<R: io::Read + io::Seek> DocumentsBatchCursor<R> {
    // /// Returns the next document, starting from the first one. Subsequent calls to
    // /// `next_document` advance the document reader until all the documents have been read.
    // pub fn next_document(&mut self) -> Result<Option<Object>, DocumentsBatchCursorError> {
    //     match self.cursor.move_on_next()? {
    //         Some((_, value)) => {
    //             let json = bincode::deserialize(value)?;
    //             Ok(Some(json))
    //         }
    //         None => Ok(None),
    //     }
    // }

    pub fn next_bump_document<'bump>(
        &mut self,
        bump: &'bump Bump,
    ) -> Result<Option<Map<'bump>>, DocumentsBatchCursorError> {
        match self.cursor.move_on_next()? {
            Some((_, value)) => {
                let json = bumpalo_json::deserialize_map(value, bump)?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }
}

/// The possible error thrown by the `DocumentsBatchCursor` when iterating on the documents.
#[derive(Debug)]
pub enum DocumentsBatchCursorError {
    Grenad(grenad::Error),
    SerdeJson(serde_json::Error),
    Bincode(bincode::Error),
}

impl From<grenad::Error> for DocumentsBatchCursorError {
    fn from(error: grenad::Error) -> DocumentsBatchCursorError {
        DocumentsBatchCursorError::Grenad(error)
    }
}

impl From<serde_json::Error> for DocumentsBatchCursorError {
    fn from(error: serde_json::Error) -> DocumentsBatchCursorError {
        DocumentsBatchCursorError::SerdeJson(error)
    }
}
impl From<bincode::Error> for DocumentsBatchCursorError {
    fn from(error: bincode::Error) -> DocumentsBatchCursorError {
        DocumentsBatchCursorError::Bincode(error)
    }
}

impl error::Error for DocumentsBatchCursorError {}

impl fmt::Display for DocumentsBatchCursorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DocumentsBatchCursorError::Grenad(e) => e.fmt(f),
            DocumentsBatchCursorError::SerdeJson(e) => e.fmt(f),
            DocumentsBatchCursorError::Bincode(e) => e.fmt(f),
        }
    }
}
