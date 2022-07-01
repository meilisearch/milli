use std::fs::File;
use std::{fmt, io, str};

use bumpalo::Bump;

use crate::documents::{
    bumpalo_json, DocumentsBatchCursor, DocumentsBatchCursorError, DocumentsBatchReader,
};
use crate::update::DocumentId;
use crate::Object;

#[derive(Debug, Clone, Copy)]
pub struct InvalidEnrichedData;
impl fmt::Display for InvalidEnrichedData {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Invalid enriched data")
    }
}
impl std::error::Error for InvalidEnrichedData {}

/// The `EnrichedDocumentsBatchReader` provides a way to iterate over the enriched documents created
/// by the [`enrich_documents_batch`](crate::update::index_documents::enrich_documents_batch) function.
///
/// Call [`self.into_cursor()`](Self::into_cursor) to iterate over the enriched documents and access
/// other information stored in `self`.
pub struct EnrichedDocumentsBatchReader<R> {
    documents: DocumentsBatchReader<R>,
    primary_key: String,
    external_ids: grenad::ReaderCursor<File>,
}

impl<R: io::Read + io::Seek> EnrichedDocumentsBatchReader<R> {
    pub fn new(
        documents: DocumentsBatchReader<R>,
        primary_key: String,
        external_ids: grenad::Reader<File>,
    ) -> Result<Self, crate::error::Error> {
        if documents.documents_count() as u64 == external_ids.len() {
            Ok(EnrichedDocumentsBatchReader {
                documents,
                primary_key,
                external_ids: external_ids.into_cursor()?,
            })
        } else {
            Err(InvalidEnrichedData.into())
        }
    }

    /// This method returns a forward cursor over the enriched documents.
    pub fn into_cursor(self) -> EnrichedDocumentsBatchCursor<R> {
        let EnrichedDocumentsBatchReader { documents, primary_key, mut external_ids } = self;
        external_ids.reset();
        EnrichedDocumentsBatchCursor {
            documents: documents.into_cursor(),
            primary_key,
            external_ids,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnrichedDocument {
    pub document: Object,
    pub document_id: DocumentId,
}

pub struct EnrichedDocumentsBatchCursor<R> {
    documents: DocumentsBatchCursor<R>,
    primary_key: String,
    external_ids: grenad::ReaderCursor<File>,
}

impl<R> EnrichedDocumentsBatchCursor<R> {
    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }
}

impl<R: io::Read + io::Seek> EnrichedDocumentsBatchCursor<R> {
    /// Returns the next document, starting from the first one. Subsequent calls to
    /// `next_document` advance the document reader until all the documents have been read.
    pub fn next_enriched_document(
        &mut self,
    ) -> Result<Option<EnrichedDocument>, DocumentsBatchCursorError> {
        let document = self.documents.next_document()?;
        let document_id = match self.external_ids.move_on_next()? {
            Some((_, bytes)) => serde_json::from_slice(bytes).map(Some)?,
            None => None,
        };

        match document.zip(document_id) {
            Some((document, document_id)) => Ok(Some(EnrichedDocument { document, document_id })),
            None => Ok(None),
        }
    }
    /// Returns the next document, starting from the first one. Subsequent calls to
    /// `next_document` advance the document reader until all the documents have been read.
    pub fn next_enriched_bump_document<'bump>(
        &mut self,
        bump: &'bump Bump,
    ) -> Result<Option<EnrichedBumpDocument<'bump>>, DocumentsBatchCursorError> {
        let document = self.documents.next_bump_document(bump)?;
        let document_id = match self.external_ids.move_on_next()? {
            Some((_, bytes)) => serde_json::from_slice(bytes).map(Some)?,
            None => None,
        };

        match document.zip(document_id) {
            Some((document, document_id)) => {
                let document = bump.alloc(document);
                Ok(Some(EnrichedBumpDocument { document, document_id }))
            }
            None => Ok(None),
        }
    }
}
#[derive(Debug)]
pub struct EnrichedBumpDocument<'bump> {
    pub document: &'bump mut bumpalo_json::Map<'bump>,
    pub document_id: DocumentId,
}
