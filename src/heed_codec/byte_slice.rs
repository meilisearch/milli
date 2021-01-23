use std::borrow::Cow;

use heed::{BytesDecode, BytesEncode};

pub type UntypedDatabase = heed::Database<ByteSlice, ByteSlice>;

pub struct ByteSlice;

impl BytesEncode for ByteSlice {
    type EItem = [u8];

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(item))
    }
}

impl<'a> BytesDecode<'a> for ByteSlice {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        Some(bytes)
    }
}

unsafe impl Send for ByteSlice {}

unsafe impl Sync for ByteSlice {}
