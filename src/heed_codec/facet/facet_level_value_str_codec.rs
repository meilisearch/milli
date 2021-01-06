use std::borrow::Cow;
use std::{str, marker};

use memchr::memchr;

use crate::FieldId;

/// To be sure that keys are compared like the numbers we must pad the left one,
/// we limit the strings to 250 bytes as the LMDB key size max is [511 bytes].
///
/// Padding can be done only for higher levels and not for the first one (level 0),
/// this forces the key length to be at least ~250 bytes for group levels (higher than 0).
///
/// [511 bytes]: http://www.lmdb.tech/doc/group__internal.html#gac929399f5d93cef85f874b9e9b1d09e0
pub struct FacetLevelValueStrCodec<'a>(marker::PhantomData<&'a ()>);

impl<'a> heed::BytesDecode<'a> for FacetLevelValueStrCodec<'_> {
    type DItem = (FieldId, u8, &'a str, &'a str);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (field_id, bytes) = bytes.split_first()?;
        let (level, bytes) = bytes.split_first()?;

        let (left, right) = if *level == 0 {
            let left = str::from_utf8(bytes).ok()?;
            (left, "")
        } else {
            let left_len = memchr(0, bytes).unwrap_or(250);
            let left = str::from_utf8(&bytes[..left_len]).ok()?;
            let right = str::from_utf8(&bytes[250..]).ok()?;
            (left, right)
        };

        Some((*field_id, *level, left, right))
    }
}

impl<'a> heed::BytesEncode for FacetLevelValueStrCodec<'a> {
    type EItem = (FieldId, u8, &'a str, &'a str);

    fn bytes_encode((field_id, level, left, right): &Self::EItem) -> Option<Cow<[u8]>> {
        if left.len() > 250 || right.len() > 250 {
            return None;
        }

        let left_length = if *level == 0 { left.len() } else { 250 };
        let mut bytes = Vec::with_capacity(2 + left_length + right.len());

        bytes.push(*field_id);
        bytes.push(*level);
        bytes.extend_from_slice(left.as_bytes());

        if *level != 0 {
            bytes.resize(250 + 2, 0); // pad with zeroes up to 250 + 2 bytes
            bytes.extend_from_slice(right.as_bytes());
        }

        Some(Cow::Owned(bytes))
    }
}
