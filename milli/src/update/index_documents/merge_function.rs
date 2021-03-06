use std::borrow::Cow;
use std::result::Result as StdResult;

use fst::IntoStreamer;
use heed::{BytesDecode, BytesEncode};
use roaring::RoaringBitmap;

use crate::error::SerializationError;
use crate::heed_codec::facet::FacetStringLevelZeroValueCodec;
use crate::heed_codec::CboRoaringBitmapCodec;
use crate::Result;

/// Only the last value associated with an id is kept.
pub fn keep_latest_obkv(_key: &[u8], obkvs: &[Cow<[u8]>]) -> Result<Vec<u8>> {
    Ok(obkvs.last().unwrap().clone().into_owned())
}

/// Merge all the obks in the order we see them.
pub fn merge_obkvs(_key: &[u8], obkvs: &[Cow<[u8]>]) -> Result<Vec<u8>> {
    let mut iter = obkvs.iter();
    let first = iter.next().map(|b| b.clone().into_owned()).unwrap();
    Ok(iter.fold(first, |acc, current| {
        let first = obkv::KvReader::new(&acc);
        let second = obkv::KvReader::new(current);
        let mut buffer = Vec::new();
        merge_two_obkvs(first, second, &mut buffer);
        buffer
    }))
}

// Union of multiple FSTs
pub fn fst_merge(_key: &[u8], values: &[Cow<[u8]>]) -> Result<Vec<u8>> {
    let fsts = values.iter().map(fst::Set::new).collect::<StdResult<Vec<_>, _>>()?;
    let op_builder: fst::set::OpBuilder = fsts.iter().map(|fst| fst.into_stream()).collect();
    let op = op_builder.r#union();

    let mut build = fst::SetBuilder::memory();
    build.extend_stream(op.into_stream()).unwrap();
    Ok(build.into_inner().unwrap())
}

pub fn keep_first(_key: &[u8], values: &[Cow<[u8]>]) -> Result<Vec<u8>> {
    Ok(values.first().unwrap().to_vec())
}

pub fn merge_two_obkvs(base: obkv::KvReaderU16, update: obkv::KvReaderU16, buffer: &mut Vec<u8>) {
    use itertools::merge_join_by;
    use itertools::EitherOrBoth::{Both, Left, Right};

    buffer.clear();

    let mut writer = obkv::KvWriter::new(buffer);
    for eob in merge_join_by(base.iter(), update.iter(), |(b, _), (u, _)| b.cmp(u)) {
        match eob {
            Both(_, (k, v)) | Left((k, v)) | Right((k, v)) => writer.insert(k, v).unwrap(),
        }
    }

    writer.finish().unwrap();
}

pub fn roaring_bitmap_merge(_key: &[u8], values: &[Cow<[u8]>]) -> Result<Vec<u8>> {
    let (head, tail) = values.split_first().unwrap();
    let mut head = RoaringBitmap::deserialize_from(&head[..])?;

    for value in tail {
        head |= RoaringBitmap::deserialize_from(&value[..])?;
    }

    let mut vec = Vec::with_capacity(head.serialized_size());
    head.serialize_into(&mut vec)?;
    Ok(vec)
}

/// Uses the FacetStringLevelZeroValueCodec to merge the values.
pub fn tuple_string_cbo_roaring_bitmap_merge(_key: &[u8], values: &[Cow<[u8]>]) -> Result<Vec<u8>> {
    let (head, tail) = values.split_first().unwrap();
    let (head_string, mut head_rb) =
        FacetStringLevelZeroValueCodec::<CboRoaringBitmapCodec>::bytes_decode(&head[..])
            .ok_or(SerializationError::Decoding { db_name: None })?;

    for value in tail {
        let (_string, rb) =
            FacetStringLevelZeroValueCodec::<CboRoaringBitmapCodec>::bytes_decode(&value[..])
                .ok_or(SerializationError::Decoding { db_name: None })?;
        head_rb |= rb;
    }

    FacetStringLevelZeroValueCodec::<CboRoaringBitmapCodec>::bytes_encode(&(head_string, head_rb))
        .map(|cow| cow.into_owned())
        .ok_or(SerializationError::Encoding { db_name: None })
        .map_err(Into::into)
}

pub fn cbo_roaring_bitmap_merge(_key: &[u8], values: &[Cow<[u8]>]) -> Result<Vec<u8>> {
    let (head, tail) = values.split_first().unwrap();
    let mut head = CboRoaringBitmapCodec::deserialize_from(&head[..])?;

    for value in tail {
        head |= CboRoaringBitmapCodec::deserialize_from(&value[..])?;
    }

    let mut vec = Vec::new();
    CboRoaringBitmapCodec::serialize_into(&head, &mut vec);
    Ok(vec)
}
