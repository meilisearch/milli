#[macro_use]
pub mod documents;

mod asc_desc;
mod criterion;
mod error;
mod external_documents_ids;
pub mod facet;
mod fields_ids_map;
pub mod heed_codec;
pub mod index;
pub mod proximity;
mod search;
pub mod update;

use std::collections::{BTreeMap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::hash::BuildHasherDefault;

pub use filter_parser::{Condition, FilterCondition};
use fxhash::{FxHasher32, FxHasher64};
pub use grenad::CompressionType;
use serde_json::{Map, Value};
pub use {heed, meilisearch_tokenizer as tokenizer};

pub use self::asc_desc::{AscDesc, AscDescError, Member, SortError};
pub use self::criterion::{default_criteria, Criterion, CriterionError};
pub use self::error::{
    Error, FieldIdMapMissingEntry, InternalError, SerializationError, UserError,
};
pub use self::external_documents_ids::ExternalDocumentsIds;
pub use self::fields_ids_map::FieldsIdsMap;
pub use self::heed_codec::{
    BEU32StrCodec, BoRoaringBitmapCodec, BoRoaringBitmapLenCodec, CboRoaringBitmapCodec,
    CboRoaringBitmapLenCodec, FieldIdWordCountCodec, ObkvCodec, RoaringBitmapCodec,
    RoaringBitmapLenCodec, StrBEU32Codec, StrStrU8Codec,
};
pub use self::index::Index;
pub use self::search::{FacetDistribution, Filter, MatchingWords, Search, SearchResult};

pub use heed;

pub type Result<T> = std::result::Result<T, error::Error>;

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec8<T> = smallvec::SmallVec<[T; 8]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type BEU64 = heed::zerocopy::U64<heed::byteorder::BE>;
pub type Attribute = u32;
pub type DocumentId = u32;
pub type FieldId = u16;
pub type Position = u32;
pub type RelativePosition = u16;
pub type FieldDistribution = BTreeMap<String, u64>;

/// A GeoPoint is a point in cartesian plan, called xyz_point in the code. Its metadata
/// is a tuple composed of 1. the DocumentId of the associated document and 2. the original point
/// expressed in term of latitude and longitude.
pub type GeoPoint = rstar::primitives::GeomWithData<[f64; 3], (DocumentId, [f64; 2])>;

pub const MAX_POSITION_PER_ATTRIBUTE: u32 = u16::MAX as u32 + 1;

// Convert an absolute word position into a relative position.
// Return the field id of the attribute related to the absolute position
// and the relative position in the attribute.
pub fn relative_from_absolute_position(absolute: Position) -> (FieldId, RelativePosition) {
    ((absolute >> 16) as u16, (absolute & 0xFFFF) as u16)
}

// Compute the absolute word position with the field id of the attribute and relative position in the attribute.
pub fn absolute_from_relative_position(field_id: FieldId, relative: RelativePosition) -> Position {
    (field_id as u32) << 16 | (relative as u32)
}

/// Transform a raw obkv store into a JSON Object.
pub fn obkv_to_json(
    displayed_fields: &[FieldId],
    fields_ids_map: &FieldsIdsMap,
    obkv: obkv::KvReaderU16,
) -> Result<Map<String, Value>> {
    displayed_fields
        .iter()
        .copied()
        .flat_map(|id| obkv.get(id).map(|value| (id, value)))
        .map(|(id, value)| {
            let name = fields_ids_map.name(id).ok_or(error::FieldIdMapMissingEntry::FieldId {
                field_id: id,
                process: "obkv_to_json",
            })?;
            let value = serde_json::from_slice(value).map_err(error::InternalError::SerdeJson)?;
            Ok((name.to_owned(), value))
        })
        .collect()
}

/// Transform a JSON value into a string that can be indexed.
pub fn json_to_string(value: &Value) -> Option<String> {
    fn inner(value: &Value, output: &mut String) -> bool {
        use std::fmt::Write;
        match value {
            Value::Null => false,
            Value::Bool(boolean) => write!(output, "{}", boolean).is_ok(),
            Value::Number(number) => write!(output, "{}", number).is_ok(),
            Value::String(string) => write!(output, "{}", string).is_ok(),
            Value::Array(array) => {
                let mut count = 0;
                for value in array {
                    if inner(value, output) {
                        output.push_str(". ");
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            }
            Value::Object(object) => {
                let mut buffer = String::new();
                let mut count = 0;
                for (key, value) in object {
                    buffer.clear();
                    let _ = write!(&mut buffer, "{}: ", key);
                    if inner(value, &mut buffer) {
                        buffer.push_str(". ");
                        // We write the "key: value. " pair only when
                        // we are sure that the value can be written.
                        output.push_str(&buffer);
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            }
        }
    }

    let mut string = String::new();
    if inner(value, &mut string) {
        Some(string)
    } else {
        None
    }
}

/// Divides one slice into two at an index, returns `None` if mid is out of bounds.
fn try_split_at<T>(slice: &[T], mid: usize) -> Option<(&[T], &[T])> {
    if mid <= slice.len() {
        Some(slice.split_at(mid))
    } else {
        None
    }
}

/// Divides one slice into an array and the tail at an index,
/// returns `None` if `N` is out of bounds.
fn try_split_array_at<T, const N: usize>(slice: &[T]) -> Option<([T; N], &[T])>
where
    [T; N]: for<'a> TryFrom<&'a [T]>,
{
    let (head, tail) = try_split_at(slice, N)?;
    let head = head.try_into().ok()?;
    Some((head, tail))
}

/// Return the distance between two points in meters. Each points are composed of two f64,
/// one latitude and one longitude.
pub fn distance_between_two_points(a: &[f64; 2], b: &[f64; 2]) -> f64 {
    let a = geoutils::Location::new(a[0], a[1]);
    let b = geoutils::Location::new(b[0], b[1]);

    a.haversine_distance_to(&b).meters()
}

/// Convert a point expressed in terms of latitude and longitude to a point in the
/// cartesian coordinate expressed in terms of x, y and z.
pub fn lat_lng_to_xyz(coord: &[f64; 2]) -> [f64; 3] {
    let [lat, lng] = coord.map(|f| f.to_radians());
    let x = lat.cos() * lng.cos();
    let y = lat.cos() * lng.sin();
    let z = lat.sin();

    [x, y, z]
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_to_string_object() {
        let value = json!({
            "name": "John Doe",
            "age": 43,
            "not_there": null,
        });

        let string = json_to_string(&value).unwrap();
        assert_eq!(string, "name: John Doe. age: 43. ");
    }

    #[test]
    fn json_to_string_array() {
        let value = json!([
            { "name": "John Doe" },
            43,
            "hello",
            [ "I", "am", "fine" ],
            null,
        ]);

        let string = json_to_string(&value).unwrap();
        // We don't care about having two point (.) after the other as
        // the distance of hard separators is clamped to 8 anyway.
        assert_eq!(string, "name: John Doe. . 43. hello. I. am. fine. . ");
    }

    #[test]
    fn test_relative_position_conversion() {
        assert_eq!((0x0000, 0x0000), relative_from_absolute_position(0x00000000));
        assert_eq!((0x0000, 0xFFFF), relative_from_absolute_position(0x0000FFFF));
        assert_eq!((0xFFFF, 0x0000), relative_from_absolute_position(0xFFFF0000));
        assert_eq!((0xFF00, 0xFF00), relative_from_absolute_position(0xFF00FF00));
        assert_eq!((0xFF00, 0x00FF), relative_from_absolute_position(0xFF0000FF));
        assert_eq!((0x1234, 0x5678), relative_from_absolute_position(0x12345678));
        assert_eq!((0xFFFF, 0xFFFF), relative_from_absolute_position(0xFFFFFFFF));
    }

    #[test]
    fn test_absolute_position_conversion() {
        assert_eq!(0x00000000, absolute_from_relative_position(0x0000, 0x0000));
        assert_eq!(0x0000FFFF, absolute_from_relative_position(0x0000, 0xFFFF));
        assert_eq!(0xFFFF0000, absolute_from_relative_position(0xFFFF, 0x0000));
        assert_eq!(0xFF00FF00, absolute_from_relative_position(0xFF00, 0xFF00));
        assert_eq!(0xFF0000FF, absolute_from_relative_position(0xFF00, 0x00FF));
        assert_eq!(0x12345678, absolute_from_relative_position(0x1234, 0x5678));
        assert_eq!(0xFFFFFFFF, absolute_from_relative_position(0xFFFF, 0xFFFF));
    }
}
