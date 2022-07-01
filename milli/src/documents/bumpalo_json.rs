use std::{alloc::GlobalAlloc, fmt::Write, io::BufWriter};

use bumpalo::collections::vec::Vec as BumpVec;
use bumpalo::Bump;
use serde::{
    de::{DeserializeSeed, Visitor},
    ser::{SerializeMap, SerializeSeq},
    Deserializer, Serialize,
};

// pub struct BumpMap<'bump> {
//     inner: BumpVec<'bump, &'bump mut (&'bump str, Value<'bump>)>,
// }
// impl<'bump> BumpMap<'bump> {
//     pub fn is_empty(&self) -> bool {
//         self.inner.is_empty()
//     }
//     pub fn len(&self) -> usize {
//         self.inner.len()
//     }
//     pub fn iter(&self) -> impl Iterator<Item = &(&'bump str, Value<'bump>)> {
//         self.inner.iter().map(|x| &**x)
//     }
// }

#[derive(Clone, Copy)]
struct StringBumpSeed<'bump> {
    bump: &'bump Bump,
}

struct StringVisitor<'bump> {
    bump: &'bump Bump,
}
impl<'de, 'bump> Visitor<'de> for StringVisitor<'bump> {
    type Value = &'bump str;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a string")
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let s = self.bump.alloc_str(v);
        Ok(s)
    }
}

impl<'de, 'bump> DeserializeSeed<'de> for StringBumpSeed<'bump> {
    type Value = &'bump str;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let visitor = StringVisitor { bump: &self.bump };
        let value = deserializer.deserialize_str(visitor)?;
        Ok(value)
    }
}

#[derive(Debug)]
pub struct Map<'bump>(pub BumpVec<'bump, (&'bump str, MaybeMut<'bump, Value<'bump>>)>);

// pub type Map<'bump> = BumpVec<'bump, (&'bump str, MaybeMut<'bump, Value<'bump>>)>;
pub type Seq<'bump> = BumpVec<'bump, MaybeMut<'bump, Value<'bump>>>;

#[derive(Debug, Default)]
pub enum Value<'bump> {
    #[default]
    Null, //
    Bool(bool),
    SignedInteger(i64),
    UnsignedInteger(u64),
    Float(f64),
    String(&'bump str),
    Sequence(Seq<'bump>),
    Map(Map<'bump>),
}
impl<'bump> Serialize for Map<'bump> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seqmap = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0.iter() {
            seqmap.serialize_entry(key, value.as_ref())?;
        }
        seqmap.end()
    }
}
impl<'bump> Serialize for Value<'bump> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::Null => serializer.serialize_str("null"),
            Value::Bool(x) => serializer.serialize_bool(*x),
            Value::SignedInteger(x) => serializer.serialize_i64(*x),
            Value::UnsignedInteger(x) => serializer.serialize_u64(*x),
            Value::Float(x) => serializer.serialize_f64(*x),
            Value::String(s) => serializer.serialize_str(s),
            Value::Sequence(xs) => {
                let mut seqser = serializer.serialize_seq(Some(xs.len()))?;
                for x in xs {
                    seqser.serialize_element(x.as_ref())?;
                }
                seqser.end()
            }
            Value::Map(kvs) => kvs.serialize(serializer),
        }
    }
}

impl<'bump> std::fmt::Display for Value<'bump> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(x) => write!(f, "{x}"),
            Value::SignedInteger(x) => write!(f, "{x}"),
            Value::UnsignedInteger(x) => write!(f, "{x}"),
            Value::Float(x) => write!(f, "{x}"),
            Value::String(s) => write!(f, "{s:?}"),
            Value::Sequence(xs) => {
                write!(f, "[")?;
                let mut iter = xs.into_iter();
                if let Some(x) = iter.next() {
                    let x = x.as_ref();
                    write!(f, "{x}")?;
                }
                while let Some(x) = iter.next() {
                    let x = x.as_ref();
                    write!(f, ",{x}")?;
                }
                write!(f, "]")?;
                Ok(())
            }
            Value::Map(kvs) => {
                write!(f, "{{")?;
                let mut iter = kvs.0.iter();
                if let Some((k, v)) = iter.next() {
                    let v = v.as_ref();
                    write!(f, "{k:?}:{v}")?;
                }
                while let Some((k, v)) = iter.next() {
                    let v = v.as_ref();
                    write!(f, ",{k:?}:{v}")?;
                }
                write!(f, "}}")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub enum MaybeMut<'bump, T> {
    Ref(&'bump T),
    Mut(&'bump mut T),
}
impl<'bump, T> MaybeMut<'bump, T> {
    pub fn as_ref(&'bump self) -> &'bump T {
        match self {
            MaybeMut::Ref(x) => x,
            MaybeMut::Mut(x) => x,
        }
    }
}

#[derive(Clone, Copy)]
struct JsonValueBumpSeed<'bump> {
    bump: &'bump Bump,
}

impl<'de, 'bump> DeserializeSeed<'de> for JsonValueBumpSeed<'bump> {
    type Value = Value<'bump>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let visitor = JsonVisitor { bump: &self.bump };
        let value = deserializer.deserialize_any(visitor)?;
        Ok(value)
    }
}

struct JsonVisitor<'bump> {
    bump: &'bump Bump,
}

impl<'de, 'bump> Visitor<'de> for JsonVisitor<'bump> {
    type Value = Value<'bump>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a valid Json value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Null)
    }
    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Bool(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Float(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::UnsignedInteger(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::SignedInteger(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let s: &'bump str = self.bump.alloc_str(v);
        let s: &'bump &'bump str = self.bump.alloc(s);
        Ok(Value::String(s))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let seed = JsonValueBumpSeed { bump: &self.bump };
        let mut vector = BumpVec::with_capacity_in(16, &self.bump);
        while let Some(value) = seq.next_element_seed(seed)? {
            vector.push(MaybeMut::Ref(self.bump.alloc(value)));
        }
        Ok(Value::Sequence(vector))
    }
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let key_seed = StringBumpSeed { bump: &self.bump };
        let value_seed = JsonValueBumpSeed { bump: &self.bump };
        let mut vector = BumpVec::with_capacity_in(16, &self.bump);
        while let Some((key, value)) = map.next_entry_seed(key_seed, value_seed)? {
            vector.push((key, MaybeMut::Ref(self.bump.alloc(value))));
        }
        Ok(Value::Map(Map(vector)))
    }
}

struct JsonMapVisitor<'bump> {
    bump: &'bump Bump,
}
impl<'de, 'bump> Visitor<'de> for JsonMapVisitor<'bump> {
    type Value = Map<'bump>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a valid Json object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let key_seed = StringBumpSeed { bump: &self.bump };
        let value_seed = JsonValueBumpSeed { bump: &self.bump };
        let mut vector = BumpVec::with_capacity_in(16, &self.bump);
        while let Some((key, value)) = map.next_entry_seed(key_seed, value_seed)? {
            vector.push((key, MaybeMut::Ref(self.bump.alloc(value))));
        }
        Ok(Map(vector))
    }
}

pub fn deserialize_map<'r, 'bump>(
    reader: impl serde_json::de::Read<'r>,
    bump: &'bump Bump,
) -> Result<Map<'bump>, serde_json::Error> {
    let mut de = serde_json::de::Deserializer::new(reader);
    let visitor = JsonMapVisitor { bump };
    de.deserialize_any(visitor)
}

pub fn deserialize_bump_json<'r, 'bump>(
    reader: impl serde_json::de::Read<'r>,
    bump: &'bump Bump,
) -> Result<Value<'bump>, serde_json::Error> {
    let mut de = serde_json::de::Deserializer::new(reader);
    let visitor = JsonVisitor { bump };
    de.deserialize_any(visitor)
}

pub fn deserialize_json_str<'s, 'bump>(
    s: &'s str,
    bump: &'bump Bump,
) -> Result<Value<'bump>, serde_json::Error> {
    let reader = serde_json::de::StrRead::new(s);
    let mut de = serde_json::de::Deserializer::new(reader);
    let visitor = JsonVisitor { bump };
    de.deserialize_any(visitor)
}

pub fn deserialize_json_slice<'s, 'bump>(
    s: &'s [u8],
    bump: &'bump Bump,
) -> Result<Value<'bump>, serde_json::Error> {
    let reader = serde_json::de::SliceRead::new(s);
    let mut de = serde_json::de::Deserializer::new(reader);
    let visitor = JsonVisitor { bump };
    de.deserialize_any(visitor)
}

pub fn serialize_json<'bump, W>(j: &Value<'bump>, writer: W) -> Result<(), serde_json::Error>
where
    W: std::io::Write,
{
    let mut serializer = serde_json::ser::Serializer::new(writer);
    j.serialize(&mut serializer)
}
pub fn serialize_map<'bump, W>(j: &Map<'bump>, writer: W) -> Result<(), serde_json::Error>
where
    W: std::io::Write,
{
    let mut serializer = serde_json::ser::Serializer::new(writer);
    j.serialize(&mut serializer)
}

pub fn flatten<'bump>(json: &'bump Map<'bump>, bump: &'bump Bump) -> &'bump mut Map<'bump> {
    let object = bump.alloc(Map(BumpVec::new_in(bump)));

    insert_object(object, None, json, bump);

    object
}

fn insert_object<'bump>(
    base_json: &mut Map<'bump>,
    base_key: Option<&str>,
    object: &'bump Map<'bump>,
    bump: &'bump Bump,
) {
    for (key, value) in object.0.iter() {
        let new_key: &'bump str = base_key.map_or_else(
            || bump.alloc_str(key) as &_,
            |base_key| {
                let mut new_key = bumpalo::collections::String::with_capacity_in(
                    base_key.len() + key.len() + 2,
                    bump,
                );
                new_key.push_str(base_key);
                new_key.push('.');
                new_key.push_str(key);
                new_key.into_bump_str()
            },
        );
        match value.as_ref() {
            Value::Sequence(seq) => {
                insert_array(base_json, &new_key, seq, bump);
            }
            Value::Map(map) => {
                insert_object(base_json, Some(&new_key), map, bump);
            }
            value => insert_value(base_json, &new_key, value, bump),
        }

        // if let Some(array) = value.as_array() {
        //     insert_array(base_json, &new_key, array);
        // } else if let Some(object) = value.as_object() {
        //     insert_object(base_json, Some(&new_key), object);
        // } else {
        //     insert_value(base_json, &new_key, value.clone());
        // }
    }
}

fn insert_array<'bump>(
    base_json: &mut Map<'bump>,
    base_key: &'bump str,
    array: &'bump Seq<'bump>,
    bump: &'bump Bump,
) {
    for value in array {
        let value = value.as_ref();
        match value {
            Value::Map(map) => {
                insert_object(base_json, Some(base_key), map, bump);
            }
            Value::Sequence(array) => {
                insert_array(base_json, base_key, array, bump);
            }
            value => {
                insert_value(base_json, base_key, value, bump);
            }
        }
    }
}

fn insert_value<'bump>(
    base_json: &mut Map<'bump>,
    key: &'bump str,
    to_insert: &'bump Value<'bump>,
    bump: &'bump Bump,
) {
    // debug_assert!(!to_insert.is_object());
    // debug_assert!(!to_insert.is_array());

    // does the field already exists?
    if let Some(index) = base_json.0.iter_mut().position(|k| k.0 == key) {
        let (_, value) = &mut base_json.0[index];
        let replace_by = match value {
            MaybeMut::Mut(value) => {
                match value {
                    // is it already an array
                    Value::Sequence(array) => {
                        array.push(MaybeMut::Ref(to_insert));
                    }
                    // or is there a collision
                    value => {
                        let new_value: &mut Value<'bump> = value;
                        let old_value =
                            bump.alloc(Value::Sequence(BumpVec::with_capacity_in(8, bump)));
                        std::mem::swap(old_value, new_value);
                        match new_value {
                            Value::Sequence(array) => {
                                array.push(MaybeMut::Ref(old_value as &'bump _));
                                array.push(MaybeMut::Ref(to_insert));
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                return;
            }
            MaybeMut::Ref(Value::Sequence(_) | Value::Map(_)) => unreachable!(),
            MaybeMut::Ref(value) => {
                let old_value: &'bump Value<'bump> = value;
                let new_value = bump.alloc(Value::Sequence(BumpVec::with_capacity_in(8, bump)));
                match new_value {
                    Value::Sequence(array) => {
                        array.push(MaybeMut::Ref(old_value));
                        array.push(MaybeMut::Ref(to_insert));
                    }
                    _ => unreachable!(),
                }
                MaybeMut::Mut(new_value)
            }
        };
        *value = replace_by;
    } else {
        // if it does not exist we can push the value untouched
        let new = (key, MaybeMut::Ref(to_insert));
        base_json.0.push(new);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bumpalo::Bump;
    use std::mem::size_of;

    #[test]
    fn size_types() {
        let size = size_of::<Value<'_>>();
        println!("{size}");

        let size = size_of::<&'_ (&'_ str, Value<'_>)>();
        println!("{size}");
    }

    #[test]
    fn deser() {
        let bump = Bump::new();
        let s = r#"{
            "a.b": [1, 2],
            "a.b": "k",
            "a": {
                "b": "c",
                "d": "e",
                "f": "g",
                "b": "h"
            }
        }"#;
        let j = deserialize_json_str(s, &bump).unwrap();
        let j = match j {
            Value::Map(kvs) => kvs,
            _ => panic!(),
        };
        let f = flatten(bump.alloc(j), &bump);
        let f = std::mem::replace(&mut f.0, BumpVec::with_capacity_in(0, &bump));
        println!("{}", Value::Map(Map(f)));
    }
}
