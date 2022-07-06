use std::io::BufRead;

use bumpalo::collections::vec::Vec as BumpVec;
use bumpalo::Bump;
use serde::{
    de::{DeserializeSeed, VariantAccess, Visitor},
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Number;

use crate::Object;

#[derive(Clone, Copy)]
pub struct StringBumpSeed<'bump> {
    pub bump: &'bump Bump,
}
#[derive(Clone, Copy)]
pub struct UnsafeStringBumpSeed<'bump> {
    pub bump: &'bump Bump,
}

struct StringVisitor<'bump> {
    bump: &'bump Bump,
}
impl<'de, 'bump> Visitor<'de> for StringVisitor<'bump>
where
    'de: 'bump,
{
    type Value = &'bump str;

    #[inline]
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a string")
    }
    #[inline]
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let s = self.bump.alloc_str(v);
        Ok(s)
    }
    #[inline]
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v)
    }
}

struct UncheckedStringVisitor<'bump> {
    bump: &'bump Bump,
}
impl<'de, 'bump> Visitor<'de> for UncheckedStringVisitor<'bump>
where
    'de: 'bump,
{
    type Value = &'bump [u8];

    #[inline]
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a string")
    }

    #[inline]
    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v)
    }

    #[inline]
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let s = self.bump.alloc_slice_copy(v);
        Ok(s)
    }
}

impl<'de, 'bump> DeserializeSeed<'de> for StringBumpSeed<'bump>
where
    'de: 'bump,
{
    type Value = &'bump str;

    #[inline]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let visitor = StringVisitor { bump: &self.bump };
        let value = deserializer.deserialize_str(visitor)?;
        Ok(value)
    }
}
impl<'de, 'bump> DeserializeSeed<'de> for UnsafeStringBumpSeed<'bump>
where
    'de: 'bump,
{
    type Value = &'bump str;

    #[inline]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let visitor = UncheckedStringVisitor { bump: &self.bump };
        let value = deserializer.deserialize_bytes(visitor)?;
        let value = unsafe { std::str::from_utf8_unchecked(value) };
        Ok(value)
    }
}

#[derive(Debug)]
pub struct Map<'bump>(pub BumpVec<'bump, (&'bump str, MaybeMut<'bump, Value<'bump>>)>);

impl<'bump> Map<'bump> {
    pub fn get(&'bump self, k: &str) -> Option<&'bump Value<'bump>> {
        if let Some(x) = self.0.iter().find(|(k2, _)| k == *k2) {
            let x: &'bump Value<'bump> = x.1.as_ref();
            Some(x)
        } else {
            None
        }
    }
}
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
    #[inline]
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
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::Null => serializer.serialize_unit_variant("Value", 0, "Null"),
            Value::Bool(x) => serializer.serialize_newtype_variant("Value", 1, "Bool", x),
            Value::SignedInteger(x) => {
                serializer.serialize_newtype_variant("Value", 2, "SignedInteger", x)
            }
            Value::UnsignedInteger(x) => {
                serializer.serialize_newtype_variant("Value", 3, "UnsignedInteger", x)
            }
            Value::Float(x) => serializer.serialize_newtype_variant("Value", 4, "Float", x),
            Value::String(x) => {
                serializer.serialize_newtype_variant("Value", 5, "String", x.as_bytes())
            }
            Value::Sequence(x) => serializer.serialize_newtype_variant(
                "Value",
                6,
                "Sequence",
                &SerializableSequence(x),
            ),
            Value::Map(x) => serializer.serialize_newtype_variant("Value", 7, "Map", x),
        }
    }
}
struct SerializableSequence<'a, 'bump>(&'a Seq<'bump>);

impl<'a, 'bump> Serialize for SerializableSequence<'a, 'bump> {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seqser = serializer.serialize_seq(Some(self.0.len()))?;
        for x in self.0.iter() {
            let x = x.as_ref();
            seqser.serialize_element(x)?;
        }
        seqser.end()
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
pub struct ValueEnumBumpSeed<'bump> {
    pub bump: &'bump Bump,
}

impl<'de, 'bump> DeserializeSeed<'de> for ValueEnumBumpSeed<'bump>
where
    'de: 'bump,
{
    type Value = Value<'bump>;

    #[inline]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let visitor = ValueVisitor { bump: &self.bump };
        let value = deserializer.deserialize_enum(
            "Value",
            &[
                "Null",
                "Bool",
                "SignedInteger",
                "UnsignedInteger",
                "Float",
                "String",
                "Sequence",
                "Map",
            ],
            visitor,
        )?;
        Ok(value)
    }
}

#[derive(Clone, Copy)]
pub struct ValueJsonBumpSeed<'bump> {
    pub bump: &'bump Bump,
}

impl<'de, 'bump> DeserializeSeed<'de> for ValueJsonBumpSeed<'bump>
where
    'de: 'bump,
{
    type Value = Value<'bump>;

    #[inline]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let visitor = ValueVisitor { bump: &self.bump };
        let value = deserializer.deserialize_any(visitor)?;
        Ok(value)
    }
}

struct ValueVisitor<'bump> {
    bump: &'bump Bump,
}

enum ValueKind {
    Null,
    Bool,
    SignedInteger,
    UnsignedInteger,
    Float,
    String,
    Sequence,
    Map,
}
struct ValueKindVisitor;
impl<'de> Visitor<'de> for ValueKindVisitor {
    type Value = ValueKind;
    #[inline]
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "variant identifier")
    }
    #[inline]
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match v {
            0 => Ok(ValueKind::Null),
            1 => Ok(ValueKind::Bool),
            2 => Ok(ValueKind::SignedInteger),
            3 => Ok(ValueKind::UnsignedInteger),
            4 => Ok(ValueKind::Float),
            5 => Ok(ValueKind::String),
            6 => Ok(ValueKind::Sequence),
            7 => Ok(ValueKind::Map),
            x => Err(E::invalid_value(
                serde::de::Unexpected::Unsigned(x),
                &"variant index 0 <= i < 8",
            )),
        }
    }
}
impl<'de> Deserialize<'de> for ValueKind {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(ValueKindVisitor)
    }
}

impl<'de, 'bump> Visitor<'de> for ValueVisitor<'bump>
where
    'de: 'bump,
{
    type Value = Value<'bump>;

    #[inline]
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a valid Json value")
    }

    #[inline]
    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::EnumAccess<'de>,
    {
        let (kind, variant) = data.variant::<ValueKind>()?;
        match kind {
            ValueKind::Null => {
                variant.unit_variant()?;
                Ok(Value::Null)
            }
            ValueKind::Bool => {
                let x = variant.newtype_variant::<bool>()?;
                Ok(Value::Bool(x))
            }
            ValueKind::SignedInteger => {
                let x = variant.newtype_variant::<i64>()?;
                Ok(Value::SignedInteger(x))
            }
            ValueKind::UnsignedInteger => {
                let x = variant.newtype_variant::<u64>()?;
                Ok(Value::UnsignedInteger(x))
            }
            ValueKind::Float => {
                let x = variant.newtype_variant::<f64>()?;
                Ok(Value::Float(x))
            }
            ValueKind::String => {
                let x = variant.newtype_variant_seed(UnsafeStringBumpSeed { bump: &self.bump })?;
                Ok(Value::String(x))
            }
            ValueKind::Sequence => {
                let x = variant.newtype_variant_seed(SeqEnumVisitor { bump: &self.bump })?;
                Ok(Value::Sequence(x))
            }
            ValueKind::Map => {
                let x = variant.newtype_variant_seed(MapEnumVisitor { bump: &self.bump })?;
                Ok(Value::Map(x))
            }
        }
    }

    #[inline]
    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Null)
    }
    #[inline]
    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Null)
    }
    #[inline]
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Bool(v))
    }
    #[inline]
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Float(v))
    }

    #[inline]
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::UnsignedInteger(v))
    }

    #[inline]
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::SignedInteger(v))
    }

    #[inline]
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let s: &'bump str = self.bump.alloc_str(v);
        Ok(Value::String(s))
    }
    #[inline]
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::String(v))
    }

    #[inline]
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let seed = ValueJsonBumpSeed { bump: &self.bump };
        let mut vector = BumpVec::with_capacity_in(seq.size_hint().unwrap_or(16), &self.bump);
        while let Some(value) = seq.next_element_seed(seed)? {
            vector.push(MaybeMut::Ref(self.bump.alloc(value)));
        }
        Ok(Value::Sequence(vector))
    }
    #[inline]
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let key_seed = StringBumpSeed { bump: &self.bump };
        let value_seed = ValueJsonBumpSeed { bump: &self.bump };
        let mut vector = BumpVec::with_capacity_in(map.size_hint().unwrap_or(16), &self.bump);
        while let Some((key, value)) = map.next_entry_seed(key_seed, value_seed)? {
            vector.push((key, MaybeMut::Ref(self.bump.alloc(value))));
        }
        Ok(Value::Map(Map(vector)))
    }
}

struct SeqEnumVisitor<'bump> {
    bump: &'bump Bump,
}
impl<'de, 'bump> Visitor<'de> for SeqEnumVisitor<'bump>
where
    'de: 'bump,
{
    type Value = BumpVec<'bump, MaybeMut<'bump, Value<'bump>>>;

    #[inline]
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a valid Json array")
    }

    #[inline]
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let seed = ValueEnumBumpSeed { bump: &self.bump };
        let mut vector = BumpVec::with_capacity_in(seq.size_hint().unwrap_or(16), &self.bump);
        while let Some(value) = seq.next_element_seed(seed)? {
            vector.push(MaybeMut::Ref(self.bump.alloc(value)));
        }
        Ok(vector)
    }
}
impl<'bump, 'de> DeserializeSeed<'de> for SeqEnumVisitor<'bump>
where
    'de: 'bump,
{
    type Value = BumpVec<'bump, MaybeMut<'bump, Value<'bump>>>;

    #[inline]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}
struct MapEnumVisitor<'bump> {
    bump: &'bump Bump,
}
impl<'de, 'bump> Visitor<'de> for MapEnumVisitor<'bump>
where
    'de: 'bump,
{
    type Value = Map<'bump>;

    #[inline]
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a valid Json object")
    }

    #[inline]
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let key_seed = UnsafeStringBumpSeed { bump: &self.bump };
        let value_seed = ValueEnumBumpSeed { bump: &self.bump };
        let mut vector = BumpVec::with_capacity_in(map.size_hint().unwrap_or(16), &self.bump);
        while let Some((key, value)) = map.next_entry_seed(key_seed, value_seed)? {
            vector.push((key, MaybeMut::Ref(self.bump.alloc(value))));
        }
        Ok(Map(vector))
    }
}
impl<'bump, 'de> DeserializeSeed<'de> for MapEnumVisitor<'bump>
where
    'de: 'bump,
{
    type Value = Map<'bump>;

    #[inline]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

pub struct MapJsonVisitor<'bump> {
    pub bump: &'bump Bump,
}
impl<'de, 'bump> Visitor<'de> for MapJsonVisitor<'bump>
where
    'de: 'bump,
{
    type Value = Map<'bump>;

    #[inline]
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a valid Json object")
    }

    #[inline]
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let key_seed = StringBumpSeed { bump: &self.bump };
        let value_seed = ValueJsonBumpSeed { bump: &self.bump };
        let mut vector = BumpVec::with_capacity_in(map.size_hint().unwrap_or(16), &self.bump);
        while let Some((key, value)) = map.next_entry_seed(key_seed, value_seed)? {
            vector.push((key, MaybeMut::Ref(self.bump.alloc(value))));
        }
        Ok(Map(vector))
    }
}
impl<'bump, 'de> DeserializeSeed<'de> for MapJsonVisitor<'bump>
where
    'de: 'bump,
{
    type Value = Map<'bump>;

    #[inline]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

pub fn deserialize_map<'bump>(
    reader: impl BufRead,
    bump: &'bump Bump,
) -> Result<Map<'bump>, bincode::Error> {
    let mut de = bincode::Deserializer::with_reader(reader, bincode::DefaultOptions::default());
    let visitor = MapEnumVisitor { bump };

    de.deserialize_map(visitor)
}

pub fn deserialize_map_slice<'r: 'bump, 'bump>(
    s: &'r [u8],
    bump: &'bump Bump,
) -> Result<Map<'bump>, bincode::Error> {
    let mut de = bincode::Deserializer::from_slice(s, bincode::DefaultOptions::default());
    let visitor = MapEnumVisitor { bump };
    de.deserialize_map(visitor)
}

pub fn deserialize_bincode_slice<'s: 'bump, 'bump>(
    s: &'s [u8],
    bump: &'bump Bump,
) -> Result<Value<'bump>, bincode::Error> {
    let mut de = bincode::Deserializer::from_slice(s, bincode::DefaultOptions::default());

    ValueEnumBumpSeed { bump }.deserialize(&mut de)
}

pub fn serialize_bincode<'bump, W>(j: &Value<'bump>, writer: W) -> Result<(), bincode::Error>
where
    W: std::io::Write,
{
    let mut serializer = bincode::Serializer::new(writer, bincode::DefaultOptions::default());
    j.serialize(&mut serializer)
}
pub fn serialize_map<'bump, W>(j: &Map<'bump>, writer: W) -> Result<(), bincode::Error>
where
    W: std::io::Write,
{
    let mut serializer = bincode::Serializer::new(writer, bincode::DefaultOptions::default());
    j.serialize(&mut serializer)
}

fn can_be_flattened<'bump>(json: &'bump Map<'bump>) -> bool {
    for (_, value) in json.0.iter() {
        match value.as_ref() {
            Value::Map(_) => return true,
            Value::Sequence(vs) => {
                for v in vs.iter() {
                    match v.as_ref() {
                        Value::Map(_) | Value::Sequence(_) => return true,
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    false
}

pub fn flatten<'bump>(json: &'bump Map<'bump>, bump: &'bump Bump) -> (bool, &'bump Map<'bump>) {
    if can_be_flattened(json) {
        let object = bump.alloc(Map(BumpVec::new_in(bump)));
        insert_object(object, None, json, bump);
        (true, object)
    } else {
        (false, json)
    }
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

impl<'bump> From<&'bump Value<'bump>> for serde_json::Value {
    #[inline]
    fn from(v: &'bump Value<'bump>) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(x) => serde_json::Value::Bool(*x),
            Value::SignedInteger(x) => serde_json::Value::Number(Number::from(*x)),
            Value::UnsignedInteger(x) => serde_json::Value::Number(Number::from(*x)),
            Value::Float(x) => Number::from_f64(*x)
                .map(serde_json::Value::Number)
                .unwrap_or_else(|| serde_json::Value::String(x.to_string())),
            Value::String(x) => serde_json::Value::String(x.to_string()),
            Value::Sequence(xs) => {
                let mut vec = Vec::new();
                for x in xs {
                    vec.push(x.as_ref().into());
                }
                serde_json::Value::Array(vec)
            }
            Value::Map(x) => serde_json::Value::Object(x.into()),
        }
    }
}
impl<'bump> From<&'bump Map<'bump>> for Object {
    #[inline]
    fn from(map: &'bump Map<'bump>) -> Self {
        let mut object = Object::new();
        for (key, value) in map.0.iter() {
            object.insert(key.to_string(), value.as_ref().into());
        }
        object
    }
}

impl<'bump> std::fmt::Display for Value<'bump> {
    #[inline]
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

impl<'bump> Value<'bump> {
    pub fn from(j: &serde_json::Value, bump: &'bump Bump) -> Self {
        match j {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(x) => Self::Bool(*x),
            serde_json::Value::Number(x) => {
                if let Some(x) = x.as_u64() {
                    Self::UnsignedInteger(x)
                } else if let Some(x) = x.as_i64() {
                    Self::SignedInteger(x)
                } else if let Some(x) = x.as_f64() {
                    Self::Float(x)
                } else {
                    panic!()
                }
            }
            serde_json::Value::String(x) => Self::String(bump.alloc_str(x.as_str())),
            serde_json::Value::Array(x) => {
                let mut vec = BumpVec::new_in(bump);
                for x in x {
                    vec.push(MaybeMut::Ref(bump.alloc(Value::from(x, bump))));
                }
                Self::Sequence(vec)
            }
            serde_json::Value::Object(x) => Self::Map(Map::from(x, bump)),
        }
    }
}
impl<'bump> Map<'bump> {
    pub fn from(obj: &crate::Object, bump: &'bump Bump) -> Self {
        let mut map = BumpVec::new_in(bump);
        for (key, v) in obj {
            map.push((
                bump.alloc_str(key.as_str()) as &_,
                MaybeMut::Ref(bump.alloc(Value::from(v, bump))),
            ));
        }
        Map(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bumpalo::Bump;
    use std::mem::size_of;

    #[inline]
    fn deserialize_bump_json<'r, 'bump>(
        reader: impl serde_json::de::Read<'r>,
        bump: &'bump Bump,
    ) -> Result<Value<'bump>, serde_json::Error> {
        let mut de = serde_json::de::Deserializer::new(reader);

        ValueEnumBumpSeed { bump }.deserialize(&mut de)
    }

    #[inline]
    fn deserialize_json_str<'s, 'bump>(
        s: &'s str,
        bump: &'bump Bump,
    ) -> Result<Value<'bump>, serde_json::Error> {
        let reader = serde_json::de::StrRead::new(s);
        let mut de = serde_json::de::Deserializer::new(reader);

        ValueEnumBumpSeed { bump }.deserialize(&mut de)
    }

    #[test]
    #[inline]
    fn size_types() {
        let size = size_of::<Value<'_>>();
        println!("{size}");

        let size = size_of::<&'_ (&'_ str, Value<'_>)>();
        println!("{size}");
    }

    #[test]
    #[inline]
    fn deser() {
        let bump = Bump::new();
        let j = serde_json::json!({
            "a.b": [1, 2],
            "a.b": "k",
            "a": {
                "b": "c",
                "d": "e",
                "f": "g",
                "b": "h"
            }
        });
        let j = j.as_object().unwrap();
        let j = Map::from(j, &bump);

        let f = flatten(bump.alloc(j), &bump);
        let f = std::mem::replace(&mut f.0, BumpVec::with_capacity_in(0, &bump));
        println!("{}", Value::Map(Map(f)));
    }
}
