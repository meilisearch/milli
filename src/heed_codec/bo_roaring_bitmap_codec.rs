use std::borrow::Cow;
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use croaring::Bitmap;

pub struct BoRoaringBitmapCodec;

impl heed::BytesDecode<'_> for BoRoaringBitmapCodec {
    type DItem = Bitmap;

    fn bytes_decode(mut bytes: &[u8]) -> Option<Self::DItem> {
        let mut bitmap = Bitmap::create();
        while let Ok(integer) = bytes.read_u32::<NativeEndian>() {
            bitmap.add(integer);
        }
        Some(bitmap)
    }
}

impl heed::BytesEncode<'_> for BoRoaringBitmapCodec {
    type EItem = Bitmap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::with_capacity(item.cardinality() as usize * 4);
        for integer in item.iter() {
            bytes.write_u32::<NativeEndian>(integer).ok()?;
        }
        Some(Cow::Owned(bytes))
    }
}
