use std::borrow::Cow;
use croaring::Bitmap;

pub struct RoaringBitmapCodec;

impl heed::BytesDecode<'_> for RoaringBitmapCodec {
    type DItem = Bitmap;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        Bitmap::try_deserialize(bytes)
    }
}

impl heed::BytesEncode<'_> for RoaringBitmapCodec {
    type EItem = Bitmap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Owned(item.serialize()))
    }
}
