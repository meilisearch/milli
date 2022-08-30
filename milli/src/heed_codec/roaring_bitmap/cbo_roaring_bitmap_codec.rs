use std::borrow::Cow;
use std::io;
use std::mem::size_of;

use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use roaring::{MultiOps, RoaringBitmap};

/// This is the limit where using a byteorder became less size efficient
/// than using a direct roaring encoding, it is also the point where we are able
/// to determine the encoding used only by using the array of bytes length.
pub const THRESHOLD: usize = 7;

/// A conditionnal codec that either use the RoaringBitmap
/// or a lighter ByteOrder en/decoding method.
pub struct CboRoaringBitmapCodec;

impl CboRoaringBitmapCodec {
    pub fn serialized_size(roaring: &RoaringBitmap) -> usize {
        if roaring.len() <= THRESHOLD as u64 {
            roaring.len() as usize * size_of::<u32>()
        } else {
            roaring.serialized_size()
        }
    }

    pub fn serialize_into(roaring: &RoaringBitmap, vec: &mut Vec<u8>) {
        if roaring.len() <= THRESHOLD as u64 {
            // If the number of items (u32s) to encode is less than or equal to the threshold
            // it means that it would weigh the same or less than the RoaringBitmap
            // header, so we directly encode them using ByteOrder instead.
            for integer in roaring {
                vec.write_u32::<NativeEndian>(integer).unwrap();
            }
        } else {
            // Otherwise, we use the classic RoaringBitmapCodec that writes a header.
            roaring.serialize_into(vec).unwrap();
        }
    }

    pub fn deserialize_from(mut bytes: &[u8]) -> io::Result<RoaringBitmap> {
        if bytes.len() <= THRESHOLD * size_of::<u32>() {
            // If there is threshold or less than threshold integers that can fit into this array
            // of bytes it means that we used the ByteOrder codec serializer.
            let mut bitmap = RoaringBitmap::new();
            while let Ok(integer) = bytes.read_u32::<NativeEndian>() {
                bitmap.insert(integer);
            }
            Ok(bitmap)
        } else {
            // Otherwise, it means we used the classic RoaringBitmapCodec and
            // that the header takes threshold integers.
            RoaringBitmap::deserialize_from(bytes)
        }
    }

    /// Merge serialized CboRoaringBitmaps in a buffer.
    /// The buffer MUST BE empty.
    ///
    /// if the merged values length is under the threshold, values are directly
    /// serialized in the buffer else a RoaringBitmap is created from the
    /// values and is serialized in the buffer.
    pub fn merge_into(slices: &[Cow<[u8]>], buffer: &mut Vec<u8>) -> io::Result<()> {
        debug_assert!(buffer.len() == 0);

        let bitmaps = slices
            .iter()
            .filter_map(|slice| {
                if slice.len() <= THRESHOLD * size_of::<u32>() {
                    buffer.extend(slice.as_ref());
                    None
                } else {
                    RoaringBitmap::deserialize_from(slice.as_ref()).into()
                }
            })
            .collect::<io::Result<Vec<_>>>()?;

        let u32_buffer: &mut Vec<u32> = unsafe { convert_vec(buffer) };
        u32_buffer.sort_unstable();
        u32_buffer.dedup();

        if bitmaps.is_empty() {
            if u32_buffer.len() > THRESHOLD {
                // We can unwrap safely because the vector is sorted above.
                let roaring = RoaringBitmap::from_sorted_iter(u32_buffer.iter().copied()).unwrap();

                let buffer: &mut Vec<u8> = unsafe { convert_vec(u32_buffer) };
                buffer.clear();
                roaring.serialize_into(buffer)?;
            } else {
                // we still need to fix the size of the buffer
                let _buffer: &mut Vec<u8> = unsafe { convert_vec(u32_buffer) };
            }
        } else {
            let bitmap = RoaringBitmap::from_sorted_iter(u32_buffer.iter().copied()).unwrap();
            let buffer: &mut Vec<u8> = unsafe { convert_vec(u32_buffer) };
            let bitmap = bitmaps.into_iter().chain(std::iter::once(bitmap)).union();
            buffer.clear();
            bitmap.serialize_into(buffer)?;
        }

        Ok(())
    }
}

/// Convert a `Vec` of `T` into a `Vec` of `U` by keeping the same allocation and
/// only updating the size of the `Vec`.
/// To make this works `size_of::<T>() * input.len() % size_of::<U>()` must be equal to zero.
unsafe fn convert_vec<T, U>(input: &mut Vec<T>) -> &mut Vec<U> {
    debug_assert!(
        size_of::<T>() * input.len() % size_of::<U>() == 0,
        "called with incompatible types"
    );

    let new_len = size_of::<T>() * input.len() / size_of::<U>();

    let ret: &mut Vec<U> = std::mem::transmute(input);
    ret.set_len(new_len);

    ret
}

impl heed::BytesDecode<'_> for CboRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        Self::deserialize_from(bytes).ok()
    }
}

impl heed::BytesEncode<'_> for CboRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let mut vec = Vec::with_capacity(Self::serialized_size(item));
        Self::serialize_into(item, &mut vec);
        Some(Cow::Owned(vec))
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use heed::{BytesDecode, BytesEncode};

    use super::*;

    #[test]
    fn verify_encoding_decoding() {
        let input = RoaringBitmap::from_iter(0..THRESHOLD as u32);
        let bytes = CboRoaringBitmapCodec::bytes_encode(&input).unwrap();
        let output = CboRoaringBitmapCodec::bytes_decode(&bytes).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn verify_threshold() {
        let input = RoaringBitmap::from_iter(0..THRESHOLD as u32);

        // use roaring bitmap
        let mut bytes = Vec::new();
        input.serialize_into(&mut bytes).unwrap();
        let roaring_size = bytes.len();

        // use byteorder directly
        let mut bytes = Vec::new();
        for integer in input {
            bytes.write_u32::<NativeEndian>(integer).unwrap();
        }
        let bo_size = bytes.len();

        assert!(roaring_size > bo_size);
    }

    #[test]
    fn merge_cbo_roaring_bitmaps() {
        let mut buffer = Vec::new();

        let small_data = vec![
            RoaringBitmap::from_sorted_iter(1..4).unwrap(),
            RoaringBitmap::from_sorted_iter(2..5).unwrap(),
            RoaringBitmap::from_sorted_iter(4..6).unwrap(),
            RoaringBitmap::from_sorted_iter(1..3).unwrap(),
        ];

        let small_data: Vec<_> =
            small_data.iter().map(|b| CboRoaringBitmapCodec::bytes_encode(b).unwrap()).collect();
        CboRoaringBitmapCodec::merge_into(small_data.as_slice(), &mut buffer).unwrap();
        let bitmap = CboRoaringBitmapCodec::deserialize_from(&buffer).unwrap();
        let expected = RoaringBitmap::from_sorted_iter(1..6).unwrap();
        assert_eq!(bitmap, expected);

        let medium_data = vec![
            RoaringBitmap::from_sorted_iter(1..4).unwrap(),
            RoaringBitmap::from_sorted_iter(2..5).unwrap(),
            RoaringBitmap::from_sorted_iter(4..8).unwrap(),
            RoaringBitmap::from_sorted_iter(0..3).unwrap(),
            RoaringBitmap::from_sorted_iter(7..23).unwrap(),
        ];

        let medium_data: Vec<_> =
            medium_data.iter().map(|b| CboRoaringBitmapCodec::bytes_encode(b).unwrap()).collect();
        buffer.clear();
        CboRoaringBitmapCodec::merge_into(medium_data.as_slice(), &mut buffer).unwrap();

        let bitmap = CboRoaringBitmapCodec::deserialize_from(&buffer).unwrap();
        let expected = RoaringBitmap::from_sorted_iter(0..23).unwrap();
        assert_eq!(bitmap, expected);
    }

    #[cfg(feature = "nightly")]
    mod bench {
        extern crate test;
        use test::Bencher;

        #[bench]
        fn bench_small_merge_cbo_roaring_bitmaps(bencher: &mut Bencher) {
            #[rustfmt::skip]
        let inputs = [
            vec![Cow::Owned(vec![255, 56, 14, 0]),  Cow::Owned(vec![196, 43, 14, 0])],
            vec![Cow::Owned(vec![63, 101, 3, 0]),   Cow::Owned(vec![71, 136, 3, 0])],
            vec![Cow::Owned(vec![68, 108, 0, 0]),   Cow::Owned(vec![85, 104, 0, 0]), Cow::Owned(vec![204, 103, 0, 0])],
            vec![Cow::Owned(vec![199, 101, 7, 0]),  Cow::Owned(vec![94, 42, 7, 0])],
            vec![Cow::Owned(vec![173, 219, 12, 0]), Cow::Owned(vec![146, 3, 13, 0])],
            vec![Cow::Owned(vec![13, 152, 3, 0]),   Cow::Owned(vec![64, 120, 3, 0])],
            vec![Cow::Owned(vec![109, 253, 13, 0]), Cow::Owned(vec![108, 232, 13, 0])],
            vec![Cow::Owned(vec![73, 176, 3, 0]),   Cow::Owned(vec![126, 167, 3, 0])],
        ];

            let mut vec = Vec::new();
            for input in inputs {
                bencher.iter(|| CboRoaringBitmapCodec::merge_into(&input, &mut vec));
                vec.clear();
            }
        }

        #[bench]
        fn bench_medium_merge_cbo_roaring_bitmaps(bencher: &mut Bencher) {
            #[rustfmt::skip]
        let inputs = [
            vec![Cow::Owned(vec![232, 35, 9, 0]), Cow::Owned(vec![192, 10, 9, 0]), Cow::Owned(vec![91, 33, 9, 0]), Cow::Owned(vec![204, 29, 9, 0])],
            vec![Cow::Owned(vec![144, 39, 9, 0]), Cow::Owned(vec![162, 66, 9, 0]), Cow::Owned(vec![146, 11, 9, 0]), Cow::Owned(vec![174, 61, 9, 0])],
            vec![Cow::Owned(vec![83, 70, 7, 0]), Cow::Owned(vec![115, 72, 7, 0]), Cow::Owned(vec![219, 54, 7, 0]), Cow::Owned(vec![1, 93, 7, 0]), Cow::Owned(vec![195, 77, 7, 0]), Cow::Owned(vec![21, 86, 7, 0])],
            vec![Cow::Owned(vec![244, 112, 0, 0]), Cow::Owned(vec![48, 126, 0, 0]), Cow::Owned(vec![72, 142, 0, 0]), Cow::Owned(vec![255, 113, 0, 0]), Cow::Owned(vec![101, 114, 0, 0]), Cow::Owned(vec![66, 88, 0, 0]), Cow::Owned(vec![84, 92, 0, 0]), Cow::Owned(vec![194, 137, 0, 0]), Cow::Owned(vec![208, 132, 0, 0])],
            vec![Cow::Owned(vec![8, 57, 7, 0]), Cow::Owned(vec![133, 115, 7, 0]), Cow::Owned(vec![219, 94, 7, 0]), Cow::Owned(vec![46, 95, 7, 0]), Cow::Owned(vec![156, 111, 7, 0]), Cow::Owned(vec![63, 107, 7, 0]), Cow::Owned(vec![31, 47, 7, 0])],
            vec![Cow::Owned(vec![165, 78, 0, 0]), Cow::Owned(vec![197, 95, 0, 0]), Cow::Owned(vec![194, 82, 0, 0]), Cow::Owned(vec![142, 91, 0, 0]), Cow::Owned(vec![120, 94, 0, 0])],
            vec![Cow::Owned(vec![185, 187, 13, 0]), Cow::Owned(vec![41, 187, 13, 0]), Cow::Owned(vec![245, 223, 13, 0]), Cow::Owned(vec![211, 251, 13, 0]), Cow::Owned(vec![192, 193, 13, 0]), Cow::Owned(vec![215, 230, 13, 0]), Cow::Owned(vec![252, 207, 13, 0]), Cow::Owned(vec![131, 213, 13, 0]), Cow::Owned(vec![219, 187, 13, 0]), Cow::Owned(vec![105, 236, 13, 0]), Cow::Owned(vec![30, 239, 13, 0]), Cow::Owned(vec![13, 200, 13, 0]), Cow::Owned(vec![111, 197, 13, 0]), Cow::Owned(vec![87, 222, 13, 0]), Cow::Owned(vec![7, 205, 13, 0]), Cow::Owned(vec![90, 211, 13, 0])],
            vec![Cow::Owned(vec![215, 253, 13, 0]), Cow::Owned(vec![225, 194, 13, 0]), Cow::Owned(vec![37, 189, 13, 0]), Cow::Owned(vec![242, 212, 13, 0])],
        ];

            let mut vec = Vec::new();
            for input in inputs {
                bencher.iter(|| CboRoaringBitmapCodec::merge_into(&input, &mut vec));
                vec.clear();
            }
        }
    }
}
