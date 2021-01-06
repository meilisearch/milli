use std::cmp;
use std::fs::File;
use std::num::NonZeroUsize;

use grenad::{CompressionType, Reader, Writer, FileFuse};
use heed::types::{ByteSlice, DecodeIgnore};
use heed::{BytesEncode, Error};
use log::debug;
use roaring::RoaringBitmap;

use crate::facet::{FacetType, FacetBounded};
use crate::heed_codec::CboRoaringBitmapCodec;
use crate::heed_codec::facet::{FacetLevelValueStrCodec, FacetLevelValueI64Codec, FacetLevelValueF64Codec};
use crate::Index;
use crate::update::index_documents::WriteMethod;
use crate::update::index_documents::{create_writer, writer_into_reader, write_into_lmdb_database};

pub struct Facets<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) chunk_fusing_shrink_size: Option<u64>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
}

impl<'t, 'u, 'i> Facets<'t, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'i, 'u>, index: &'i Index) -> Facets<'t, 'u, 'i> {
        Facets {
            wtxn,
            index,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            chunk_fusing_shrink_size: None,
            level_group_size: NonZeroUsize::new(4).unwrap(),
            min_level_size: NonZeroUsize::new(5).unwrap(),
        }
    }

    pub fn level_group_size(&mut self, value: NonZeroUsize) -> &mut Self {
        self.level_group_size = NonZeroUsize::new(cmp::max(value.get(), 2)).unwrap();
        self
    }

    pub fn min_level_size(&mut self, value: NonZeroUsize) -> &mut Self {
        self.min_level_size = value;
        self
    }

    pub fn execute(self) -> anyhow::Result<()> {
        // We get the faceted fields to be able to create the facet levels.
        let faceted_fields = self.index.faceted_fields(self.wtxn)?;

        debug!("Computing and writing the facet values levels docids into LMDB on disk...");
        for (field_id, facet_type) in faceted_fields {

            // TODO create a function to reduce the amount of code here,
            //      I tried to do it but ha a some problems with lifetimes.
            let faceted_documents_ids = match facet_type {
                FacetType::String => {
                    let db = self.index.facet_field_id_str_docids;

                    clear_field_levels::<&str, FacetLevelValueStrCodec>(
                        self.wtxn,
                        db,
                        field_id,
                    )?;

                    let documents_ids = compute_faceted_documents_ids(
                        self.wtxn,
                        db.remap_key_type::<ByteSlice>(),
                        field_id,
                    )?;

                    let content = compute_facet_levels::<&str, FacetLevelValueStrCodec>(
                        self.wtxn,
                        db,
                        self.chunk_compression_type,
                        self.chunk_compression_level,
                        self.chunk_fusing_shrink_size,
                        self.level_group_size,
                        self.min_level_size,
                        field_id,
                    )?;

                    write_into_lmdb_database(
                        self.wtxn,
                        db.remap_types::<ByteSlice, ByteSlice>(),
                        content,
                        |_, _| anyhow::bail!("invalid facet level merging"),
                        WriteMethod::GetMergePut,
                    )?;

                    documents_ids
                },
                FacetType::Float => {
                    let db = self.index.facet_field_id_f64_docids;

                    clear_field_levels::<f64, FacetLevelValueF64Codec>(
                        self.wtxn,
                        db,
                        field_id,
                    )?;

                    let documents_ids = compute_faceted_documents_ids(
                        self.wtxn,
                        db.remap_key_type::<ByteSlice>(),
                        field_id,
                    )?;

                    let content = compute_facet_levels::<f64, FacetLevelValueF64Codec>(
                        self.wtxn,
                        db,
                        self.chunk_compression_type,
                        self.chunk_compression_level,
                        self.chunk_fusing_shrink_size,
                        self.level_group_size,
                        self.min_level_size,
                        field_id,
                    )?;

                    write_into_lmdb_database(
                        self.wtxn,
                        db.remap_types::<ByteSlice, ByteSlice>(),
                        content,
                        |_, _| anyhow::bail!("invalid facet level merging"),
                        WriteMethod::GetMergePut,
                    )?;

                    documents_ids
                },
                FacetType::Integer => {
                    let db = self.index.facet_field_id_i64_docids;

                    clear_field_levels::<i64, FacetLevelValueI64Codec>(
                        self.wtxn,
                        db,
                        field_id,
                    )?;

                    let documents_ids = compute_faceted_documents_ids(
                        self.wtxn,
                        db.remap_key_type::<ByteSlice>(),
                        field_id,
                    )?;

                    let content = compute_facet_levels::<i64, FacetLevelValueI64Codec>(
                        self.wtxn,
                        db,
                        self.chunk_compression_type,
                        self.chunk_compression_level,
                        self.chunk_fusing_shrink_size,
                        self.level_group_size,
                        self.min_level_size,
                        field_id,
                    )?;

                    write_into_lmdb_database(
                        self.wtxn,
                        db.remap_types::<ByteSlice, ByteSlice>(),
                        content,
                        |_, _| anyhow::bail!("invalid facet level merging"),
                        WriteMethod::GetMergePut,
                    )?;

                    documents_ids
                },
            };

            self.index.put_faceted_documents_ids(self.wtxn, field_id, &faceted_documents_ids)?;
        }

        Ok(())
    }
}

fn clear_field_levels<'t, T: 't, KC>(
    wtxn: &'t mut heed::RwTxn,
    db: heed::Database<KC, CboRoaringBitmapCodec>,
    field_id: u8,
) -> heed::Result<()>
where
    T: Copy + FacetBounded,
    KC: heed::BytesEncode<EItem = (u8, u8, T, T)>,
    KC: heed::BytesDecode<'t, DItem = (u8, u8, T, T)>,
{
    let left = (field_id, 1, T::min_value(), T::min_value());
    let right = (field_id, u8::MAX, T::max_value(), T::max_value());
    let range = left..=right;
    db.remap_key_type::<KC>().delete_range(wtxn, range).map(drop)
}

fn compute_facet_levels<'t, T: 't, KC>(
    rtxn: &'t heed::RoTxn,
    db: heed::Database<KC, CboRoaringBitmapCodec>,
    compression_type: CompressionType,
    compression_level: Option<u32>,
    shrink_size: Option<u64>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
    field_id: u8,
) -> anyhow::Result<Reader<FileFuse>>
where
    T: Copy + PartialEq + PartialOrd + FacetBounded,
    KC: heed::BytesEncode<EItem = (u8, u8, T, T)>,
    KC: heed::BytesDecode<'t, DItem = (u8, u8, T, T)>,
{
    let first_level_size = db
        .remap_key_type::<ByteSlice>()
        .prefix_iter(rtxn, &&[field_id][..])?
        .remap_types::<DecodeIgnore, DecodeIgnore>()
        .fold(Ok(0usize), |count, result| result.and(count).map(|c| c + 1))?;

    // It is forbidden to keep a cursor and write in a database at the same time with LMDB
    // therefore we write the facet levels entries into a grenad file before transfering them.
    let mut writer = tempfile::tempfile().and_then(|file| {
        create_writer(compression_type, compression_level, file)
    })?;

    let level_0_range = {
        let left = (field_id, 0, T::min_value(), T::min_value());
        let right = (field_id, 0, T::max_value(), T::max_value());
        left..=right
    };

    // Groups sizes are always a power of the original level_group_size and therefore a group
    // always maps groups of the previous level and never splits previous levels groups in half.
    let group_size_iter = (1u8..)
        .map(|l| (l, level_group_size.get().pow(l as u32)))
        .take_while(|(_, s)| first_level_size / *s >= min_level_size.get());

    for (level, group_size) in group_size_iter {
        let mut left = T::min_value();
        let mut right = T::min_value();
        let mut group_docids = RoaringBitmap::new();

        let db = db.remap_key_type::<KC>();
        for (i, result) in db.range(rtxn, level_0_range.clone())?.enumerate() {
            let ((_field_id, _level, value, _right), docids) = result?;

            if i == 0 {
                left = value;
            } else if i % group_size == 0 {
                // we found the first bound of the next group, we must store the left
                // and right bounds associated with the docids.
                write_entry::<T, KC>(&mut writer, field_id, level, left, right, &group_docids)?;

                // We save the left bound for the new group and also reset the docids.
                group_docids = RoaringBitmap::new();
                left = value;
            }

            // The right bound is always the bound we run through.
            group_docids.union_with(&docids);
            right = value;
        }

        if !group_docids.is_empty() {
            write_entry::<T, KC>(&mut writer, field_id, level, left, right, &group_docids)?;
        }
    }

    writer_into_reader(writer, shrink_size)
}

fn compute_faceted_documents_ids(
    rtxn: &heed::RoTxn,
    db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
    field_id: u8,
) -> anyhow::Result<RoaringBitmap>
{
    let mut documents_ids = RoaringBitmap::new();
    for result in db.prefix_iter(rtxn, &&[field_id][..])? {
        let (_key, docids) = result?;
        documents_ids.union_with(&docids);
    }
    Ok(documents_ids)
}

fn write_entry<T, KC>(
    writer: &mut Writer<File>,
    field_id: u8,
    level: u8,
    left: T,
    right: T,
    ids: &RoaringBitmap,
) -> anyhow::Result<()>
where
    KC: heed::BytesEncode<EItem = (u8, u8, T, T)>,
{
    let key = (field_id, level, left, right);
    let key = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
    let data = CboRoaringBitmapCodec::bytes_encode(&ids).ok_or(Error::Encoding)?;
    writer.insert(&key, &data)?;
    Ok(())
}
