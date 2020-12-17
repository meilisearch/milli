use std::iter::{Chain, FromIterator};
use std::ops::RangeInclusive;
use std::vec::IntoIter;
use croaring::Bitmap;

pub struct AvailableDocumentsIds {
    iter: Chain<IntoIter<u32>, RangeInclusive<u32>>,
}

impl AvailableDocumentsIds {
    pub fn from_documents_ids(docids: &Bitmap) -> AvailableDocumentsIds {
        match docids.maximum() {
            Some(last_id) => {
                let mut available = Bitmap::from_iter(0..last_id);
                available.andnot_inplace(&docids);

                let iter = match last_id.checked_add(1) {
                    Some(id) => id..=u32::max_value(),
                    None => 1..=0, // empty range iterator
                };

                AvailableDocumentsIds {
                    iter: available.to_vec().into_iter().chain(iter),
                }
            },
            None => AvailableDocumentsIds {
                iter: vec![].into_iter().chain(0..=u32::max_value()),
            },
        }
    }
}

impl Iterator for AvailableDocumentsIds {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let base = Bitmap::create();
        let left = AvailableDocumentsIds::from_documents_ids(&base);
        let right = 0..=u32::max_value();
        left.zip(right).take(500).for_each(|(l, r)| assert_eq!(l, r));
    }

    #[test]
    fn scattered() {
        let mut base = Bitmap::create();
        base.add(0);
        base.add(10);
        base.add(100);
        base.add(405);

        let left = AvailableDocumentsIds::from_documents_ids(&base);
        let right = (0..=u32::max_value()).filter(|&n| n != 0 && n != 10 && n != 100 && n != 405);
        left.zip(right).take(500).for_each(|(l, r)| assert_eq!(l, r));
    }
}
