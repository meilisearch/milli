use roaring::RoaringBitmap;

use crate::search::query_tree::Operation;

use super::{Criterion, CriterionParameters, CriterionResult};

pub struct Initial {
    answer: Option<CriterionResult>,
}

impl Initial {
    pub fn new(
        query_tree: Option<Operation>,
        filtered_candidates: Option<RoaringBitmap>,
    ) -> Initial {
        let answer = CriterionResult {
            query_tree,
            candidates: None,
            filtered_candidates,
            bucket_candidates: None,
        };
        Initial { answer: Some(answer) }
    }
}

impl Criterion for Initial {
    #[logging_timer::time("Initial::{}")]
    fn next(&mut self, _: &mut CriterionParameters) -> anyhow::Result<Option<CriterionResult>> {
        Ok(self.answer.take())
    }
}
