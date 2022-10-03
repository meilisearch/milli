use super::Context;
use crate::{
    absolute_from_relative_position, update::index_documents::helpers::MAX_WORD_LENGTH, Result,
    SerializationError,
};
use charabia::{SeparatorKind, Token, TokenKind, Tokenizer};
use std::convert::TryInto;

// TODO: make it clear that it returns absolute positions
pub fn tokenize(
    ctx: &Context,
    field_id: u16,
    field: &str,
    tokenizer: &Tokenizer<&[u8]>,
    mut cb: impl FnMut(u32, &[u8]) -> Result<()>,
) -> Result<()> {
    let tokens = process_tokens(tokenizer.tokenize(field))
        .take_while(|(p, _)| (*p as u32) < ctx.max_positions_per_attributes);
    for (index, token) in tokens {
        let token = token.lemma().trim();
        if !token.is_empty() && token.len() <= MAX_WORD_LENGTH {
            let token_bytes = token.as_bytes();

            let position: u16 =
                index.try_into().map_err(|_| SerializationError::InvalidNumberSerialization)?;
            let position = absolute_from_relative_position(field_id, position);
            cb(position, token_bytes)?;
        }
    }
    Ok(())
}

/// take an iterator on tokens and compute their relative position depending on separator kinds
/// if it's an `Hard` separator we add an additional relative proximity of 8 between words,
/// else we keep the standart proximity of 1 between words.
fn process_tokens<'a>(
    tokens: impl Iterator<Item = Token<'a>>,
) -> impl Iterator<Item = (usize, Token<'a>)> {
    tokens
        .skip_while(|token| token.is_separator())
        .scan((0, None), |(offset, prev_kind), token| {
            match token.kind {
                TokenKind::Word | TokenKind::StopWord | TokenKind::Unknown => {
                    *offset += match *prev_kind {
                        Some(TokenKind::Separator(SeparatorKind::Hard)) => 8,
                        Some(_) => 1,
                        None => 0,
                    };
                    *prev_kind = Some(token.kind)
                }
                TokenKind::Separator(SeparatorKind::Hard) => {
                    *prev_kind = Some(token.kind);
                }
                TokenKind::Separator(SeparatorKind::Soft)
                    if *prev_kind != Some(TokenKind::Separator(SeparatorKind::Hard)) =>
                {
                    *prev_kind = Some(token.kind);
                }
                _ => (),
            }
            Some((*offset, token))
        })
        .filter(|(_, t)| t.is_word())
}
