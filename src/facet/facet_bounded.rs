pub trait FacetBounded {
    fn min_value() -> Self;
    fn max_value() -> Self;
}

impl FacetBounded for f64 {
    fn min_value() -> Self {
        f64::MIN
    }

    fn max_value() -> Self {
        f64::MAX
    }
}

impl FacetBounded for i64 {
    fn min_value() -> Self {
        i64::MIN
    }

    fn max_value() -> Self {
        i64::MAX
    }
}

impl FacetBounded for &'_ str {
    fn min_value() -> Self {
        ""
    }

    fn max_value() -> Self {
        // It is 62 times the \u{10ffff} character (length of 4 bytes)
        // which is the biggest utf8 char and gives a string that is 248 bytes long.
        "\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}"
    }
}
