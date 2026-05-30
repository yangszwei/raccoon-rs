/// Which table owns a given indexed attribute column.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TableId {
    Study,
    Series,
    Instance,
}

impl TableId {
    /// SQL alias used in every compiled query.
    pub(crate) fn alias(self) -> &'static str {
        match self {
            Self::Study => "s",
            Self::Series => "se",
            Self::Instance => "i",
        }
    }
}
