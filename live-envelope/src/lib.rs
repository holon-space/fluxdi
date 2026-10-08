//! The observation envelope: what an observer sees of a value that is
//! produced or kept up to date elsewhere.
//!
//! Producers start on their first observer. Whether a state can change again
//! is up to the producer: a service may finish for good, while a query result
//! may move from `Ready` back to `Partial` when its sources change.

/// The state of an observed value. More states may be added, so a match
/// needs a wildcard arm:
///
/// ```compile_fail,E0004
/// use live_envelope::Observed;
///
/// fn label(state: &Observed<u8, ()>) -> &'static str {
///     match state {
///         Observed::Pending => "pending",
///         Observed::Partial(_) => "partial",
///         Observed::Ready(_) => "ready",
///         Observed::Failed(_) => "failed",
///     }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Observed<V, E> {
    /// Nothing usable yet.
    Pending,
    /// A usable value that does not claim to be complete.
    Partial(V),
    /// A complete value.
    Ready(V),
    /// The producer failed; there is no value to show.
    Failed(E),
}

/// Whether an observed value claims to be complete.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Completeness {
    Partial,
    Complete,
}

/// Which production of an observed value a state belongs to. A producer
/// that starts over publishes the next generation, so an observer holding a
/// value can tell whether a newer one exists. Generations of different
/// observed values are unrelated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(u64);

impl Generation {
    pub const FIRST: Self = Self(1);

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// A value together with the generation it belongs to. Fields may be added,
/// so it is built with [`Generational::new`]:
///
/// ```compile_fail,E0639
/// use live_envelope::{Generation, Generational};
///
/// let tagged = Generational {
///     generation: Generation::FIRST,
///     value: 7,
/// };
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Generational<V> {
    pub generation: Generation,
    pub value: V,
}

impl<V> Generational<V> {
    pub fn new(generation: Generation, value: V) -> Self {
        Self { generation, value }
    }
}

impl<V, E> Observed<V, E> {
    /// The value of a `Partial` or `Ready` state.
    pub fn value(&self) -> Option<&V> {
        match self {
            Self::Partial(value) | Self::Ready(value) => Some(value),
            Self::Pending | Self::Failed(_) => None,
        }
    }

    /// The completeness of the value; `None` when there is no value.
    pub fn completeness(&self) -> Option<Completeness> {
        match self {
            Self::Partial(_) => Some(Completeness::Partial),
            Self::Ready(_) => Some(Completeness::Complete),
            Self::Pending | Self::Failed(_) => None,
        }
    }

    pub fn error(&self) -> Option<&E> {
        match self {
            Self::Failed(error) => Some(error),
            Self::Pending | Self::Partial(_) | Self::Ready(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_values_have_a_completeness() {
        let states: [Observed<u8, &str>; 4] = [
            Observed::Pending,
            Observed::Partial(1),
            Observed::Ready(2),
            Observed::Failed("down"),
        ];
        let seen: Vec<_> = states
            .iter()
            .map(|state| {
                (
                    state.value().copied(),
                    state.completeness(),
                    state.error().copied(),
                )
            })
            .collect();
        assert_eq!(
            seen,
            vec![
                (None, None, None),
                (Some(1), Some(Completeness::Partial), None),
                (Some(2), Some(Completeness::Complete), None),
                (None, None, Some("down")),
            ]
        );
    }
}
