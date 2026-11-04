use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Category {
    Input,
    Worker,
    Write,
}
impl Category {
    pub(crate) fn name(self) -> &'static str {
        match self { Self::Input => "input", Self::Worker => "worker", Self::Write => "write" }
    }
    pub(crate) fn exit(self) -> i32 {
        match self { Self::Input => 3, Self::Worker => 5, Self::Write => 4 }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reason {
    Ordinary,
    SyntheticCancellation,
    ApiCancellation,
    PairIdentifiers,
    PairCount,
    NoEligibleMotif,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    pub(crate) category: Category,
    pub(crate) stage: &'static str,
    pub(crate) ordinal: Option<u64>,
    pub(crate) lane: Option<usize>,
    pub(crate) reason: Reason,
    pub(crate) message: String,
}
impl Failure {
    pub(crate) fn new(category: Category, stage: &'static str, message: impl Into<String>) -> Self {
        Self { category, stage, ordinal: None, lane: None, reason: Reason::Ordinary, message: message.into() }
    }
    pub(crate) fn input(message: impl Into<String>) -> Self { Self::new(Category::Input, "input", message) }
    pub(crate) fn worker(message: impl Into<String>) -> Self { Self::new(Category::Worker, "worker", message) }
    pub(crate) fn write(message: impl Into<String>) -> Self { Self::new(Category::Write, "write", message) }
    pub(crate) fn at(mut self, ordinal: u64) -> Self { self.ordinal = Some(ordinal); self }
    pub(crate) fn because(mut self, reason: Reason) -> Self { self.reason = reason; self }
    pub(crate) fn with_message(mut self, message: impl Into<String>) -> Self { self.message = message.into(); self }
    pub(crate) fn synthetic_cancel(message: impl Into<String>) -> Self {
        Self::input(message).because(Reason::SyntheticCancellation)
    }
    pub(crate) fn process_message(&self) -> String {
        let prefix = match self.reason {
            Reason::PairIdentifiers => "pair.identifier_mismatch: ",
            Reason::PairCount => "pair.unequal_eof: ",
            _ => "",
        };
        format!("{prefix}{}", self.message)
    }
    #[cfg(test)]
    pub(crate) fn contains(&self, text: &str) -> bool { self.message.contains(text) }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.message.fmt(f) }
}
impl std::error::Error for Failure {}
impl From<Failure> for String {
    fn from(value: Failure) -> Self { value.message }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_words_do_not_control_failure_facts() {
        for (failure, name, code) in [
            (Failure::input("opaque"), "input", 3),
            (Failure::worker("opaque"), "worker", 5),
            (Failure::write("opaque"), "write", 4),
        ] {
            let changed = failure.clone().with_message("FASTQ quality [input] [worker] panicked");
            assert_eq!(changed.category, failure.category);
            assert_eq!((changed.category.name(), changed.category.exit()), (name, code));
            assert_eq!((changed.stage, changed.ordinal, changed.reason), (failure.stage, failure.ordinal, failure.reason));
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceError(pub(crate) Failure);
impl SourceError {
    pub(crate) fn worker(message: impl Into<String>) -> Self { Self(Failure::worker(message)) }
    pub(crate) fn synthetic_cancel(message: impl Into<String>) -> Self { Self(Failure::synthetic_cancel(message)) }
    pub(crate) fn api_cancelled() -> Self {
        // Preserve the legacy Process report class; the library stage is cancelled.
        Self(Failure::new(Category::Write,"cancelled","API cancelled").because(Reason::ApiCancellation))
    }
    pub(crate) fn with_message(mut self, message: impl Into<String>) -> Self { self.0.message = message.into(); self }
    #[cfg(test)]
    pub(crate) fn contains(&self, text: &str) -> bool { self.0.message.contains(text) }
}
impl From<String> for SourceError {
    fn from(message: String) -> Self { Self(Failure::input(message)) }
}
impl From<&str> for SourceError {
    fn from(message: &str) -> Self { message.to_owned().into() }
}
impl From<SourceError> for Failure {
    fn from(error: SourceError) -> Self { error.0 }
}
impl From<SourceError> for String {
    fn from(error: SourceError) -> Self { error.0.message }
}
impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
}

#[derive(Clone, Debug)]
pub(crate) struct OutputError(pub(crate) Failure);
impl OutputError {
    pub(crate) fn worker(message: impl Into<String>) -> Self { Self(Failure::worker(message)) }
    #[cfg(test)]
    pub(crate) fn contains(&self, text: &str) -> bool { self.0.message.contains(text) }
}
impl From<String> for OutputError {
    fn from(message: String) -> Self { Self(Failure::write(message)) }
}
impl From<&str> for OutputError {
    fn from(message: &str) -> Self { message.to_owned().into() }
}
impl From<OutputError> for Failure {
    fn from(error: OutputError) -> Self { error.0 }
}
impl From<OutputError> for String {
    fn from(error: OutputError) -> Self { error.0.message }
}
impl fmt::Display for OutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
}

#[derive(Clone, Debug)]
pub(crate) struct BudgetError(pub(crate) Failure);
impl BudgetError {
    pub(crate) fn cancelled(message: impl Into<String>) -> Self { Self(Failure::synthetic_cancel(message)) }
    #[cfg(test)]
    pub(crate) fn contains(&self, text: &str) -> bool { self.0.message.contains(text) }
}
impl From<String> for BudgetError {
    fn from(message: String) -> Self { Self(Failure::worker(message)) }
}
impl From<&str> for BudgetError {
    fn from(message: &str) -> Self { message.to_owned().into() }
}
impl From<BudgetError> for Failure {
    fn from(error: BudgetError) -> Self { error.0 }
}
impl From<BudgetError> for SourceError {
    fn from(error: BudgetError) -> Self { Self(error.0) }
}
impl From<BudgetError> for OutputError {
    fn from(error: BudgetError) -> Self { Self(error.0) }
}
impl fmt::Display for BudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
}

impl From<Failure> for SourceError { fn from(error: Failure) -> Self { Self(error) } }

#[derive(Clone, Debug)]
pub(crate) struct UnitError {
    pub ordinal: u64,
    pub stage: &'static str,
    pub message: String,
    pub code: i32,
}
impl UnitError {
    pub(crate) fn failure(&self, message: String) -> Failure {
        let category=match self.code {3=>Category::Input,4=>Category::Write,_=>Category::Worker};
        Failure::new(category,self.stage,message).at(self.ordinal)
    }
}
