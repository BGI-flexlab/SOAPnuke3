//! Small immutable matcher for explicitly supplied 3-prime adapter sequences.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Match {
    pub start: usize,
    pub compared: usize,
    pub mismatches: usize,
    pub declared_order: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct MatcherOptions {
    pub min_overlap: usize,
    pub max_error: f64,
}

#[derive(Clone, Debug)]
struct Adapter {
    bases: Box<[u8]>,
    declared_order: usize,
}

/// Owns normalized adapter motifs and fixed matching options for one mate.
#[derive(Clone, Debug)]
pub struct AdapterMatcher {
    adapters: Vec<Adapter>,
    min_overlap: usize,
    max_error: f64,
}

impl AdapterMatcher {
    /// Compile a caller-ordered list once; matching borrows the read and allocates nothing.
    pub fn new(adapters: &[String], options: MatcherOptions) -> Result<Self, String> {
        if options.min_overlap == 0 {
            return Err("minimum adapter overlap must be at least 1".into());
        }
        if !options.max_error.is_finite() || !(0.0..=1.0).contains(&options.max_error) {
            return Err("maximum adapter error must be finite and between 0 and 1".into());
        }

        let mut compiled = Vec::with_capacity(adapters.len());
        for (declared_order, adapter) in adapters.iter().enumerate() {
            if adapter.is_empty() {
                return Err("adapter sequence must not be empty".into());
            }
            let mut bases = Vec::with_capacity(adapter.len());
            for base in adapter.bytes() {
                let base = base.to_ascii_uppercase();
                if !matches!(base, b'A' | b'C' | b'G' | b'T') {
                    return Err("adapter sequence must contain only A, C, G, or T".into());
                }
                bases.push(base);
            }
            compiled.push(Adapter {
                bases: bases.into_boxed_slice(),
                declared_order,
            });
        }

        Ok(Self {
            adapters: compiled,
            min_overlap: options.min_overlap,
            max_error: options.max_error,
        })
    }

    /// Return the globally best maximal-overlap hit, if any.
    pub fn best_match(&self, read: &[u8]) -> Option<Match> {
        self.select(read, false)
    }
    pub fn best_match_anchored(&self, read: &[u8]) -> Option<Match> {
        self.select(read, true)
    }
    fn select(&self, read: &[u8], anchored: bool) -> Option<Match> {
        let mut best = None;
        for adapter in &self.adapters {
            for start in 0..if anchored {
                read.len().min(1)
            } else {
                read.len()
            } {
                let compared = adapter.bases.len().min(read.len() - start);
                if compared < self.min_overlap {
                    continue;
                }

                let mut mismatches = 0;
                let mut rejected = false;
                for offset in 0..compared {
                    if read[start + offset].to_ascii_uppercase() != adapter.bases[offset] {
                        mismatches += 1;
                        // The final denominator is fixed at this maximal overlap. Later
                        // comparisons can only add mismatches, so this rejection is safe.
                        if self.max_error == 0.0
                            || mismatches as f64 / compared as f64 > self.max_error
                        {
                            rejected = true;
                            break;
                        }
                    }
                }
                if rejected || mismatches as f64 / compared as f64 > self.max_error {
                    continue;
                }

                let candidate = Match {
                    start,
                    compared,
                    mismatches,
                    declared_order: adapter.declared_order,
                };
                if best.is_none_or(|current| better(candidate, current)) {
                    best = Some(candidate);
                }
            }
        }
        best
    }
}

fn better(candidate: Match, current: Match) -> bool {
    candidate.compared > current.compared
        || (candidate.compared == current.compared
            && (candidate.mismatches < current.mismatches
                || (candidate.mismatches == current.mismatches
                    && (candidate.start < current.start
                        || (candidate.start == current.start
                            && candidate.declared_order < current.declared_order)))))
}

#[cfg(test)]
mod tests {
    use super::{AdapterMatcher, Match, MatcherOptions};

    fn matcher(adapters: &[&str], min_overlap: usize, max_error: f64) -> AdapterMatcher {
        let owned = adapters.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        AdapterMatcher::new(
            &owned,
            MatcherOptions {
                min_overlap,
                max_error,
            },
        )
        .unwrap()
    }

    fn brute(
        adapters: &[String],
        read: &[u8],
        min_overlap: usize,
        max_error: f64,
    ) -> Option<Match> {
        let mut hits = Vec::new();
        for (declared_order, adapter) in adapters.iter().enumerate() {
            let bases = adapter
                .bytes()
                .map(|b| b.to_ascii_uppercase())
                .collect::<Vec<_>>();
            for start in 0..read.len() {
                let compared = bases.len().min(read.len() - start);
                if compared < min_overlap {
                    continue;
                }
                let mismatches = (0..compared)
                    .filter(|&i| read[start + i].to_ascii_uppercase() != bases[i])
                    .count();
                if mismatches as f64 / compared as f64 <= max_error {
                    hits.push(Match {
                        start,
                        compared,
                        mismatches,
                        declared_order,
                    });
                }
            }
        }
        hits.into_iter().min_by(|a, b| {
            b.compared
                .cmp(&a.compared)
                .then_with(|| a.mismatches.cmp(&b.mismatches))
                .then_with(|| a.start.cmp(&b.start))
                .then_with(|| a.declared_order.cmp(&b.declared_order))
        })
    }

    fn seq(mut value: u64, len: usize) -> String {
        let alphabet = [b'A', b'C', b'G', b'T'];
        let mut out = vec![b'A'; len];
        for base in &mut out {
            *base = alphabet[(value & 3) as usize];
            value = value.rotate_right(7) ^ value.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn anchored_primer_never_selects_internal_hits_and_recomputes_three_prime_coordinates() {
        let m = matcher(&["GGG"], 3, 0.0);
        assert_eq!(
            m.best_match_anchored(b"GGGAC"),
            Some(Match {
                start: 0,
                compared: 3,
                mismatches: 0,
                declared_order: 0
            })
        );
        assert!(m.best_match_anchored(b"AAGGG").is_none());
        let m = matcher(&["GGG"], 3, 1.0 / 3.0);
        assert_eq!(m.best_match_anchored(b"GAGAC").unwrap().mismatches, 1);
        assert!(m.best_match_anchored(b"GG").is_none());
        assert!(m.best_match_anchored(b"").is_none());
    }
    #[test]
    fn direct_error_fraction_boundaries() {
        let adapters = vec!["ACGT".to_owned()];
        let read = b"ACCT";
        assert_eq!(
            brute(&adapters, read, 4, 0.25),
            Some(Match {
                start: 0,
                compared: 4,
                mismatches: 1,
                declared_order: 0
            })
        );
        assert_eq!(
            matcher(&["ACGT"], 4, 0.25).best_match(read),
            brute(&adapters, read, 4, 0.25)
        );
        assert_eq!(matcher(&["ACGT"], 4, 0.249).best_match(read), None);

        let adapter = "A".repeat(50);
        let read = format!("{}{}", "A".repeat(21), "C".repeat(29));
        let adapters = vec![adapter.clone()];
        let expected = Some(Match {
            start: 0,
            compared: 50,
            mismatches: 29,
            declared_order: 0,
        });
        assert_eq!(brute(&adapters, read.as_bytes(), 50, 0.58), expected);
        assert_eq!(
            matcher(&[&adapter], 50, 0.58).best_match(read.as_bytes()),
            expected
        );
        assert_eq!(
            matcher(&[&adapter], 50, 0.5799).best_match(read.as_bytes()),
            None
        );
    }

    #[test]
    fn overlap_minimum_is_not_clamped_to_adapter_length() {
        assert_eq!(matcher(&["ACGT"], 5, 1.0).best_match(b"ACGT"), None);
        assert_eq!(
            matcher(&["ACGT"], 4, 0.0).best_match(b"ACGT"),
            Some(Match {
                start: 0,
                compared: 4,
                mismatches: 0,
                declared_order: 0
            })
        );
    }

    #[test]
    fn terminal_partial_prefix_is_eligible_but_interior_short_prefix_is_not() {
        assert_eq!(
            matcher(&["ACGT"], 3, 0.0).best_match(b"TTACG"),
            Some(Match {
                start: 2,
                compared: 3,
                mismatches: 0,
                declared_order: 0
            })
        );
        assert_eq!(matcher(&["ACGT"], 3, 0.0).best_match(b"TACGAAA"), None);
    }

    #[test]
    fn ranks_globally_by_overlap_mismatches_start_then_declaration() {
        assert_eq!(
            matcher(&["ACGA", "ACG"], 3, 0.25).best_match(b"ACGT"),
            Some(Match {
                start: 0,
                compared: 4,
                mismatches: 1,
                declared_order: 0
            })
        );
        assert_eq!(
            matcher(&["ACGA", "ACGT"], 4, 0.25).best_match(b"ACGT"),
            Some(Match {
                start: 0,
                compared: 4,
                mismatches: 0,
                declared_order: 1
            })
        );
        assert_eq!(
            matcher(&["ACG"], 3, 0.0).best_match(b"ACGACG"),
            Some(Match {
                start: 0,
                compared: 3,
                mismatches: 0,
                declared_order: 0
            })
        );
        assert_eq!(
            matcher(&["ACG", "ACG"], 3, 0.0).best_match(b"ACG"),
            Some(Match {
                start: 0,
                compared: 3,
                mismatches: 0,
                declared_order: 0
            })
        );
    }

    #[test]
    fn normalizes_adapter_and_read_case_and_validates_configuration() {
        assert_eq!(
            matcher(&["acgt"], 4, 0.0).best_match(b"acgt"),
            Some(Match {
                start: 0,
                compared: 4,
                mismatches: 0,
                declared_order: 0
            })
        );
        assert!(AdapterMatcher::new(
            &[String::new()],
            MatcherOptions {
                min_overlap: 1,
                max_error: 0.0
            }
        )
        .is_err());
        assert!(AdapterMatcher::new(
            &["ACGN".into()],
            MatcherOptions {
                min_overlap: 1,
                max_error: 0.0
            }
        )
        .is_err());
        assert!(AdapterMatcher::new(
            &["ACGT".into()],
            MatcherOptions {
                min_overlap: 0,
                max_error: 0.0
            }
        )
        .is_err());
        for error in [-0.01, 1.01, f64::NAN, f64::INFINITY] {
            assert!(AdapterMatcher::new(
                &["ACGT".into()],
                MatcherOptions {
                    min_overlap: 1,
                    max_error: error
                }
            )
            .is_err());
        }
    }

    #[test]
    fn deterministic_generated_cases_match_unpruned_reference() {
        for seed in [
            2026102301u64,
            2026102302,
            2026102303,
            2026102304,
            2026102305,
        ] {
            let mut state = seed;
            for _ in 0..10_000 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let read = seq(state, (state as usize >> 9) % 21);
                let count = 1 + ((state >> 21) as usize % 4);
                let mut adapters = Vec::with_capacity(count);
                for i in 0..count {
                    adapters.push(seq(
                        state.rotate_left(i as u32 + 1),
                        1 + ((state >> (i + 13)) as usize % 12),
                    ));
                }
                let min_overlap = 1 + ((state >> 33) as usize % 14);
                let max_error = [0.0, 0.25, 0.42, 0.58, 0.75, 1.0][(state >> 41) as usize % 6];
                let expected = brute(&adapters, read.as_bytes(), min_overlap, max_error);
                let actual = AdapterMatcher::new(
                    &adapters,
                    MatcherOptions {
                        min_overlap,
                        max_error,
                    },
                )
                .unwrap()
                .best_match(read.as_bytes());
                assert_eq!(actual, expected, "seed={seed:#x} state={state:#x} read={read} adapters={adapters:?} min={min_overlap} error={max_error}");
            }
        }
    }
}
