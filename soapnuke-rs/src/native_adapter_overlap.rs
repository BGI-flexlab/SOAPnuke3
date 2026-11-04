//! Conservative paired-end adapter trimming from a uniquely supported insert boundary.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    NoEvidence,
    Ambiguous,
    Clip { insert: usize, adapters: [usize; 2], mismatches: [usize; 2] },
}

fn complement(base: u8) -> u8 {
    match base.to_ascii_uppercase() {
        b'A' => b'T', b'C' => b'G', b'G' => b'C', b'T' => b'A',
        _ => b'N',
    }
}

fn tail_adapter(tail: &[u8], adapters: &[String]) -> Option<(usize, usize)> {
    if tail.iter().any(|b| !matches!(b.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T')) {
        return None;
    }
    adapters.iter().enumerate().find_map(|(index, adapter)| {
        let motif = adapter.as_bytes();
        if tail.is_empty() || tail.len() > motif.len() { return None; }
        // Short A/AG tails have no substitution allowance. For longer tails,
        // allow at most one error per ten observed adapter bases.
        let maximum = tail.len() / 10;
        let errors = tail.iter().zip(motif).filter(|(a,b)| a.to_ascii_uppercase() != **b).count();
        (errors <= maximum).then_some((index, errors))
    })
}

fn full_overlap(left: &[u8], right: &[u8], insert: usize) -> bool {
    let maximum = (insert / 50).min(3);
    let mut errors = 0;
    for x in 0..insert {
        let a = left[x].to_ascii_uppercase();
        let b = right[insert - 1 - x].to_ascii_uppercase();
        if !matches!(a, b'A' | b'C' | b'G' | b'T')
            || !matches!(b, b'A' | b'C' | b'G' | b'T') {
            return false;
        }
        errors += usize::from(a != complement(b));
        if errors > maximum { return false; }
    }
    true
}

pub fn select(left: &[u8], right: &[u8], adapters: [&[String]; 2]) -> Decision {
    let end = left.len().min(right.len());
    if end <= 30 { return Decision::NoEvidence; }
    let mut selected = None;
    for insert in 30..end {
        // Both mates must expose adapter sequence; no cut is inferred from only
        // one tail. A competing complete mate overlap also vetoes the cut even
        // when its exposed tails do not match the configured adapters.
        if !full_overlap(left, right, insert) { continue; }
        if selected.is_some() { return Decision::Ambiguous; }
        selected = Some(insert);
    }
    let Some(insert) = selected else { return Decision::NoEvidence; };
    let (Some(r1), Some(r2)) = (
        tail_adapter(&left[insert..], adapters[0]),
        tail_adapter(&right[insert..], adapters[1]),
    ) else { return Decision::NoEvidence; };
    Decision::Clip { insert, adapters: [r1.0, r2.0], mismatches: [r1.1, r2.1] }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rc(input: &str) -> String {
        input.bytes().rev().map(complement).map(char::from).collect()
    }
    #[test]
    fn clips_one_and_two_base_tails_but_rejects_unsupported_and_competing_boundaries() {
        let insert = "ACGTTGCATCAGTCCGATGCTACGTAGCTGACTAACGCTAGTCG";
        let adapters = [vec!["AGATCGGAAGAG".to_string()], vec!["AGATCGGAAGAG".to_string()]];
        let right = rc(insert);
        assert_eq!(select(format!("{insert}A").as_bytes(),format!("{right}A").as_bytes(), [&adapters[0], &adapters[1]]), Decision::Clip { insert: insert.len(), adapters: [0,0], mismatches: [0,0] });
        assert_eq!(select(format!("{insert}AG").as_bytes(),format!("{right}AG").as_bytes(), [&adapters[0], &adapters[1]]), Decision::Clip { insert: insert.len(), adapters: [0,0], mismatches: [0,0] });
        assert_eq!(select(format!("{insert}T").as_bytes(),format!("{right}A").as_bytes(), [&adapters[0], &adapters[1]]), Decision::NoEvidence);
        assert_eq!(select(b"ATATATATATATATATATATATATATATATATATATA", b"TATATATATATATATATATATATATATATATATATAT", [&adapters[0], &adapters[1]]), Decision::Ambiguous);
    }
    #[test]
    fn whole_insert_checked_and_single_tail_not_enough() {
        let insert = "ACGTTGCATCAGTCCGATGCTACGTAGCTGACTAACGCTAGTCG";
        let adapters = [vec!["AGATCGGAAGAG".to_string()], vec!["AGATCGGAAGAG".to_string()]];
        let right = rc(insert);
        let mut noisy = format!("{insert}A").into_bytes();
        noisy[35] = if noisy[35] == b'A' { b'C' } else { b'A' };
        noisy[36] = if noisy[36] == b'A' { b'C' } else { b'A' };
        assert_eq!(select(&noisy,format!("{right}A").as_bytes(), [&adapters[0], &adapters[1]]), Decision::NoEvidence);
        assert_eq!(select(format!("{insert}A").as_bytes(),right.as_bytes(), [&adapters[0], &adapters[1]]), Decision::NoEvidence);
    }
    #[test]
    fn full_overlap_length_and_error_boundaries_are_exact() {
        let fragment = "ACGTTGCATCAGTCCGATGCTACGTAGCTGACTAACGCTAGTCGCTAGCA";
        assert_eq!(fragment.len(), 50);
        let adapters = [vec!["AGATCGGAAGAG".into()], vec!["AGATCGGAAGAG".into()]];
        for n in [29, 30, 49, 50] {
            let insert = &fragment[..n];
            let right = format!("{}A", rc(insert));
            let left = format!("{insert}A");
            let expected = if n >= 30 { Decision::Clip { insert: n, adapters: [0, 0], mismatches: [0, 0] } } else { Decision::NoEvidence };
            assert_eq!(select(left.as_bytes(), right.as_bytes(), [&adapters[0], &adapters[1]]), expected, "n={n}");
            if n >= 49 {
                let mut changed = left.into_bytes();
                changed[20] = if changed[20] == b'A' { b'C' } else { b'A' };
                let expected = if n == 50 { Decision::Clip { insert: n, adapters: [0, 0], mismatches: [0, 0] } } else { Decision::NoEvidence };
                assert_eq!(select(&changed, right.as_bytes(), [&adapters[0], &adapters[1]]), expected, "one mismatch n={n}");
                if n == 50 {
                    changed[21] = if changed[21] == b'A' { b'C' } else { b'A' };
                    assert_eq!(select(&changed, right.as_bytes(), [&adapters[0], &adapters[1]]), Decision::NoEvidence);
                }
            }
        }
    }
    #[test]
    fn late_overlap_errors_and_ambiguous_adapter_tail_abstain() {
        let fragment = "ACGTTGCATCAGTCCGATGCTACGTAGCTGACTAACGCTAGTCGCTAGCACCTGTACCGTACGATCATGACCGTACGTAACCGATTACGA";
        let adapters = [vec!["AGATCGGAAGAG".into()], vec!["AGATCGGAAGAG".into()]];
        let right = format!("{}A", rc(fragment));
        let mut left = format!("{fragment}A").into_bytes();
        assert!(matches!(select(&left, right.as_bytes(), [&adapters[0], &adapters[1]]), Decision::Clip { .. }));
        for position in [65, 66] {
            left[position] = if left[position] == b'A' { b'C' } else { b'A' };
        }
        assert_eq!(select(&left, right.as_bytes(), [&adapters[0], &adapters[1]]), Decision::NoEvidence);
        let left = format!("{fragment}AGATCNGAAG");
        let right = format!("{}AGATCGGAAG", rc(fragment));
        assert_eq!(select(left.as_bytes(), right.as_bytes(), [&adapters[0], &adapters[1]]), Decision::NoEvidence);
    }
}
