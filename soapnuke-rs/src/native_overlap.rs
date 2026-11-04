use crate::record::FastqRecord;

pub const MERGED_MAX_LEN: usize = 2 * crate::params::READ_MAX_LEN - 1;

#[derive(Clone, Debug)]
pub struct Settings {
    pub merge: bool,
    pub correct: bool,
    pub minimum: usize,
    pub max_error: f64,
    pub min_insert: Option<usize>,
    pub discard_unmerged: bool,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            merge: false,
            correct: false,
            minimum: 20,
            max_error: 0.1,
            min_insert: None,
            discard_unmerged: false,
        }
    }
}
impl Settings {
    pub fn channel(&self, index: usize, interleaved: bool) -> usize {
        if self.merge && (self.discard_unmerged || index == if interleaved { 1 } else { 2 }) {
            2
        } else {
            index
        }
    }
    pub fn enabled(&self) -> bool {
        self.merge || self.correct || self.min_insert.is_some()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Evidence {
    pub offset: isize,
    pub compared: usize,
    pub matches: usize,
    pub mismatches: usize,
    pub insert: usize,
    pub union: usize,
}
fn complement(base: u8) -> u8 {
    match base {
        b'A' => b'T',
        b'C' => b'G',
        b'G' => b'C',
        b'T' => b'A',
        b'a' => b't',
        b'c' => b'g',
        b'g' => b'c',
        b't' => b'a',
        _ => base,
    }
}
fn same_base(left: u8, right: u8) -> bool {
    left == right
        || (matches!(left.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T')
            && left.eq_ignore_ascii_case(&right))
}
pub fn select(left: &[u8], right: &[u8], settings: &Settings) -> Option<Evidence> {
    if left.len() < settings.minimum || right.len() < settings.minimum {
        return None;
    }
    let mut best: Option<Evidence> = None;
    for offset in -(right.len() as isize) + settings.minimum as isize
        ..=left.len() as isize - settings.minimum as isize
    {
        let lo = 0.max(offset);
        let hi = (left.len() as isize).min(offset + right.len() as isize);
        let compared = (hi - lo) as usize;
        let matches = (lo..hi)
            .filter(|&x| {
                let a = left[x as usize];
                same_base(a, complement(right[right.len() - 1 - (x - offset) as usize]))
                    && matches!(a.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T')
            })
            .count();
        let mismatches = compared - matches;
        if mismatches as f64 / compared as f64 > settings.max_error {
            continue;
        }
        let candidate = Evidence {
            offset,
            compared,
            matches,
            mismatches,
            insert: (offset + right.len() as isize) as usize,
            union: ((left.len() as isize).max(offset + right.len() as isize) - 0.min(offset))
                as usize,
        };
        let rank = |e: Evidence| {
            (
                e.matches as isize - 4 * e.mismatches as isize,
                e.compared,
                -e.offset,
            )
        };
        if best.is_none_or(|old| rank(candidate) > rank(old)) {
            best = Some(candidate);
        }
    }
    best
}
pub fn correct(
    left: &mut FastqRecord,
    right: &mut FastqRecord,
    e: Evidence,
    phred: i32,
) -> u64 {
    let mut a = std::mem::take(&mut left.sequence).into_bytes();
    let mut b = std::mem::take(&mut right.sequence).into_bytes();
    let mut aq = std::mem::take(&mut left.qual_seq).into_bytes();
    let mut bq = std::mem::take(&mut right.qual_seq).into_bytes();
    let mut changed = 0;
    for x in 0.max(e.offset)..(a.len() as isize).min(e.offset + b.len() as isize) {
        let i = x as usize;
        let j = b.len() - 1 - (x - e.offset) as usize;
        let other = complement(b[j]);
        let q1 = aq[i] as i32 - phred;
        let q2 = bq[j] as i32 - phred;
        if !same_base(a[i], other) {
            // fastp 1.4.0 BaseCorrector: fixed inclusive Q30/Q14 gates,
            // copying the donor quality rather than assigning a Q difference.
            if q1 >= 30 && q2 <= 14 {
                b[j] = complement(a[i]);
                bq[j] = aq[i];
                changed += 1;
            } else if q2 >= 30 && q1 <= 14 {
                a[i] = other;
                aq[i] = bq[j];
                changed += 1;
            }
        }
    }
    left.sequence = String::from_utf8(a).unwrap();
    left.qual_seq = String::from_utf8(aq).unwrap();
    right.sequence = String::from_utf8(b).unwrap();
    right.qual_seq = String::from_utf8(bq).unwrap();
    changed
}
pub fn merge(left: &FastqRecord, right: &FastqRecord, e: Evidence, _phred: i32) -> FastqRecord {
    // fastp 1.4.0 OverlapAnalysis::merge takes R1 through the overlap,
    // then appends only the non-overlapping reverse-complemented R2 tail.
    // A negative-offset overlap emits the overlapping R1 prefix only.
    let left_len = e.compared + e.offset.max(0) as usize;
    let tail_len = if e.offset > 0 { right.sequence.len() - e.compared } else { 0 };
    let mut sequence = Vec::with_capacity(left_len + tail_len);
    let mut quality = Vec::with_capacity(left_len + tail_len);
    sequence.extend_from_slice(&left.sequence.as_bytes()[..left_len]);
    quality.extend_from_slice(&left.qual_seq.as_bytes()[..left_len]);
    sequence.extend(right.sequence.as_bytes()[..tail_len].iter().rev().map(|&b| complement(b)));
    quality.extend(right.qual_seq.as_bytes()[..tail_len].iter().rev().copied());
    let mut result = left.clone();
    result.sequence = String::from_utf8(sequence).unwrap();
    result.qual_seq = String::from_utf8(quality).unwrap();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    fn read(seq: &str, q: u8) -> FastqRecord {
        FastqRecord {
            seq_id: "full/1 comment".into(),
            sequence: seq.into(),
            qual_seq: String::from_utf8(vec![33 + q; seq.len()]).unwrap(),
            ..Default::default()
        }
    }
    #[test]
    fn m6_signed_geometry_scoring_and_consensus_literals() {
        let s = Settings {
            minimum: 2,
            max_error: 0.,
            ..Default::default()
        };
        let e = select(b"CCGG", b"CCGGTT", &s).unwrap();
        assert_eq!((e.offset, e.insert, e.union), (-2, 4, 6));
        assert_eq!(
            merge(&read("CCGG", 40), &read("CCGGTT", 20), e, 33).sequence,
            "CCGG"
        );
        assert!(select(
            b"AN",
            b"NT",
            &Settings {
                max_error: 0.499,
                ..s.clone()
            }
        )
        .is_none());
        assert_eq!(
            select(
                b"AN",
                b"NT",
                &Settings {
                    max_error: 0.5,
                    ..s.clone()
                }
            )
            .unwrap()
            .mismatches,
            1
        );
        let left = read("AACCGG", 40);
        let right = read("TCGGTT", 40);
        let e = select(
            left.sequence.as_bytes(),
            right.sequence.as_bytes(),
            &Settings {
                minimum: 6,
                max_error: 0.2,
                ..s.clone()
            },
        )
        .unwrap();
        let out = merge(&left, &right, e, 33);
        assert_eq!(out.sequence, "AACCGG");
        assert_eq!(out.qual_seq.as_bytes()[5], 73);
        assert_eq!(out.seq_id, "full/1 comment");
        let n_left = read("N", 40);
        let n_right = read("N", 20);
        let n_e = select(
            b"N",
            b"N",
            &Settings {
                minimum: 1,
                max_error: 1.,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(merge(&n_left, &n_right, n_e, 33).qual_seq.as_bytes(), &[73]);
        let mut left = read("AACCGG", 40);
        let mut right = read("TCGGTT", 10);
        assert_eq!(
            correct(
                &mut left,
                &mut right,
                e,
                33
            ),
            1
        );
        assert_eq!(right.sequence, "CCGGTT");
        assert_eq!(right.qual_seq.as_bytes()[0], 73);
    }
    #[test]
    fn m6_maximum_union_and_pre_correction_eligibility() {
        let left = read(&format!("{}C", "A".repeat(999)), 40);
        let right = read(&format!("{}G", "A".repeat(999)), 40);
        let s = Settings {
            minimum: 1,
            max_error: 0.,
            ..Default::default()
        };
        let e = select(left.sequence.as_bytes(), right.sequence.as_bytes(), &s).unwrap();
        assert_eq!((e.offset, e.union), (999, 1999));
        let out = merge(&left, &right, e, 33);
        assert_eq!(out.sequence.len(), MERGED_MAX_LEN);
        assert_eq!(out.qual_seq.len(), MERGED_MAX_LEN);
        assert!(select(b"AAAA", b"AAAA", &s).is_none());
    }
    #[test]
    fn m6_1999_qc_capacity_and_t32_statistics_stay_within_original_caps() {
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.overlap.merge = true;
        let extra = settings.additional_statistics(true);
        let (stats, _, _) =
            crate::native_execution::Execution::validate_budget_with_statistics(32, 2, None, extra)
                .unwrap();
        assert!(stats <= 128 * 1024 * 1024);
        let mut q = crate::native_qc::Qc::merged();
        q.set_merged_maximum(1999);
        q.observe_validated(&vec![b'A'; 1999], &vec![b'I'; 1999], 33);
        let before = (q.lengths.as_ptr(), q.cycles.as_ptr(), q.allocated_bytes());
        let copy = q.clone();
        q.merge(&copy);
        assert_eq!(
            before,
            (q.lengths.as_ptr(), q.cycles.as_ptr(), q.allocated_bytes())
        );
        assert_eq!((q.reads, q.lengths[1999], q.cycles.len()), (2, 2, 1999));
        assert!(
            q.allocated_bytes()
                <= std::mem::size_of_val(&q) + crate::native_qc::Qc::merged_maximum_heap_bytes()
        );
        let pop = crate::native_qc::population(&[&q], "written", 2, "complete", true);
        assert_eq!(pop["bases"], 3998);
        eprintln!(
            "M6_CAPACITY stats32={stats} extra={extra} merged_qc_bytes={} maximum=1999",
            q.allocated_bytes()
        );
    }
}

#[cfg(test)]
mod r93_case_tests {
    use super::*;
    fn read(s: &str, q: u8) -> FastqRecord {
        FastqRecord { sequence: s.into(), qual_seq: String::from_utf8(vec![33+q;s.len()]).unwrap(), ..Default::default() }
    }
    #[test]
    fn canonical_case_geometry_correction_and_consensus() {
        let s=Settings {minimum:4,max_error:0.,..Default::default()};
        for a in ["ACGT","acgt","AcGt"] { for b in ["ACGT","acgt","aCgT"] {
            let(mut l,mut r)=(read(a,40),read(b,20));
            let e=select(a.as_bytes(),b.as_bytes(),&s).unwrap();
            assert_eq!(e,Evidence{offset:0,compared:4,matches:4,mismatches:0,insert:4,union:4});
            assert_eq!(correct(&mut l,&mut r,e,33),0);
            assert_eq!((&*l.sequence,&*r.sequence,&*l.qual_seq,&*r.qual_seq),(a,b,"IIII","5555"));
            let m=merge(&l,&r,e,33);assert_eq!((&*m.sequence,&*m.qual_seq),(a,"IIII"));
        }}
        let s=Settings {minimum:6,max_error:0.2,..s};
        let(mut l,mut r)=(read("aaCCgg",40),read("tCGGtt",10));
        let e=select(l.sequence.as_bytes(),r.sequence.as_bytes(),&s).unwrap();
        assert_eq!((e.offset,e.matches,e.mismatches),(0,5,1));
        let m=merge(&l,&r,e,33);assert_eq!((&*m.sequence,&*m.qual_seq),("aaCCgg","IIIIII"));
        assert_eq!(correct(&mut l,&mut r,e,33),1);assert_eq!(r.sequence,"cCGGtt");assert_eq!(r.qual_seq,"I+++++");
        let e=select(b"AACCGG",b"TCGGTT",&s).unwrap();
        let m=merge(&read("AACCGG",40),&read("TCGGTT",40),e,33);assert_eq!((&*m.sequence,&*m.qual_seq),("AACCGG","IIIIII"));
        let s=Settings{minimum:1,max_error:0.,..s};
        assert_eq!(select(b"aaC",b"gTT",&s).unwrap().matches,3);
    }
    #[test]
    fn ambiguity_consensus_is_not_canonical_credit() {
        let s=Settings{minimum:1,max_error:0.,..Default::default()};
        for(a,b)in [("N","N"),("n","n"),("N","n")] {assert!(select(a.as_bytes(),b.as_bytes(),&s).is_none());}
        let s=Settings{max_error:1.,..s};
        for(a,b,q,expected)in [("N","N",40,"I"),("n","n",40,"I"),("N","n",40,"I"),("N","n",20,"I")] {
            let e=select(a.as_bytes(),b.as_bytes(),&s).unwrap();assert_eq!((e.matches,e.mismatches),(0,1));
            let m=merge(&read(a,40),&read(b,q),e,33);assert_eq!(m.sequence,a);assert_eq!(m.qual_seq,expected);
        }
    }
}
