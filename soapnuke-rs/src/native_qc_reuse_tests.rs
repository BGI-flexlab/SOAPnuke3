use super::*;
use crate::native_processing::ProcessSettings;
use crate::native_qc::Qc;
use std::sync::Arc;

fn params() -> GlobalParams {
    let mut gp = GlobalParams::new();
    gp.native_m1 = true;
    gp.quality_phred = 33;
    gp.output_quality_phred = 33;
    gp
}
fn read(seq: &str, scores: &[u8], phred: i32) -> FastqRecord {
    let mut r = FastqRecord::new(1);
    r.sequence = seq.into();
    r.qual_seq = scores
        .iter()
        .map(|q| char::from(*q + phred as u8))
        .collect();
    r
}
fn assert_scalar(qc: &Qc, reads: &[FastqRecord], phred: i32) {
    let max = reads.iter().map(|r| r.sequence.len()).max().unwrap_or(0);
    assert_eq!(qc.reads, reads.len() as u64);
    for length in 0..qc.lengths.len() {
        assert_eq!(
            qc.lengths[length],
            reads.iter().filter(|r| r.sequence.len() == length).count() as u64
        );
    }
    for base in 0..6 {
        let expected = reads
            .iter()
            .flat_map(|r| r.sequence.bytes())
            .filter(|b| {
                b"ACGTN"
                    .iter()
                    .position(|x| *x == b.to_ascii_uppercase())
                    .unwrap_or(5)
                    == base
            })
            .count() as u64;
        assert_eq!(qc.bases[base], expected);
    }
    for score in 0..94 {
        assert_eq!(
            qc.qualities[score],
            reads
                .iter()
                .flat_map(|r| r.qual_seq.bytes())
                .filter(|q| *q as i32 - phred == score as i32)
                .count() as u64
        );
    }
    assert!(qc.cycles.len() >= max);
    for (i, cycle) in qc.cycles.iter().enumerate() {
        for base in 0..6 {
            assert_eq!(
                cycle.bases[base],
                reads
                    .iter()
                    .filter_map(|r| r.sequence.as_bytes().get(i))
                    .filter(|b| b"ACGTN"
                        .iter()
                        .position(|x| *x == b.to_ascii_uppercase())
                        .unwrap_or(5)
                        == base)
                    .count() as u64
            );
        }
        for score in 0..94 {
            assert_eq!(
                cycle.qualities[score],
                reads
                    .iter()
                    .filter_map(|r| r.qual_seq.as_bytes().get(i))
                    .filter(|q| **q as i32 - phred == score as i32)
                    .count() as u64
            );
        }
    }
}
fn qc(stat: &FileStat) -> &Qc {
    stat.native_qc.as_ref().unwrap()
}

#[test]
fn variable_se_pe_histograms_match_scalar() {
    for phred in [33, 64] {
        for paired in [false, true] {
            let mut gp = params();
            gp.quality_phred = phred;
            gp.output_quality_phred = phred;
            let a = vec![
                read(
                    "ACGTNAC",
                    &[0, 19, 20, 29, 30, 62, if phred == 33 { 93 } else { 62 }],
                    phred,
                ),
                read("n", &[40], phred),
            ];
            let b = vec![
                read("TG", &[40, 0], phred),
                read("ACN", &[20, 30, 62], phred),
            ];
            let mut stats = PeLocalStats::new(&gp);
            let out = apply(a.clone(), paired.then(|| b.clone()), &mut stats, &gp).unwrap();
            assert_scalar(qc(&stats.raw1), &a, phred);
            assert_scalar(qc(&stats.clean1), &a, phred);
            assert_eq!(out.0.len(), 2);
            assert_eq!(stats.fs.native.rejected, 0);
            if paired {
                assert_scalar(qc(&stats.raw2), &b, phred);
                assert_scalar(qc(&stats.clean2), &b, phred);
            }
        }
    }
}
#[test]
fn existing_raw_and_multiple_batch_clean_scope() {
    let gp = params();
    let mut stats = PeLocalStats::new(&gp);
    let a = read("AC", &[0, 40], 33);
    let b = read("N", &[30], 33);
    apply(vec![a.clone()], None, &mut stats, &gp).unwrap();
    stats.clean1.native_qc = Some(Box::new(Qc::default()));
    apply(vec![b.clone()], None, &mut stats, &gp).unwrap();
    assert_scalar(qc(&stats.raw1), &[a, b.clone()], 33);
    assert_scalar(qc(&stats.clean1), &[b], 33);
}
#[test]
fn existing_clean_accumulator_uses_normal_path() {
    let gp = params();
    let mut stats = PeLocalStats::new(&gp);
    let a = read("A", &[0], 33);
    let b = read("CN", &[20, 93], 33);
    apply(vec![a.clone()], None, &mut stats, &gp).unwrap();
    apply(vec![b.clone()], None, &mut stats, &gp).unwrap();
    assert_scalar(qc(&stats.raw1), &[a.clone(), b.clone()], 33);
    assert_scalar(qc(&stats.clean1), &[a, b], 33);
}
#[test]
fn invalid_first_later_and_second_mate_preserve_prefix() {
    for paired in [false, true] {
        for first in [false, true] {
            for bad_mate in 0..if paired { 2 } else { 1 } {
                let gp = params();
                let mut stats = PeLocalStats::new(&gp);
                let good = read("AC", &[19, 40], 33);
                let bad = read("AX", &[20, 40], 33);
                let mut a = if first { vec![] } else { vec![good.clone()] };
                let mut b = a.clone();
                a.push(if bad_mate == 0 {
                    bad.clone()
                } else {
                    good.clone()
                });
                b.push(if bad_mate == 1 {
                    bad.clone()
                } else {
                    good.clone()
                });
                assert!(apply(a, paired.then_some(b), &mut stats, &gp).is_err());
                let prefix = if first { vec![] } else { vec![good.clone()] };
                assert_scalar(qc(&stats.raw1), &prefix, 33);
                assert_scalar(qc(&stats.clean1), &prefix, 33);
                if paired {
                    assert_scalar(qc(&stats.raw2), &prefix, 33);
                    assert_scalar(qc(&stats.clean2), &prefix, 33);
                }
            }
        }
    }
}
#[test]
fn empty_reads_use_default_minimum_rejection() {
    for paired in [false, true] {
        let gp = params();
        let mut stats = PeLocalStats::new(&gp);
        let good = read("AC", &[20, 40], 33);
        let empty = read("", &[], 33);
        let a = vec![
            good.clone(),
            if paired { good.clone() } else { empty.clone() },
        ];
        let b = vec![good.clone(), empty];
        let out = apply(a.clone(), paired.then(|| b.clone()), &mut stats, &gp).unwrap();
        assert_eq!(out.0.len(), 1);
        assert_eq!(stats.fs.native.rejected, 1);
        assert_scalar(qc(&stats.raw1), &a, 33);
        assert_scalar(qc(&stats.clean1), &[good], 33);
        if paired {
            assert_scalar(qc(&stats.raw2), &b, 33);
        }
    }
}
#[test]
fn transform_filter_and_adapter_fallbacks_keep_literal_answers() {
    for mode in 0..8 {
        let mut gp = params();
        let mut settings = ProcessSettings::default();
        match mode {
            0 => gp.native_cuts[0] = [1, 0],
            1 => gp.native_quality_q = Some(20),
            2 => {
                settings.adapters[0] = vec!["CG".into()];
                settings.min_overlap = 2;
                settings.max_error = 0.;
            }
            3 => settings.min_length = 5,
            4 => settings.max_length = Some(2),
            5 => settings.max_n_count = Some(0),
            6 => {
                settings.low_q = Some(20);
                settings.max_low_q_fraction = Some(0.);
            }
            _ => settings.min_mean_q = Some(40.),
        }
        settings.compile().unwrap();
        gp.native_process = Arc::new(settings);
        let mut stats = PeLocalStats::new(&gp);
        let input = read("ACGN", &[0, 40, 40, 40], 33);
        let out = apply(vec![input.clone()], None, &mut stats, &gp).unwrap();
        assert_scalar(qc(&stats.raw1), &[input], 33);
        let answer = match mode {
            0 | 1 => vec![read("CGN", &[40, 40, 40], 33)],
            2 => vec![read("A", &[0], 33)],
            _ => vec![],
        };
        assert_scalar(qc(&stats.clean1), &answer, 33);
        assert_eq!(out.0.len(), answer.len());
    }
}
#[test]
fn conversion_error_keeps_raw_and_accepted_distinct() {
    let mut gp = params();
    gp.output_quality_phred = 64;
    let mut stats = PeLocalStats::new(&gp);
    let good = read("A", &[20], 33);
    let high = read("N", &[93], 33);
    assert!(apply(vec![good.clone(), high.clone()], None, &mut stats, &gp).is_err());
    assert_scalar(qc(&stats.raw1), &[good.clone(), high], 33);
    assert_scalar(qc(&stats.clean1), &[good], 33);
    let mut gp = params();
    gp.quality_phred = 64;
    let mut stats = PeLocalStats::new(&gp);
    let input = read("N", &[62], 64);
    apply(vec![input.clone()], None, &mut stats, &gp).unwrap();
    assert_scalar(qc(&stats.raw1), &[input.clone()], 64);
    assert_scalar(qc(&stats.clean1), &[input], 64);
}
#[test]
fn reuse_retains_existing_qc_objects_and_max_cycle_capacity() {
    let gp = params();
    let mut stats = PeLocalStats::new(&gp);
    let pointers = [
        qc(&stats.raw1) as *const Qc,
        qc(&stats.raw2) as *const Qc,
        qc(&stats.clean1) as *const Qc,
        qc(&stats.clean2) as *const Qc,
    ];
    let input = read(&"A".repeat(READ_MAX_LEN), &vec![40; READ_MAX_LEN], 33);
    apply(vec![input.clone()], Some(vec![input]), &mut stats, &gp).unwrap();
    assert_eq!(
        pointers,
        [
            qc(&stats.raw1) as *const Qc,
            qc(&stats.raw2) as *const Qc,
            qc(&stats.clean1) as *const Qc,
            qc(&stats.clean2) as *const Qc
        ]
    );
    for stat in [&stats.raw1, &stats.raw2, &stats.clean1, &stats.clean2] {
        assert_eq!(qc(stat).cycles.len(), READ_MAX_LEN);
        assert_eq!(qc(stat).cycles.capacity(), READ_MAX_LEN);
    }
    assert_eq!(stats.fs.native.evaluated[0][1], 1);
    assert_eq!(stats.fs.native.evaluated[1][2], 1);
}
