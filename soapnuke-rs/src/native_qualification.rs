use super::*;
use crate::native_processing::{FateStats, ProcessSettings};

const SEEDS: [u64; 5] = [2026102301, 2026102302, 2026102303, 2026102304, 2026102305];
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
fn check_transform(
    mut read: FastqRecord,
    phred: i32,
    head: usize,
    tail: usize,
    threshold: u8,
) -> FastqRecord {
    let original: Vec<_> = read.sequence.bytes().zip(read.qual_seq.bytes()).collect();
    let fixed: Vec<_> = original
        .iter()
        .copied()
        .enumerate()
        .filter(|(i, _)| *i >= head && tail < original.len() - *i)
        .map(|(_, pair)| pair)
        .collect();
    let first = fixed
        .iter()
        .position(|(_, q)| *q as i32 - phred >= threshold as i32);
    let last = fixed
        .iter()
        .rposition(|(_, q)| *q as i32 - phred >= threshold as i32);
    let expected = match (first, last) {
        (Some(a), Some(b)) => fixed[a..=b].to_vec(),
        _ => Vec::new(),
    };
    clip(&mut read, [head, tail]);
    peel_quality_ends(&mut read, Some(threshold), phred);
    assert_eq!(
        read.sequence
            .bytes()
            .zip(read.qual_seq.bytes())
            .collect::<Vec<_>>(),
        expected
    );
    read
}
#[test]
fn seeded_transform_predicate_fate_and_partition_reference() {
    for seed in SEEDS {
        let mut state = seed;
        let settings = ProcessSettings {
            min_length: 3,
            max_length: Some(19),
            max_n_count: Some(2),
            max_n_fraction: Some(0.25),
            low_q: Some(20),
            max_low_q_fraction: Some(0.25),
            min_mean_q: Some(29.5),
            ..ProcessSettings::default()
        };
        let mut direct = FateStats::default();
        let mut parts = [FateStats::default(), FateStats::default()];
        let mut first_reference = [0u64; 9];
        let mut overlaps = [0u64; 9];
        let mut accepted = 0;
        for index in 0..10_000 {
            let mut pair = [[None; 9]; 2];
            for values in &mut pair {
                let length = next(&mut state) as usize % 41;
                let phred = if next(&mut state) & 1 == 0 { 33 } else { 64 };
                let mut read = FastqRecord::new(1);
                for _ in 0..length {
                    read.sequence
                        .push(b"ACGTN"[next(&mut state) as usize % 5] as char);
                    read.qual_seq.push(
                        (phred as u8 + (next(&mut state) % if phred == 33 { 94 } else { 63 }) as u8)
                            as char,
                    );
                }
                let head = next(&mut state) as usize % 24;
                let tail = next(&mut state) as usize % 24;
                let q = (next(&mut state) % 32) as u8;
                let read = check_transform(read, phred, head, tail, q);
                let n = read.sequence.bytes().filter(|&b| b == b'N').count();
                let qs: Vec<_> = read.qual_seq.bytes().map(|v| v as i32 - phred).collect();
                let len = qs.len();
                let reference = [
                    None,
                    Some(len == 0),
                    Some(len < 3),
                    Some(len > 19),
                    Some(n > 2),
                    (len > 0).then(|| n as f64 / len as f64 > 0.25),
                    (len > 0)
                        .then(|| qs.iter().filter(|&&v| v < 20).count() as f64 / len as f64 > 0.25),
                    (len > 0).then(|| qs.iter().sum::<i32>() as f64 / (len as f64) < 29.5),
                    None,
                ];
                *values = settings.evaluate(&read, phred, false, 0);
                assert_eq!(*values, reference, "seed={seed} index={index}");
            }
            let failures: Vec<_> = (0..9)
                .filter(|&i| pair.iter().any(|mate| mate[i] == Some(true)))
                .collect();
            let reject = !failures.is_empty();
            accepted += u64::from(!reject);
            if let Some(&first) = failures.first() {
                first_reference[first] += 1;
            }
            for reason in failures {
                overlaps[reason] += 1;
            }
            assert_eq!(direct.record(pair, &settings, true), reject);
            parts[index % 2].record(pair, &settings, true);
        }
        parts[0].merge(&parts[1].clone());
        assert_eq!(
            direct.report(&settings, true, [10_000; 2]),
            parts[0].report(&settings, true, [10_000; 2])
        );
        assert_eq!(direct.first, first_reference);
        assert_eq!(direct.overlapping_units, overlaps);
        assert_eq!(accepted + direct.rejected, 10_000);
        assert_eq!(direct.first.iter().sum::<u64>(), direct.rejected);
    }
}
#[test]
#[ignore = "explicit 120-second qualification smoke; run separately"]
fn parser_transform_mutation_smoke_120_seconds() {
    let started = std::time::Instant::now();
    let mut state = 2026102306u64;
    let mut iterations = 0u64;
    let mut categories = [0u64; 8];
    while started.elapsed() < std::time::Duration::from_secs(120) {
        let length = 1 + next(&mut state) as usize % 64;
        let phred = if next(&mut state) & 1 == 0 { 33 } else { 64 };
        let sequence: String = (0..length)
            .map(|_| b"ACGTN"[next(&mut state) as usize % 5] as char)
            .collect();
        let quality: String = (0..length)
            .map(|_| {
                (phred as u8 + (next(&mut state) % if phred == 33 { 94 } else { 63 }) as u8) as char
            })
            .collect();
        let category = iterations as usize % 8;
        let mut lines = [
            "@mutation full comment".to_owned(),
            sequence.clone(),
            "+".to_owned(),
            quality.clone(),
        ];
        match category {
            1 => {
                lines[0] = "missing-at".into();
            }
            2 => {
                lines[2] = "not-plus".into();
            }
            3 => {
                lines[3].pop();
            }
            5 => {
                lines[3].replace_range(0..1, if phred == 64 { "?" } else { " " });
            }
            6 => {
                lines[1].replace_range(0..1, "X");
            }
            _ => (),
        }
        let ending = if category == 7 { "\r\n" } else { "\n" };
        let mut bytes = (lines.join(ending) + ending).into_bytes();
        if category == 4 {
            bytes.truncate(bytes.len() - 1);
        }
        let head = next(&mut state) as usize % 70;
        let tail = next(&mut state) as usize % 70;
        let q = (next(&mut state) % 64) as u8;
        let result = std::panic::catch_unwind(|| {
            let parsed = crate::pipeline::qualification_parse(&bytes);
            if (1..=4).contains(&category) {
                let error = match parsed {
                    Ok(_) => panic!("invalid framing accepted"),
                    Err(e) => e,
                };
                let expected = match category {
                    1 => "identifier",
                    2 => "+",
                    3 => "length",
                    _ => "truncated",
                };
                assert!(error.contains(expected), "unexpected error: {error}");
            } else {
                let mut reads = parsed.unwrap();
                assert_eq!(reads.len(), 1);
                let read = reads.remove(0);
                if category == 5 {
                    assert!(validate(&read, phred)
                        .unwrap_err()
                        .contains("quality.encoding"));
                } else if category == 6 {
                    assert!(validate(&read, phred).unwrap_err().contains("non-ACGTN"));
                } else {
                    validate(&read, phred).unwrap();
                    assert_eq!(read.sequence, sequence);
                    assert_eq!(read.qual_seq, quality);
                    check_transform(read, phred, head, tail, q);
                }
            }
        });
        if let Err(error) = result {
            let path = std::path::PathBuf::from(std::env::var("TMPDIR").unwrap())
                .join("mutation-failure.json");
            std::fs::write(path, serde_json::to_vec_pretty(&serde_json::json!({"seed":2026102306u64,"iteration":iterations,"state":state,"category":category,"bytes":bytes,"phred":phred,"head":head,"tail":tail,"threshold":q})).unwrap()).unwrap();
            std::panic::resume_unwind(error);
        }
        categories[category] += 1;
        iterations += 1;
    }
    let path =
        std::path::PathBuf::from(std::env::var("TMPDIR").unwrap()).join("mutation-summary.json");
    std::fs::write(path, serde_json::to_vec_pretty(&serde_json::json!({"seed":2026102306u64,"iterations":iterations,"categories":categories,"elapsed_seconds":started.elapsed().as_secs_f64(),"mode":"ordinary Rust release-profile test; no sanitizer/libFuzzer or UB-proof claim"})).unwrap()).unwrap();
}
