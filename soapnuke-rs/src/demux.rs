use crate::record::FastqRecord;

#[derive(Clone, Debug)]
pub(crate) struct Sample {
    pub name: String,
    pub barcodes: Vec<String>,
}
#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub ranges: Vec<[usize; 2]>,
    pub errors: usize,
    pub min_q: Option<u8>,
    pub max_n: usize,
    pub remove: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Destination {
    Sample(usize),
    Ambiguous,
    Unassigned(&'static str),
}
#[derive(Clone, Debug)]
pub(crate) struct Table {
    pub samples: Vec<Sample>,
}
impl Table {
    pub(crate) fn validate(samples: Vec<Sample>) -> Result<Self, String> {
        if samples.is_empty() {
            return Err("samples table must not be empty".into());
        }
        let widths = samples[0]
            .barcodes
            .iter()
            .map(String::len)
            .collect::<Vec<_>>();
        if !(1..=2).contains(&widths.len()) || widths.contains(&0) || widths.iter().any(|n|*n>crate::params::READ_MAX_LEN) {
            return Err("samples require one or two nonempty barcode columns".into());
        }
        for (i, s) in samples.iter().enumerate() {
            if s.name.is_empty()
                || !s
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
            {
                return Err("invalid sample name".into());
            }
            if s.barcodes.len() != widths.len()
                || s.barcodes
                    .iter()
                    .zip(&widths)
                    .any(|(s, n)| s.len() != *n || !s.bytes().all(|b| b"ACGT".contains(&b)))
            {
                return Err("samples barcode2 columns, barcode lengths or alphabet disagree".into());
            }
            if samples[..i].iter().any(|p| p.name == s.name) {
                return Err("duplicate sample name".into());
            }
            if samples[..i].iter().any(|p| p.barcodes == s.barcodes) {
                return Err("duplicate sample barcode tuple".into());
            }
        }
        Ok(Self { samples })
    }
    pub(crate) fn route(
        &self,
        reads: &mut [FastqRecord],
        indices: Option<&[FastqRecord]>,
        settings: &Settings,
        phred: i32,
    ) -> Result<Destination, String> {
        let mut observed = Vec::with_capacity(2);
        let mut low = false;
        let mut ns = 0usize;
        if let Some(indices) = indices {
            if indices.len() != self.samples[0].barcodes.len() {
                return Err("[input] index column count mismatch".into());
            }
            for index in indices {
                let declared = index.input_phred.unwrap_or(phred);
                low |= settings.min_q.is_some_and(|q| {
                    index
                        .qual_seq
                        .bytes()
                        .any(|b| b as i32 - declared < (q as i32))
                });
                ns += index
                    .sequence
                    .bytes()
                    .filter(|b| matches!(b, b'N' | b'n'))
                    .count();
                observed.push(index.sequence.to_ascii_uppercase());
            }
        } else {
            for (mate, [start, end]) in settings.ranges.iter().copied().enumerate() {
                let Some(read) = reads.get(mate) else {
                    return Ok(Destination::Unassigned("missing-coordinate"));
                };
                if start >= end || end > read.sequence.len() {
                    return Ok(Destination::Unassigned("missing-coordinate"));
                }
                let declared = read.input_phred.unwrap_or(phred);
                low |= settings.min_q.is_some_and(|q| {
                    read.qual_seq.as_bytes()[start..end]
                        .iter()
                        .any(|b| *b as i32 - declared < (q as i32))
                });
                ns += read.sequence.as_bytes()[start..end]
                    .iter()
                    .filter(|b| matches!(b, b'N' | b'n'))
                    .count();
                observed.push(read.sequence[start..end].to_ascii_uppercase());
            }
        }
        if observed.len() != self.samples[0].barcodes.len()
            || observed
                .iter()
                .zip(&self.samples[0].barcodes)
                .any(|(a, b)| a.len() != b.len())
        {
            return Err("[input] barcode length mismatch".into());
        }
        if low {
            return Ok(Destination::Unassigned("label-quality"));
        }
        if ns > settings.max_n {
            return Ok(Destination::Unassigned("label-N"));
        }
        let mut best = usize::MAX;
        let mut winner = 0;
        let mut ties = 0;
        for (i, sample) in self.samples.iter().enumerate() {
            let distance = sample
                .barcodes
                .iter()
                .zip(&observed)
                .map(|(a, b)| a.bytes().zip(b.bytes()).filter(|(a, b)| a != b).count())
                .sum();
            if distance < best {
                best = distance;
                winner = i;
                ties = 1;
            } else if distance == best {
                ties += 1;
            }
        }
        if best > settings.errors {
            return Ok(Destination::Unassigned("no-match"));
        }
        if ties != 1 {
            return Ok(Destination::Ambiguous);
        }
        if settings.remove && indices.is_none() {
            if settings
                .ranges
                .iter()
                .enumerate()
                .any(|(mate, [start, end])| reads[mate].sequence.len() == end - start)
            {
                return Ok(Destination::Unassigned("empty-after-removal"));
            }
            let token = observed.join("-");
            let mut headers = Vec::with_capacity(reads.len());
            for read in reads.iter() {
                headers.push(bc_header(&read.seq_id, &token).map_err(|e| {
                    format!(
                        "[worker] barcode-metadata ordinal {}: {e}",
                        read.source_index
                    )
                })?);
            }
            for (mate, read) in reads.iter_mut().enumerate() {
                if let Some([start, end]) = settings.ranges.get(mate) {
                    read.sequence.replace_range(*start..*end, "");
                    read.qual_seq.replace_range(*start..*end, "");
                }
                read.seq_id = std::mem::take(&mut headers[mate]);
            }
        }
        Ok(Destination::Sample(winner))
    }
}
fn bc_header(header: &str, token: &str) -> Result<String, String> {
    let expected = format!("BC:Z:{token}");
    for old in header
        .split_ascii_whitespace()
        .skip(1)
        .filter(|v| v.starts_with("BC:Z:"))
    {
        if old != expected {
            return Err("conflicting BC metadata".into());
        }
    }
    let mut output = String::with_capacity(header.len() + expected.len() + 1);
    let bytes = header.as_bytes();
    let mut cursor = 0;
    let mut seen = false;
    let mut first = true;
    while cursor < bytes.len() {
        let begin = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        let start = cursor;
        while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        let part = &header[start..cursor];
        if !first && part.starts_with("BC:Z:") {
            if seen {
                continue;
            }
            seen = true;
        }
        output.push_str(&header[begin..cursor]);
        first = false;
    }
    if !seen {
        output.push(' ');
        output.push_str(&expected);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn read(name: &str, sequence: &str, q: u8) -> FastqRecord {
        let mut r = FastqRecord::new(1);
        r.seq_id = name.into();
        r.sequence = sequence.into();
        r.qual_seq = String::from_utf8(vec![q + 33; sequence.len()]).unwrap();
        r.input_phred = Some(33);
        r.source_index = 0;
        r
    }
    #[test]
    fn literal_route_hamming_quality_n_atomic_bc() {
        let table = Table::validate(vec![
            Sample {
                name: "S1".into(),
                barcodes: vec!["ACGT".into()],
            },
            Sample {
                name: "S2".into(),
                barcodes: vec!["TGCA".into()],
            },
        ])
        .unwrap();
        let config = Settings {
            ranges: vec![[0, 4]],
            errors: 1,
            min_q: Some(20),
            max_n: 0,
            remove: true,
        };
        let mut unit = vec![read("@a full  comment", "ACGTCC", 40)];
        assert_eq!(
            table.route(&mut unit, None, &config, 33).unwrap(),
            Destination::Sample(0)
        );
        assert_eq!(unit[0].sequence, "CC");
        assert_eq!(unit[0].qual_seq, "II");
        assert_eq!(unit[0].seq_id, "@a full  comment BC:Z:ACGT");
        let mut low = vec![read("@low", "ACGTCC", 19)];
        assert_eq!(
            table.route(&mut low, None, &config, 33).unwrap(),
            Destination::Unassigned("label-quality")
        );
        assert_eq!(low[0].sequence, "ACGTCC");
        let mut n = vec![read("@n", "ACGNCC", 40)];
        assert_eq!(
            table.route(&mut n, None, &config, 33).unwrap(),
            Destination::Unassigned("label-N")
        );
        let mut empty = vec![read("@e BC:Z:wrong", "ACGT", 40)];
        assert_eq!(
            table.route(&mut empty, None, &config, 33).unwrap(),
            Destination::Unassigned("empty-after-removal")
        );
        assert_eq!(empty[0].seq_id, "@e BC:Z:wrong");
        let mut conflict = vec![read("@x BC:Z:wrong", "ACGTCC", 40)];
        assert!(table.route(&mut conflict, None, &config, 33).is_err());
        assert_eq!(conflict[0].sequence, "ACGTCC");
        assert_eq!(
            bc_header("@a\tfull  BC:Z:ACGT\tBC:Z:ACGT comment", "ACGT").unwrap(),
            "@a\tfull  BC:Z:ACGT comment"
        );
        let tie = Table::validate(vec![
            Sample {
                name: "A".into(),
                barcodes: vec!["AAAA".into()],
            },
            Sample {
                name: "B".into(),
                barcodes: vec!["AAAT".into()],
            },
        ])
        .unwrap();
        let mut t = vec![read("@tie", "AAANCC", 40)];
        let mut allowed = config.clone();
        allowed.max_n = 1;
        assert_eq!(
            tie.route(&mut t, None, &allowed, 33).unwrap(),
            Destination::Ambiguous
        );
    }
}
