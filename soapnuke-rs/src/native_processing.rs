use crate::native_adapter::{AdapterMatcher, MatcherOptions};
use crate::record::FastqRecord;
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QualityMode {
    Ends,
    Window,
    Mott,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessStage {
    Tails,
    Quality,
    Adapters,
}
pub const DEFAULT_STAGE_ORDER: [ProcessStage; 3] = [
    ProcessStage::Tails,
    ProcessStage::Quality,
    ProcessStage::Adapters,
];
impl ProcessStage {
    pub fn name(self) -> &'static str {
        match self {
            Self::Tails => "tails",
            Self::Quality => "quality",
            Self::Adapters => "adapters",
        }
    }
}

pub const REASONS: [&str; 9] = [
    "adapter-present",
    "empty",
    "min-length",
    "max-length",
    "N-count",
    "N-fraction",
    "low-quality-fraction",
    "mean-quality",
    "complexity",
];

#[derive(Clone, Debug)]
pub struct LabelSpec {
    pub name: String,
    pub mate: usize,
    pub start: usize,
    pub end: usize,
}
impl LabelSpec {
    pub fn parse(value: &str) -> Result<Self, String> {
        let (name, range) = value
            .split_once('@')
            .ok_or("extract-label requires name@r1:start:length")?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err("label name requires ASCII letters, digits, _ or -".into());
        }
        let fields: Vec<_> = range.split(':').collect();
        if fields.len() != 3 {
            return Err("extract-label requires name@r1:start:length".into());
        }
        let mate = match fields[0] {
            "r1" => 0,
            "r2" => 1,
            _ => return Err("label mate must be r1 or r2".into()),
        };
        let number = |v: &str| -> Result<usize, String> {
            if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
                return Err("label coordinates require positive integers".into());
            }
            v.parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or("label coordinates require positive integers".into())
        };
        let start = number(fields[1])? - 1;
        let end = start
            .checked_add(number(fields[2])?)
            .ok_or("label coordinate overflow")?;
        Ok(Self {
            name: name.into(),
            mate,
            start,
            end,
        })
    }
}
#[derive(Clone, Debug)]
pub struct CaptureSettings {
    pub labels: Vec<LabelSpec>,
    pub remove: bool,
    pub kind: String,
    pub min_q: Option<u8>,
    pub max_n: usize,
    pub retain: [Option<[usize; 2]>; 2],
}
impl Default for CaptureSettings {
    fn default() -> Self {
        Self {
            labels: Vec::new(),
            remove: false,
            kind: "umi".into(),
            min_q: None,
            max_n: 0,
            retain: [None, None],
        }
    }
}
impl CaptureSettings {
    pub fn enabled(&self) -> bool {
        !self.labels.is_empty() || self.retain.iter().any(Option::is_some)
    }
    pub fn apply(&self, reads: &mut [&mut FastqRecord], phred: i32) -> Result<[bool; 4], String> {
        let mut failures = [false; 4];
        for label in &self.labels {
            if reads
                .get(label.mate)
                .is_none_or(|r| label.end > r.sequence.len())
            {
                failures[0] = true;
            }
        }
        for (mate, range) in self.retain.iter().enumerate() {
            if let Some([start, end]) = range {
                if reads
                    .get(mate)
                    .is_none_or(|r| *start >= *end || *end > r.sequence.len())
                {
                    failures[1] = true;
                }
            }
        }
        if self.remove {
            for (i, a) in self.labels.iter().enumerate() {
                for b in &self.labels[..i] {
                    if a.mate == b.mate && a.start < b.end && b.start < a.end {
                        failures[0] = true;
                    }
                }
                if self.retain[a.mate].is_some_and(|[start, end]| a.start < end && start < a.end) {
                    failures[1] = true;
                }
            }
        }
        if failures[0] || failures[1] {
            return Ok(failures);
        }
        let mut n = 0usize;
        for label in &self.labels {
            let r = &reads[label.mate];
            failures[2] |= self.min_q.is_some_and(|min| {
                r.qual_seq.as_bytes()[label.start..label.end]
                    .iter()
                    .any(|q| *q as i32 - phred < min as i32)
            });
            n += r.sequence.as_bytes()[label.start..label.end]
                .iter()
                .filter(|b| matches!(b, b'N' | b'n'))
                .count();
        }
        failures[3] = n > self.max_n;
        if failures[2] || failures[3] {
            return Ok(failures);
        }
        let prefix = if self.kind == "barcode" {
            "BC:Z:"
        } else {
            "RX:Z:"
        };
        let tags: Vec<String> = self
            .labels
            .iter()
            .map(|label| {
                format!(
                    "{prefix}{}={}",
                    label.name,
                    &reads[label.mate].sequence[label.start..label.end]
                )
            })
            .collect();
        for r in reads.iter() {
            for tag in &tags {
                let key = tag.split_once('=').unwrap().0;
                if r.seq_id.split_ascii_whitespace().skip(1).any(|old| {
                    old.strip_prefix(key)
                        .is_some_and(|s| s.starts_with('=') && old != tag)
                }) {
                    return Err(format!(
                        "[worker] label metadata tag conflict at source ordinal {}",
                        r.source_index
                    ));
                }
            }
        }
        for (mate, r) in reads.iter_mut().enumerate() {
            for tag in &tags {
                if !r.seq_id.split_ascii_whitespace().any(|old| old == tag) {
                    r.seq_id.push(' ');
                    r.seq_id.push_str(tag);
                }
            }
            if let Some([start, end]) = self.retain[mate] {
                r.sequence = r.sequence[start..end].into();
                r.qual_seq = r.qual_seq[start..end].into();
            } else if self.remove {
                for index in (0..r.sequence.len()).rev() {
                    if self
                        .labels
                        .iter()
                        .any(|l| l.mate == mate && index >= l.start && index < l.end)
                    {
                        r.sequence.remove(index);
                        r.qual_seq.remove(index);
                    }
                }
            }
        }
        Ok(failures)
    }
}

pub struct SelectedAdapter {
    pub mate: usize,
    pub adapter: String,
    pub start: usize,
    pub end: usize,
    pub mismatches: usize,
}
pub struct MatchedUnit {
    pub ordinal: u64,
    pub hits: Vec<SelectedAdapter>,
}

#[derive(Clone, Debug)]
pub struct ProcessSettings {
    pub html_report: bool,
    pub qc_only: bool,
    pub repetition: bool,
    pub repetition_units: usize,
    pub overrep_fraction: Option<f64>,
    pub screen: crate::native_screen::Settings,
    pub profile: String,
    pub srna_min_insert: usize,
    pub srna_reject5: bool,
    pub srna_reject_poly_a: bool,
    pub overlap: crate::native_overlap::Settings,
    pub adapter_overlap: bool,
    pub pair_policy: String,
    pub sample_fraction: f64,
    pub sample_seed: u64,
    pub split_units: usize,
    pub split_metadata_unit_bound: usize,
    pub capture: CaptureSettings,
    pub infer_mode: String,
    pub infer_units: usize,
    pub infer_min_support: f64,
    pub primers: [Vec<String>; 2],
    pub primer_matchers: [Option<AdapterMatcher>; 2],
    pub adapter_names: [Vec<String>; 2],
    pub primer_names: [Vec<String>; 2],
    pub adapters: [Vec<String>; 2],
    pub matchers: [Option<AdapterMatcher>; 2],
    pub adapter_discard: bool,
    pub min_overlap: usize,
    pub max_error: f64,
    pub min_length: usize,
    pub max_length: Option<usize>,
    pub max_n_count: Option<usize>,
    pub max_n_fraction: Option<f64>,
    pub low_q: Option<u8>,
    pub max_low_q_fraction: Option<f64>,
    pub min_mean_q: Option<f64>,
    pub min_complexity: Option<f64>,
}
impl Default for ProcessSettings {
    fn default() -> Self {
        Self {
            html_report: false,
            qc_only: false,
            repetition:false,repetition_units:100000,overrep_fraction:None,
            screen: Default::default(),
            profile: "ordinary".into(),
            srna_min_insert: 1,
            srna_reject5: false,
            srna_reject_poly_a: false,
            overlap: Default::default(),
            adapter_overlap: false,
            pair_policy: "both-pass".into(),
            sample_fraction: 1.0,
            sample_seed: 0,
            split_units: 0,
            split_metadata_unit_bound: 0,
            capture: CaptureSettings::default(),
            infer_mode: "off".into(),
            infer_units: 100000,
            infer_min_support: 0.01,
            primers: Default::default(),
            primer_matchers: [None, None],
            adapter_names: Default::default(),
            primer_names: Default::default(),
            adapters: Default::default(),
            matchers: [None, None],
            adapter_discard: false,
            min_overlap: 3,
            max_error: 0.1,
            min_length: 1,
            max_length: None,
            max_n_count: None,
            max_n_fraction: None,
            low_q: None,
            max_low_q_fraction: None,
            min_mean_q: None,
            min_complexity: None,
        }
    }
}
impl ProcessSettings {
    pub fn additional_statistics(&self, auxiliary: bool) -> usize {
        let regular = if auxiliary
            || self.overlap.enabled()
            || self.profile == "srna"
            || self.screen.enabled()
        {
            10 * (std::mem::size_of::<crate::native_qc::Qc>()
                + crate::native_qc::Qc::maximum_heap_bytes())
        } else {
            0
        };
        regular
            + if self.overlap.merge {
                4 * (std::mem::size_of::<crate::native_qc::Qc>()
                    + crate::native_qc::Qc::merged_maximum_heap_bytes())
            } else {
                0
            }
    }
    pub fn feature_chunk_bound(
        &self,
        units: usize,
        raw: usize,
        auxiliary: bool,
        audit: bool,
    ) -> Result<usize, String> {
        if !self.capture.enabled()
            && !auxiliary
            && !audit
            && self.sample_fraction == 1.0
            && self.split_units == 0
            && !self.overlap.enabled()
            && self.profile == "ordinary"
            && !self.screen.enabled()
            && !self.repetition
        {
            return Ok(0);
        }
        let mut tags = 0usize;
        for label in &self.capture.labels {
            tags = tags
                .checked_add(label.name.len())
                .and_then(|n| {
                    n.checked_add((label.end - label.start).min(crate::params::READ_MAX_LEN))
                })
                .and_then(|n| n.checked_add(16))
                .ok_or("[worker] capture capacity overflow")?;
        }
        if audit {
            let name = self
                .adapter_names
                .iter()
                .chain(self.primer_names.iter())
                .flat_map(|v| v.iter())
                .map(String::len)
                .max()
                .unwrap_or(0)
                .max("inferred.r1".len());
            tags = tags
                .checked_add(
                    name.checked_add(64)
                        .and_then(|n| n.checked_mul(4))
                        .ok_or("[worker] audit capacity overflow")?,
                )
                .ok_or("[worker] audit capacity overflow")?;
        }
        let repetition_bytes=if self.repetition {raw.checked_mul(4).ok_or("[worker] repetition chunk capacity overflow")?}else{0};
        let screen_bytes = if self.screen.enabled() {
            raw.checked_mul(32)
                .and_then(|n| {
                    self.screen
                        .references
                        .len()
                        // Per resource/unit: two <=46-byte numeric fields; String
                        // payload200, Vec growth slots144, join94, formatted188,
                        // manifest old/new282, and count-vector overlap32 bytes.
                        // Sum940 rounds to1024; fixed row fields use4096 below.
                        .checked_mul(1024)
                        .and_then(|r| r.checked_add(4096))
                        .and_then(|m| m.checked_mul(units))
                        .and_then(|m| n.checked_add(m))
                })
                .ok_or("[worker] reference query capacity overflow")?
        } else {
            0
        };
        let overlap_bytes = if self.overlap.enabled() {
            raw.checked_mul(32)
                .and_then(|n| n.checked_add(3 * crate::native_qc::Qc::merged_maximum_heap_bytes()))
                .ok_or("[worker] overlap feature capacity overflow")?
        } else {
            0
        };
        tags.checked_mul(32)
            .and_then(|n| n.checked_add(10 * std::mem::size_of::<FastqRecord>() + 4096))
            .and_then(|n| n.checked_mul(units))
            .and_then(|n| {
                raw.checked_mul(if auxiliary || self.split_units > 0 {
                    48
                } else {
                    0
                })
                .and_then(|m| n.checked_add(m))
            })
            .and_then(|n| n.checked_add(10 * crate::native_execution::QC_BYTES))
            .and_then(|n| {
                self.split_metadata_unit_bound
                    .checked_mul(units)
                    .and_then(|m| n.checked_add(m))
            })
            .and_then(|n| n.checked_add(overlap_bytes))
            .and_then(|n| n.checked_add(screen_bytes))
            .and_then(|n| n.checked_add(repetition_bytes))
            .ok_or("[worker] optional channel capacity overflow".into())
    }
    pub fn runtime_settings_bound(&self) -> Result<usize, String> {
        let mut bytes = 8 * std::mem::size_of::<Self>() + 4096;
        for group in self
            .adapters
            .iter()
            .chain(self.primers.iter())
            .chain(self.adapter_names.iter())
            .chain(self.primer_names.iter())
        {
            for value in group {
                bytes = bytes
                    .checked_add(
                        value
                            .capacity()
                            .checked_mul(16)
                            .and_then(|n| n.checked_add(256))
                            .ok_or("[worker] adapter capacity overflow")?,
                    )
                    .ok_or("[worker] adapter capacity overflow")?;
            }
        }
        for label in &self.capture.labels {
            bytes = bytes
                .checked_add(
                    label
                        .name
                        .capacity()
                        .checked_mul(16)
                        .and_then(|n| n.checked_add(8 * std::mem::size_of::<LabelSpec>()))
                        .ok_or("[worker] label capacity overflow")?,
                )
                .ok_or("[worker] label capacity overflow")?;
        }
        Ok(bytes)
    }
    pub fn compile_three_prime(&mut self) -> Result<(), String> {
        for mate in 0..2 {
            if self.primers[mate].iter().any(|s| {
                s.is_empty()
                    || !s
                        .bytes()
                        .all(|b| matches!(b.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T'))
            }) {
                return Err("primer must contain canonical ACGT".into());
            }
            self.matchers[mate] = if self.adapters[mate].is_empty() {
                None
            } else {
                Some(AdapterMatcher::new(
                    &self.adapters[mate],
                    MatcherOptions {
                        min_overlap: self.min_overlap,
                        max_error: self.max_error,
                    },
                )?)
            };
        }
        Ok(())
    }
    pub fn compile(&mut self) -> Result<(), String> {
        self.compile_three_prime()?;
        for mate in 0..2 {
            self.primer_matchers[mate] = if self.primers[mate].is_empty() {
                None
            } else {
                Some(AdapterMatcher::new(
                    &self.primers[mate],
                    MatcherOptions {
                        min_overlap: self.min_overlap,
                        max_error: self.max_error,
                    },
                )?)
            };
        }
        Ok(())
    }
    pub fn srna_evidence(
        &self,
        read: &FastqRecord,
        tail_min: usize,
    ) -> ([Option<bool>; 4], Option<crate::native_adapter::Match>) {
        let hit = self.matchers[0]
            .as_ref()
            .and_then(|m| m.best_match(read.sequence.as_bytes()));
        let five = if self.srna_reject5 {
            Some(
                self.primer_matchers[0]
                    .as_ref()
                    .is_some_and(|m| m.best_match_anchored(read.sequence.as_bytes()).is_some()),
            )
        } else {
            None
        };
        let poly = if self.srna_reject_poly_a {
            Some(
                read.sequence
                    .as_bytes()
                    .iter()
                    .rev()
                    .take_while(|&&b| b == b'A')
                    .count()
                    >= tail_min,
            )
        } else {
            None
        };
        (
            [
                Some(hit.is_none()),
                hit.map(|h| h.start < self.srna_min_insert),
                five,
                poly,
            ],
            hit,
        )
    }
    pub fn enabled(&self, mate: usize) -> [bool; 9] {
        [
            self.adapter_discard
                && (!self.adapters[mate].is_empty()
                    || !self.primers[mate].is_empty()
                    || self.infer_mode == "both"
                    || self.infer_mode == if mate == 0 { "r1" } else { "r2" }),
            true,
            true,
            self.max_length.is_some(),
            self.max_n_count.is_some(),
            self.max_n_fraction.is_some(),
            self.low_q.is_some(),
            self.min_mean_q.is_some(),
            self.min_complexity.is_some(),
        ]
    }
    pub fn settings(&self) -> Value {
        let mut result = json!({"adapter3_r1":self.adapters[0],"adapter3_r2":self.adapters[1],"adapter_action":if self.adapter_discard {"discard"} else {"trim"},"adapter_min_overlap":self.min_overlap,"adapter_max_error":self.max_error,"min_length":self.min_length,"max_length":self.max_length,"max_n_count":self.max_n_count,"max_n_fraction":self.max_n_fraction,"low_q":self.low_q,"max_low_q_fraction":self.max_low_q_fraction,"min_mean_q":self.min_mean_q,"min_complexity":self.min_complexity,"pair_policy":self.pair_policy});
        if self.adapter_overlap { result["adapter_overlap"] = json!(true); }
        result
    }
    pub fn apply_adapter(&self, read: &mut FastqRecord, mate: usize) -> bool {
        self.apply_adapter_recorded(read, mate, None)
    }
    pub fn apply_adapter_recorded(
        &self,
        read: &mut FastqRecord,
        mate: usize,
        mut audit: Option<&mut Vec<SelectedAdapter>>,
    ) -> bool {
        let primer = self.primer_matchers[mate]
            .as_ref()
            .and_then(|m| m.best_match_anchored(read.sequence.as_bytes()));
        let mut discarded = false;
        if let Some(hit) = primer {
            if let Some(rows) = audit.as_deref_mut() {
                rows.push(SelectedAdapter {
                    mate,
                    adapter: self.primer_names[mate]
                        .get(hit.declared_order)
                        .unwrap_or(&self.primers[mate][hit.declared_order])
                        .clone(),
                    start: 0,
                    end: hit.compared,
                    mismatches: hit.mismatches,
                });
            }
            if self.adapter_discard {
                discarded = true;
            } else {
                read.sequence.drain(..hit.compared);
                read.qual_seq.drain(..hit.compared);
            }
        }
        let hit = self.matchers[mate]
            .as_ref()
            .and_then(|m| m.best_match(read.sequence.as_bytes()));
        if let Some(hit) = hit {
            if let Some(rows) = audit.as_deref_mut() {
                rows.push(SelectedAdapter {
                    mate,
                    adapter: self.adapter_names[mate]
                        .get(hit.declared_order)
                        .unwrap_or(&self.adapters[mate][hit.declared_order])
                        .clone(),
                    start: hit.start,
                    end: hit.start + hit.compared,
                    mismatches: hit.mismatches,
                });
            }
            if self.adapter_discard {
                return true;
            }
            read.sequence.truncate(hit.start);
            read.qual_seq.truncate(hit.start);
        }
        discarded
    }
    pub fn evaluate(
        &self,
        read: &FastqRecord,
        phred: i32,
        adapter_discard: bool,
        mate: usize,
    ) -> [Option<bool>; 9] {
        let enabled = self.enabled(mate);
        let len = read.sequence.len();
        let n = if self.max_n_count.is_some() || self.max_n_fraction.is_some() {
            read.sequence
                .bytes()
                .filter(|b| matches!(b, b'N' | b'n'))
                .count()
        } else {
            0
        };
        let mut values = [None; 9];
        if enabled[0] {
            values[0] = Some(adapter_discard);
        }
        values[1] = Some(len == 0);
        values[2] = Some(len < self.min_length);
        if let Some(max) = self.max_length {
            values[3] = Some(len > max);
        }
        if let Some(max) = self.max_n_count {
            values[4] = Some(n > max);
        }
        if len > 0 {
            if let Some(max) = self.max_n_fraction {
                values[5] = Some(n as f64 / len as f64 > max);
            }
            if self.low_q.is_some() || self.min_mean_q.is_some() {
                let mut sum = 0u64;
                let mut low = 0usize;
                for encoded in read.qual_seq.bytes() {
                    let q = (encoded as i32 - phred) as u64;
                    sum += q;
                    low += usize::from(self.low_q.is_some_and(|threshold| q < threshold as u64));
                }
                if let Some(max) = self.max_low_q_fraction {
                    values[6] = Some(low as f64 / len as f64 > max);
                }
                if let Some(min) = self.min_mean_q {
                    values[7] = Some((sum as f64 / len as f64) < min);
                }
            }
        }
        if let Some(minimum) = self.min_complexity {
            // fastp 1.4.0 Filter::passLowComplexityFilter includes every
            // adjacent byte comparison, including N, and rejects length <= 1.
            let changed = read.sequence.as_bytes().windows(2).filter(|p| p[0] != p[1]).count();
            values[8] = Some(len <= 1 || (changed as f64 / (len - 1) as f64) < minimum);
        }
        values
    }
}

#[derive(Clone, Default)]
pub struct FateStats {
    pub adapter_overlap_evaluated: u64,
    pub adapter_overlap_clipped: u64,
    pub adapter_overlap_ambiguous: u64,
    pub reference_first: u64,
    pub srna_evaluated: [u64; 4],
    pub srna_failed: [u64; 4],
    pub srna_first: [u64; 4],
    pub overlap_evaluated: u64,
    pub overlap_scored: u64,
    pub insert_failed: u64,
    pub insert_first: u64,
    pub merged: u64,
    pub unmerged: u64,
    pub corrected_bases: u64,
    pub selected: u64,
    pub not_selected: u64,
    pub mate_passed: [u64; 2],
    pub mate_failed: [u64; 2],
    pub evaluated: [[u64; 9]; 2],
    pub failed: [[u64; 9]; 2],
    pub not_evaluated: [[u64; 9]; 2],
    pub overlapping_units: [u64; 9],
    pub both_mates_failed: [u64; 9],
    pub first: [u64; 9],
    pub rejected: u64,
    pub capture_failed: [u64; 4],
    pub capture_evaluated: [u64; 4],
    pub capture_first: [u64; 4],
    pub fallback: u64,
    pub transformed: u64,
}
impl FateStats {
    pub fn record_capture(
        &mut self,
        failures: [bool; 4],
        settings: &ProcessSettings,
        paired: bool,
    ) -> bool {
        let enabled = [
            !settings.capture.labels.is_empty(),
            settings.capture.retain.iter().any(Option::is_some),
            settings.capture.min_q.is_some(),
            !settings.capture.labels.is_empty(),
        ];
        for i in 0..4 {
            if enabled[i] && (i < 2 || !(failures[0] || failures[1])) {
                self.capture_evaluated[i] += 1;
            }
        }
        let rejected = failures.iter().any(|v| *v);
        for (i, failed) in failures.iter().enumerate() {
            self.capture_failed[i] += u64::from(*failed);
        }
        if let Some(first) = failures.iter().position(|v| *v) {
            self.capture_first[first] += 1;
        }
        if rejected {
            self.record([[None; 9]; 2], settings, paired);
            self.rejected += 1;
            self.fallback += 1;
        }
        rejected
    }

    pub fn record(
        &mut self,
        values: [[Option<bool>; 9]; 2],
        settings: &ProcessSettings,
        paired: bool,
    ) -> bool {
        let failed = values.map(|v| v.iter().any(|x| *x == Some(true)));
        let reject = if !paired {
            failed[0]
        } else {
            match settings.pair_policy.as_str() {
                "either-pass" => failed[0] && failed[1],
                "r1-pass" => failed[0],
                _ => failed[0] || failed[1],
            }
        };
        for mate in 0..if paired { 2 } else { 1 } {
            if values[mate].iter().any(Option::is_some) {
                self.mate_failed[mate] += u64::from(failed[mate]);
                self.mate_passed[mate] += u64::from(!failed[mate]);
            }
        }
        let mut first = true;
        for (reason, _) in REASONS.iter().enumerate() {
            let mut failures = 0;
            for (mate, values) in values.iter().enumerate().take(if paired { 2 } else { 1 }) {
                match values[reason] {
                    Some(failed) => {
                        self.evaluated[mate][reason] += 1;
                        self.failed[mate][reason] += u64::from(failed);
                        failures += usize::from(failed);
                    }
                    None if settings.enabled(mate)[reason] => self.not_evaluated[mate][reason] += 1,
                    None => (),
                }
            }
            if failures > 0 {
                self.overlapping_units[reason] += 1;
                self.both_mates_failed[reason] += u64::from(paired && failures == 2);
                let eligible =
                    !paired || settings.pair_policy != "r1-pass" || values[0][reason] == Some(true);
                if reject && first && eligible {
                    self.first[reason] += 1;
                    first = false;
                }
            }
        }
        self.rejected += u64::from(reject);
        reject
    }
    pub fn merge(&mut self, src: &Self) {
        self.adapter_overlap_evaluated += src.adapter_overlap_evaluated;
        self.adapter_overlap_clipped += src.adapter_overlap_clipped;
        self.adapter_overlap_ambiguous += src.adapter_overlap_ambiguous;
        self.reference_first += src.reference_first;
        for i in 0..4 {
            self.srna_evaluated[i] += src.srna_evaluated[i];
            self.srna_failed[i] += src.srna_failed[i];
            self.srna_first[i] += src.srna_first[i];
        }
        self.overlap_evaluated += src.overlap_evaluated;
        self.overlap_scored += src.overlap_scored;
        self.insert_failed += src.insert_failed;
        self.insert_first += src.insert_first;
        self.merged += src.merged;
        self.unmerged += src.unmerged;
        self.corrected_bases += src.corrected_bases;
        self.selected += src.selected;
        self.not_selected += src.not_selected;
        for mate in 0..2 {
            self.mate_passed[mate] += src.mate_passed[mate];
            self.mate_failed[mate] += src.mate_failed[mate];
        }
        for reason in 0..REASONS.len() {
            for mate in 0..2 {
                self.evaluated[mate][reason] += src.evaluated[mate][reason];
                self.failed[mate][reason] += src.failed[mate][reason];
                self.not_evaluated[mate][reason] += src.not_evaluated[mate][reason];
            }
            self.overlapping_units[reason] += src.overlapping_units[reason];
            self.both_mates_failed[reason] += src.both_mates_failed[reason];
            self.first[reason] += src.first[reason];
        }
        self.rejected += src.rejected;
        self.fallback += src.fallback;
        self.transformed += src.transformed;
        for i in 0..4 {
            self.capture_failed[i] += src.capture_failed[i];
            self.capture_evaluated[i] += src.capture_evaluated[i];
            self.capture_first[i] += src.capture_first[i];
        }
    }
    pub fn report(&self, settings: &ProcessSettings, paired: bool, input: [u64; 2]) -> Value {
        let mut map = serde_json::Map::new();
        for (i, reason) in REASONS.iter().enumerate() {
            if i == 8 && settings.min_complexity.is_none() {
                continue;
            }
            let mates: Vec<Value> = (0..if paired {2} else {1}).map(|mate| json!({"mate":mate+1,"enabled":settings.enabled(mate)[i],"evaluated":self.evaluated[mate][i],"failed":self.failed[mate][i],"not_evaluated":self.not_evaluated[mate][i],"disabled":if settings.enabled(mate)[i] {0} else {input[mate]}})).collect();
            map.insert((*reason).into(), json!({"mates":mates,"overlapping_failed_units":self.overlapping_units[i],"both_mates_failed_units":self.both_mates_failed[i],"first_rejected_units":self.first[i]}));
        }
        Value::Object(map)
    }
}

pub fn suffix_key(sequence: &[u8]) -> Option<u128> {
    if !(6..=30).contains(&sequence.len()) {
        return None;
    }
    let mut code = 0u128;
    for base in sequence {
        code = (code << 2)
            | match base.to_ascii_uppercase() {
                b'A' => 0,
                b'C' => 1,
                b'G' => 2,
                b'T' => 3,
                _ => return None,
            };
    }
    Some(((sequence.len() as u128) << 64) | code)
}
pub fn select_inferred(keys: &mut [u128], sampled: u64, minimum: f64) -> Option<(String, u64)> {
    if sampled == 0 {
        return None;
    }
    keys.sort_unstable();
    let mut best: Option<(u64, usize, u128)> = None;
    let mut i = 0;
    while i < keys.len() {
        let key = keys[i];
        let mut end = i + 1;
        while end < keys.len() && keys[end] == key {
            end += 1;
        }
        let count = (end - i) as u64;
        let len = (key >> 64) as usize;
        if count as f64 / sampled as f64 >= minimum
            && best
                .is_none_or(|(c, l, k)| count > c || count == c && (len > l || len == l && key < k))
        {
            best = Some((count, len, key));
        }
        i = end;
    }
    best.map(|(count, len, key)| {
        let mut text = vec![b'A'; len];
        let mut bits = key as u64;
        for i in (0..len).rev() {
            text[i] = b"ACGT"[(bits & 3) as usize];
            bits >>= 2;
        }
        (String::from_utf8(text).unwrap(), count)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn audit_selected_coordinates_are_recomputed_after_the_five_prime_cut() {
        let mut s = ProcessSettings::default();
        s.primers[0] = vec!["GGG".into()];
        s.adapters[0] = vec!["TTT".into()];
        s.primer_names[0] = vec!["p5".into()];
        s.adapter_names[0] = vec!["p3".into()];
        s.max_error = 0.0;
        s.compile().unwrap();
        let mut r = read("GGGACTTT", &[40; 8]);
        let mut rows = Vec::new();
        assert!(!s.apply_adapter_recorded(&mut r, 0, Some(&mut rows)));
        assert_eq!(r.sequence, "AC");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (&rows[0].adapter, rows[0].start, rows[0].end),
            (&"p5".into(), 0, 3)
        );
        assert_eq!(
            (&rows[1].adapter, rows[1].start, rows[1].end),
            (&"p3".into(), 2, 5)
        );
    }
    #[test]
    fn inference_ranking_uses_support_then_length_then_lexical_and_ignores_ambiguous_suffixes() {
        let mut keys = vec![
            suffix_key(b"AGATCG").unwrap(),
            suffix_key(b"AGATCG").unwrap(),
            suffix_key(b"CTAGCT").unwrap(),
            suffix_key(b"CTAGCT").unwrap(),
        ];
        assert_eq!(
            select_inferred(&mut keys, 4, 0.5),
            Some(("AGATCG".into(), 2))
        );
        assert!(select_inferred(&mut keys, 4, 0.75).is_none());
        let mut keys = vec![
            suffix_key(b"AGATCG").unwrap(),
            suffix_key(b"TAGATCG").unwrap(),
        ];
        assert_eq!(
            select_inferred(&mut keys, 1, 1.0),
            Some(("TAGATCG".into(), 1))
        );
        assert!(suffix_key(b"AGATCN").is_none());
        assert!(suffix_key(b"AGATC").is_none());
        assert_eq!(suffix_key(b"agatcg"), suffix_key(b"AGATCG"));
        assert!(select_inferred(&mut [], 0, 0.0).is_none());
    }
    #[test]
    fn five_prime_precedes_three_prime_and_both_quality_slices_follow_the_sequence() {
        let mut s = ProcessSettings::default();
        s.primers[0] = vec!["GGG".into()];
        s.adapters[0] = vec!["TTT".into()];
        s.max_error = 0.0;
        s.compile().unwrap();
        let mut r = read("GGGACTTT", &[0, 1, 2, 3, 4, 5, 6, 7]);
        assert!(!s.apply_adapter(&mut r, 0));
        assert_eq!(r.sequence, "AC");
        assert_eq!(r.qual_seq.as_bytes(), &[36, 37]);
        let mut r = read("AAGGG", &[40; 5]);
        assert!(!s.apply_adapter(&mut r, 0));
        assert_eq!(r.sequence, "AAGGG");
        s.adapter_discard = true;
        let mut r = read("GGGACTTT", &[40; 8]);
        assert!(s.apply_adapter(&mut r, 0));
        assert_eq!(r.sequence, "GGGACTTT");
    }
    #[test]
    fn capture_is_atomic_on_both_mates_and_preserves_comments_and_original_coordinates() {
        let mut settings = CaptureSettings::default();
        settings.labels = vec![LabelSpec::parse("U@r1:1:2").unwrap()];
        settings.remove = true;
        let mut a = read("AACCGG", &[40; 6]);
        a.seq_id = "@pair/1 full comment".into();
        a.source_index = 0;
        let mut b = read("TGCA", &[40; 4]);
        b.seq_id = "@pair/2 mate".into();
        let original = (a.clone(), b.clone());
        settings.labels.push(LabelSpec::parse("V@r2:5:1").unwrap());
        assert_eq!(
            settings.apply(&mut [&mut a, &mut b], 33).unwrap(),
            [true, false, false, false]
        );
        assert_eq!(a.seq_id, original.0.seq_id);
        assert_eq!(a.sequence, original.0.sequence);
        assert_eq!(b.sequence, original.1.sequence);
        settings.labels.pop();
        settings.min_q = Some(41);
        assert_eq!(
            settings.apply(&mut [&mut a, &mut b], 33).unwrap(),
            [false, false, true, false]
        );
        assert_eq!(a.seq_id, original.0.seq_id);
        settings.min_q = Some(40);
        assert_eq!(
            settings.apply(&mut [&mut a, &mut b], 33).unwrap(),
            [false; 4]
        );
        assert_eq!(a.sequence, "CCGG");
        assert_eq!(a.qual_seq, "IIII");
        assert_eq!(a.seq_id, "@pair/1 full comment RX:Z:U=AA");
        assert_eq!(b.seq_id, "@pair/2 mate RX:Z:U=AA");
    }
    #[test]
    fn capture_conflicts_never_commit_partial_metadata_and_ranges_are_checked() {
        let mut settings = CaptureSettings::default();
        settings.labels = vec![LabelSpec::parse("U@r1:1:2").unwrap()];
        assert!(LabelSpec::parse("U@r1:0:2").is_err());
        assert!(LabelSpec::parse("U@r1:18446744073709551615:9").is_err());
        let mut a = read("AACCGG", &[40; 6]);
        a.seq_id = "@p/1 comment".into();
        let mut b = read("TGCA", &[40; 4]);
        b.seq_id = "@p/2 RX:Z:U=TT".into();
        assert!(
            settings
                .apply(&mut [&mut a, &mut b], 33)
                .unwrap_err()
                .contains("conflict")
        );
        assert_eq!(a.seq_id, "@p/1 comment");
        b.seq_id = "@p/2 RX:Z:U=AA".into();
        assert_eq!(
            settings.apply(&mut [&mut a, &mut b], 33).unwrap(),
            [false; 4]
        );
        assert_eq!(b.seq_id, "@p/2 RX:Z:U=AA");
        settings.remove = true;
        settings.retain[0] = Some([1, 5]);
        let seq = a.sequence.clone();
        assert_eq!(
            settings.apply(&mut [&mut a], 33).unwrap(),
            [false, true, false, false]
        );
        assert_eq!(a.sequence, seq);
        settings.retain[0] = Some([2, 5]);
        assert_eq!(settings.apply(&mut [&mut a], 33).unwrap(), [false; 4]);
        assert_eq!(a.sequence, "CCG");
    }
    #[test]
    fn capture_quality_and_n_are_evaluated_together_before_any_mutation() {
        let mut s = CaptureSettings::default();
        s.labels = vec![LabelSpec::parse("U@r1:1:2").unwrap()];
        s.min_q = Some(20);
        s.remove = true;
        let mut r = read("NACC", &[40, 0, 40, 40]);
        r.seq_id = "@original comment".into();
        assert_eq!(
            s.apply(&mut [&mut r], 33).unwrap(),
            [false, false, true, true]
        );
        assert_eq!(r.seq_id, "@original comment");
        assert_eq!(r.sequence, "NACC");
    }
    #[test]
    fn complexity_matches_fastp_adjacent_bytes_and_short_read_boundary() {
        for (seq, min, failed) in [
            ("AACC", 0.5, true),
            ("ACCA", 0.5, false),
            ("ANCT", 1.0, false),
            ("NN", 0.5, true),
            ("A", 0.0, true),
            ("", 0.5, true),
            ("acgt", 1.0, false),
            ("AAAAANAAAAA", 0.2, false),
            ("AAAAANAAAAA", 0.21, true),
            ("AaAa", 1.0, false),
            ("NN", 0.0, false),
        ] {
            let mut settings = ProcessSettings::default();
            settings.min_complexity = Some(min);
            let r = read(seq, &vec![40; seq.len()]);
            assert_eq!(settings.evaluate(&r, 33, false, 0)[8], Some(failed));
        }
    }
    fn read(seq: &str, qualities: &[u8]) -> FastqRecord {
        let mut r = FastqRecord::new(1);
        r.sequence = seq.into();
        r.qual_seq = qualities.iter().map(|q| char::from(q + 33)).collect();
        r
    }
    #[test]
    fn strict_count_fraction_and_fractional_mean_boundaries() {
        let mut settings = ProcessSettings::default();
        settings.max_n_count = Some(1);
        settings.max_n_fraction = Some(0.5);
        settings.low_q = Some(20);
        settings.max_low_q_fraction = Some(0.5);
        settings.min_mean_q = Some(20.5);
        let r = read("NA", &[20, 21]);
        let values = settings.evaluate(&r, 33, false, 0);
        assert_eq!(
            values[4..8],
            [Some(false), Some(false), Some(false), Some(false)]
        );
        settings.min_mean_q = Some(20.5001);
        assert_eq!(settings.evaluate(&r, 33, false, 0)[7], Some(true));
        let r = read("NN", &[19, 20]);
        let values = settings.evaluate(&r, 33, false, 0);
        assert_eq!(
            values[4..8],
            [Some(true), Some(true), Some(false), Some(true)]
        );
        settings.max_low_q_fraction = Some(0.499);
        assert_eq!(settings.evaluate(&r, 33, false, 0)[6], Some(true));
    }
    #[test]
    fn empty_has_unevaluated_ratios_and_all_available_failures() {
        let mut settings = ProcessSettings::default();
        settings.max_n_count = Some(0);
        settings.max_n_fraction = Some(0.0);
        settings.low_q = Some(0);
        settings.max_low_q_fraction = Some(0.0);
        settings.min_mean_q = Some(0.0);
        let values = settings.evaluate(&read("", &[]), 33, false, 0);
        assert_eq!(
            values,
            [
                None,
                Some(true),
                Some(true),
                None,
                Some(false),
                None,
                None,
                None,
                None
            ]
        );
        let mut stats = FateStats::default();
        assert!(stats.record([values, [None; 9]], &settings, false));
        assert_eq!(stats.first, [0, 1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(stats.overlapping_units[2], 1);
        assert_eq!(stats.not_evaluated[0][5..8], [1, 1, 1]);
    }
    #[test]
    fn both_pass_reports_mate_overlap_and_only_one_first_reason() {
        let mut settings = ProcessSettings::default();
        settings.max_n_count = Some(0);
        settings.min_length = 3;
        let a = settings.evaluate(&read("N", &[40]), 33, false, 0);
        let b = settings.evaluate(&read("AA", &[40, 40]), 33, false, 1);
        let mut stats = FateStats::default();
        assert!(stats.record([a, b], &settings, true));
        assert_eq!(stats.rejected, 1);
        assert_eq!(stats.first[2], 1);
        assert_eq!(stats.first[4], 0);
        assert_eq!(stats.both_mates_failed[2], 1);
        assert_eq!(stats.overlapping_units[4], 1);
        let mut total = FateStats::default();
        total.merge(&stats);
        total.merge(&stats);
        assert_eq!(total.rejected, 2);
        assert_eq!(total.failed[0][4], 2);
    }
    #[test]
    fn adapter_trim_and_discard_preserve_aligned_quality() {
        let mut settings = ProcessSettings::default();
        settings.adapters[0] = vec!["ACGT".into()];
        settings.max_error = 0.0;
        settings.compile().unwrap();
        let mut r = read("TTACG", &[5, 10, 20, 30, 40]);
        assert!(!settings.apply_adapter(&mut r, 0));
        assert_eq!(r.sequence, "TT");
        assert_eq!(r.qual_seq, "&+");
        settings.adapter_discard = true;
        let mut r = read("TTACG", &[5, 10, 20, 30, 40]);
        assert!(settings.apply_adapter(&mut r, 0));
        assert_eq!(r.sequence, "TTACG");
    }
}

// The accepted test contract hashes exactly one 16-byte message, so SHA256 uses
// one stack-owned block and never allocates or invokes an external process.
pub fn sample_hash(seed: u64, ordinal: u64) -> u64 {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let initial = [
        0x6a09e667u32,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut w = [0u32; 64];
    w[0] = (seed >> 32) as u32;
    w[1] = seed as u32;
    w[2] = (ordinal >> 32) as u32;
    w[3] = ordinal as u32;
    w[4] = 0x80000000;
    w[15] = 128;
    for i in 16..64 {
        let x = w[i - 15];
        let y = w[i - 2];
        let a = x.rotate_right(7) ^ x.rotate_right(18) ^ (x >> 3);
        let b = y.rotate_right(17) ^ y.rotate_right(19) ^ (y >> 10);
        w[i] = w[i - 16]
            .wrapping_add(a)
            .wrapping_add(w[i - 7])
            .wrapping_add(b);
    }
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = initial;
    for i in 0..64 {
        let t1 = h
            .wrapping_add(e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25))
            .wrapping_add((e & f) ^ (!e & g))
            .wrapping_add(K[i])
            .wrapping_add(w[i]);
        let t2 = (a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22))
            .wrapping_add((a & b) ^ (a & c) ^ (b & c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    ((initial[0].wrapping_add(a) as u64) << 32) | initial[1].wrapping_add(b) as u64
}
pub fn sample_threshold(fraction: f64) -> u128 {
    assert!(fraction.is_finite() && (0.0..=1.0).contains(&fraction));
    if fraction == 0.0 {
        return 0;
    }
    if fraction == 1.0 {
        return 1u128 << 64;
    }
    let bits = fraction.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = (bits & ((1u64 << 52) - 1)) | if exponent == 0 { 0 } else { 1u64 << 52 };
    let shift = if exponent == 0 {
        -1010
    } else {
        exponent - 1011
    };
    // H/2^64 < p is exactly H < ceil(p*2^64), including subnormal p.
    if shift >= 0 {
        (mantissa as u128) << (shift as u32)
    } else {
        let n = (-shift) as u32;
        if n >= 64 {
            1
        } else {
            ((mantissa >> n) + u64::from(mantissa & ((1u64 << n) - 1) != 0)) as u128
        }
    }
}
impl ProcessSettings {
    pub fn selects(&self, ordinal: u64) -> bool {
        self.sample_fraction == 1.0
            || (self.sample_fraction > 0.0
                && (sample_hash(self.sample_seed, ordinal) as u128)
                    < sample_threshold(self.sample_fraction))
    }
}
#[cfg(test)]
mod selection_tests {
    use super::*;
    #[test]
    fn exact_sampling_matches_independent_sha256_literal_anchors_and_binary64_boundaries() {
        assert_eq!(sample_hash(0u64, 0u64), 3983162290893594069u64);
        assert_eq!(sample_hash(0u64, 1u64), 8952255631054521211u64);
        assert_eq!(sample_hash(0u64, 2500u64), 15717293248599134447u64);
        assert_eq!(
            sample_hash(0u64, 18446744073709551615u64),
            8681103821628823450u64
        );
        assert_eq!(sample_hash(17u64, 0u64), 11247560618850451649u64);
        assert_eq!(sample_hash(17u64, 1u64), 11125534065297799009u64);
        assert_eq!(sample_hash(17u64, 2500u64), 9757631327567844013u64);
        assert_eq!(
            sample_hash(17u64, 18446744073709551615u64),
            2553541081303656397u64
        );
        assert_eq!(
            sample_hash(18446744073709551615u64, 0u64),
            6973430666385906767u64
        );
        assert_eq!(
            sample_hash(18446744073709551615u64, 1u64),
            9110530055553361591u64
        );
        assert_eq!(
            sample_hash(18446744073709551615u64, 2500u64),
            3807662602079279943u64
        );
        assert_eq!(
            sample_hash(18446744073709551615u64, 18446744073709551615u64),
            6541097565471264777u64
        );
        assert_eq!(sample_threshold(0.0), 0);
        assert_eq!(sample_threshold(-0.0), 0);
        assert_eq!(sample_threshold(1.0), 1u128 << 64);
        assert_eq!(sample_threshold(0.5), 1u128 << 63);
        assert_eq!(sample_threshold(f64::from_bits(1)), 1);
        let p = f64::from_bits(0.5f64.to_bits() + 1);
        assert_eq!(sample_threshold(p), (1u128 << 63) + 2048);
        let p = f64::from_bits(0.5f64.to_bits() - 1);
        assert_eq!(sample_threshold(p), (1u128 << 63) - 1024);
    }
}
