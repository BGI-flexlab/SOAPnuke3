use crate::failure::Failure;
use crate::params::{GlobalParams, READ_MAX_LEN};
use crate::record::FastqRecord;
use crate::stats::{FileStat, PeLocalStats};

pub(crate) fn validate(read: &FastqRecord, phred: i32) -> Result<(), String> {
    if read.sequence.len() > READ_MAX_LEN {
        return Err(format!(
            "input: record {} mate {} length exceeds supported limit {}",
            read.source_index + 1,
            read.pair_pos,
            READ_MAX_LEN
        ));
    }
    for (&base, &encoded) in read
        .sequence
        .as_bytes()
        .iter()
        .zip(read.qual_seq.as_bytes())
    {
        if !matches!(base.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T' | b'N') {
            return Err(format!(
                "input: record {} mate {} unsupported non-ACGTN base",
                read.source_index + 1,
                read.pair_pos
            ));
        }
        if encoded as i32 - phred < 0 || encoded > 126 {
            return Err(format!(
                "input: quality.encoding record {} mate {} is outside declared Phred{}",
                read.source_index + 1,
                read.pair_pos,
                phred
            ));
        }
    }
    Ok(())
}
pub(crate) fn validate_normalize(read:&mut FastqRecord, fallback:i32)->Result<(),String>{
    let declared=read.input_phred.unwrap_or(fallback);
    validate(read,declared)?;
    if read.input_phred.is_some(){
        if declared!=33 {for b in unsafe{read.qual_seq.as_mut_vec()}{*b=(*b as i32-declared+33)as u8;}}
        read.input_phred=Some(33);
    }
    Ok(())
}
pub(crate) fn observe(read: &FastqRecord, stat: &mut FileStat, phred: i32) {
    stat.native_qc
        .as_mut()
        .expect("native QC initialized")
        .observe_validated(read.sequence.as_bytes(), read.qual_seq.as_bytes(), phred);
}

fn output_representable(read: &FastqRecord, gp: &GlobalParams) -> Result<(), String> {
    output_representable_channel(read,gp,if read.pair_pos==2{"R2"}else{"R1"})
}
fn output_representable_channel(read:&FastqRecord,gp:&GlobalParams,channel:&str)->Result<(),String>{
    let target=gp.native_io.as_ref().map_or(gp.output_quality_phred,|io|io.output_phred(channel));
    if gp.quality_phred == target {
        return Ok(());
    }
    if read
        .qual_seq
        .bytes()
        .any(|b| b as i32 - gp.quality_phred + target > 126)
    {
        return Err(format!(
            "input: quality.unrepresentable record {} mate {}",
            read.source_index + 1,
            read.pair_pos
        ));
    }
    Ok(())
}

fn clip(read: &mut FastqRecord, [head, tail]: [usize; 2]) {
    if head == 0 && tail == 0 {
        return;
    }
    let end = read.sequence.len().saturating_sub(tail);
    if head >= end {
        read.sequence.clear();
        read.qual_seq.clear();
    } else {
        read.sequence.truncate(end);
        read.qual_seq.truncate(end);
        if head > 0 {
            read.sequence.drain(..head);
            read.qual_seq.drain(..head);
        }
    }
}

fn peel_quality_ends(read: &mut FastqRecord, threshold: Option<u8>, phred: i32) {
    let Some(threshold) = threshold else {
        return;
    };
    let quality = read.qual_seq.as_bytes();
    let mut head = 0;
    let mut end = quality.len();
    while head < end && quality[head] as i32 - phred < threshold as i32 {
        head += 1;
    }
    while end > head && quality[end - 1] as i32 - phred < threshold as i32 {
        end -= 1;
    }
    clip(read, [head, quality.len() - end]);
}

fn trim_tail(read: &mut FastqRecord, gp: &GlobalParams) {
    if gp.native_tail_mask == 0 {
        return;
    }
    let sequence = read.sequence.as_bytes();
    let Some(&last) = sequence.last() else {
        return;
    };
    let base = last.to_ascii_uppercase();
    let bit = match base {
        b'A' => 1,
        b'C' => 2,
        b'G' => 4,
        b'T' => 8,
        b'N' => 16,
        _ => 0,
    };
    if gp.native_tail_mask & bit == 0 {
        return;
    }
    let mut start = sequence.len();
    while start > 0 && sequence[start - 1].to_ascii_uppercase() == base {
        start -= 1;
    }
    if sequence.len() - start >= gp.native_tail_min {
        read.sequence.truncate(start);
        read.qual_seq.truncate(start);
    }
}

fn trim_quality(read: &mut FastqRecord, gp: &GlobalParams) {
    let Some(threshold) = gp.native_quality_q else {
        return;
    };
    if gp.native_quality_mode == crate::native_processing::QualityMode::Ends {
        peel_quality_ends(read, Some(threshold), gp.quality_phred);
        return;
    }
    if gp.native_quality_mode == crate::native_processing::QualityMode::Mott {
        let mut score = 0i32;
        let mut start = 0usize;
        let mut best_score = 0i32;
        let mut best = [0usize, 0usize];
        for (i, &encoded) in read.qual_seq.as_bytes().iter().enumerate() {
            score += encoded as i32 - gp.quality_phred - threshold as i32;
            if score > best_score {
                best_score = score;
                best = [start, i + 1];
            }
            // Keep a zero-score prefix: it gives the earliest starting tie.
            if score < 0 {
                score = 0;
                start = i + 1;
            }
        }
        clip(read, [best[0], read.sequence.len() - best[1]]);
        return;
    }
    let qualities = read.qual_seq.as_bytes();
    let width = gp.native_quality_window.min(qualities.len());
    if width == 0 {
        return;
    }
    // At most1000 validated bases; wide integer arithmetic avoids rounding means.
    let mut sum: u64 = qualities[..width]
        .iter()
        .map(|&q| (q as i32 - gp.quality_phred) as u64)
        .sum();
    let target = threshold as u64 * width as u64;
    let mut cut = qualities.len();
    for start in 0..=qualities.len() - width {
        if sum < target {
            cut = start;
            break;
        }
        if start + width < qualities.len() {
            sum += (qualities[start + width] as i32 - gp.quality_phred) as u64;
            sum -= (qualities[start] as i32 - gp.quality_phred) as u64;
        }
    }
    read.sequence.truncate(cut);
    read.qual_seq.truncate(cut);
}

fn apply_stages_recorded(
    read: &mut FastqRecord,
    gp: &GlobalParams,
    mate: usize,
    mut audit: Option<&mut Vec<crate::native_processing::SelectedAdapter>>,
) -> bool {
    use crate::native_processing::{DEFAULT_STAGE_ORDER, ProcessStage};
    if gp.native_stage_order == DEFAULT_STAGE_ORDER {
        trim_tail(read, gp);
        trim_quality(read, gp);
        return gp.native_process.apply_adapter_recorded(read, mate, audit);
    }
    let mut discard = false;
    for stage in gp.native_stage_order {
        match stage {
            ProcessStage::Tails => trim_tail(read, gp),
            ProcessStage::Quality => trim_quality(read, gp),
            ProcessStage::Adapters => {
                discard = gp
                    .native_process
                    .apply_adapter_recorded(read, mate, audit.as_deref_mut())
            }
        }
    }
    discard
}

fn apply_pair_stages_recorded(
    r1: &mut FastqRecord,
    r2: &mut FastqRecord,
    gp: &GlobalParams,
    mut audit: Option<&mut Vec<crate::native_processing::SelectedAdapter>>,
) -> (bool, bool, crate::native_adapter_overlap::Decision) {
    use crate::native_processing::ProcessStage;
    let settings = &gp.native_process;
    let mut discarded = [false; 2];
    let mut decision = crate::native_adapter_overlap::Decision::NoEvidence;
    for stage in gp.native_stage_order {
        match stage {
            ProcessStage::Tails => {
                trim_tail(r1, gp);
                trim_tail(r2, gp);
            }
            ProcessStage::Quality => {
                trim_quality(r1, gp);
                trim_quality(r2, gp);
            }
            ProcessStage::Adapters => {
                decision = crate::native_adapter_overlap::select(
                    r1.sequence.as_bytes(), r2.sequence.as_bytes(),
                    [&settings.adapters[0], &settings.adapters[1]],
                );
                // Preserve any stronger existing sequence-based clip. A09 may
                // add clipping, but must never rescue a region the ordinary
                // supplied-adapter matcher would already remove.
                if let crate::native_adapter_overlap::Decision::Clip { insert, .. } = decision {
                    let earlier = [(&*r1, 0), (&*r2, 1)].into_iter().any(|(read, mate)| {
                        settings.matchers[mate].as_ref()
                            .and_then(|m| m.best_match(read.sequence.as_bytes()))
                            .is_some_and(|hit| hit.start < insert)
                    });
                    if earlier { decision = crate::native_adapter_overlap::Decision::NoEvidence; }
                }
                if let crate::native_adapter_overlap::Decision::Clip { insert, adapters, mismatches } = decision {
                    if let Some(rows) = audit.as_deref_mut() {
                        for (mate, read) in [&*r1, &*r2].into_iter().enumerate() {
                            rows.push(crate::native_processing::SelectedAdapter {
                                mate,
                                adapter: format!("overlap:{}", settings.adapter_names[mate][adapters[mate]]),
                                start: insert,
                                end: read.sequence.len(),
                                mismatches: mismatches[mate],
                            });
                        }
                    }
                    r1.sequence.truncate(insert);
                    r1.qual_seq.truncate(insert);
                    r2.sequence.truncate(insert);
                    r2.qual_seq.truncate(insert);
                }
                discarded[0] = settings.apply_adapter_recorded(r1, 0, audit.as_deref_mut());
                discarded[1] = settings.apply_adapter_recorded(r2, 1, audit.as_deref_mut());
            }
        }
    }
    (discarded[0], discarded[1], decision)
}

#[cfg(test)]
fn apply_stages(read: &mut FastqRecord, gp: &GlobalParams, mate: usize) -> bool {
    apply_stages_recorded(read, gp, mate, None)
}

pub struct DiagnosticBatch {
    pub reference: Option<crate::native_screen::Evidence>,
    pub srna: [Option<bool>; 4],
    pub merged: Vec<FastqRecord>,
    pub selected_ordinals: Vec<u64>,
    pub singletons: [Vec<FastqRecord>; 2],
    pub rejected: [Vec<FastqRecord>; 2],
    pub transformed: [Vec<FastqRecord>; 2],
    pub stages: [crate::native_qc::Qc; 4],
    pub manifest: Vec<u8>,
    pub matches: Vec<crate::native_processing::MatchedUnit>,
}
impl DiagnosticBatch {
    pub fn new(maximum: usize) -> Self {
        Self {
            reference: None,
            srna: [None; 4],
            merged: Vec::new(),
            selected_ordinals: Vec::new(),
            singletons: Default::default(),
            rejected: Default::default(),
            transformed: Default::default(),
            stages: std::array::from_fn(|_| {
                let mut q = crate::native_qc::Qc::batch();
                q.set_batch_maximum(maximum);
                q
            }),
            manifest: Vec::new(),
            matches: Vec::new(),
        }
    }
    fn record(
        &mut self,
        r1: &FastqRecord,
        r2: Option<&FastqRecord>,
        gp: &GlobalParams,
        fallback: bool,
        rejected: bool,
        capture: [bool; 4],
        values: [[Option<bool>; 9]; 2],
        overlap: Option<crate::native_overlap::Evidence>,
    ) -> Result<(), String> {
        let mate_failed = values.map(|v| v.iter().any(|x| *x == Some(true)));
        let mate_rejected = if r2.is_none() {
            mate_failed[0]
        } else {
            match gp.native_process.pair_policy.as_str() {
                "either-pass" => mate_failed[0] && mate_failed[1],
                "r1-pass" => mate_failed[0],
                _ => mate_failed[0] || mate_failed[1],
            }
        };
        let mut membership = Vec::new();
        for (mate, read) in std::iter::once(r1).chain(r2).enumerate() {
            self.stages[if fallback { 2 + mate } else { mate }].observe_validated(
                read.sequence.as_bytes(),
                read.qual_seq.as_bytes(),
                gp.quality_phred,
            );
            let needs_reject = rejected && !gp.native_aux[mate].is_empty();
            let needs_transformed = !gp.native_aux[2 + mate].is_empty();
            let rescue = !gp
                .native_process
                .overlap
                .min_insert
                .zip(overlap)
                .is_some_and(|(minimum, e)| e.insert < minimum)
                && !self.reference.as_ref().is_some_and(|e| e.rejected)
                && rejected
                && !fallback
                && r2.is_some()
                && !values[mate].iter().any(|v| *v == Some(true))
                && !gp.native_aux[4 + mate].is_empty();
            if rescue {
                self.singletons[mate].push(read.clone());
                membership.push(format!("singleton{}", mate + 1));
            }
            if needs_reject || needs_transformed || rescue {
                output_representable(read, gp)?;
            }
            if needs_reject {
                self.rejected[mate].push(read.clone());
                membership.push(format!("reject{}", mate + 1));
            }
            if needs_transformed {
                self.transformed[mate].push(read.clone());
                membership.push(format!("transformed{}", mate + 1));
            }
            if !rejected && gp.native_process.selects(r1.source_index) {
                if !gp.native_process.overlap.merge
                    || (overlap.is_none() && !gp.native_process.overlap.discard_unmerged)
                {
                    membership.push(format!("accepted{}", mate + 1));
                } else if mate == 0 && overlap.is_some() {
                    membership.push("merged".into());
                }
            }
        }
        if !gp.native_record_manifest.is_empty() {
            let names = [
                "label-coordinate",
                "retain-coordinate",
                "label-quality",
                "label-N",
            ];
            let c = &gp.native_process.capture;
            let enabled = [
                !c.labels.is_empty(),
                c.retain.iter().any(Option::is_some),
                c.min_q.is_some(),
                !c.labels.is_empty(),
            ];
            let mut reasons = Vec::new();
            let mut evaluation = Vec::new();
            if gp.native_process.sample_fraction != 1.0 {
                evaluation.push(format!(
                    "selection={}",
                    if rejected {
                        "not-eligible"
                    } else if gp.native_process.selects(r1.source_index) {
                        "selected"
                    } else {
                        "not-selected"
                    }
                ));
            }
            for i in 0..4 {
                if capture[i] {
                    reasons.push(names[i]);
                }
                let state = if !enabled[i] {
                    "disabled"
                } else if i >= 2 && (capture[0] || capture[1]) {
                    "not-evaluated"
                } else if capture[i] {
                    "fail"
                } else {
                    "pass"
                };
                evaluation.push(format!("{}={state}", names[i]));
            }
            if gp.native_process.profile == "srna" {
                for (i, name) in [
                    "srna-missing-3prime",
                    "srna-short-insert",
                    "srna-5prime",
                    "srna-polyA",
                ]
                .iter()
                .enumerate()
                {
                    if self.srna[i] == Some(true) {
                        reasons.push(name);
                    }
                    let enabled = i < 2
                        || (i == 2 && gp.native_process.srna_reject5)
                        || (i == 3 && gp.native_process.srna_reject_poly_a);
                    let state = if !enabled {
                        "disabled"
                    } else {
                        match self.srna[i] {
                            Some(true) => "fail",
                            Some(false) => "pass",
                            None => "not-evaluated",
                        }
                    };
                    evaluation.push(format!("{name}={state}"));
                }
            }
            for mate in 0..if r2.is_some() { 2 } else { 1 } {
                if gp.native_process.pair_policy != "both-pass" || !gp.native_aux[4].is_empty() {
                    evaluation.push(format!(
                        "r{}.pass={}",
                        mate + 1,
                        if fallback {
                            "not-evaluated"
                        } else if values[mate].iter().any(|v| *v == Some(true)) {
                            "fail"
                        } else {
                            "pass"
                        }
                    ));
                }
                for (i, name) in crate::native_processing::REASONS.iter().enumerate() {
                    if rejected
                        && mate_rejected
                        && (gp.native_process.pair_policy != "r1-pass" || mate == 0)
                        && values[mate][i] == Some(true)
                        && !reasons.contains(name)
                    {
                        reasons.push(name);
                    }
                    let state = if !gp.native_process.enabled(mate)[i] {
                        "disabled"
                    } else {
                        match values[mate][i] {
                            Some(true) => "fail",
                            Some(false) => "pass",
                            None => "not-evaluated",
                        }
                    };
                    evaluation.push(format!("r{}.{name}={state}", mate + 1));
                }
            }
            if gp.native_process.overlap.min_insert.is_some() {
                let state = if fallback {
                    "not-evaluated"
                } else if let Some(e) = overlap {
                    if e.insert < gp.native_process.overlap.min_insert.unwrap() {
                        if rejected {
                            reasons.push("inferred-short-insert");
                        }
                        "fail"
                    } else {
                        "pass"
                    }
                } else {
                    "unscorable"
                };
                evaluation.push(format!("inferred-short-insert={state}"));
            }
            if let Some(reference) = &self.reference {
                for mate in 0..if r2.is_some() { 2 } else { 1 } {
                    evaluation.push(format!(
                        "r{}.reference={}",
                        mate + 1,
                        reference.mates[mate].name()
                    ));
                }
                evaluation.push(format!("reference-unit={}", reference.unit.name()));
                evaluation.push(format!(
                    "reference-action={}",
                    if fallback {
                        "not-evaluated"
                    } else if gp.native_process.screen.action == "inspect" {
                        "disabled"
                    } else if reference.rejected {
                        "fail"
                    } else {
                        "pass"
                    }
                ));
                if reference.rejected {
                    reasons.push("reference-action");
                }
                for (mate, counts) in
                    reference
                        .counts
                        .iter()
                        .enumerate()
                        .take(if r2.is_some() { 2 } else { 1 })
                {
                    for (i, count) in counts.iter().enumerate() {
                        evaluation.push(format!(
                            "r{}.reference{}.distinct={count}",
                            mate + 1,
                            i + 1
                        ));
                    }
                }
            }
            self.manifest.extend_from_slice(
                format!(
                    "{}\t{}\t{}\t{}\t{}\n",
                    r1.source_index,
                    if fallback {
                        "original-fallback"
                    } else {
                        "transformed"
                    },
                    reasons.join(","),
                    evaluation.join(";"),
                    membership.join(",")
                )
                .as_bytes(),
            );
        }
        Ok(())
    }
}
pub fn apply(
    reads1: Vec<FastqRecord>,
    reads2: Option<Vec<FastqRecord>>,
    local: &mut PeLocalStats,
    gp: &GlobalParams,
) -> Result<(Vec<FastqRecord>, Vec<FastqRecord>), String> {
    apply_recorded(reads1, reads2, local, gp, None)
}
pub fn apply_recorded(
    reads1: Vec<FastqRecord>,
    reads2: Option<Vec<FastqRecord>>,
    local: &mut PeLocalStats,
    gp: &GlobalParams,
    diagnostic: Option<&mut DiagnosticBatch>,
) -> Result<(Vec<FastqRecord>, Vec<FastqRecord>), String> {
    apply_recorded_ticket(reads1,reads2,local,gp,diagnostic,None).map_err(String::from)
}
pub fn apply_recorded_ticket(
    reads1: Vec<FastqRecord>,
    reads2: Option<Vec<FastqRecord>>,
    local: &mut PeLocalStats,
    gp: &GlobalParams,
    mut diagnostic: Option<&mut DiagnosticBatch>,
    mut ticket:Option<&mut crate::native_execution::Ticket>,
) -> Result<(Vec<FastqRecord>, Vec<FastqRecord>), Failure> {
    for stat in [
        &mut local.raw1,
        &mut local.raw2,
        &mut local.clean1,
        &mut local.clean2,
    ] {
        if stat.native_qc.is_none() {
            stat.native_qc = Some(Box::new(crate::native_qc::Qc::default()));
        }
    }
    let settings = &gp.native_process;
    let repetition=if settings.repetition{gp.native_runtime.as_ref().and_then(|r|r.repetition.lock().unwrap_or_else(|p|p.into_inner()).clone())}else{None};
    let screen = gp
        .native_runtime
        .as_ref()
        .and_then(|r| r.screen.lock().unwrap_or_else(|p| p.into_inner()).clone());
    // Canonical API qualities do not imply that every channel can encode them.
    let output_infallible=gp.native_io.as_ref().is_none_or(|io|io.spec.outputs.iter().all(|d|d.phred==33));
    let reuse_qc = gp.native_inference_observed == 0
        && diagnostic.is_none()
        && !settings.overlap.enabled()
        && !settings.capture.enabled()
        && gp.native_m1
        && gp.native_cuts == [[0, 0], [0, 0]]
        && gp.native_quality_q.is_none()
        && gp.native_tail_mask == 0
        && settings.adapters.iter().all(Vec::is_empty)
        && settings.matchers.iter().all(Option::is_none)
        && settings.primer_matchers.iter().all(Option::is_none)
        && settings.min_length == 1
        && settings.max_length.is_none()
        && settings.max_n_count.is_none()
        && settings.max_n_fraction.is_none()
        && settings.low_q.is_none()
        && settings.max_low_q_fraction.is_none()
        && settings.min_mean_q.is_none()
        && settings.min_complexity.is_none()
        && gp.quality_phred == gp.output_quality_phred
        && output_infallible
        && reads1.iter().all(|r| !r.sequence.is_empty())
        && reads2
            .as_ref()
            .is_none_or(|reads| reads.iter().all(|r| !r.sequence.is_empty()))
        && local.clean1.native_qc.as_ref().unwrap().reads == 0
        && local.clean2.native_qc.as_ref().unwrap().reads == 0;
    let deferred_qc = !reuse_qc
        && gp.native_m1
        && gp.native_inference_observed == 0
        && settings.infer_mode == "off"
        && settings.profile == "ordinary"
        && !settings.qc_only
        && !settings.overlap.enabled()
        && !settings.capture.enabled()
        && !settings.screen.enabled()
        && screen.is_none()
        && !settings.repetition
        && diagnostic.is_none()
        && gp.native_aux.iter().all(String::is_empty)
        && gp.native_record_manifest.is_empty()
        && gp.native_match_audit.is_empty()
        && settings.pair_policy == "both-pass"
        && settings.sample_fraction == 1.0
        && settings.split_units == 0
        && gp.quality_phred == gp.output_quality_phred
        && output_infallible
        && local.clean1.native_qc.as_ref().unwrap().reads == 0
        && local.clean2.native_qc.as_ref().unwrap().reads == 0;
    let mut identical_records = true;
    let mut clean1 = Vec::with_capacity(reads1.len());
    let mut clean2 = Vec::with_capacity(reads2.as_ref().map_or(0, Vec::len));
    let result: Result<(),Failure> = (|| {
        if let Some(reads2) = reads2 {
            for (mut r1, mut r2) in reads1.into_iter().zip(reads2) {
                validate_normalize(&mut r1, gp.quality_phred).map_err(|e|Failure::input(e).at(r1.source_index))?;
                validate_normalize(&mut r2, gp.quality_phred).map_err(|e|Failure::input(e).at(r2.source_index))?;
                let original_lengths = deferred_qc.then(|| (r1.sequence.len(), r1.qual_seq.len(), r2.sequence.len(), r2.qual_seq.len()));
                if reuse_qc || deferred_qc {
                    observe(&r1, &mut local.clean1, gp.quality_phred);
                    observe(&r2, &mut local.clean2, gp.quality_phred);
                } else {
                    if r1.source_index >= gp.native_inference_observed {
                        observe(&r1, &mut local.raw1, gp.quality_phred);
                        observe(&r2, &mut local.raw2, gp.quality_phred);
                    } else if let Some(runtime) = &gp.native_runtime {
                        runtime.replayed_input();
                    }
                }
                if let Some(estimator)=&repetition{estimator.observe(&r1,Some(&r2),ticket.as_deref_mut()).map_err(Failure::from)?;}
                if settings.capture.enabled() {
                    let failures = match settings
                        .capture
                        .apply(&mut [&mut r1, &mut r2], gp.quality_phred)
                    {
                        Ok(v) => v,
                        Err(e) => {
                            local.fs.native.record_capture([false; 4], settings, true);
                            return Err(Failure::worker(e));
                        }
                    };
                    if local.fs.native.record_capture(failures, settings, true) {
                        if let Some(screen) = &screen {
                            let e = screen.evaluate(r1.source_index, &[&r1, &r2], false).map_err(Failure::input)?;
                            if let Some(d) = diagnostic.as_deref_mut() {
                                d.reference = Some(e);
                            }
                        }
                        if let Some(d) = diagnostic.as_deref_mut() {
                            d.record(
                                &r1,
                                Some(&r2),
                                gp,
                                true,
                                true,
                                failures,
                                [[None; 9]; 2],
                                None,
                            ).map_err(Failure::worker)?;
                        }
                        local.fs.short_len_num += 1;
                        continue;
                    }
                }
                clip(&mut r1, gp.native_cuts[0]);
                clip(&mut r2, gp.native_cuts[1]);
                let mut hits = if gp.native_match_audit.is_empty() {
                    Vec::new()
                } else {
                    Vec::with_capacity(4)
                };
                let audit = !gp.native_match_audit.is_empty();
                let (discard1, discard2) = if settings.adapter_overlap {
                    let (a, b, decision) = apply_pair_stages_recorded(
                        &mut r1, &mut r2, gp, audit.then_some(&mut hits),
                    );
                    local.fs.native.adapter_overlap_evaluated += 1;
                    local.fs.native.adapter_overlap_clipped += u64::from(matches!(decision, crate::native_adapter_overlap::Decision::Clip { .. }));
                    local.fs.native.adapter_overlap_ambiguous += u64::from(decision == crate::native_adapter_overlap::Decision::Ambiguous);
                    (a, b)
                } else {
                    (
                        apply_stages_recorded(&mut r1, gp, 0, audit.then_some(&mut hits)),
                        apply_stages_recorded(&mut r2, gp, 1, audit.then_some(&mut hits)),
                    )
                };
                if !hits.is_empty() {
                    diagnostic.as_deref_mut().unwrap().matches.push(
                        crate::native_processing::MatchedUnit {
                            ordinal: r1.source_index,
                            hits,
                        },
                    );
                }
                if let Some(lengths) = original_lengths {
                    identical_records &= lengths == (r1.sequence.len(), r1.qual_seq.len(), r2.sequence.len(), r2.qual_seq.len());
                }
                let values = [
                    gp.native_process
                        .evaluate(&r1, gp.quality_phred, discard1, 0),
                    gp.native_process
                        .evaluate(&r2, gp.quality_phred, discard2, 1),
                ];
                let overlap = if settings.overlap.enabled() {
                    local.fs.native.overlap_evaluated += 1;
                    let e = crate::native_overlap::select(
                        r1.sequence.as_bytes(),
                        r2.sequence.as_bytes(),
                        &settings.overlap,
                    );
                    local.fs.native.overlap_scored += u64::from(e.is_some());
                    e
                } else {
                    None
                };
                let insert_failed = settings
                    .overlap
                    .min_insert
                    .zip(overlap)
                    .is_some_and(|(minimum, e)| e.insert < minimum);
                let mate_rejected = local.fs.native.record(values, &gp.native_process, true);
                local.fs.native.insert_failed += u64::from(insert_failed);
                if insert_failed && !mate_rejected {
                    local.fs.native.rejected += 1;
                    local.fs.native.insert_first += 1;
                }
                let reference = screen
                    .as_ref()
                    .map(|screen| screen.evaluate(r1.source_index, &[&r1, &r2], true))
                    .transpose().map_err(Failure::input)?;
                let reference_failed = reference.as_ref().is_some_and(|e| e.rejected);
                if reference_failed && !mate_rejected && !insert_failed {
                    local.fs.native.rejected += 1;
                    local.fs.native.reference_first += 1;
                }
                let rejected = mate_rejected || insert_failed || reference_failed;
                if let Some(d) = diagnostic.as_deref_mut() {
                    d.reference = reference;
                }

                if let Some(d) = diagnostic.as_deref_mut() {
                    local.fs.native.transformed += 1;
                    d.record(
                        &r1,
                        Some(&r2),
                        gp,
                        false,
                        rejected,
                        [false; 4],
                        values,
                        overlap,
                    ).map_err(Failure::worker)?;
                }
                if rejected {
                    local.fs.short_len_num += 1;
                    continue;
                }
                let selected = settings.selects(r1.source_index);
                // Accepted QC is pre-correction; check only final actually emitted qualities.
                let accepted1 = if selected && (settings.overlap.correct || settings.overlap.merge)
                {
                    Some(r1.clone())
                } else {
                    None
                };
                let accepted2 = if accepted1.is_some() {
                    Some(r2.clone())
                } else {
                    None
                };
                let mut merged = None;
                if selected {
                    if let Some(e) = overlap {
                        if settings.overlap.correct || settings.overlap.merge {
                            local.fs.native.corrected_bases += crate::native_overlap::correct(
                                &mut r1,
                                &mut r2,
                                e,
                                gp.quality_phred,
                            );
                        }
                        if settings.overlap.merge {
                            merged =
                                Some(crate::native_overlap::merge(&r1, &r2, e, gp.quality_phred));
                        }
                    }
                    if let Some(m) = merged.as_ref() {
                        output_representable_channel(m, gp,"merged-out").map_err(|e|Failure::input(e).at(m.source_index))?;
                    } else if !settings.overlap.merge || !settings.overlap.discard_unmerged {
                        output_representable(&r1, gp).map_err(|e|Failure::input(e).at(r1.source_index))?;
                        output_representable(&r2, gp).map_err(|e|Failure::input(e).at(r2.source_index))?;
                    }
                }
                if !reuse_qc && !deferred_qc {
                    observe(
                        accepted1.as_ref().unwrap_or(&r1),
                        &mut local.clean1,
                        gp.quality_phred,
                    );
                    observe(
                        accepted2.as_ref().unwrap_or(&r2),
                        &mut local.clean2,
                        gp.quality_phred,
                    );
                }
                if selected {
                    local.fs.native.selected += 1;
                    if settings.overlap.merge {
                        let d = diagnostic
                            .as_deref_mut()
                            .expect("merge diagnostic storage initialized");
                        d.selected_ordinals.push(r1.source_index);
                        if let Some(m) = merged {
                            local.fs.native.merged += 1;
                            d.merged.push(m);
                        } else {
                            local.fs.native.unmerged += 1;
                            if !settings.overlap.discard_unmerged {
                                clean1.push(r1);
                                clean2.push(r2);
                            }
                        }
                    } else {
                        clean1.push(r1);
                        clean2.push(r2);
                    }
                } else {
                    local.fs.native.not_selected += 1;
                }
            }
        } else {
            for mut r1 in reads1 {
                validate_normalize(&mut r1, gp.quality_phred).map_err(|e|Failure::input(e).at(r1.source_index))?;
                let original_lengths = deferred_qc.then(|| (r1.sequence.len(), r1.qual_seq.len()));
                if reuse_qc || deferred_qc {
                    observe(&r1, &mut local.clean1, gp.quality_phred);
                } else {
                    if r1.source_index >= gp.native_inference_observed {
                        observe(&r1, &mut local.raw1, gp.quality_phred);
                    } else if let Some(runtime) = &gp.native_runtime {
                        runtime.replayed_input();
                    }
                }
                if let Some(estimator)=&repetition{estimator.observe(&r1,None,ticket.as_deref_mut()).map_err(Failure::from)?;}
                if settings.capture.enabled() {
                    let failures = match settings.capture.apply(&mut [&mut r1], gp.quality_phred) {
                        Ok(v) => v,
                        Err(e) => {
                            local.fs.native.record_capture([false; 4], settings, false);
                            return Err(Failure::worker(e));
                        }
                    };
                    if local.fs.native.record_capture(failures, settings, false) {
                        if let Some(screen) = &screen {
                            let e = screen.evaluate(r1.source_index, &[&r1], false).map_err(Failure::input)?;
                            if let Some(d) = diagnostic.as_deref_mut() {
                                d.reference = Some(e);
                            }
                        }
                        if let Some(d) = diagnostic.as_deref_mut() {
                            d.record(&r1, None, gp, true, true, failures, [[None; 9]; 2], None).map_err(Failure::worker)?;
                        }
                        local.fs.short_len_num += 1;
                        continue;
                    }
                }
                let mut hits = Vec::new();
                let discard = if settings.profile == "srna" {
                    let (values, hit) = settings.srna_evidence(&r1, gp.native_tail_min);
                    for i in 0..4 {
                        local.fs.native.srna_evaluated[i] += u64::from(values[i].is_some());
                        local.fs.native.srna_failed[i] += u64::from(values[i] == Some(true));
                    }
                    if let Some(d) = diagnostic.as_deref_mut() {
                        d.srna = values;
                    }
                    if let Some(first) = values.iter().position(|v| *v == Some(true)) {
                        if let Some(screen) = &screen {
                            let e = screen.evaluate(r1.source_index, &[&r1], false).map_err(Failure::input)?;
                            if let Some(d) = diagnostic.as_deref_mut() {
                                d.reference = Some(e);
                            }
                        }
                        local.fs.native.srna_first[first] += 1;
                        local.fs.native.fallback += 1;
                        local.fs.native.record([[None; 9]; 2], settings, false);
                        local.fs.native.rejected += 1;
                        if let Some(d) = diagnostic.as_deref_mut() {
                            d.record(&r1, None, gp, true, true, [false; 4], [[None; 9]; 2], None).map_err(Failure::worker)?;
                        }
                        local.fs.short_len_num += 1;
                        continue;
                    }
                    let hit = hit.expect("passing expected-adapter evidence has a selected match");
                    if !gp.native_match_audit.is_empty() {
                        hits.push(crate::native_processing::SelectedAdapter {
                            mate: 0,
                            adapter: settings.adapter_names[0][hit.declared_order].clone(),
                            start: hit.start,
                            end: hit.start + hit.compared,
                            mismatches: hit.mismatches,
                        });
                    }
                    r1.sequence.truncate(hit.start);
                    r1.qual_seq.truncate(hit.start);
                    false
                } else {
                    clip(&mut r1, gp.native_cuts[0]);
                    apply_stages_recorded(
                        &mut r1,
                        gp,
                        0,
                        if gp.native_match_audit.is_empty() {
                            None
                        } else {
                            Some(&mut hits)
                        },
                    )
                };
                if !hits.is_empty() {
                    diagnostic.as_deref_mut().unwrap().matches.push(
                        crate::native_processing::MatchedUnit {
                            ordinal: r1.source_index,
                            hits,
                        },
                    );
                }
                if let Some(lengths) = original_lengths {
                    identical_records &= lengths == (r1.sequence.len(), r1.qual_seq.len());
                }
                let values = [
                    gp.native_process
                        .evaluate(&r1, gp.quality_phred, discard, 0),
                    [None; 9],
                ];
                let mate_rejected = local.fs.native.record(values, &gp.native_process, false);
                let reference = screen
                    .as_ref()
                    .map(|screen| screen.evaluate(r1.source_index, &[&r1], true))
                    .transpose().map_err(Failure::input)?;
                let reference_failed = reference.as_ref().is_some_and(|e| e.rejected);
                if reference_failed && !mate_rejected {
                    local.fs.native.rejected += 1;
                    local.fs.native.reference_first += 1;
                }
                let rejected = mate_rejected || reference_failed;
                if let Some(d) = diagnostic.as_deref_mut() {
                    d.reference = reference;
                }

                if let Some(d) = diagnostic.as_deref_mut() {
                    local.fs.native.transformed += 1;
                    d.record(&r1, None, gp, false, rejected, [false; 4], values, None).map_err(Failure::worker)?;
                }
                if rejected {
                    local.fs.short_len_num += 1;
                    continue;
                }
                let selected = settings.selects(r1.source_index);
                if selected {
                    output_representable(&r1, gp).map_err(|e|Failure::input(e).at(r1.source_index))?;
                }
                if !reuse_qc && !deferred_qc {
                    observe(&r1, &mut local.clean1, gp.quality_phred);
                }
                if selected {
                    local.fs.native.selected += 1;
                    clean1.push(r1);
                } else {
                    local.fs.native.not_selected += 1;
                }
            }
        }
        Ok(())
    })();
    if reuse_qc || deferred_qc {
        // Merge this batch's validated original prefix, including on error.
        // Deferred accepted QC is reconciled below.
        for (raw, clean) in [
            (&mut local.raw1, &local.clean1),
            (&mut local.raw2, &local.clean2),
        ] {
            raw.native_qc
                .as_mut()
                .unwrap()
                .merge(clean.native_qc.as_ref().unwrap());
        }
    }
    if deferred_qc {
        // This QC temporarily covers the validated original prefix. Reuse it
        // only when both accepted vectors prove the same records and bytes.
        let same_population = local.clean1.native_qc.as_ref().unwrap().reads == clean1.len() as u64
            && local.clean2.native_qc.as_ref().unwrap().reads == clean2.len() as u64;
        if !identical_records || !same_population {
            local.clean1.native_qc.as_mut().unwrap().reset_counts();
            local.clean2.native_qc.as_mut().unwrap().reset_counts();
            for read in &clean1 { observe(read, &mut local.clean1, gp.quality_phred); }
            for read in &clean2 { observe(read, &mut local.clean2, gp.quality_phred); }
        }
    }
    for stat in [
        &mut local.raw1,
        &mut local.raw2,
        &mut local.clean1,
        &mut local.clean2,
    ] {
        stat.gs = stat.native_qc.as_ref().unwrap().general();
    }
    result.map(|()| (clean1, clean2))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn m7_srna_raw_evidence_atomic_fallback_and_prefix_only() {
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.quality_phred = 33;
        gp.output_quality_phred = 33;
        gp.native_tail_min = 3;
        gp.native_record_manifest = "records.tsv".into();
        gp.native_aux[0] = "reject.fq".into();
        gp.native_aux[2] = "transformed.fq".into();
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.profile = "srna".into();
        settings.srna_min_insert = 3;
        settings.srna_reject5 = true;
        settings.srna_reject_poly_a = true;
        settings.adapters[0] = vec!["GGG".into()];
        settings.primers[0] = vec!["AAA".into()];
        settings.adapter_names[0] = settings.adapters[0].clone();
        settings.primer_names[0] = settings.primers[0].clone();
        settings.min_overlap = 3;
        settings.max_error = 0.;
        settings.compile().unwrap();
        gp.native_process = std::sync::Arc::new(settings);
        let make = |seq: &str| {
            let mut r = FastqRecord::new(1);
            r.source_index = 0;
            r.seq_id = "@p full comment".into();
            r.sequence = seq.into();
            r.qual_seq = "I".repeat(seq.len());
            r
        };
        for (seq, expected, failed) in [
            ("AAATTTAAAA", None, [true, false, true, true]),
            ("TTGGGAAAA", None, [false, true, false, true]),
            ("TTTTGGG", Some("TTTT"), [false; 4]),
        ] {
            let raw = make(seq);
            let evidence = gp.native_process.srna_evidence(&raw, 3).0;
            for i in 0..4 {
                assert_eq!(evidence[i] == Some(true), failed[i]);
            }
            if seq == "AAATTTAAAA" {
                assert_eq!(evidence[1], None);
            }
            let mut stats = PeLocalStats::new(&gp);
            let mut d = DiagnosticBatch::new(seq.len());
            let out =
                apply_recorded(vec![raw.clone()], None, &mut stats, &gp, Some(&mut d)).unwrap();
            assert_eq!(stats.raw1.native_qc.as_ref().unwrap().reads, 1);
            if let Some(expected) = expected {
                assert_eq!(out.0[0].sequence, expected);
                assert_eq!(out.0[0].seq_id, raw.seq_id);
                assert_eq!(stats.clean1.native_qc.as_ref().unwrap().reads, 1);
                assert_eq!(stats.fs.native.fallback, 0);
            } else {
                assert!(out.0.is_empty());
                assert_eq!(d.rejected[0][0].sequence, raw.sequence);
                assert_eq!(d.rejected[0][0].qual_seq, raw.qual_seq);
                assert_eq!(stats.fs.native.fallback, 1);
                assert_eq!(stats.fs.native.rejected, 1);
                assert_eq!(stats.fs.native.evaluated[0][1], 0);
                assert_eq!(stats.fs.native.not_evaluated[0][1], 1);
            }
        }
    }
    #[test]
    fn m6_insert_global_veto_keeps_mate_causes_policy_eligible() {
        for (policy, failed, veto) in [
            ("either-pass", [true, false], true),
            ("either-pass", [false, true], true),
            ("either-pass", [true, true], true),
            ("r1-pass", [false, true], true),
            ("both-pass", [false, true], true),
            ("either-pass", [false, true], false),
            ("r1-pass", [false, true], false),
            ("both-pass", [false, true], false),
        ] {
            let mut gp = GlobalParams::new();
            gp.native_m1 = true;
            gp.fq2_path = "mate".into();
            gp.quality_phred = 33;
            gp.output_quality_phred = 33;
            gp.native_record_manifest = "records.tsv".into();
            let mut settings = crate::native_processing::ProcessSettings::default();
            settings.pair_policy = policy.into();
            settings.low_q = Some(6);
            settings.max_low_q_fraction = Some(0.5);
            settings.overlap.minimum = 2;
            settings.overlap.max_error = 0.5;
            settings.overlap.min_insert = if veto { Some(3) } else { None };
            gp.native_process = std::sync::Arc::new(settings);
            let make = |mate: usize| {
                let mut r = FastqRecord::new((mate + 1) as i32);
                r.source_index = 0;
                r.seq_id = format!("@p/{}", mate + 1);
                r.sequence = if mate == 0 { "AN" } else { "NT" }.into();
                r.qual_seq = if failed[mate] { "!!" } else { "II" }.into();
                r
            };
            let mut stats = PeLocalStats::new(&gp);
            let mut d = DiagnosticBatch::new(2);
            apply_recorded(
                vec![make(0)],
                Some(vec![make(1)]),
                &mut stats,
                &gp,
                Some(&mut d),
            )
            .unwrap();
            let mate_rejected = match policy {
                "either-pass" => failed[0] && failed[1],
                "r1-pass" => failed[0],
                _ => failed[0] || failed[1],
            };
            let mut reasons = Vec::new();
            if mate_rejected {
                reasons.push("low-quality-fraction");
            }
            if veto {
                reasons.push("inferred-short-insert");
            }
            let text = String::from_utf8(d.manifest).unwrap();
            let cells: Vec<_> = text.trim_end().split('\t').collect();
            assert_eq!(cells[2], reasons.join(","), "{policy} {failed:?} {veto}");
            for mate in 0..2 {
                assert!(cells[3].contains(&format!(
                    "r{}.low-quality-fraction={}",
                    mate + 1,
                    if failed[mate] { "fail" } else { "pass" }
                )));
                assert_eq!(stats.fs.native.failed[mate][6], u64::from(failed[mate]));
            }
            assert_eq!(
                stats.fs.native.overlapping_units[6],
                u64::from(failed[0] || failed[1])
            );
            assert_eq!(stats.fs.native.first[6], u64::from(mate_rejected));
            assert_eq!(
                stats.fs.native.insert_first,
                u64::from(veto && !mate_rejected)
            );
            assert_eq!(stats.fs.native.rejected, u64::from(mate_rejected || veto));
        }
    }
    #[test]
    fn m6_insert_veto_blocks_rescue_and_merge_validates_only_emitted_qualities() {
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.fq2_path = "mate".into();
        gp.quality_phred = 33;
        gp.output_quality_phred = 33;
        gp.native_record_manifest = "records.tsv".into();
        gp.native_aux[4] = "singleton1.fq".into();
        gp.native_aux[5] = "singleton2.fq".into();
        let make = |seq: &str, q: u8, mate: i32| {
            let mut r = FastqRecord::new(mate);
            r.seq_id = format!("@p/{mate} full comment");
            r.sequence = seq.into();
            r.qual_seq = String::from_utf8(vec![33 + q; seq.len()]).unwrap();
            r.source_index = 0;
            r
        };
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.pair_policy = "either-pass".into();
        settings.overlap.minimum = 2;
        settings.overlap.max_error = 0.5;
        settings.overlap.min_insert = Some(3);
        gp.native_process = std::sync::Arc::new(settings.clone());
        let mut stats = PeLocalStats::new(&gp);
        let mut d = DiagnosticBatch::new(2);
        let out = apply_recorded(
            vec![make("AN", 40, 1)],
            Some(vec![make("NT", 40, 2)]),
            &mut stats,
            &gp,
            Some(&mut d),
        )
        .unwrap();
        assert!(out.0.is_empty() && out.1.is_empty() && d.singletons.iter().all(Vec::is_empty));
        assert_eq!(
            (
                stats.fs.native.rejected,
                stats.fs.native.insert_first,
                stats.fs.native.mate_passed
            ),
            (1, 1, [1, 1])
        );
        assert!(
            String::from_utf8(d.manifest)
                .unwrap()
                .contains("inferred-short-insert")
        );
        settings.low_q = Some(6);
        settings.max_low_q_fraction = Some(0.5);
        gp.native_process = std::sync::Arc::new(settings.clone());
        let mut stats = PeLocalStats::new(&gp);
        let mut d = DiagnosticBatch::new(2);
        apply_recorded(
            vec![make("AN", 40, 1)],
            Some(vec![make("NT", 0, 2)]),
            &mut stats,
            &gp,
            Some(&mut d),
        )
        .unwrap();
        let manifest = String::from_utf8(d.manifest).unwrap();
        let cells: Vec<_> = manifest.trim_end().split('\t').collect();
        assert_eq!(cells[2], "inferred-short-insert");
        assert!(cells[3].contains("r2.low-quality-fraction=fail"));
        assert_eq!(stats.fs.native.insert_first, 1);
        assert_eq!(stats.fs.native.first[6], 0);
        settings.low_q = None;
        settings.max_low_q_fraction = None;
        gp.native_aux = Default::default();
        gp.output_quality_phred = 64;
        settings.pair_policy = "both-pass".into();
        settings.overlap.min_insert = None;
        settings.overlap.merge = true;
        settings.overlap.minimum = 1;
        settings.overlap.max_error = 1.;
        gp.native_process = std::sync::Arc::new(settings);
        let mut stats = PeLocalStats::new(&gp);
        let mut d = DiagnosticBatch::new(1);
        let out = apply_recorded(
            vec![make("A", 60, 1)],
            Some(vec![make("A", 83, 2)]),
            &mut stats,
            &gp,
            Some(&mut d),
        )
        .unwrap();
        assert!(out.0.is_empty() && out.1.is_empty());
        assert_eq!(d.merged[0].qual_seq.as_bytes(), &[93]);
        assert_eq!(stats.clean1.native_qc.as_ref().unwrap().qualities[60], 1);
        assert_eq!(stats.clean2.native_qc.as_ref().unwrap().qualities[83], 1);
        let error = apply_recorded(
            vec![make("A", 93, 1)], Some(vec![make("A", 83, 2)]),
            &mut PeLocalStats::new(&gp), &gp, Some(&mut DiagnosticBatch::new(1)),
        ).err().unwrap();
        assert!(error.contains("quality.unrepresentable"));

    }
    #[test]
    fn policy_eligible_manifest_and_first_causes_preserve_all_mate_failures() {
        use crate::native_processing::{FateStats, ProcessSettings};
        for (policy, rejected, global, first) in [
            ("either-pass", false, false, None),
            ("r1-pass", false, false, None),
            ("r1-pass", true, false, Some(2)),
            ("either-pass", true, false, Some(0)),
            ("either-pass", true, true, None),
        ] {
            let mut gp = GlobalParams::new();
            gp.native_m1 = true;
            gp.fq2_path = "mate".into();
            gp.quality_phred = 33;
            gp.output_quality_phred = 33;
            gp.native_record_manifest = "records.tsv".into();
            gp.native_aux = [
                "r1.fq".into(),
                "r2.fq".into(),
                "t1.fq".into(),
                "t2.fq".into(),
                String::new(),
                String::new(),
            ];
            let mut settings = ProcessSettings::default();
            settings.pair_policy = policy.into();
            let mut values = [[None; 9]; 2];
            if rejected && !global {
                settings.min_length = 3;
                settings.adapter_discard = true;
                settings.adapters[1].push("ACGT".into());
                values[0][2] = Some(true);
                values[1][0] = Some(true);
            } else {
                settings.low_q = Some(20);
                settings.max_low_q_fraction = Some(0.0);
                values[0][6] = Some(false);
                values[1][6] = Some(true);
            }
            if global {
                settings
                    .capture
                    .labels
                    .push(crate::native_processing::LabelSpec::parse("U@r1:9:1").unwrap());
                values = [[None; 9]; 2];
            }
            let mut stats = FateStats::default();
            if global {
                assert!(stats.record_capture([true, false, false, false], &settings, true));
            } else {
                assert_eq!(stats.record(values, &settings, true), rejected);
            }
            gp.native_process = std::sync::Arc::new(settings);
            let make = |mate| {
                let mut r = FastqRecord::new(mate);
                r.seq_id = format!("@p/{mate} comment");
                r.sequence = "ACGT".into();
                r.qual_seq = "IIII".into();
                r
            };
            let mut d = DiagnosticBatch::new(4);
            d.record(
                &make(1),
                Some(&make(2)),
                &gp,
                global,
                rejected,
                [global, false, false, false],
                values,
                None,
            )
            .unwrap();
            let text = String::from_utf8(d.manifest).unwrap();
            let cells = text.trim_end().split('\t').collect::<Vec<_>>();
            let actual = cells[2]
                .split(',')
                .filter(|x| !x.is_empty())
                .collect::<std::collections::BTreeSet<_>>();
            let expected = if global {
                vec!["label-coordinate"]
            } else if !rejected {
                vec![]
            } else if policy == "r1-pass" {
                vec!["min-length"]
            } else {
                vec!["adapter-present", "min-length"]
            };
            assert_eq!(actual, expected.into_iter().collect());
            for reason in 0..9 {
                assert_eq!(stats.first[reason], u64::from(first == Some(reason)));
            }
            if global {
                assert_eq!(stats.capture_first[0], 1);
                assert!(cells[3].contains("r2.low-quality-fraction=not-evaluated"));
            } else if rejected {
                assert_eq!(stats.failed[1][0], 1);
                assert_eq!(stats.overlapping_units[0], 1);
                assert!(cells[3].contains("r2.adapter-present=fail"));
            } else {
                assert_eq!(stats.failed[1][6], 1);
                assert_eq!(stats.overlapping_units[6], 1);
                assert!(cells[3].contains("r2.low-quality-fraction=fail"));
            }
        }
    }

    #[test]
    fn sampling_preserves_accepted_qc_and_checks_only_actually_written_encoding() {
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.quality_phred = 33;
        gp.output_quality_phred = 64;
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.sample_fraction = 0.0;
        gp.native_process = std::sync::Arc::new(settings);
        let mut r = FastqRecord::new(1);
        r.seq_id = "@q63".into();
        r.sequence = "AC".into();
        r.qual_seq = "`I".into();
        r.source_index = 2500;
        let mut stats = PeLocalStats::new(&gp);
        let result = apply_recorded(vec![r.clone()], None, &mut stats, &gp, None).unwrap();
        assert!(result.0.is_empty());
        assert_eq!(stats.raw1.native_qc.as_ref().unwrap().reads, 1);
        assert_eq!(stats.clean1.native_qc.as_ref().unwrap().reads, 1);
        assert_eq!(
            (stats.fs.native.selected, stats.fs.native.not_selected),
            (0, 1)
        );
        gp.native_aux[2] = "transformed.fq".into();
        let mut d = DiagnosticBatch::new(2);
        let mut stats = PeLocalStats::new(&gp);
        assert!(
            matches!(apply_recorded(vec![r.clone()],None,&mut stats,&gp,Some(&mut d)),Err(e) if e.contains("unrepresentable"))
        );
        gp.native_aux = Default::default();
        std::sync::Arc::make_mut(&mut gp.native_process).sample_fraction = 1.0;
        let mut stats = PeLocalStats::new(&gp);
        assert!(
            matches!(apply_recorded(vec![r],None,&mut stats,&gp,None),Err(e) if e.contains("unrepresentable"))
        );
        assert_eq!(stats.raw1.native_qc.as_ref().unwrap().reads, 1);
        assert_eq!(stats.clean1.native_qc.as_ref().unwrap().reads, 0);
    }

    #[test]
    fn pair_policy_keeps_true_mate_flags_and_singletons_never_rescue_failed_mates() {
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.quality_phred = 33;
        gp.output_quality_phred = 33;
        gp.native_record_manifest = "records.tsv".into();
        let make = |mate, quality: &str| {
            let mut r = FastqRecord::new(mate);
            r.seq_id = format!("@p/{mate}");
            r.sequence = "ACGT".into();
            r.qual_seq = quality.into();
            r
        };
        for policy in ["both-pass", "either-pass", "r1-pass"] {
            let mut settings = crate::native_processing::ProcessSettings::default();
            settings.pair_policy = policy.into();
            settings.low_q = Some(20);
            settings.max_low_q_fraction = Some(0.0);
            gp.native_process = std::sync::Arc::new(settings);
            gp.native_aux = Default::default();
            if policy == "both-pass" {
                gp.native_aux[4] = "singleton1.fq".into();
                gp.native_aux[5] = "singleton2.fq".into();
            }
            for (quality1, quality2, accepted) in [
                ("IIII", "!!!!", policy != "both-pass"),
                ("!!!!", "IIII", policy == "either-pass"),
                ("!!!!", "!!!!", false),
            ] {
                let mut stats = PeLocalStats::new(&gp);
                let mut d = DiagnosticBatch::new(4);
                let result = apply_recorded(
                    vec![make(1, quality1)],
                    Some(vec![make(2, quality2)]),
                    &mut stats,
                    &gp,
                    Some(&mut d),
                )
                .unwrap();
                assert_eq!(!result.0.is_empty(), accepted);
                assert_eq!(stats.fs.native.rejected, u64::from(!accepted));
                assert_eq!(
                    stats.fs.native.mate_failed,
                    [u64::from(quality1 == "!!!!"), u64::from(quality2 == "!!!!")]
                );
                assert_eq!(
                    d.singletons[0].len(),
                    usize::from(policy == "both-pass" && quality1 == "IIII")
                );
                assert_eq!(
                    d.singletons[1].len(),
                    usize::from(policy == "both-pass" && quality2 == "IIII")
                );
                assert_eq!(stats.fs.native.first[6], u64::from(!accepted));
            }
        }
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings
            .capture
            .labels
            .push(crate::native_processing::LabelSpec::parse("U@r1:9:1").unwrap());
        gp.native_process = std::sync::Arc::new(settings);
        gp.native_aux[4] = "singleton1.fq".into();
        gp.native_aux[5] = "singleton2.fq".into();
        let mut stats = PeLocalStats::new(&gp);
        let mut d = DiagnosticBatch::new(4);
        let result = apply_recorded(
            vec![make(1, "IIII")],
            Some(vec![make(2, "IIII")]),
            &mut stats,
            &gp,
            Some(&mut d),
        )
        .unwrap();
        assert!(result.0.is_empty() && d.singletons.iter().all(Vec::is_empty));
        assert_eq!(stats.fs.native.fallback, 1);
        assert_eq!(stats.fs.native.mate_passed, [0, 0]);
    }

    #[test]
    fn diagnostic_channels_distinguish_completed_rejection_from_atomic_original_fallback() {
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.quality_phred = 33;
        gp.output_quality_phred = 33;
        gp.native_aux = [
            "reject1.fq".into(),
            String::new(),
            "transformed1.fq".into(),
            String::new(),
            String::new(),
            String::new(),
        ];
        gp.native_record_manifest = "records.tsv".into();
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.min_length = 2;
        gp.native_process = std::sync::Arc::new(settings);
        gp.native_cuts[0] = [0, 1];
        let mut r = FastqRecord::new(1);
        r.seq_id = "@x comment".into();
        r.source_index = 0;
        r.sequence = "AC".into();
        r.qual_seq = "I5".into();
        let mut stats = PeLocalStats::new(&gp);
        let mut d = DiagnosticBatch::new(2);
        let result = apply_recorded(vec![r.clone()], None, &mut stats, &gp, Some(&mut d)).unwrap();
        assert!(result.0.is_empty());
        assert_eq!(d.rejected[0][0].sequence, "A");
        assert_eq!(d.transformed[0][0].qual_seq, "I");
        assert_eq!(d.stages[0].reads, 1);
        assert_eq!(d.stages[2].reads, 0);
        assert!(
            String::from_utf8(d.manifest)
                .unwrap()
                .contains("0\ttransformed\tmin-length")
        );
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.capture.labels =
            vec![crate::native_processing::LabelSpec::parse("U@r1:3:1").unwrap()];
        settings.capture.min_q = Some(40);
        gp.native_process = std::sync::Arc::new(settings);
        let mut stats = PeLocalStats::new(&gp);
        let mut d = DiagnosticBatch::new(2);
        apply_recorded(vec![r.clone()], None, &mut stats, &gp, Some(&mut d)).unwrap();
        assert_eq!(d.rejected[0][0].sequence, "AC");
        assert_eq!(d.transformed[0][0].seq_id, "@x comment");
        assert_eq!(d.stages[2].reads, 1);
        assert_eq!(d.stages[0].reads, 0);
        let manifest = String::from_utf8(d.manifest).unwrap();
        assert!(manifest.contains("original-fallback"));
        assert!(manifest.contains("label-quality=not-evaluated"));
        assert!(manifest.contains("r1.min-length=not-evaluated"));
    }
    #[test]
    fn window_cuts_first_strict_below_and_preserves_quality_encoding() {
        for phred in [33, 64] {
            for (q, width, expected) in [
                (vec![40, 0, 0, 40], 2, 1),
                (vec![20, 20], 2, 2),
                (vec![19], 4, 0),
                (vec![20], 4, 1),
                (vec![], 4, 0),
                (vec![40, 40, 0], 2, 3),
            ] {
                let mut r = FastqRecord::new(1);
                r.sequence = "A".repeat(q.len());
                r.qual_seq =
                    String::from_utf8(q.iter().map(|&x| x + phred as u8).collect()).unwrap();
                let original = r.qual_seq.clone();
                let mut gp = GlobalParams::new();
                gp.quality_phred = phred;
                gp.native_quality_q = Some(20);
                gp.native_quality_mode = crate::native_processing::QualityMode::Window;
                gp.native_quality_window = width;
                trim_quality(&mut r, &gp);
                assert_eq!(r.sequence.len(), expected);
                assert_eq!(r.qual_seq, &original[..expected]);
            }
        }
    }
    #[test]
    fn mott_intervals_match_independent_exhaustive_scores_and_ties() {
        for len in 0..=5 {
            for code in 0..3usize.pow(len as u32) {
                let mut v = code;
                let q: Vec<u8> = (0..len)
                    .map(|_| {
                        let x = (v % 3) as u8 * 20;
                        v /= 3;
                        x
                    })
                    .collect();
                let mut expected = (0, 0);
                let mut best = 0i32;
                for end in 1..=len {
                    for start in 0..end {
                        let score: i32 = q[start..end].iter().map(|&x| x as i32 - 20).sum();
                        if score > best {
                            best = score;
                            expected = (start, end);
                        }
                    }
                }
                for phred in [33, 64] {
                    let mut r = FastqRecord::new(1);
                    let seq = "ACGTN"[..len].to_string();
                    r.sequence = seq.clone();
                    r.qual_seq =
                        String::from_utf8(q.iter().map(|&x| x + phred as u8).collect()).unwrap();
                    let quality = r.qual_seq.clone();
                    let mut gp = GlobalParams::new();
                    gp.quality_phred = phred;
                    gp.native_quality_q = Some(20);
                    gp.native_quality_mode = crate::native_processing::QualityMode::Mott;
                    trim_quality(&mut r, &gp);
                    assert_eq!(r.sequence, &seq[expected.0..expected.1]);
                    assert_eq!(r.qual_seq, &quality[expected.0..expected.1]);
                }
            }
        }
    }
    #[test]
    fn terminal_tails_are_literal_single_runs_at_inclusive_threshold() {
        for (seq, mask, min, expected) in [
            ("AAAGGG", 31, 3, "AAA"),
            ("AAAGG", 31, 3, "AAAGG"),
            ("ACNNN", 16, 3, "AC"),
            ("acggg", 4, 3, "ac"),
            ("ACGGG", 1, 1, "ACGGG"),
            ("", 31, 1, ""),
            ("A", 1, 1, ""),
            ("AAAGGG", 0, 1, "AAAGGG"),
        ] {
            let mut r = FastqRecord::new(1);
            r.sequence = seq.into();
            r.qual_seq = "I".repeat(seq.len());
            let mut gp = GlobalParams::new();
            gp.native_tail_mask = mask;
            gp.native_tail_min = min;
            trim_tail(&mut r, &gp);
            assert_eq!(r.sequence, expected);
            assert_eq!(r.qual_seq.len(), expected.len());
        }
    }
    #[test]
    fn m1_pair_validation_retains_valid_prefix_and_original_error_priority() {
        for mode in [
            crate::native_processing::QualityMode::Window,
            crate::native_processing::QualityMode::Mott,
        ] {
            let mut gp = GlobalParams::new();
            gp.native_m1 = true;
            gp.native_quality_mode = mode;
            gp.native_quality_q = Some(20);
            gp.native_quality_window = 2;
            let make = |index, sequence: &str, quality: &str, mate| {
                let mut r = FastqRecord::new(mate);
                r.source_index = index;
                r.sequence = sequence.into();
                r.qual_seq = quality.into();
                r
            };
            let r1 = vec![make(0, "AC", "II", 1), make(1, "AC", " I", 1)];
            let r2 = vec![
                make(0, "GT", "II", 2),
                make(1, &"A".repeat(1001), &"I".repeat(1001), 2),
            ];
            let mut stats = PeLocalStats::new(&gp);
            let error = apply(r1, Some(r2), &mut stats, &gp).err().unwrap();
            assert!(
                error.contains("quality.encoding record 2 mate 1"),
                "{error}"
            );
            assert_eq!(stats.raw1.gs.reads_number, 1);
            assert_eq!(stats.raw2.gs.reads_number, 1);
            assert_eq!(stats.clean1.gs.reads_number, 1);
            assert_eq!(stats.clean2.gs.reads_number, 1);
        }
    }
    #[test]
    fn adapters_follow_ends_and_predicates_use_transformed_data() {
        let mut r = FastqRecord::new(1);
        r.sequence = "NTAACG".into();
        r.qual_seq = "!IIIII".into();
        let mut gp = GlobalParams::new();
        gp.native_cuts[0] = [1, 0];
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.adapters[0] = vec!["ACGT".into()];
        settings.max_error = 0.0;
        settings.max_n_count = Some(0);
        settings.min_length = 2;
        settings.min_mean_q = Some(40.0);
        settings.compile().unwrap();
        gp.native_process = std::sync::Arc::new(settings);
        let mut stats = PeLocalStats::new(&gp);
        let (a, _) = apply(vec![r], None, &mut stats, &gp).unwrap();
        assert_eq!(a[0].sequence, "TA");
        assert_eq!(stats.raw1.gs.n_number, 1);
        assert_eq!(stats.clean1.gs.base_number, 2);
        assert_eq!(stats.fs.native.rejected, 0);
        let mut r = FastqRecord::new(1);
        r.sequence = "TTACG".into();
        r.qual_seq = "III!!".into();
        gp.native_cuts[0] = [0, 0];
        gp.native_quality_q = Some(20);
        let mut stats = PeLocalStats::new(&gp);
        let (a, _) = apply(vec![r], None, &mut stats, &gp).unwrap();
        assert_eq!(a[0].sequence, "TTA");
    }
    #[test]
    fn end_peeling_keeps_equality_and_internal_low_quality() {
        for phred in [33, 64] {
            let mut r = FastqRecord::new(1);
            r.sequence = "ACGTA".into();
            r.qual_seq = [5, 20, 0, 40, 5]
                .iter()
                .map(|q| char::from((phred + q) as u8))
                .collect();
            peel_quality_ends(&mut r, Some(20), phred);
            assert_eq!(r.sequence, "CGT");
            assert_eq!(
                r.qual_seq
                    .bytes()
                    .map(|v| v as i32 - phred)
                    .collect::<Vec<_>>(),
                vec![20, 0, 40]
            );
        }
    }
    #[test]
    fn fixed_cut_precedes_peeling_and_qc_keeps_input() {
        let mut r = FastqRecord::new(1);
        r.sequence = "ACGT".into();
        r.qual_seq = "I!II".into();
        let mut gp = GlobalParams::new();
        gp.native_cuts[0] = [1, 0];
        gp.native_quality_q = Some(20);
        let mut stat = PeLocalStats::new(&gp);
        let (a, _) = apply(vec![r], None, &mut stat, &gp).unwrap();
        assert_eq!(a[0].sequence, "GT");
        assert_eq!(a[0].qual_seq, "II");
        assert_eq!(stat.raw1.gs.base_number, 4);
        assert_eq!(stat.clean1.gs.base_number, 2);
    }
    #[test]
    fn quality_peeling_off_zero_and_all_low_boundaries() {
        for (q, expected) in [(None, "AC"), (Some(0), "AC"), (Some(1), "")] {
            let mut r = FastqRecord::new(1);
            r.sequence = "AC".into();
            r.qual_seq = "!!".into();
            peel_quality_ends(&mut r, q, 33);
            assert_eq!(r.sequence, expected);
            assert_eq!(r.sequence.len(), r.qual_seq.len());
        }
    }
    #[test]
    fn fixed_cut_retains_identical_sequence_quality_positions() {
        for head in [0, 1, 2, 5, 6, 7, usize::MAX] {
            for tail in [0, 1, 2, 5, 6, 7, usize::MAX] {
                let mut read = FastqRecord::new(1);
                read.sequence = "AACCGG".into();
                read.qual_seq = "!5?I]~".into();
                let end = 6usize.saturating_sub(tail);
                let expected = if head < end {
                    (&"AACCGG"[head..end], &"!5?I]~"[head..end])
                } else {
                    ("", "")
                };
                clip(&mut read, [head, tail]);
                assert_eq!((read.sequence.as_str(), read.qual_seq.as_str()), expected);
            }
        }
    }
    #[test]
    fn clipping_fate_and_raw_qc_are_separate() {
        let mut r1 = FastqRecord::new(1);
        r1.sequence = "NACG".into();
        r1.qual_seq = "!III".into();
        let mut r2 = FastqRecord::new(2);
        r2.sequence = "TT".into();
        r2.qual_seq = "II".into();
        let mut gp = GlobalParams::new();
        gp.native_cuts = [[1, 0], [2, 0]];
        let mut stats = PeLocalStats::new(&gp);
        let (a, b) = apply(vec![r1], Some(vec![r2]), &mut stats, &gp).unwrap();
        assert!(a.is_empty() && b.is_empty());
        assert_eq!(stats.raw1.gs.n_number, 1);
        assert_eq!(stats.raw1.gs.base_number, 4);
        assert_eq!(stats.raw2.gs.base_number, 2);
        assert_eq!(stats.clean1.gs.reads_number, 0);
        assert_eq!(stats.fs.short_len_num, 1);
    }
}

#[cfg(test)]
#[path = "native_qualification.rs"]
mod qualification;

#[cfg(test)]
#[path = "native_qc_reuse_tests.rs"]
mod qc_reuse_tests;

#[cfg(test)]
mod m7_integration_tests {
    use super::*;
    use std::sync::{Arc, atomic::AtomicBool};
    #[test]
    fn m7_global_reference_veto_policy_first_causes_and_no_rescue() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../.scratch/m7-policy-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("ref.fa");
        std::fs::write(&file, ">refA\nACG\n").unwrap();
        for policy in ["both-pass", "either-pass", "r1-pass"] {
            for failed in [[false, true], [true, false], [true, true], [false, false]] {
                let mut gp = GlobalParams::new();
                gp.native_m1 = true;
                gp.fq2_path = "mate".into();
                gp.quality_phred = 33;
                gp.output_quality_phred = 33;
                gp.native_aux[4] = "singleton1".into();
                gp.native_aux[5] = "singleton2".into();
                gp.native_record_manifest = "records.tsv".into();
                let mut settings = crate::native_processing::ProcessSettings::default();
                settings.pair_policy = policy.into();
                settings.low_q = Some(6);
                settings.max_low_q_fraction = Some(0.5);
                settings.screen = crate::native_screen::Settings {
                    method: "kmer-exact".into(),
                    references: vec![file.to_string_lossy().into_owned()],
                    action: "remove".into(),
                    k: 3,
                    min_hits: 1,
                    ..Default::default()
                };
                gp.native_process = Arc::new(settings);
                let runtime = Arc::new(crate::native_runtime::Runtime::new(0, "proof".into()));
                let e = crate::native_execution::Execution::with_statistics_budget(
                    2,
                    2,
                    Arc::new(AtomicBool::new(false)),
                    None,
                    0,
                )
                .unwrap();
                let screen = crate::native_screen::Screen::prepare(
                    &gp,
                    &e,
                    &crate::cpu_budget::CpuBudget::new(2),
                )
                .unwrap();
                *runtime.screen.lock().unwrap() = Some(screen);
                gp.native_runtime = Some(runtime);
                let make = |mate: usize| {
                    let mut r = FastqRecord::new((mate + 1) as i32);
                    r.sequence = if mate == 0 { "ACG" } else { "AAA" }.into();
                    r.qual_seq = if failed[mate] { "!!!" } else { "III" }.into();
                    r.seq_id = format!("@p/{}", mate + 1);
                    r
                };
                let mut local = PeLocalStats::new(&gp);
                let mut d = DiagnosticBatch::new(3);
                let out = apply_recorded(
                    vec![make(0)],
                    Some(vec![make(1)]),
                    &mut local,
                    &gp,
                    Some(&mut d),
                )
                .unwrap();
                assert!(out.0.is_empty() && out.1.is_empty());
                assert!(d.singletons.iter().all(Vec::is_empty));
                let mate_rejected = match policy {
                    "either-pass" => failed[0] && failed[1],
                    "r1-pass" => failed[0],
                    _ => failed[0] || failed[1],
                };
                assert_eq!(local.fs.native.rejected, 1);
                assert_eq!(local.fs.native.first[6], u64::from(mate_rejected));
                assert_eq!(local.fs.native.reference_first, u64::from(!mate_rejected));
                let manifest = String::from_utf8(d.manifest).unwrap();
                let cells: Vec<_> = manifest.trim_end().split('\t').collect();
                assert_eq!(
                    cells[2],
                    if mate_rejected {
                        "low-quality-fraction,reference-action"
                    } else {
                        "reference-action"
                    }
                );
                for mate in 0..2 {
                    assert!(cells[3].contains(&format!(
                        "r{}.low-quality-fraction={}",
                        mate + 1,
                        if failed[mate] { "fail" } else { "pass" }
                    )));
                }
            }
        }
    }
}

#[cfg(test)]
mod m7_capacity_charge_tests {
    use super::*;
    use std::sync::{Arc, atomic::AtomicBool};
    #[test]
    fn m7_capacity_charge_covers_actual_reference_manifest_growth_and_transients() {
        use std::mem::size_of;
        let root=std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../.scratch/m7-capacity-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));std::fs::create_dir_all(&root).unwrap();
        assert_eq!(usize::BITS,64);assert_eq!(size_of::<String>(),24);
        let maximum_field=format!("r1.reference{}.distinct={}",usize::MAX,crate::params::READ_MAX_LEN).len();assert_eq!(maximum_field,46);
        let mut indices=vec![1usize,usize::MAX];let mut power=10usize;while let Some(next)=power.checked_mul(10){indices.extend([power-1,power]);power=next;}indices.extend([power-1,power]);
        let mut maximum_capacity=0;
        for index in indices {for count in [0,1,9,10,99,100,999,1000] {let field=format!("r1.reference{index}.distinct={count}");maximum_capacity=maximum_capacity.max(field.capacity());assert!(field.len()<=maximum_field);assert!(field.capacity()<=2*field.len()+8);}}
        // Two PE fields/resource; all terms coexist conservatively, even when
        // actual Vec-growth and formatting epochs do not overlap.
        let payload=2*(2*maximum_field+8);
        let slots=3*2*size_of::<String>();
        let joined=2*(maximum_field+1);
        let formatted=2*joined;
        let manifest_growth=3*joined;
        let old_and_new_counts=4*size_of::<usize>();
        let required=payload+slots+joined+formatted+manifest_growth+old_and_new_counts;
        assert_eq!(required,940);
        let mut gp=GlobalParams::new();gp.native_m1=true;gp.fq2_path="mate".into();gp.quality_phred=33;gp.output_quality_phred=33;gp.native_record_manifest="records.tsv".into();
        let mut settings=crate::native_processing::ProcessSettings::default();settings.screen.method="kmer-exact".into();settings.screen.k=3;settings.screen.min_hits=2;
        for i in 0..129 {let p=root.join(format!("reference{i}.fa"));std::fs::write(&p,format!(">resource{i}\nACGTACGT\n")).unwrap();settings.screen.references.push(p.to_string_lossy().into_owned());}
        gp.native_process=Arc::new(settings.clone());
        let execution=crate::native_execution::Execution::with_statistics_budget(32,2,Arc::new(AtomicBool::new(false)),None,settings.additional_statistics(true)).unwrap();
        let screen=crate::native_screen::Screen::prepare(&gp,&execution,&crate::cpu_budget::CpuBudget::new(32)).unwrap();
        let make=|mate:i32,ordinal:u64|{let mut r=FastqRecord::new(mate);r.source_index=ordinal;r.seq_id=format!("@unit{ordinal}/{mate} full comment");r.sequence="ACGTACGT".into();r.qual_seq="IIIIIIII".into();r};
        let query_ticket=execution.admit_with_feature(0,1,0,settings.feature_chunk_bound(1,0,false,false).unwrap()).unwrap();
        let a=make(1,0);let b=make(2,0);let query=screen.evaluate(0,&[&a,&b],true).unwrap();assert_eq!(query.counts[0].len(),129);assert!(query.counts.iter().all(|v|v.capacity()==129));
        drop(query);drop(query_ticket);
        let persistent=execution.facts()["memory_budget"]["current_feature_reserved_bytes"].as_u64().unwrap();let mut observations=Vec::new();
        for references in [1usize,2,3,7,8,9,31,32,33,63,64,65,127,128,129] {
            let mut evaluations=Vec::new();let mut growth_slots=0;let mut field_payload=0;
            for mate in 1..=2 {for index in 1..=references {let text=format!("r{mate}.reference{index}.distinct=1000");field_payload+=text.capacity();let old=evaluations.capacity();evaluations.push(text);growth_slots=growth_slots.max((if old==evaluations.capacity(){old}else{old+evaluations.capacity()})*size_of::<String>());}}
            let joined_fields=evaluations.join(";");assert_eq!(joined_fields.capacity(),joined_fields.len());
            assert!(growth_slots<=slots*references+4*size_of::<String>());
            for units in [1usize,2,3,7,8,9,15,16,17,31,32,33] {
                let mut local=gp.clone();let mut s=settings.clone();s.screen.references.truncate(references);local.native_process=Arc::new(s);
                let feature=local.native_process.feature_chunk_bound(units,0,false,false).unwrap();
                let ticket=execution.admit_with_feature(0,units,0,feature).unwrap();let mut d=DiagnosticBatch::new(8);let mut base=DiagnosticBatch::new(8);let mut peak_manifest_old_new=0;
                for i in 0..units {
                    let a=make(1,u64::MAX-units as u64+i as u64);let b=make(2,a.source_index);
                    d.reference=Some(crate::native_screen::Evidence{mates:[crate::native_screen::State::H;2],unit:crate::native_screen::State::H,rejected:false,counts:[vec![1000;references],vec![1000;references]]});
                    base.reference=Some(crate::native_screen::Evidence{mates:[crate::native_screen::State::H;2],unit:crate::native_screen::State::H,rejected:false,counts:Default::default()});
                    let old=d.manifest.capacity();d.record(&a,Some(&b),&local,false,false,[false;4],[[Some(false);9];2],None).unwrap();base.record(&a,Some(&b),&local,false,false,[false;4],[[Some(false);9];2],None).unwrap();peak_manifest_old_new=peak_manifest_old_new.max(if old==d.manifest.capacity(){old}else{old+d.manifest.capacity()});
                }
                let bytes=std::str::from_utf8(&d.manifest).unwrap();let last=bytes.lines().last().unwrap();let cells:Vec<_>=last.split('\t').collect();let row=format!("{}\t{}\t{}\t{}\t{}\n",make(1,u64::MAX-1).source_index,cells[1],cells[2],cells[3],cells[4]);assert!(row.capacity()<=2*row.len()+8);
                // Include the entire observed manifest overlap. Fixed row fields
                // are separately bounded by the explicit screen4096/unit term.
                let fixed=base.manifest.len()/units;
                let actual_envelope=peak_manifest_old_new+field_payload+growth_slots+joined_fields.capacity()+row.capacity()+4*references*size_of::<usize>();
                let dedicated=1024usize.checked_mul(references).and_then(|n|n.checked_mul(units)).unwrap()+4096*units+4*4096;
                assert!(actual_envelope<=dedicated,"refs={references} units={units} actual={actual_envelope} dedicated={dedicated}");
                assert!(peak_manifest_old_new<=3*d.manifest.len()+16);assert!(d.manifest.len()<=units*(fixed+joined*references));
                observations.push(serde_json::json!({"references":references,"units":units,"manifest_len":d.manifest.len(),"manifest_capacity":d.manifest.capacity(),"manifest_old_new_peak":peak_manifest_old_new,"evaluation_string_payload":field_payload,"evaluation_vec_old_new_peak":growth_slots,"joined_capacity":joined_fields.capacity(),"row_format_capacity":row.capacity(),"simultaneous_upper_bytes":actual_envelope,"dedicated_resource_and_fixed_screen_bytes":dedicated}));
                drop(d);drop(base);drop(ticket);assert_eq!(execution.facts()["memory_budget"]["current_feature_reserved_bytes"].as_u64().unwrap(),persistent);
            }
        }
        let with=settings.feature_chunk_bound(33,0,false,false).unwrap();let mut no_resources=settings.clone();no_resources.screen.references.clear();let without=no_resources.feature_chunk_bound(33,0,false,false).unwrap();
        let proof=serde_json::json!({"maximum_field_length":maximum_field,"maximum_observed_field_capacity":maximum_capacity,"payload_per_resource":payload,"evaluation_slots_old_new_per_resource":slots,"joined_per_resource":joined,"formatted_per_resource":formatted,"manifest_old_new_per_resource_unit":manifest_growth,"count_vectors_old_new_per_resource":old_and_new_counts,"required_per_resource_unit":required,"actual_charge_delta":with-without,"required_delta":required*129*33,"raw_allowance_used":0,"observations":observations});std::fs::write(root.join("proof.json"),serde_json::to_vec_pretty(&proof).unwrap()).unwrap();
        drop(screen);assert_eq!(execution.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
        assert!(settings.feature_chunk_bound(usize::MAX,0,false,false).is_err());assert!(settings.feature_chunk_bound(1,usize::MAX,false,false).is_err());assert_eq!(crate::native_processing::ProcessSettings::default().feature_chunk_bound(1,0,false,false).unwrap(),0);
        assert!(with-without>=required*129*33,"configured charge does not cover derived simultaneous reference envelope; proof={}",root.join("proof.json").display());
    }
}

#[cfg(test)]
mod r66_tests {
    use super::*;
    use serde_json::{Value, json};
    use std::sync::Arc;

    fn read(index: u64, mate: i32, sequence: &str, qualities: &[u8], phred: i32) -> FastqRecord {
        let mut r = FastqRecord::new(mate);
        r.source_index = index;
        r.seq_id = format!("@r{index}/{mate} preserved comment");
        r.sequence = sequence.into();
        r.qual_seq = String::from_utf8(qualities.iter().map(|q| q + phred as u8).collect()).unwrap();
        r
    }
    fn signature(stat: &FileStat) -> Value {
        let q = stat.native_qc.as_ref().unwrap();
        json!({"reads":q.reads,"bases":q.bases,"qualities":q.qualities.as_slice(),"lengths":q.lengths,
            "cycles":q.cycles.iter().map(|c|json!({"bases":c.bases,"qualities":c.qualities.as_slice()})).collect::<Vec<_>>()})
    }
    fn differential(gp: &GlobalParams, first: Vec<FastqRecord>, second: Option<Vec<FastqRecord>>) -> PeLocalStats {
        let mut actual = PeLocalStats::new(gp);
        let mut original = PeLocalStats::new(gp);
        let mut diagnostic = DiagnosticBatch::new(1000);
        let a = apply_recorded(first.clone(), second.clone(), &mut actual, gp, None);
        let b = apply_recorded(first, second, &mut original, gp, Some(&mut diagnostic));
        match (a,b) {
            (Ok(a),Ok(b)) => {
                assert_eq!(a.0.iter().map(FastqRecord::to_string).collect::<Vec<_>>(), b.0.iter().map(FastqRecord::to_string).collect::<Vec<_>>());
                assert_eq!(a.1.iter().map(FastqRecord::to_string).collect::<Vec<_>>(), b.1.iter().map(FastqRecord::to_string).collect::<Vec<_>>());
            },
            (Err(a),Err(b)) => assert_eq!(a,b),
            _ => panic!("different outcome"),
        }
        for (a,b) in [(&actual.raw1,&original.raw1),(&actual.raw2,&original.raw2),(&actual.clean1,&original.clean1),(&actual.clean2,&original.clean2)] { assert_eq!(signature(a),signature(b)); }
        assert_eq!(actual.fs.native.rejected,original.fs.native.rejected);
        assert_eq!(actual.fs.native.selected,original.fs.native.selected);
        assert_eq!(actual.fs.native.not_selected,original.fs.native.not_selected);
        actual
    }
    fn settings(phred: i32) -> GlobalParams {
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.quality_phred = phred;
        gp.output_quality_phred = phred;
        Arc::make_mut(&mut gp.native_process).max_length = Some(1000);
        gp
    }
    #[test]
    fn r66_unchanged_components_and_encodings_match_original_path() {
        for phred in [33,64] {
            for paired in [false,true] {
                for mode in 0..5 {
                    let mut gp = settings(phred);
                    if mode==1 || mode==4 { gp.native_quality_mode=crate::native_processing::QualityMode::Window;gp.native_quality_q=Some(20);gp.native_quality_window=4; }
                    if mode==2 || mode==4 { gp.native_tail_mask=31;gp.native_tail_min=10; }
                    if mode==3 || mode==4 { Arc::make_mut(&mut gp.native_process).min_complexity=Some(0.5); }
                    let first=(0..3).map(|i|read(i,1,&"ACGT".repeat(37),&vec![40;148],phred)).collect();
                    let second=paired.then(||(0..3).map(|i|read(i,2,&"TGCA".repeat(37),&vec![40;148],phred)).collect());
                    let local=differential(&gp,first,second);
                    assert_eq!(local.raw1.native_qc.as_ref().unwrap().reads,3);
                    assert_eq!(local.clean1.native_qc.as_ref().unwrap().qualities[40],444);
                    assert_eq!(local.clean1.native_qc.as_ref().unwrap().lengths[148],3);
                }
            }
        }
    }
    #[test]
    fn r66_changed_and_rejected_records_rebuild_accepted_literal_qc() {
        let mut gp=settings(33);gp.native_quality_mode=crate::native_processing::QualityMode::Window;gp.native_quality_q=Some(20);gp.native_quality_window=2;Arc::make_mut(&mut gp.native_process).min_complexity=Some(0.5);
        let local=differential(&gp,vec![read(0,1,"ACGT",&[40;4],33),read(1,1,"TTTT",&[40;4],33),read(2,1,"ACGT",&[40,40,0,0],33)],None);
        let raw=local.raw1.native_qc.as_ref().unwrap();let clean=local.clean1.native_qc.as_ref().unwrap();
        assert_eq!(raw.reads,3);assert_eq!(raw.qualities[40],10);assert_eq!(raw.qualities[0],2);
        assert_eq!(clean.reads,2);assert_eq!(clean.qualities[40],6);assert_eq!(clean.bases,[2,2,1,1,0,0]);assert_eq!(clean.lengths[2],1);assert_eq!(clean.lengths[4],1);
    }
    #[test]
    fn r66_failed_prefix_keeps_changed_rejected_and_atomic_mate_counts() {
        for first_kind in 0..3 {
            for bad_mate in [1,2] {
                for invalid in 0..3 {
                    let mut gp=settings(33);gp.fq2_path="r2".into();
                    if first_kind==1 {gp.native_cuts=[[1,0],[1,0]];}
                    if first_kind==2 {Arc::make_mut(&mut gp.native_process).min_complexity=Some(0.5);}
                    let sequence=if first_kind==2 {"TTTT"}else{"ACGT"};
                    let mut first=vec![read(0,1,sequence,&[40;4],33),read(1,1,"ACGT",&[40;4],33)];
                    let mut second=vec![read(0,2,sequence,&[40;4],33),read(1,2,"TGCA",&[40;4],33)];
                    let bad=if bad_mate==1 {&mut first[1]}else{&mut second[1]};
                    match invalid {0=>bad.sequence="ZCGT".into(),1=>bad.qual_seq=" III".into(),_=>{bad.sequence="A".repeat(1001);bad.qual_seq="I".repeat(1001);}}
                    let local=differential(&gp,first,Some(second));
                    for raw in [&local.raw1,&local.raw2] {assert_eq!(raw.native_qc.as_ref().unwrap().reads,1);assert_eq!(raw.native_qc.as_ref().unwrap().lengths[4],1);}
                    for clean in [&local.clean1,&local.clean2] {assert_eq!(clean.native_qc.as_ref().unwrap().reads,if first_kind==2 {0}else{1});if first_kind!=2 {assert_eq!(clean.native_qc.as_ref().unwrap().qualities[40],if first_kind==1 {3}else{4});}}
                }
            }
        }
    }
    #[test]
    fn r66_selection_conversion_empty_and_rejection_fallbacks() {
        let mut gp=settings(33);Arc::make_mut(&mut gp.native_process).sample_fraction=0.0;
        let local=differential(&gp,vec![read(0,1,"ACGT",&[40;4],33)],None);assert_eq!(local.clean1.native_qc.as_ref().unwrap().reads,1);assert_eq!(local.fs.native.not_selected,1);
        Arc::make_mut(&mut gp.native_process).sample_fraction=1.0;gp.output_quality_phred=64;
        let local=differential(&gp,vec![read(0,1,"ACGT",&[40;4],33),read(1,1,"ACGT",&[63;4],33)],None);assert_eq!(local.raw1.native_qc.as_ref().unwrap().reads,2);assert_eq!(local.clean1.native_qc.as_ref().unwrap().reads,1);
        gp.output_quality_phred=33;Arc::make_mut(&mut gp.native_process).min_length=5;
        let local=differential(&gp,vec![read(0,1,"",&[],33),read(1,1,"ACGT",&[40;4],33)],None);assert_eq!(local.raw1.native_qc.as_ref().unwrap().reads,2);assert_eq!(local.clean1.native_qc.as_ref().unwrap().reads,0);
    }
    #[test]
    fn r66_two_batches_merge_only_the_current_raw_prefix() {
        let gp=settings(33);let mut local=PeLocalStats::new(&gp);
        apply_recorded(vec![read(0,1,"ACGT",&[40;4],33)],None,&mut local,&gp,None).unwrap();
        let prior=local.clean1.native_qc.replace(Box::new(crate::native_qc::Qc::batch())).unwrap();
        apply_recorded(vec![read(1,1,"TGCA",&[40;4],33),read(2,1,"ACGT",&[40;4],33)],None,&mut local,&gp,None).unwrap();
        assert_eq!(local.raw1.native_qc.as_ref().unwrap().reads,3);assert_eq!(local.raw1.native_qc.as_ref().unwrap().qualities[40],12);assert_eq!(local.clean1.native_qc.as_ref().unwrap().reads,2);
        let mut combined=prior;combined.merge(local.clean1.native_qc.as_ref().unwrap());assert_eq!(combined.reads,3);assert_eq!(combined.qualities[40],12);
    }
}

#[cfg(test)]
mod typed_ordinal_tests {
    use super::*;
    #[test]
    fn invalid_quality_carries_known_ordinal_without_message_parsing() {
        let mut gp=GlobalParams::new();gp.native_m1=true;gp.quality_phred=33;
        let mut read=FastqRecord::new(1);read.source_index=17;
        read.seq_id="u".into();read.sequence="AC".into();read.qual_seq="\u{1}I".into();
        let mut stats=PeLocalStats::new(&gp);
        let error=apply_recorded_ticket(vec![read],None,&mut stats,&gp,None,None).err().expect("invalid quality must fail");
        assert_eq!(error.category,crate::failure::Category::Input);
        assert_eq!(error.stage,"input");assert_eq!(error.ordinal,Some(17));
        let changed=error.with_message("unrelated worker/write/FASTQ words");
        assert_eq!(changed.ordinal,Some(17));assert_eq!(changed.category,crate::failure::Category::Input);
    }
}
