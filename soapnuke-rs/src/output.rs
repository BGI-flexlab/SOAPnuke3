use crate::failure::OutputError;
// Output layer: gzip shard writing (libdeflate level 2), ordered concatenation
// sink, empty-output members, log file. Mirrors v2 ordered_output_sink.cpp +
// fastq_shared_process.cpp output behavior.
use crate::cpu_budget::{CpuBudget, Stage};
use crate::ffi::Compressor;
use crate::params::GlobalParams;
use crate::record::FastqRecord;
use std::borrow::Cow;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

pub fn wants_gzip(path: &str) -> bool {
    path.ends_with(".gz")
}

pub fn total_output_name(name: &str) -> Result<String, String> {
    let path = Path::new(name);
    let basename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("invalid clean output name: {}", name))?;
    let total_basename = format!("total.{basename}");
    Ok(path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(|parent| parent.join(&total_basename))
        .unwrap_or_else(|| PathBuf::from(total_basename))
        .to_string_lossy()
        .into_owned())
}

pub fn serialize_record(rec: &FastqRecord, is_fasta: bool, out: &mut Vec<u8>) {
    if is_fasta {
        out.push(b'>');
        let seq_id = rec.seq_id.strip_prefix('@').unwrap_or(&rec.seq_id);
        out.extend_from_slice(seq_id.as_bytes());
        out.push(b'\n');
        out.extend_from_slice(rec.sequence.as_bytes());
        out.push(b'\n');
    } else {
        out.extend_from_slice(rec.seq_id.as_bytes());
        out.push(b'\n');
        out.extend_from_slice(rec.sequence.as_bytes());
        out.push(b'\n');
        out.push(b'+');
        out.push(b'\n');
        out.extend_from_slice(rec.qual_seq.as_bytes());
        out.push(b'\n');
    }
}

pub(crate) fn serialize_native_record(rec:&FastqRecord,fasta:bool,gp:&GlobalParams,channel:&str,out:&mut Vec<u8>)->Result<(),String>{
    if let Some(io)=&gp.native_io {
        let phred=io.output_phred(channel);
        if !fasta && phred!=33 {
            if rec.qual_seq.bytes().any(|b|b as i32-33+phred>126){return Err("input: quality.unrepresentable emitted channel".into());}
            out.extend_from_slice(rec.seq_id.as_bytes());out.push(b'\n');out.extend_from_slice(rec.sequence.as_bytes());out.extend_from_slice(b"\n+\n");
            out.extend(rec.qual_seq.bytes().map(|b|(b as i32-33+phred)as u8));out.push(b'\n');return Ok(());
        }
    }
    serialize_record(rec,fasta,out);Ok(())
}
pub fn prepare_shared_output(records: &mut [FastqRecord], gp: &GlobalParams) {
    if gp.output_file_type != "fastq" || gp.output_quality_phred == gp.quality_phred {
        return;
    }
    let delta = gp.output_quality_phred - gp.quality_phred;
    for rec in records.iter_mut() {
        for b in unsafe { rec.qual_seq.as_mut_vec() }.iter_mut() {
            *b = (*b as i32 + delta) as u8;
        }
    }
}

fn prefix_member<'a>(
    member: &'a crate::pipeline::Member,
    records: u64,
    gp: &GlobalParams,
    budget: &Arc<CpuBudget>,
) -> Result<Cow<'a, [u8]>, String> {
    if records >= member.recs {
        return Ok(Cow::Borrowed(&member.bytes));
    }
    let _permit = budget.acquire(Stage::Sink);
    let raw = if member.gzip {
        Cow::Owned(inflate_gz_member(&member.bytes)?)
    } else {
        Cow::Borrowed(&member.bytes)
    };
    let lines_per_record = if gp.output_file_type == "fasta" { 2 } else { 4 };
    let mut end = 0usize;
    for _ in 0..records {
        for _ in 0..lines_per_record {
            let newline = raw[end..]
                .iter()
                .position(|&byte| byte == b'\n')
                .ok_or_else(|| "head output: record boundary not found".to_string())?;
            end += newline + 1;
        }
    }
    if !member.gzip {
        return Ok(Cow::Owned(raw[..end].to_vec()));
    }
    let compressor = Compressor::new(2)?;
    let compressed = compressor.gzip_compress_alloc(&raw[..end]);
    if compressed.is_empty() {
        return Err("libdeflate gzip compress failed".into());
    }
    Ok(Cow::Owned(compressed))
}

// Fully decompress a (possibly multi-member) gzip byte string into memory
// (split-mode sink path). compress_split emits one gzip member per segment,
// so several members may be concatenated in a single Member payload.
fn inflate_gz_member(bytes: &[u8]) -> Result<Vec<u8>, String> {
    crate::zlib_reader::inflate_gzip_bytes(bytes)
}

pub struct Sink {
    staging_dir: String,
    output_dir: String,
    clean1: Option<File>,
    clean2: Option<File>,
    trim1: Option<File>,
    trim2: Option<File>,
    output_bytes: [u64; 4],
    // Split mode (cleanOutSplit > 0): per-mate split.N. families, each with
    // its own file number and per-file record counter (v2 semantics: mate1
    // and mate2 count independently).
    split1: Option<SplitState>,
    split2: Option<SplitState>,
    split_comp: Option<Compressor>,
    sample_comp: Option<Compressor>,
    sample_state: Option<SamplingState>,
    head_mode: bool,
    head_remaining: u64,
    #[cfg(test)]
    promotion_fail_after: Option<usize>,
    #[cfg(test)]
    write_fail_after: Option<usize>,
}

#[derive(Clone)]
struct SplitState {
    size: u64,
    next: u64,
    recs: u64,
    staged: Vec<(String, String)>,
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> SplitMix64 {
        SplitMix64 { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn uniform_below(&mut self, bound: u64) -> u64 {
        if bound <= 1 {
            return 0;
        }
        let threshold = bound.wrapping_neg() % bound;
        loop {
            let value = self.next_u64();
            if value >= threshold {
                return value % bound;
            }
        }
    }
}

struct SamplingState {
    remaining_needed: u64,
    remaining_total: u64,
    rng: SplitMix64,
}

impl SamplingState {
    fn new(target: u64, retained: u64) -> SamplingState {
        SamplingState {
            remaining_needed: target,
            remaining_total: retained,
            rng: SplitMix64::new(0x534F41504E554B45),
        }
    }

    fn select_next(&mut self) -> Result<bool, String> {
        if self.remaining_total == 0 {
            return Err("sampling pass produced more retained records than its first pass".into());
        }
        let selected = if self.remaining_needed == 0 {
            false
        } else if self.remaining_needed >= self.remaining_total {
            true
        } else {
            self.rng.uniform_below(self.remaining_total) < self.remaining_needed
        };
        self.remaining_total -= 1;
        if selected {
            self.remaining_needed -= 1;
        }
        Ok(selected)
    }

    fn is_complete(&self) -> bool {
        self.remaining_needed == 0 && self.remaining_total == 0
    }
}

fn write_member_group<F>(
    slot: usize,
    name: &str,
    file: &mut Option<File>,
    members: &Option<Vec<crate::pipeline::Member>>,
    write_payload: &mut F,
) -> Result<(), String>
where
    F: FnMut(usize, &str, &mut Option<File>, &[u8]) -> Result<(), String>,
{
    if let Some(members) = members {
        for member in members {
            write_payload(slot, name, file, &member.bytes)?;
        }
    }
    Ok(())
}

fn split_target(base: &str, file_number: u64) -> String {
    let suffix = format!("split.{}.", file_number);
    match base.rfind('/') {
        Some(slash) => format!("{}{}{}", &base[..=slash], suffix, &base[slash + 1..]),
        None => format!("{}{}", suffix, base),
    }
}

fn fastq_record_end(bytes: &[u8], start: usize) -> Result<usize, String> {
    let mut position = start;
    for _ in 0..4 {
        let newline = bytes[position..]
            .iter()
            .position(|&byte| byte == b'\n')
            .ok_or_else(|| "sampled output member has an incomplete FASTQ record".to_string())?;
        position += newline + 1;
    }
    Ok(position)
}

pub(crate) struct NativeSplitFile {
    number: u64,
    paths: [Option<PathBuf>; 3],
    staged: [Option<PathBuf>; 3],
    units: u64,
    records: [u64; 3],
    bases: [u64; 3],
    first_ordinal: Option<u64>,
    last_ordinal: Option<u64>,
    closed: bool,
    _storage: Option<crate::native_execution::FeatureTicket>,
}
pub(crate) struct NativeSplit {
    index: Vec<NativeSplitFile>,
    index_storage: Option<crate::native_execution::FeatureTicket>,
    base_storage: Option<crate::native_execution::FeatureTicket>,
    files: [Option<File>; 3],
    runtime: Arc<crate::native_runtime::Runtime>,
    compressor: Compressor,
    finished: bool,
}
// Controlled-capacity expansion also supports repeated occurrences of the same
// chunk variable; decimal conversion is stack-only and zero pads to six digits.
pub(crate) fn native_chunk_path(template: &str, number: u64) -> PathBuf {
    let mut digits = [b'0'; 20];
    let mut n = number;
    let mut start = 20;
    loop {
        start -= 1;
        digits[start] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    start = start.min(14);
    let decimal = std::str::from_utf8(&digits[start..]).unwrap();
    let mut result =
        String::with_capacity(template.len() + template.matches("{chunk}").count() * 20);
    let mut pieces = template.split("{chunk}");
    result.push_str(pieces.next().unwrap());
    for piece in pieces {
        result.push_str(decimal);
        result.push_str(piece);
    }
    PathBuf::from(result)
}
impl NativeSplit {
    pub(crate) fn new(gp: &GlobalParams) -> Result<Self, OutputError> {
        let runtime = gp
            .native_runtime
            .as_ref()
            .ok_or("split requires native runtime")?
            .clone();
        let bound = gp.native_process.split_metadata_unit_bound;
        let storage = runtime.reserve_split_storage(
            bound
                + 2 * crate::native_execution::QC_BYTES
                + if gp.native_process.overlap.merge {
                    crate::native_qc::Qc::merged_maximum_heap_bytes()
                } else {
                    0
                }
                + std::mem::size_of::<Self>(),
        ).map_err(OutputError::from)?;
        let mut result = Self {
            index: Vec::with_capacity(1),
            index_storage: None,
            base_storage: Some(storage),
            files: Default::default(),
            runtime,
            compressor: Compressor::new(gp.native_gzip_level)?,
            finished: false,
        };
        result.open_chunk(gp, None)?;
        Ok(result)
    }
    fn open_chunk(
        &mut self,
        gp: &GlobalParams,
        mut ticket: Option<&mut crate::native_execution::Ticket>,
    ) -> Result<(), OutputError> {
        let templates = gp
            .native_split_templates
            .as_ref()
            .ok_or("split templates unavailable")?;
        let storage = if self.index.is_empty() {
            None
        } else {
            Some(
                ticket
                    .as_deref_mut()
                    .ok_or("split metadata requires admitted chunk")?
                    .retain_feature(gp.native_process.split_metadata_unit_bound).map_err(OutputError::from)?,
            )
        };
        if self.index.len() == self.index.capacity() {
            let capacity = self
                .index
                .capacity()
                .checked_mul(2)
                .ok_or("split index capacity overflow")?;
            let new_storage = ticket
                .as_deref_mut()
                .ok_or("split index requires admitted chunk")?
                .retain_feature(
                    capacity
                        .checked_mul(std::mem::size_of::<NativeSplitFile>())
                        .ok_or("split index bytes overflow")?,
                ).map_err(OutputError::from)?;
            let mut replacement = Vec::with_capacity(capacity);
            replacement.append(&mut self.index);
            self.index = replacement;
            self.index_storage = Some(new_storage);
        }
        let number = (self.index.len() as u64)
            .checked_add(1)
            .ok_or("split number overflow")?;
        let mut entry = NativeSplitFile {
            number,
            paths: Default::default(),
            staged: Default::default(),
            units: 0,
            records: [0; 3],
            bases: [0; 3],
            first_ordinal: None,
            last_ordinal: None,
            closed: false,
            _storage: storage,
        };
        for (i, template) in templates.iter().enumerate() {
            let destination = native_chunk_path(&template.to_string_lossy(), number);
            let suffix = if wants_gzip(
                gp.native_main_name(
                    gp.native_process
                        .overlap
                        .channel(i, gp.native_out_interleaved),
                ),
            ) {
                ".fq.gz"
            } else {
                ".fq"
            };
            let mut stage_template = String::with_capacity(gp.output_dir.len() + 64);
            stage_template.push_str(&gp.output_dir);
            stage_template.push_str(
                match gp
                    .native_process
                    .overlap
                    .channel(i, gp.native_out_interleaved)
                {
                    0 => "/split-{chunk}.r1",
                    1 => "/split-{chunk}.r2",
                    2 => "/split-{chunk}.merged",
                    _ => unreachable!(),
                },
            );
            stage_template.push_str(suffix);
            let staged = native_chunk_path(&stage_template, number);
            entry.paths[i] = Some(destination);
            entry.staged[i] = Some(staged);
        }
        self.index.push(entry);
        for i in 0..templates.len() {
            let entry = self.index.last().unwrap();
            let destination = entry.paths[i].as_ref().unwrap();
            let staged = entry.staged[i].as_ref().unwrap();
            if destination.symlink_metadata().is_ok() {
                return Err(format!(
                    "split destination exists: {}",
                    destination.display()
                ).into());
            }
            self.files[i] = Some(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(staged)
                    .map_err(|e| format!("split create: {e}"))?,
            );
        }
        Ok(())
    }
    fn close_chunk(&mut self, gp: &GlobalParams) -> Result<(), OutputError> {
        let entry = self.index.last_mut().unwrap();
        for (i, file) in self.files.iter_mut().enumerate() {
            if let Some(mut file) = file.take() {
                if entry.records[i] == 0
                    && wants_gzip(
                        gp.native_main_name(
                            gp.native_process
                                .overlap
                                .channel(i, gp.native_out_interleaved),
                        ),
                    )
                {
                    let member = self.compressor.gzip_compress_alloc(&[]);
                    if member.is_empty() {
                        return Err("split empty gzip compression failed".into());
                    }
                    file.write_all(&member)
                        .map_err(|e| format!("split empty member: {e}"))?;
                }
                file.flush()
                    .and_then(|_| file.sync_all())
                    .map_err(|e| format!("split required close: {e}"))?;
            }
        }
        entry.closed = true;
        Ok(())
    }
    pub(crate) fn append(
        &mut self,
        gp: &GlobalParams,
        members: &mut crate::pipeline::MemberSet,
        budget: &Arc<CpuBudget>,
    ) -> Result<(), OutputError> {
        let _permit = budget.acquire(Stage::Sink);
        let records = members
            .native_split_records
            .as_ref()
            .ok_or("split selected records unavailable")?;
        let ticket = members
            .ticket
            .as_mut()
            .ok_or("split admitted ticket unavailable")?;
        let ordinals = &members.native_selected_ordinals;
        let total = ordinals.len();
        let mut position = 0;
        while position < total {
            if self.index.last().unwrap().units == gp.native_process.split_units as u64 {
                self.close_chunk(gp)?;
                self.open_chunk(gp, Some(&mut *ticket))?;
            }
            let count = ((gp.native_process.split_units as u64 - self.index.last().unwrap().units)
                as usize)
                .min(total - position);
            let end = position + count;
            let first = ordinals[position];
            let last = ordinals[end - 1];
            let ranges: [std::ops::Range<usize>; 3] = std::array::from_fn(|c| {
                records[c].partition_point(|r| r.source_index < first)
                    ..records[c].partition_point(|r| r.source_index <= last)
            });
            let mut qcs = [
                crate::native_qc::Qc::batch(),
                crate::native_qc::Qc::batch(),
                crate::native_qc::Qc::merged(),
            ];
            for channel in 0..3 {
                let slice = &records[channel][ranges[channel].clone()];
                let maximum = slice.iter().map(|r| r.sequence.len()).max().unwrap_or(0);
                if channel == 2 {
                    qcs[channel].set_merged_maximum(maximum);
                } else {
                    qcs[channel].set_batch_maximum(maximum);
                }
                for r in slice {
                    qcs[channel].observe_validated(
                        r.sequence.as_bytes(),
                        r.qual_seq.as_bytes(),
                        gp.output_quality_phred,
                    );
                }
            }
            let channels = gp.native_split_templates.as_ref().unwrap().len();
            for physical in 0..channels {
                let channel = gp
                    .native_process
                    .overlap
                    .channel(physical, gp.native_out_interleaved);
                let mut payload = Vec::new();
                let slice = &records[channel][ranges[channel].clone()];
                if channel == 0 && gp.native_out_interleaved {
                    let mates = &records[1][ranges[1].clone()];
                    if mates.len() != slice.len() {
                        return Err("split paired association lost".into());
                    }
                    for (a, b) in slice.iter().zip(mates) {
                        serialize_record(a, false, &mut payload);
                        serialize_record(b, false, &mut payload);
                    }
                } else {
                    for r in slice {
                        serialize_record(r, false, &mut payload);
                    }
                }
                #[cfg(test)]
                if physical == 1
                    && std::env::var_os("SOAPNUKE_TEST_NATIVE_FAIL_R2_APPEND").is_some()
                {
                    return Err("injected actual native split R2 append failure".into());
                }
                if wants_gzip(gp.native_main_name(channel)) {
                    let member = self.compressor.gzip_compress_alloc(&payload);
                    if member.is_empty() {
                        return Err("split gzip compression failed".into());
                    }
                    self.files[physical]
                        .as_mut()
                        .unwrap()
                        .write_all(&member)
                        .map_err(|e| format!("split append: {e}"))?;
                } else {
                    self.files[physical]
                        .as_mut()
                        .unwrap()
                        .write_all(&payload)
                        .map_err(|e| format!("split append: {e}"))?;
                }
            }
            let entry = self.index.last_mut().unwrap();
            entry.units += count as u64;
            entry.first_ordinal.get_or_insert(first);
            entry.last_ordinal = Some(last);
            for physical in 0..channels {
                let channel = gp
                    .native_process
                    .overlap
                    .channel(physical, gp.native_out_interleaved);
                if channel == 2 {
                    self.runtime.appended_merged(&qcs[2]);
                } else {
                    self.runtime.appended(channel, &qcs[channel]);
                }
                entry.records[physical] += qcs[channel].reads;
                entry.bases[physical] += qcs[channel].bases.iter().sum::<u64>();
                if channel == 0 && gp.native_out_interleaved {
                    self.runtime.appended(1, &qcs[1]);
                    entry.records[physical] += qcs[1].reads;
                    entry.bases[physical] += qcs[1].bases.iter().sum::<u64>();
                }
            }
            position = end;
        }
        Ok(())
    }
    pub(crate) fn finish(
        &mut self,
        gp: &GlobalParams,
        budget: &Arc<CpuBudget>,
    ) -> Result<(), OutputError> {
        let _permit = budget.acquire(Stage::Sink);
        self.close_chunk(gp)?;
        self.finished = true;
        Ok(())
    }
}
impl Drop for NativeSplit {
    fn drop(&mut self) {
        self.files = Default::default();
        let index_capacity_bytes = self.index.capacity() * std::mem::size_of::<NativeSplitFile>();
        let path_capacity_bytes = self
            .index
            .iter()
            .flat_map(|e| e.paths.iter().chain(e.staged.iter()))
            .flatten()
            .map(PathBuf::capacity)
            .sum::<usize>();
        let reserved_bytes = self.base_storage.as_ref().map_or(0, |t| t.reserved_bytes())
            + self
                .index_storage
                .as_ref()
                .map_or(0, |t| t.reserved_bytes())
            + self
                .index
                .iter()
                .filter_map(|e| e._storage.as_ref())
                .map(|t| t.reserved_bytes())
                .sum::<usize>();
        let entries:Vec<serde_json::Value>=self.index.iter().map(|e|serde_json::json!({"chunk":e.number,"selected_units":e.units,"first_global_ordinal":e.first_ordinal,"last_global_ordinal":e.last_ordinal,"paths":e.paths,"staged":e.staged,"read_records":e.records,"bases":e.bases,"closed":e.closed})).collect();
        // Processing index capacity remains charged through the conversion;
        // only the completed JSON report representation is excluded metadata.
        self.index = Vec::new();
        self.index_storage = None;
        self.base_storage = None;
        *self.runtime.split.lock().unwrap_or_else(|p| p.into_inner()) = serde_json::json!({"chunks":entries,"processing_complete":self.finished,"index_capacity_bytes":index_capacity_bytes,"path_capacity_bytes":path_capacity_bytes,"retained_feature_reservation_bytes":reserved_bytes,"scope":"selected source-unit boundaries; physical counts include only successfully appended complete fragments; failed current fragment excluded","index_handoff":"charged processing index released after conversion to report metadata"});
    }
}

impl Sink {
    pub fn new(gp: &GlobalParams, tmp_dir: &str) -> Result<Sink, String> {
        Self::new_with_sample(gp, tmp_dir, None)
    }

    pub fn new_with_sample(
        gp: &GlobalParams,
        tmp_dir: &str,
        sample_target: Option<(u64, u64)>,
    ) -> Result<Sink, String> {
        // Split mode writes split.N.<base> files on demand; the base names are
        // never created as files (v2 split_target family).
        let split = gp.clean_out_split > 0;
        let staging_dir = format!("{}/{}", gp.output_dir, tmp_dir);
        let open_opt = |name: &str, split_clean: bool| -> Result<Option<File>, String> {
            if name.is_empty() || (split && split_clean) {
                return Ok(None);
            }
            let path = format!("{}/{}", staging_dir, name);
            if let Some(parent) = Path::new(&path).parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    format!(
                        "cannot create staging output directory {} ({})",
                        parent.display(),
                        e
                    )
                })?;
            }
            File::create(&path)
                .map(Some)
                .map_err(|e| format!("cannot open such file,{} ({})", path, e))
        };
        let clean1 = open_opt(&gp.clean_fq1, true)?;
        let clean2 = open_opt(&gp.clean_fq2, true)?;
        let trim1 = open_opt(&gp.trim_fq1, false)?;
        let trim2 = open_opt(&gp.trim_fq2, false)?;
        let split_comp = if split {
            Some(Compressor::new(2)?)
        } else {
            None
        };
        let sample_comp = if sample_target.is_some()
            && (wants_gzip(&gp.clean_fq1) || wants_gzip(&gp.clean_fq2))
        {
            Some(Compressor::new(2)?)
        } else {
            None
        };
        let mk_st = |size: u64| -> Option<SplitState> {
            if size > 0 {
                Some(SplitState {
                    size,
                    next: 0,
                    recs: 0,
                    staged: Vec::new(),
                })
            } else {
                None
            }
        };
        Ok(Sink {
            staging_dir,
            output_dir: gp.output_dir.clone(),
            clean1,
            clean2,
            trim1,
            trim2,
            output_bytes: [0; 4],
            split1: mk_st(gp.clean_out_split),
            split2: mk_st(gp.clean_out_split),
            split_comp,
            sample_comp,
            sample_state: sample_target
                .map(|(target, retained)| SamplingState::new(target, retained)),
            head_mode: !gp.total_reads_num_random && gp.total_reads_num > 0.0,
            head_remaining: if !gp.total_reads_num_random && gp.total_reads_num > 0.0 {
                gp.l_total_reads_num
            } else {
                0
            },
            #[cfg(test)]
            promotion_fail_after: None,
            #[cfg(test)]
            write_fail_after: None,
        })
    }

    // Append the compressed members of one batch to the final output files.
    // Members arrive over the order channel (no tmp_dir round-trip).
    pub fn append_members(
        &mut self,
        gp: &GlobalParams,
        members: &crate::pipeline::MemberSet,
        budget: &Arc<CpuBudget>,
    ) -> Result<(), String> {
        if gp.native_m1 {
            #[cfg(test)]
            let mut write_count = 0;
            for (channel, (file, group)) in [
                (&mut self.clean1, &members.clean1),
                (&mut self.clean2, &members.clean2),
            ]
            .into_iter()
            .enumerate()
            {
                if let (Some(file), Some(group)) = (file.as_mut(), group) {
                    for member in group {
                        #[cfg(test)]
                        {
                            if self
                                .write_fail_after
                                .is_some_and(|limit| write_count >= limit)
                            {
                                return Err(format!(
                                    "injected sink write failure channel{}",
                                    channel + 1
                                ));
                            }
                            write_count += 1;
                        }
                        #[cfg(test)]
                        if channel == 1
                            && std::env::var_os("SOAPNUKE_TEST_NATIVE_FAIL_R2_APPEND").is_some()
                        {
                            return Err("injected actual native R2 append failure".into());
                        }
                        file.write_all(&member.bytes).map_err(|e| {
                            format!("cannot append native output channel{}: {e}", channel + 1)
                        })?;
                        self.output_bytes[channel] += member.bytes.len() as u64;
                    }
                    if let (Some(runtime), Some(qc)) =
                        (&gp.native_runtime, &members.native_written[channel])
                    {
                        // A complete append is already observed. Its metadata must be
                        // merged even if cancellation races it; withdrawn CPU tickets
                        // keep this non-cancellable accounting acquisition live.
                        {
                            let _permit = budget.acquire(Stage::Sink);
                            runtime.appended(channel, qc);
                            if channel == 0 && gp.native_out_interleaved {
                                if let Some(qc) = &members.native_written[1] {
                                    runtime.appended(1, qc);
                                }
                            }
                        }
                        runtime.event("appended_complete_chunk", false);
                    }
                }
            }
            return Ok(());
        }
        let split = gp.clean_out_split > 0;
        let mut written_bytes = [0u64; 4];
        #[cfg(test)]
        let mut write_count = 0usize;
        #[cfg(test)]
        let write_fail_after = self.write_fail_after;
        let mut write_payload =
            |slot: usize, _name: &str, f: &mut Option<File>, bytes: &[u8]| -> Result<(), String> {
                if let Some(fd) = f.as_mut() {
                    #[cfg(test)]
                    {
                        if write_fail_after.is_some_and(|limit| write_count >= limit) {
                            return Err(format!("injected sink write failure for {}", _name));
                        }
                        write_count += 1;
                    }
                    fd.write_all(bytes)
                        .map_err(|e| format!("cannot append to output: {}", e))?;
                    written_bytes[slot] = written_bytes[slot].saturating_add(bytes.len() as u64);
                }
                Ok(())
            };
        if split {
            // Sink-side split: members arrive as one whole batch of `recs`
            // records; decompress and re-slice at record boundaries across
            // batches so every split.N.<name> file holds at most split_size
            // records (v2 semantics: mate1/mate2 count independently).
            // trim output is not split.
            let comp = self.split_comp.as_ref().unwrap();
            Self::append_split(
                comp,
                &gp.clean_fq1,
                &members.clean1,
                gp,
                self.split1.as_mut().unwrap(),
                &self.staging_dir,
                &self.output_dir,
                budget,
            )?;
            Self::append_split(
                comp,
                &gp.clean_fq2,
                &members.clean2,
                gp,
                self.split2.as_mut().unwrap(),
                &self.staging_dir,
                &self.output_dir,
                budget,
            )?;
        } else if self.sample_state.is_some() {
            self.append_sample(gp, members, budget, &mut write_payload)?;
        } else if self.head_mode {
            self.append_head(gp, members, budget, &mut write_payload)?;
        } else {
            write_member_group(
                0,
                &gp.clean_fq1,
                &mut self.clean1,
                &members.clean1,
                &mut write_payload,
            )?;
            write_member_group(
                1,
                &gp.clean_fq2,
                &mut self.clean2,
                &members.clean2,
                &mut write_payload,
            )?;
        }
        write_member_group(
            2,
            &gp.trim_fq1,
            &mut self.trim1,
            &members.trim1,
            &mut write_payload,
        )?;
        write_member_group(
            3,
            &gp.trim_fq2,
            &mut self.trim2,
            &members.trim2,
            &mut write_payload,
        )?;
        #[cfg(test)]
        {
            self.write_fail_after = write_fail_after;
        }
        for (total, written) in self.output_bytes.iter_mut().zip(written_bytes) {
            *total = total.saturating_add(written);
        }
        Ok(())
    }

    fn append_sample(
        &mut self,
        gp: &GlobalParams,
        members: &crate::pipeline::MemberSet,
        budget: &Arc<CpuBudget>,
        write_payload: &mut impl FnMut(usize, &str, &mut Option<File>, &[u8]) -> Result<(), String>,
    ) -> Result<(), String> {
        let clean1 = members.clean1.as_ref().map(Vec::as_slice).unwrap_or(&[]);
        let clean2 = members.clean2.as_ref().map(Vec::as_slice).unwrap_or(&[]);
        let paired = !gp.clean_fq2.is_empty();
        if paired && clean1.len() != clean2.len() {
            return Err("paired FASTQ output members have different batch counts".into());
        }
        let mut selected1 = Vec::new();
        let mut selected2 = Vec::new();
        for (index, first) in clean1.iter().enumerate() {
            let second = if paired { Some(&clean2[index]) } else { None };
            if let Some(second) = second {
                if first.recs != second.recs {
                    return Err("paired FASTQ output members have different record counts".into());
                }
            }
            let (raw1, raw2) = {
                // Member inflation and decoded-buffer construction are CPU
                // work too; keep them under the same narrow sink permit and
                // release it before scanning and before any file write.
                let _permit = budget.acquire(Stage::Sink);
                let raw1 = if first.gzip {
                    inflate_gz_member(&first.bytes)?
                } else {
                    first.bytes.clone()
                };
                let raw2 = match second {
                    Some(member) if member.gzip => Some(inflate_gz_member(&member.bytes)?),
                    Some(member) => Some(member.bytes.clone()),
                    None => None,
                };
                (raw1, raw2)
            };
            let mut pos1 = 0usize;
            let mut pos2 = 0usize;
            {
                // Sampling needs decompression, FASTQ framing, selection, and
                // copying accounted as sink CPU. The permit is dropped before
                // the later file writes and before gzip recompression so it
                // cannot be held across blocking I/O or nested acquisition.
                let _permit = budget.acquire(Stage::Sink);
                for _ in 0..first.recs {
                    let end1 = fastq_record_end(&raw1, pos1)?;
                    let end2 = match raw2.as_ref() {
                        Some(raw) => Some(fastq_record_end(raw, pos2)?),
                        None => None,
                    };
                    let selected = self
                        .sample_state
                        .as_mut()
                        .expect("sample state checked by caller")
                        .select_next()?;
                    if selected {
                        selected1.extend_from_slice(&raw1[pos1..end1]);
                        if let Some(raw) = raw2.as_ref() {
                            selected2.extend_from_slice(&raw[pos2..end2.unwrap()]);
                        }
                    }
                    pos1 = end1;
                    if let Some(end) = end2 {
                        pos2 = end;
                    }
                }
            }
            if pos1 != raw1.len() || raw2.as_ref().is_some_and(|raw| pos2 != raw.len()) {
                return Err("sampled output member has trailing FASTQ bytes".into());
            }
        }
        let gzip1 = wants_gzip(&gp.clean_fq1);
        let gzip2 = wants_gzip(&gp.clean_fq2);
        let out1 = self.encode_sample_payload(&selected1, gzip1, budget)?;
        if !selected1.is_empty() {
            write_payload(0, &gp.clean_fq1, &mut self.clean1, &out1)?;
        }
        if paired {
            let out2 = self.encode_sample_payload(&selected2, gzip2, budget)?;
            if !selected2.is_empty() {
                write_payload(1, &gp.clean_fq2, &mut self.clean2, &out2)?;
            }
        }
        Ok(())
    }

    fn encode_sample_payload(
        &self,
        payload: &[u8],
        gzip: bool,
        budget: &Arc<CpuBudget>,
    ) -> Result<Vec<u8>, String> {
        if payload.is_empty() {
            return Ok(Vec::new());
        }
        if !gzip {
            return Ok(payload.to_vec());
        }
        let _permit = budget.acquire(Stage::Sink);
        let compressor = self
            .sample_comp
            .as_ref()
            .ok_or_else(|| "sampling gzip compressor is not initialized".to_string())?;
        let encoded = compressor.gzip_compress_alloc(payload);
        if encoded.is_empty() {
            return Err("libdeflate gzip compress failed".into());
        }
        Ok(encoded)
    }

    fn append_head(
        &mut self,
        gp: &GlobalParams,
        members: &crate::pipeline::MemberSet,
        budget: &Arc<CpuBudget>,
        write_payload: &mut impl FnMut(usize, &str, &mut Option<File>, &[u8]) -> Result<(), String>,
    ) -> Result<(), String> {
        let clean1 = members.clean1.as_ref().map(Vec::as_slice).unwrap_or(&[]);
        let clean2 = members.clean2.as_ref().map(Vec::as_slice).unwrap_or(&[]);
        let paired = !gp.clean_fq2.is_empty();
        if paired && clean1.len() != clean2.len() {
            return Err("paired FASTQ output members have different batch counts".into());
        }
        let mut remaining = self.head_remaining;
        for index in 0..clean1.len() {
            if remaining == 0 {
                break;
            }
            let first = &clean1[index];
            let second = if paired { Some(&clean2[index]) } else { None };
            if paired && second.is_some_and(|member| member.recs != first.recs) {
                return Err("paired FASTQ output members have different record counts".into());
            }
            let take = remaining.min(first.recs);
            if take == 0 {
                continue;
            }
            let first_bytes = prefix_member(first, take, gp, budget)?;
            write_payload(0, &gp.clean_fq1, &mut self.clean1, first_bytes.as_ref())?;
            if let Some(second) = second {
                let second_bytes = prefix_member(second, take, gp, budget)?;
                write_payload(1, &gp.clean_fq2, &mut self.clean2, second_bytes.as_ref())?;
            }
            remaining -= take;
        }
        self.head_remaining = remaining;
        Ok(())
    }

    fn append_split(
        comp: &Compressor,
        name: &str,
        ms: &Option<Vec<crate::pipeline::Member>>,
        gp: &GlobalParams,
        st: &mut SplitState,
        staging_dir: &str,
        output_dir: &str,
        budget: &Arc<CpuBudget>,
    ) -> Result<(), String> {
        let ms = match ms {
            Some(m) => m,
            None => return Ok(()),
        };
        let is_fasta = gp.output_file_type == "fasta";
        let lines_per_rec = if is_fasta { 2 } else { 4 };
        for m in ms {
            if m.recs == 0 {
                continue;
            }
            let inflated = if m.gzip {
                let _permit = budget.acquire(Stage::Sink);
                Some(inflate_gz_member(&m.bytes)?)
            } else {
                None
            };
            let raw = inflated.as_deref().unwrap_or(&m.bytes);
            let mut pos = 0usize;
            let mut done: u64 = 0;
            while done < m.recs {
                let (staging_path, gz, end, take) = {
                    let _permit = budget.acquire(Stage::Sink);
                    let room = st.size - st.recs;
                    let take = room.min(m.recs - done);
                    let mut end = pos;
                    for _ in 0..take {
                        for _ in 0..lines_per_rec {
                            let nl = raw[end..]
                                .iter()
                                .position(|&b| b == b'\n')
                                .ok_or_else(|| "split record: newline not found".to_string())?;
                            end += nl + 1;
                        }
                    }
                    let encoded = if wants_gzip(name) {
                        let gz = comp.gzip_compress_alloc(&raw[pos..end]);
                        if gz.is_empty() {
                            return Err("libdeflate gzip compress failed".into());
                        }
                        gz
                    } else {
                        raw[pos..end].to_vec()
                    };
                    let path = split_target(&format!("{}/{}", output_dir, name), st.next);
                    let staging_path = split_target(&format!("{}/{}", staging_dir, name), st.next);
                    if !Self::staged_path_seen(st, &staging_path) {
                        st.staged.push((staging_path.clone(), path));
                    }
                    st.recs += take;
                    if st.recs >= st.size {
                        st.next += 1;
                        st.recs = 0;
                    }
                    (staging_path, encoded, end, take)
                };
                // Directory creation and writes are deliberately outside the
                // sink compute permit; they may block on the filesystem.
                if let Some(parent) = Path::new(&staging_path).parent() {
                    fs::create_dir_all(parent).map_err(|e| {
                        format!(
                            "cannot create staging output directory {} ({})",
                            parent.display(),
                            e
                        )
                    })?;
                }
                let mut fd = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&staging_path)
                    .map_err(|e| format!("cannot open such file,{} ({})", staging_path, e))?;
                fd.write_all(&gz)
                    .map_err(|e| format!("cannot append to output: {}", e))?;
                pos = end;
                done += take;
            }
        }
        Ok(())
    }

    fn staged_path_seen(st: &SplitState, path: &str) -> bool {
        st.staged.iter().any(|(staged, _)| staged == path)
    }

    // v2 ensure_empty_output: each direct gzip output that received no bytes
    // still gets one empty gzip member (gzopen "wb" + close).  This is based
    // on bytes written, not on the number of batches presented to the sink.
    pub fn finish(&mut self, gp: &GlobalParams, budget: &Arc<CpuBudget>) -> Result<(), String> {
        if let Some(state) = self.sample_state.as_ref() {
            if !state.is_complete() {
                return Err(format!(
                    "sampling pass ended with {} records unaccounted and {} records still required",
                    state.remaining_total, state.remaining_needed
                ));
            }
        }
        self.ensure_empty_gzip(&gp.clean_fq1, 0, budget)?;
        self.ensure_empty_gzip(&gp.clean_fq2, 1, budget)?;
        self.ensure_empty_gzip(&gp.trim_fq1, 2, budget)?;
        self.ensure_empty_gzip(&gp.trim_fq2, 3, budget)?;
        let mut paths = Vec::new();
        Self::collect_file_path(
            &self.staging_dir,
            &self.output_dir,
            &mut self.clean1,
            &gp.clean_fq1,
            &mut paths,
        )?;
        Self::collect_file_path(
            &self.staging_dir,
            &self.output_dir,
            &mut self.clean2,
            &gp.clean_fq2,
            &mut paths,
        )?;
        Self::collect_file_path(
            &self.staging_dir,
            &self.output_dir,
            &mut self.trim1,
            &gp.trim_fq1,
            &mut paths,
        )?;
        Self::collect_file_path(
            &self.staging_dir,
            &self.output_dir,
            &mut self.trim2,
            &gp.trim_fq2,
            &mut paths,
        )?;
        Self::collect_split_paths(&mut self.split1, &mut paths);
        Self::collect_split_paths(&mut self.split2, &mut paths);
        Self::validate_destinations(&paths)?;
        #[cfg(test)]
        let fail_after = self.promotion_fail_after;
        #[cfg(not(test))]
        let fail_after = None;
        Self::promote_paths(&paths, fail_after)
    }

    fn ensure_empty_gzip(
        &mut self,
        name: &str,
        slot: usize,
        budget: &Arc<CpuBudget>,
    ) -> Result<(), String> {
        if self.output_bytes[slot] != 0 || !wants_gzip(name) {
            return Ok(());
        }
        let file = match slot {
            0 => self.clean1.as_mut(),
            1 => self.clean2.as_mut(),
            2 => self.trim1.as_mut(),
            3 => self.trim2.as_mut(),
            _ => unreachable!(),
        };
        let Some(file) = file else {
            return Ok(());
        };
        let empty = {
            let _permit = budget.acquire(Stage::Sink);
            let comp = Compressor::new(2)?;
            comp.gzip_compress_alloc(&[])
        };
        if empty.is_empty() {
            return Err("libdeflate gzip compress failed".into());
        }
        file.write_all(&empty)
            .map_err(|e| format!("cannot write output: {}", e))?;
        self.output_bytes[slot] = empty.len() as u64;
        Ok(())
    }

    fn collect_file_path(
        staging_dir: &str,
        output_dir: &str,
        file: &mut Option<File>,
        name: &str,
        paths: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        if name.is_empty() || file.is_none() {
            return Ok(());
        }
        if let Some(fd) = file.take() {
            fd.sync_all()
                .map_err(|e| format!("cannot flush staging output {}: {}", name, e))?;
        }
        let staging = format!("{}/{}", staging_dir, name);
        let final_path = format!("{}/{}", output_dir, name);
        paths.push((staging, final_path));
        Ok(())
    }

    fn collect_split_paths(state: &mut Option<SplitState>, paths: &mut Vec<(String, String)>) {
        let state = match state.take() {
            Some(state) => state,
            None => return,
        };
        paths.extend(state.staged);
    }

    fn validate_destinations(paths: &[(String, String)]) -> Result<(), String> {
        for (index, (_, destination)) in paths.iter().enumerate() {
            if Path::new(destination).exists() {
                return Err(format!("output already exists: {}", destination));
            }
            if paths[..index]
                .iter()
                .any(|(_, previous)| previous == destination)
            {
                return Err(format!("duplicate output destination: {}", destination));
            }
        }
        Ok(())
    }

    fn promote_paths(paths: &[(String, String)], fail_after: Option<usize>) -> Result<(), String> {
        let mut moved = Vec::new();
        for (staging, destination) in paths {
            if fail_after.is_some_and(|limit| moved.len() >= limit) {
                let error = "injected output promotion failure".to_string();
                return Err(Self::rollback_promotions(&moved, error));
            }
            if let Err(error) = fs::hard_link(staging, destination) {
                let message = format!(
                    "cannot finalize output {} from staging file {}: {}",
                    destination, staging, error
                );
                return Err(Self::rollback_promotions(&moved, message));
            }
            if let Err(error) = fs::remove_file(staging) {
                let _ = fs::remove_file(destination);
                let message = format!(
                    "cannot remove staging output {} after publication: {}",
                    staging, error
                );
                return Err(Self::rollback_promotions(&moved, message));
            }
            moved.push((staging.clone(), destination.clone()));
        }
        Ok(())
    }

    fn rollback_promotions(moved: &[(String, String)], error: String) -> String {
        let mut rollback_error = None;
        for (staging, destination) in moved.iter().rev() {
            if let Err(rollback) = fs::rename(destination, staging) {
                if rollback_error.is_none() {
                    rollback_error = Some(format!(
                        "cannot roll back published output {} to {}: {}",
                        destination, staging, rollback
                    ));
                }
            }
        }
        match rollback_error {
            Some(rollback) => format!("{}; {}", error, rollback),
            None => error,
        }
    }

    /// Close an interrupted sink without manufacturing an empty gzip member.
    /// Any bytes already written remain available for failure diagnosis, but
    /// the output is deliberately not presented as a successful run.
    pub fn abort(&mut self) {
        self.clean1.take();
        self.clean2.take();
        self.trim1.take();
        self.trim2.take();
        self.split_comp.take();
    }

    #[cfg(test)]
    fn fail_promotion_after(&mut self, count: usize) {
        self.promotion_fail_after = Some(count);
    }

    #[cfg(test)]
    fn fail_write_after(&mut self, count: usize) {
        self.write_fail_after = Some(count);
    }
}

pub fn remove_tmp_dir(gp: &GlobalParams, tmp_dir: &str) -> Result<(), String> {
    let dir = format!("{}/{}", gp.output_dir, tmp_dir);
    if let Ok(rd) = fs::read_dir(&dir) {
        for entry in rd.flatten() {
            let _ = fs::remove_file(entry.path());
        }
    }
    fs::remove_dir(&dir)
        .map_err(|e| format!("cannot remove temporary output directory {}: {}", dir, e))
}

pub fn make_tmp_dir(gp: &GlobalParams) -> Result<String, String> {
    let pattern = format!("{}/TMPXXXXXX", gp.output_dir);
    let v: Vec<u8> = pattern.into_bytes();
    // mkdtemp-style: replace trailing XXXXXX
    let rand_suffix: String = (0..6)
        .map(|_| {
            let c = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
                % 26) as u8;
            (b'a' + c) as char
        })
        .collect();
    let mut name = String::from_utf8(v).unwrap();
    let len = name.len();
    name.replace_range(len - 6.., &rand_suffix);
    fs::create_dir(&name)
        .map_err(|e| format!("cannot create temporary output directory: {} ({})", name, e))?;
    Ok(name[gp.output_dir.len() + 1..].to_string())
}

pub fn get_local_time() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    // local time via libc
    let t = now;
    let tm = unsafe {
        let mut tm: libc_tm = std::mem::zeroed();
        let mut v = t;
        localtime_r(&mut v, &mut tm);
        tm
    };
    format!(
        "{}-{}-{}  {}:{}:{}",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec
    )
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
struct libc_tm {
    tm_sec: i32,
    tm_min: i32,
    tm_hour: i32,
    tm_mday: i32,
    tm_mon: i32,
    tm_year: i32,
    tm_wday: i32,
    tm_yday: i32,
    tm_isdst: i32,
    tm_gmtoff: i64,
    tm_zone: *const i8,
}
extern "C" {
    fn localtime_r(timep: *const i64, result: *mut libc_tm) -> *mut libc_tm;
}

pub fn write_log_line(path: &str, msg: &str) -> Result<(), String> {
    let mut f = File::options()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("cannot open such file,{} ({})", path, e))?;
    writeln!(f, "{}\t{}", get_local_time(), msg).map_err(|e| e.to_string())
}

fn normalize_destination(output_dir: &str, name: &str) -> Result<PathBuf, String> {
    let mut relative = PathBuf::new();
    for component in Path::new(name).components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => relative.push(part),
            Component::ParentDir => {
                if !relative.pop() {
                    return Err(format!(
                        "output destination escapes output directory: {}",
                        name
                    ));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "output destination must be relative to output directory: {}",
                    name
                ));
            }
        }
    }
    Ok(Path::new(output_dir).join(relative))
}

fn split_family_exists(base: &Path) -> Result<bool, String> {
    let parent = base.parent().unwrap_or_else(|| Path::new("."));
    let basename = base
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("invalid split output name: {}", base.display()))?;
    let suffix = format!(".{}", basename);
    let entries = match fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(format!(
                "cannot inspect split output directory {}: {}",
                parent.display(),
                error
            ))
        }
    };
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "cannot inspect split output directory {}: {}",
                parent.display(),
                error
            )
        })?;
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();
        if filename.starts_with("split.") && filename.ends_with(&suffix) {
            let number_end = filename.len() - suffix.len();
            let number = &filename["split.".len()..number_end];
            if !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit()) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn is_generated_split_destination(base: &Path, candidate: &Path) -> bool {
    if base.parent() != candidate.parent() {
        return false;
    }
    let Some(base_name) = base.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let Some(candidate_name) = candidate.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let suffix = format!(".{base_name}");
    if !candidate_name.starts_with("split.") || !candidate_name.ends_with(&suffix) {
        return false;
    }
    let number_end = candidate_name.len() - suffix.len();
    let number = &candidate_name["split.".len()..number_end];
    !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
}

// v2 ordered_output_sink preflight: reject if clean/trim outputs already exist.
pub fn preflight(gp: &GlobalParams) -> Result<(), String> {
    if gp.is_streaming {
        // Streaming writes only to stdout. The clean/trim names are retained
        // for CLI compatibility but are not filesystem destinations.
        return Ok(());
    }
    if gp.clean_out_split > 0 {
        let split_bases: Vec<PathBuf> = [gp.clean_fq1.as_str(), gp.clean_fq2.as_str()]
            .into_iter()
            .filter(|destination| !destination.is_empty())
            .map(|destination| normalize_destination(&gp.output_dir, destination))
            .collect::<Result<_, _>>()?;
        if split_bases
            .iter()
            .enumerate()
            .any(|(index, path)| split_bases[..index].contains(path))
        {
            return Err("clean split bases duplicate another output destination".into());
        }
        for destination in [gp.clean_fq1.as_str(), gp.clean_fq2.as_str()] {
            if destination.is_empty() {
                continue;
            }
            let path = normalize_destination(&gp.output_dir, destination)?;
            if split_family_exists(&path)? {
                return Err("split output already exists".into());
            }
        }
        let direct_destinations: Vec<PathBuf> = [gp.trim_fq1.as_str(), gp.trim_fq2.as_str()]
            .into_iter()
            .filter(|destination| !destination.is_empty())
            .map(|destination| normalize_destination(&gp.output_dir, destination))
            .collect::<Result<_, _>>()?;
        for (index, path) in direct_destinations.iter().enumerate() {
            if path.exists() {
                return Err(format!("output already exists: {}", path.display()));
            }
            if direct_destinations[..index].contains(path) {
                return Err(
                    "direct output destinations duplicate another output destination".into(),
                );
            }
        }
        for direct in &direct_destinations {
            if split_bases
                .iter()
                .any(|base| is_generated_split_destination(base, direct))
            {
                return Err(format!(
                    "split output collides with direct output destination: {}",
                    direct.display()
                ));
            }
        }
    } else {
        let mut destinations = vec![
            ("cleanFq1", gp.clean_fq1.clone()),
            ("cleanFq2", gp.clean_fq2.clone()),
            ("trimFq1", gp.trim_fq1.clone()),
            ("trimFq2", gp.trim_fq2.clone()),
        ];
        if gp.sampling_target.is_some() {
            let total1 = total_output_name(&gp.clean_fq1)?;
            destinations.push(("totalCleanFq1", total1));
            if !gp.clean_fq2.is_empty() {
                let total2 = total_output_name(&gp.clean_fq2)?;
                destinations.push(("totalCleanFq2", total2));
            }
        }
        let mut normalized: Vec<PathBuf> = Vec::new();
        for (name, destination) in destinations {
            if destination.is_empty() {
                continue;
            }
            let path = normalize_destination(&gp.output_dir, &destination)?;
            if path.exists() {
                return Err(format!("output already exists: {}", path.display()));
            }
            if normalized.iter().any(|previous| previous == &path) {
                return Err(format!("{} duplicates another output destination", name));
            }
            normalized.push(path);
        }
    }
    Ok(())
}

fn sampling_output_pairs(gp: &GlobalParams) -> Result<Vec<(PathBuf, PathBuf)>, String> {
    let mut pairs = Vec::new();
    for clean_name in [gp.clean_fq1.as_str(), gp.clean_fq2.as_str()] {
        if clean_name.is_empty() {
            continue;
        }
        let requested = normalize_destination(&gp.output_dir, clean_name)?;
        let total = normalize_destination(&gp.output_dir, &total_output_name(clean_name)?)?;
        pairs.push((total, requested));
    }
    Ok(pairs)
}

pub fn remove_sampling_outputs(gp: &GlobalParams) -> Result<(), String> {
    for (total, requested) in sampling_output_pairs(gp)? {
        for path in [total, requested] {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "cannot remove sampling output {}: {}",
                        path.display(),
                        error
                    ))
                }
            }
        }
    }
    Ok(())
}

pub fn rename_sampling_totals(gp: &GlobalParams) -> Result<(), String> {
    let pairs = sampling_output_pairs(gp)?;
    let mut moved = Vec::new();
    for (total, requested) in &pairs {
        if let Err(error) = fs::rename(total, requested) {
            for (old, new) in moved.iter().rev() {
                let _ = fs::rename(new, old);
            }
            return Err(format!(
                "cannot publish sampled output {} from {}: {}",
                requested.display(),
                total.display(),
                error
            ));
        }
        moved.push((total, requested));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn native_split_orders_pairs_across_batches_preserves_empty_chunks_and_releases_index() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../.scratch/native-split-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut gp = crate::params::GlobalParams::new();
        gp.native_m1 = true;
        gp.fq2_path = "mate".into();
        gp.output_quality_phred = 33;
        gp.clean_fq1 = "r1.fq".into();
        gp.clean_fq2 = "r2.fq".into();
        gp.output_dir = dir.to_string_lossy().into_owned();
        gp.native_split_templates = Some(std::sync::Arc::new(vec![
            dir.join("final-{chunk}.r1.fq"),
            dir.join("final-{chunk}.r2.fq"),
        ]));
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.split_units = 1;
        settings.split_metadata_unit_bound = 16384;
        gp.native_process = std::sync::Arc::new(settings);
        let runtime = std::sync::Arc::new(crate::native_runtime::Runtime::new(0, "split".into()));
        let execution = crate::native_execution::Execution::new(
            2,
            2,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .unwrap();
        runtime.set_execution(Some(execution.clone()));
        gp.native_runtime = Some(runtime.clone());
        let cpu = crate::cpu_budget::CpuBudget::new(2);
        let mut sink = super::NativeSplit::new(&gp).unwrap();
        for ordinal in [0, 2500] {
            let mut members = crate::pipeline::MemberSet::default();
            members.ticket = Some(execution.admit_with_feature(128, 1, 128, 65536).unwrap());
            let make = |mate| {
                let mut r = crate::record::FastqRecord::new(mate);
                r.source_index = ordinal;
                r.seq_id = format!("@u{ordinal}/{mate}");
                r.sequence = "AC".into();
                r.qual_seq = "II".into();
                r
            };
            members.native_selected_ordinals = vec![make(1).source_index];
            members.native_split_records = Some([vec![make(1)], vec![make(2)], Vec::new()]);
            sink.append(&gp, &mut members, &cpu).unwrap();
            drop(members);
        }
        sink.finish(&gp, &cpu).unwrap();
        drop(sink);
        let facts = runtime.split.lock().unwrap();
        assert_eq!(facts["chunks"].as_array().unwrap().len(), 2);
        assert_eq!(facts["chunks"][1]["first_global_ordinal"], 2500);
        assert_eq!(facts["chunks"][0]["selected_units"], 1);
        let allocated = facts["index_capacity_bytes"].as_u64().unwrap()
            + facts["path_capacity_bytes"].as_u64().unwrap();
        assert!(
            facts["retained_feature_reservation_bytes"]
                .as_u64()
                .unwrap()
                >= allocated
        );
        drop(facts);
        assert_eq!(
            execution.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        assert_eq!(runtime.written.lock().unwrap()[0].reads, 2);
        let second = std::fs::read_to_string(dir.join("split-000002.r2.fq")).unwrap();
        assert!(second.starts_with("@u2500/2\n"));
        assert!(!dir.join("final-000001.r1.fq").exists());
        let empty = dir.join("empty");
        std::fs::create_dir(&empty).unwrap();
        gp.output_dir = empty.to_string_lossy().into_owned();
        gp.clean_fq1 = "r1.fq.gz".into();
        gp.clean_fq2 = "r2.fq.gz".into();
        gp.native_split_templates = Some(std::sync::Arc::new(vec![
            empty.join("out-{chunk}.r1.fq.gz"),
            empty.join("out-{chunk}.r2.fq.gz"),
        ]));
        let mut sink = super::NativeSplit::new(&gp).unwrap();
        sink.finish(&gp, &cpu).unwrap();
        drop(sink);
        let facts = runtime.split.lock().unwrap();
        assert_eq!(facts["chunks"].as_array().unwrap().len(), 1);
        assert_eq!(facts["chunks"][0]["selected_units"], 0);
        drop(facts);
        let bytes = std::fs::read(empty.join("split-000001.r1.fq.gz")).unwrap();
        assert_eq!(&bytes[..2], &[31, 139]);
        assert_eq!(&bytes[bytes.len() - 8..], &[0; 8]);
        assert_eq!(
            execution.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
    }
    use super::{
        inflate_gz_member, preflight, rename_sampling_totals, serialize_record, split_target,
        SamplingState, Sink,
    };
    use crate::cpu_budget::CpuBudget;
    use crate::ffi::Compressor;
    use crate::params::GlobalParams;
    use crate::pipeline::{Member, MemberSet};
    use crate::record::FastqRecord;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.scratch/tests");
        fs::create_dir_all(&root).unwrap();
        root.join(format!(
            "soapnuke-output-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn output_params(dir: &PathBuf) -> GlobalParams {
        let mut gp = GlobalParams::new();
        gp.output_dir = dir.to_string_lossy().into_owned();
        gp.clean_fq1 = "clean-r1.fastq.gz".to_string();
        gp.clean_fq2 = "clean-r2.fastq.gz".to_string();
        gp
    }

    #[test]
    fn exact_sampler_is_reproducible_and_selects_the_requested_count() {
        let mut first = SamplingState::new(7, 20);
        let first_choices: Vec<bool> = (0..20).map(|_| first.select_next().unwrap()).collect();
        let mut second = SamplingState::new(7, 20);
        let second_choices: Vec<bool> = (0..20).map(|_| second.select_next().unwrap()).collect();
        assert_eq!(first_choices, second_choices);
        assert_eq!(
            first_choices,
            vec![
                true, true, false, false, false, false, false, true, false, false, false, true,
                true, true, false, false, false, true, false, false,
            ]
        );
        assert_eq!(first_choices.iter().filter(|choice| **choice).count(), 7);
        assert_eq!(first.remaining_needed, 0);
        assert_eq!(first.remaining_total, 0);
    }

    #[test]
    fn sampler_rejects_overrun_and_detects_underrun() {
        let mut fewer = SamplingState::new(3, 4);
        for _ in 0..3 {
            fewer.select_next().unwrap();
        }
        assert!(!fewer.is_complete());
        assert_eq!(fewer.remaining_total, 1);

        let mut more = SamplingState::new(3, 2);
        more.select_next().unwrap();
        more.select_next().unwrap();
        let error = more.select_next().unwrap_err();
        assert!(error.contains("more retained records"));
    }

    #[test]
    fn fasta_serialization_removes_fastq_at_header_marker() {
        let mut record = FastqRecord::new(1);
        record.seq_id = "@read/1".to_string();
        record.sequence = "ACGT".to_string();
        record.qual_seq = "IIII".to_string();
        let mut output = Vec::new();
        serialize_record(&record, true, &mut output);
        assert_eq!(output, b">read/1\nACGT\n");
    }

    #[test]
    fn preflight_rejects_existing_numbered_split_family() {
        let dir = fixture_dir("split-preflight");
        fs::create_dir_all(&dir).unwrap();
        let mut gp = output_params(&dir);
        gp.clean_out_split = 2500;
        fs::write(dir.join("split.7.clean-r1.fastq.gz"), b"existing").unwrap();
        assert!(preflight(&gp).is_err());
        assert!(dir.is_dir());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn preflight_allows_existing_unsplit_clean_base() {
        let dir = fixture_dir("split-base-existing");
        fs::create_dir_all(&dir).unwrap();
        let mut gp = output_params(&dir);
        gp.clean_out_split = 2500;
        fs::write(dir.join(&gp.clean_fq1), b"pre-existing base").unwrap();
        assert!(preflight(&gp).is_ok());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn preflight_normalizes_aliases_and_rejects_escape_without_side_effects() {
        let dir = fixture_dir("destination-alias");
        fs::create_dir_all(&dir).unwrap();
        let mut gp = output_params(&dir);
        gp.clean_fq1 = "nested/../clean-r1.fastq.gz".to_string();
        gp.clean_fq2 = "clean-r1.fastq.gz".to_string();
        assert!(preflight(&gp).is_err());
        assert!(!dir.join("nested").exists());
        assert!(!dir.join("clean-r1.fastq.gz").exists());

        let mut escape = output_params(&dir);
        escape.clean_fq1 = "../soapnuke-destination-escape.fastq.gz".to_string();
        escape.clean_fq2.clear();
        assert!(preflight(&escape).is_err());
        assert!(!dir.join("soapnuke-destination-escape.fastq.gz").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn preflight_rejects_split_family_colliding_with_trim_destination() {
        let dir = fixture_dir("split-trim-collision");
        fs::create_dir_all(&dir).unwrap();
        let mut gp = output_params(&dir);
        gp.clean_fq2.clear();
        gp.clean_out_split = 1;
        gp.trim_fq1 = "split.0.clean-r1.fastq.gz".to_string();
        assert!(preflight(&gp).is_err());
        assert!(!dir.join("split.0.clean-r1.fastq.gz").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn preflight_rejects_nested_split_family_colliding_with_trim_destination() {
        let dir = fixture_dir("nested-split-trim-collision");
        fs::create_dir_all(&dir).unwrap();
        let mut gp = output_params(&dir);
        gp.clean_fq1 = "nested/clean-r1.fastq.gz".to_string();
        gp.clean_fq2.clear();
        gp.trim_fq1 = "nested/split.0.clean-r1.fastq.gz".to_string();
        gp.clean_out_split = 1;
        assert!(preflight(&gp).is_err());
        assert!(!dir.join("nested").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn preflight_does_not_treat_split_clean_bases_as_direct_files() {
        let dir = fixture_dir("split-clean-base");
        fs::create_dir_all(&dir).unwrap();
        let mut gp = output_params(&dir);
        gp.clean_out_split = 1;
        gp.clean_fq1 = "mate1.fastq.gz".to_string();
        gp.clean_fq2 = "split.0.mate1.fastq.gz".to_string();
        assert!(preflight(&gp).is_ok());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sampling_preflight_rejects_requested_and_total_destination_conflicts() {
        let dir = fixture_dir("sampling-preflight");
        fs::create_dir_all(&dir).unwrap();
        let mut gp = output_params(&dir);
        gp.clean_fq2.clear();
        gp.clean_fq1 = "nested/clean.fastq.gz".to_string();
        gp.sampling_target = Some(crate::params::SamplingTarget::Count(1));
        fs::create_dir_all(dir.join("nested")).unwrap();
        fs::write(dir.join("nested/total.clean.fastq.gz"), b"existing").unwrap();
        assert!(preflight(&gp).is_err());
        fs::remove_file(dir.join("nested/total.clean.fastq.gz")).unwrap();
        fs::write(dir.join("nested/clean.fastq.gz"), b"existing").unwrap();
        assert!(preflight(&gp).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ordinary_promotion_failure_rolls_back_every_final_path() {
        let dir = fixture_dir("ordinary-rollback");
        fs::create_dir_all(&dir).unwrap();
        let tmp = "TMP-ordinary";
        fs::create_dir_all(dir.join(tmp)).unwrap();
        let gp = output_params(&dir);
        let final1 = dir.join(&gp.clean_fq1);
        let final2 = dir.join(&gp.clean_fq2);
        let staged1 = dir.join(tmp).join(&gp.clean_fq1);
        let staged2 = dir.join(tmp).join(&gp.clean_fq2);

        let mut sink = Sink::new(&gp, tmp).unwrap();
        sink.fail_promotion_after(1);
        let budget = CpuBudget::new(8);
        let error = sink.finish(&gp, &budget).unwrap_err();
        assert!(error.contains("injected output promotion failure"));
        assert!(!final1.exists());
        assert!(!final2.exists());
        assert!(staged1.exists());
        assert!(staged2.exists());

        drop(sink);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sink_write_failure_is_reported_before_final_publication() {
        let dir = fixture_dir("write-failure");
        fs::create_dir_all(&dir).unwrap();
        let tmp = "TMP-write-failure";
        fs::create_dir_all(dir.join(tmp)).unwrap();
        let gp = output_params(&dir);
        let mut sink = Sink::new(&gp, tmp).unwrap();
        sink.fail_write_after(0);
        let compressor = Compressor::new(2).unwrap();
        let members = MemberSet {
            clean1: Some(vec![Member {
                bytes: compressor.gzip_compress_alloc(b"@r/1\nACGT\n+\nIIII\n"),
                recs: 1,
                gzip: true,
            }]),
            clean2: Some(vec![Member {
                bytes: compressor.gzip_compress_alloc(b"@r/2\nACGT\n+\nIIII\n"),
                recs: 1,
                gzip: true,
            }]),
            ..MemberSet::default()
        };
        let budget = CpuBudget::new(1);
        let error = sink
            .append_members(&gp, &members, &budget)
            .expect_err("injected sink write unexpectedly succeeded");
        assert!(error.contains("injected sink write failure"));
        assert!(!dir.join(&gp.clean_fq1).exists());
        assert!(!dir.join(&gp.clean_fq2).exists());
        sink.abort();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sampling_write_failure_preserves_transactional_state() {
        let dir = fixture_dir("sampling-write-failure");
        fs::create_dir_all(&dir).unwrap();
        let tmp = "TMP-sampling-write-failure";
        fs::create_dir_all(dir.join(tmp)).unwrap();
        let mut gp = output_params(&dir);
        gp.sampling_target = Some(crate::params::SamplingTarget::Count(1));
        let mut sink = Sink::new_with_sample(&gp, tmp, Some((1, 1))).unwrap();
        sink.fail_write_after(0);
        let compressor = Compressor::new(2).unwrap();
        let members = MemberSet {
            clean1: Some(vec![Member {
                bytes: compressor.gzip_compress_alloc(b"@r/1\nACGT\n+\nIIII\n"),
                recs: 1,
                gzip: true,
            }]),
            clean2: Some(vec![Member {
                bytes: compressor.gzip_compress_alloc(b"@r/2\nACGT\n+\nIIII\n"),
                recs: 1,
                gzip: true,
            }]),
            ..MemberSet::default()
        };
        let budget = CpuBudget::new(1);
        let error = sink
            .append_members(&gp, &members, &budget)
            .expect_err("sampling sink write unexpectedly succeeded");
        assert!(error.contains("injected sink write failure"));
        assert!(!dir.join(&gp.clean_fq1).exists());
        assert!(!dir.join(&gp.clean_fq2).exists());
        sink.abort();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sampling_paired_rename_rolls_back_first_mate() {
        let dir = fixture_dir("sampling-paired-rename-rollback");
        fs::create_dir_all(&dir).unwrap();
        let gp = output_params(&dir);
        let total1 = dir.join("total.clean-r1.fastq.gz");
        let total2 = dir.join("total.clean-r2.fastq.gz");
        fs::write(&total1, b"r1").unwrap();
        fs::write(&total2, b"r2").unwrap();
        fs::create_dir(dir.join(&gp.clean_fq2)).unwrap();
        let error = rename_sampling_totals(&gp).unwrap_err();
        assert!(error.contains("cannot publish sampled output"));
        assert!(total1.exists());
        assert!(total2.exists());
        assert!(!dir.join(&gp.clean_fq1).exists());
        assert!(dir.join(&gp.clean_fq2).is_dir());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn split_promotion_failure_rolls_back_every_final_path() {
        let dir = fixture_dir("split-rollback");
        fs::create_dir_all(&dir).unwrap();
        let tmp = "TMP-split";
        fs::create_dir_all(dir.join(tmp)).unwrap();
        let mut gp = output_params(&dir);
        gp.clean_out_split = 1;
        let mut sink = Sink::new(&gp, tmp).unwrap();
        let compressor = Compressor::new(2).unwrap();
        let members = MemberSet {
            clean1: Some(vec![Member {
                bytes: compressor.gzip_compress_alloc(b"@r/1\nACGT\n+\nIIII\n"),
                recs: 1,
                gzip: true,
            }]),
            clean2: Some(vec![Member {
                bytes: compressor.gzip_compress_alloc(b"@r/2\nACGT\n+\nIIII\n"),
                recs: 1,
                gzip: true,
            }]),
            ..MemberSet::default()
        };
        let budget = CpuBudget::new(8);
        sink.append_members(&gp, &members, &budget).unwrap();
        sink.fail_promotion_after(1);

        let error = sink.finish(&gp, &budget).unwrap_err();
        assert!(error.contains("injected output promotion failure"));
        for name in [&gp.clean_fq1, &gp.clean_fq2] {
            let final_path = split_target(&format!("{}/{}", dir.display(), name), 0);
            let staged_path = split_target(&format!("{}/{}", dir.join(tmp).display(), name), 0);
            assert!(!PathBuf::from(final_path).exists());
            assert!(PathBuf::from(staged_path).exists());
        }

        drop(sink);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn split_member_rejects_corrupt_isize() {
        let compressor = Compressor::new(2).unwrap();
        let mut member = compressor.gzip_compress_alloc(b"@r/1\nACGT\n+\nIIII\n");
        let trailer_size = member.len() - 4;
        member[trailer_size] ^= 1;
        let error = inflate_gz_member(&member).expect_err("wrong ISIZE unexpectedly accepted");
        assert!(error.contains("gzip ISIZE mismatch"));
    }

    #[test]
    fn split_plain_output_contains_plain_fastq_bytes() {
        let dir = fixture_dir("plain-split");
        fs::create_dir_all(&dir).unwrap();
        let tmp = "TMP-plain-split";
        let mut gp = output_params(&dir);
        gp.clean_fq1 = "clean-r1.fastq".to_string();
        gp.clean_fq2.clear();
        gp.clean_out_split = 1;
        let mut sink = Sink::new(&gp, tmp).unwrap();
        let members = MemberSet {
            clean1: Some(vec![Member {
                bytes: b"@r/1\nACGT\n+\nIIII\n".to_vec(),
                recs: 1,
                gzip: false,
            }]),
            ..MemberSet::default()
        };
        let budget = CpuBudget::new(1);
        sink.append_members(&gp, &members, &budget).unwrap();
        sink.finish(&gp, &budget).unwrap();
        let path = split_target(&format!("{}/{}", dir.display(), gp.clean_fq1), 0);
        assert_eq!(fs::read(path).unwrap(), b"@r/1\nACGT\n+\nIIII\n");
        drop(sink);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn head_all_filtered_outputs_are_valid_empty_files() {
        for (label, name, gzip) in [
            ("gzip", "clean.fastq.gz", true),
            ("plain", "clean.fastq", false),
        ] {
            let dir = fixture_dir(&format!("head-empty-{label}"));
            fs::create_dir_all(&dir).unwrap();
            let tmp = "TMP-head-empty";
            let mut gp = output_params(&dir);
            gp.clean_fq1 = name.to_string();
            gp.clean_fq2.clear();
            gp.total_reads_num = 1.0;
            gp.l_total_reads_num = 1;
            gp.total_reads_num_random = false;
            let mut sink = Sink::new(&gp, tmp).unwrap();
            let members = MemberSet {
                clean1: Some(Vec::new()),
                ..MemberSet::default()
            };
            let budget = CpuBudget::new(1);
            sink.append_members(&gp, &members, &budget).unwrap();
            sink.finish(&gp, &budget).unwrap();
            let output = fs::read(dir.join(name)).unwrap();
            if gzip {
                assert!(!output.is_empty());
                assert!(inflate_gz_member(&output).unwrap().is_empty());
            } else {
                assert!(output.is_empty());
            }
            drop(sink);
            fs::remove_dir_all(dir).unwrap();
        }
    }
}
