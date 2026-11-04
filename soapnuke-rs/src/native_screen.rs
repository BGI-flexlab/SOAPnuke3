use crate::failure::{SourceError, BudgetError};
use crate::native_execution::{Execution, FeatureTicket};
use crate::params::GlobalParams;
use crate::record::FastqRecord;
use serde_json::{Value, json};
use std::fs::File;
use std::io::Read;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Settings {
    pub method: String,
    pub action: String,
    pub references: Vec<String>,
    pub k: usize,
    pub min_hits: usize,
    pub pair: String,
    pub unscorable: String,
    pub hits: Option<String>,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            method: "off".into(),
            action: "inspect".into(),
            references: Vec::new(),
            k: 31,
            min_hits: 2,
            pair: "any".into(),
            unscorable: "report".into(),
            hits: None,
        }
    }
}
impl Settings {
    pub fn enabled(&self) -> bool {
        self.method != "off"
    }
    pub fn action_enabled(&self) -> bool {
        self.enabled() && self.action != "inspect"
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    H,
    N,
    U,
    NotEvaluated,
}
impl State {
    pub fn name(self) -> &'static str {
        match self {
            Self::H => "H",
            Self::N => "N",
            Self::U => "U",
            Self::NotEvaluated => "NOT_EVALUATED",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::H => 0,
            Self::N => 1,
            Self::U => 2,
            Self::NotEvaluated => 3,
        }
    }
}
pub fn aggregate(mates: &[State], mode: &str) -> State {
    if mates.contains(&State::NotEvaluated) {
        return State::NotEvaluated;
    }
    if mode == "any" {
        if mates.contains(&State::H) {
            State::H
        } else if mates.iter().all(|s| *s == State::N) {
            State::N
        } else {
            State::U
        }
    } else if mates.contains(&State::N) {
        State::N
    } else if mates.iter().all(|s| *s == State::H) {
        State::H
    } else {
        State::U
    }
}
pub fn veto(state: State, settings: &Settings) -> bool {
    match (settings.action.as_str(), state) {
        ("inspect", _) | (_, State::NotEvaluated) => false,
        (_, State::U) => settings.unscorable == "reject",
        ("remove", State::H) | ("retain", State::N) => true,
        _ => false,
    }
}
#[derive(Debug)]
pub struct Evidence {
    pub mates: [State; 2],
    pub unit: State,
    pub rejected: bool,
    pub counts: [Vec<usize>; 2],
}
struct Resource {
    path: String,
    sha256: String,
    ids: Vec<String>,
    blob: Vec<u8>,
    words: Vec<usize>,
    _reservation: FeatureTicket,
}
fn canonical(b: u8) -> bool {
    matches!(b.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T')
}
fn complement(b: u8) -> u8 {
    match b.to_ascii_uppercase() {
        b'A' => b'T',
        b'C' => b'G',
        b'G' => b'C',
        b'T' => b'A',
        _ => b'N',
    }
}
fn checked(n: Option<usize>) -> Result<usize, BudgetError> {
    n.ok_or_else(|| "[worker] reference capacity overflow".into())
}
pub fn sha256(path: &str) -> Result<String, String> {
    let output = std::process::Command::new("sha256sum")
        .arg("--")
        .arg(path)
        .output()
        .map_err(|e| format!("[input] resource hash: {e}"))?;
    if !output.status.success() {
        return Err(format!("[input] cannot hash resource {path}"));
    }
    String::from_utf8(output.stdout)
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_owned))
        .filter(|s| s.len() == 64)
        .ok_or_else(|| "[input] invalid resource hash".into())
}
fn read_exact_file(path: &str, size: usize) -> Result<Vec<u8>, SourceError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|e| SourceError::worker(format!("[worker] resource allocation: {e}")))?;
    if bytes.capacity() != size {
        return Err(SourceError::worker("[worker] resource capacity differs from reservation"));
    }
    File::open(path)
        .and_then(|f| f.take(size as u64 + 1).read_to_end(&mut bytes))
        .map_err(|e| format!("[input] resource {path}: {e}"))?;
    if bytes.len() != size {
        return Err("[input] resource changed during read".into());
    }
    Ok(bytes)
}
impl Resource {
    fn load(path: &str, k: usize, e: &Arc<Execution>) -> Result<Self, SourceError> {
        let size = usize::try_from(
            std::fs::metadata(path)
                .map_err(|e| format!("[input] reference {path}: {e}"))?
                .len(),
        )
        .map_err(|_| SourceError::worker("[worker] reference too large"))?;
        // File snapshot, two orientation blobs, index, entry labels and parser scratch coexist.
        let bound = checked(
            size.checked_mul(96)
                .and_then(|n| n.checked_add(path.len() * 4 + 8192)),
        )?;
        let reservation = e.reserve_persistent_feature(bound)?;
        let hash = sha256(path)?;
        let bytes = read_exact_file(path, size)?;
        let text =
            std::str::from_utf8(&bytes).map_err(|_| "[input] reference FASTA must be UTF-8")?;
        let mut blob = Vec::with_capacity(checked(size.checked_mul(2))?);
        let mut words = Vec::with_capacity(checked(size.checked_mul(2))?);
        let mut ids = Vec::with_capacity(text.lines().filter(|l| l.starts_with('>')).count());
        let mut seq = Vec::with_capacity(size);
        let mut current = None;
        fn append(
            seq: &[u8],
            blob: &mut Vec<u8>,
            words: &mut Vec<usize>,
            k: usize,
        ) -> Result<(), String> {
            if seq.is_empty() {
                return Err("[input] empty reference FASTA entry".into());
            }
            for reverse in [false, true] {
                let start = blob.len();
                if reverse {
                    blob.extend(seq.iter().rev().map(|b| complement(*b)));
                } else {
                    blob.extend(seq.iter().map(|b| b.to_ascii_uppercase()));
                }
                for i in 0..seq
                    .len()
                    .saturating_sub(k)
                    .saturating_add(usize::from(seq.len() >= k))
                {
                    if blob[start + i..start + i + k].iter().all(|b| canonical(*b)) {
                        words.push(start + i);
                    }
                }
            }
            Ok(())
        }
        for line in text.lines() {
            let line = line.strip_suffix('\r').unwrap_or(line);
            if let Some(header) = line.strip_prefix('>') {
                if current.take().is_some() {
                    append(&seq, &mut blob, &mut words, k)?;
                    seq.clear();
                }
                let id = header
                    .split_whitespace()
                    .next()
                    .filter(|s| !s.is_empty())
                    .ok_or("[input] reference FASTA requires named entries")?;
                if ids.iter().any(|s| s == id) {
                    return Err("[input] duplicate reference FASTA identifier".into());
                }
                ids.push(id.to_owned());
                current = Some(());
            } else {
                if current.is_none() || line.is_empty() {
                    return Err("[input] malformed or empty reference FASTA".into());
                }
                if !line.bytes().all(|b| {
                    matches!(
                        b.to_ascii_uppercase(),
                        b'A' | b'C'
                            | b'G'
                            | b'T'
                            | b'R'
                            | b'Y'
                            | b'S'
                            | b'W'
                            | b'K'
                            | b'M'
                            | b'B'
                            | b'D'
                            | b'H'
                            | b'V'
                            | b'N'
                    )
                }) {
                    return Err("[input] reference contains non-IUPAC base".into());
                }
                seq.extend_from_slice(line.as_bytes());
            }
        }
        if current.is_none() {
            return Err("[input] reference FASTA is empty".into());
        }
        append(&seq, &mut blob, &mut words, k)?;
        words.sort_unstable_by(|a, b| blob[*a..*a + k].cmp(&blob[*b..*b + k]));
        words.dedup_by(|a, b| blob[*a..*a + k] == blob[*b..*b + k]);
        if words.is_empty() {
            return Err("[input] reference has no canonical window at configured k".into());
        }
        if sha256(path)? != hash {
            return Err("[input] reference changed during indexing".into());
        }
        let actual = bytes.capacity()
            + blob.capacity()
            + words.capacity() * std::mem::size_of::<usize>()
            + seq.capacity()
            + ids.capacity() * std::mem::size_of::<String>()
            + ids.iter().map(String::capacity).sum::<usize>();
        if actual > bound {
            return Err(SourceError::worker("[worker] reference actual capacity exceeded reservation"));
        }
        Ok(Self {
            path: path.into(),
            sha256: hash,
            ids,
            blob,
            words,
            _reservation: reservation,
        })
    }
    fn count(&self, read: &[&[u8]], k: usize) -> usize {
        read.iter()
            .filter(|word| {
                self.words
                    .binary_search_by(|p| {
                        self.blob[*p..*p + k].iter().copied()
                            .cmp(word.iter().map(|b| b.to_ascii_uppercase()))
                    })
                    .is_ok()
            })
            .count()
    }
}
#[derive(Default)]
struct Totals {
    mates: [u64; 4],
    units: [u64; 4],
    actions: u64,
    action_not_evaluated: u64,
    hits: Vec<u64>,
    per_states: Vec<[u64; 4]>,
    histograms: Vec<Vec<u64>>,
}
pub struct Screen {
    original_population: bool,
    settings: Settings,
    references: Vec<Resource>,
    supplied: Option<Supplied>,
    totals: Mutex<Totals>,
    _reservation: FeatureTicket,
}
struct Supplied {
    values: Vec<u8>,
    units: u64,
    mates: usize,
    provenance: Value,
    _reservations: Vec<FeatureTicket>,
}
impl Screen {
    pub fn prepare(
        gp: &GlobalParams,
        e: &Arc<Execution>,
        cpu: &Arc<crate::cpu_budget::CpuBudget>,
    ) -> Result<Arc<Self>, SourceError> {
        let settings = &gp.native_process.screen;
        let count = settings.references.len();
        let reserve = checked(count.checked_mul(65536).and_then(|n| {
            n.checked_add(
                16384
                    + settings
                        .references
                        .iter()
                        .map(|s| s.capacity() * 4)
                        .sum::<usize>(),
            )
        }))?;
        let reservation = e.reserve_persistent_feature(reserve)?;
        let mut references = Vec::with_capacity(count);
        if settings.method == "kmer-exact" {
            for path in &settings.references {
                let r = Resource::load(path, settings.k, e)?;
                if references
                    .iter()
                    .any(|prior: &Resource| prior.ids.iter().any(|id| r.ids.contains(id)))
                {
                    return Err("[input] duplicate reference resource identity".into());
                }
                references.push(r);
            }
        }
        let supplied = if settings.method == "supplied-hits" {
            Some(Supplied::load(gp, e, cpu)?)
        } else {
            None
        };
        let totals = Totals {
            per_states: vec![[0; 4]; references.len()],
            hits: vec![0; references.len()],
            histograms: (0..references.len())
                .map(|_| vec![0; crate::params::READ_MAX_LEN + 1])
                .collect(),
            ..Totals::default()
        };
        Ok(Arc::new(Self {
            original_population: gp.native_process.qc_only,
            settings: settings.clone(),
            references,
            supplied,
            totals: Mutex::new(totals),
            _reservation: reservation,
        }))
    }
    pub fn evaluate(
        &self,
        ordinal: u64,
        reads: &[&FastqRecord],
        eligible: bool,
    ) -> Result<Evidence, String> {
        let mut mates = [State::NotEvaluated; 2];
        let mut counts: [Vec<usize>; 2] = Default::default();
        for (mate, read) in reads.iter().enumerate() {
            if let Some(s) = &self.supplied {
                let at = usize::try_from(ordinal)
                    .ok()
                    .and_then(|n| n.checked_mul(s.mates))
                    .and_then(|n| n.checked_add(mate))
                    .ok_or("[input] supplied hit ordinal overflow")?;
                mates[mate] = match s.values.get(at).copied() {
                    Some(1) => State::N,
                    Some(2) => State::H,
                    _ => return Err("[input] supplied hit missing original mate".into()),
                };
            } else if eligible {
                let sequence = read.sequence.as_bytes();
                let mut words: Vec<&[u8]> = if sequence.len() < self.settings.k {
                    Vec::new()
                } else {
                    sequence
                        .windows(self.settings.k)
                        .filter(|w| w.iter().all(|b| canonical(*b)))
                        .collect()
                };
                words.sort_unstable_by(|a, b| {
                    a.iter().map(|v| v.to_ascii_uppercase())
                        .cmp(b.iter().map(|v| v.to_ascii_uppercase()))
                });
                words.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
                counts[mate] = self
                    .references
                    .iter()
                    .map(|r| r.count(&words, self.settings.k))
                    .collect();
                mates[mate] = if words.is_empty() {
                    State::U
                } else if counts[mate].iter().any(|n| *n >= self.settings.min_hits) {
                    State::H
                } else {
                    State::N
                };
            }
        }
        let unit = aggregate(&mates[..reads.len()], &self.settings.pair);
        let rejected = eligible && veto(unit, &self.settings);
        let mut totals = self.totals.lock().unwrap_or_else(|p| p.into_inner());
        for mate in 0..reads.len() {
            totals.mates[mates[mate].index()] += 1;
            for reference in 0..self.references.len() {
                let state = if mates[mate] == State::NotEvaluated {
                    State::NotEvaluated
                } else if mates[mate] == State::U {
                    State::U
                } else if counts[mate][reference] >= self.settings.min_hits {
                    State::H
                } else {
                    State::N
                };
                totals.per_states[reference][state.index()] += 1;
            }
            for (reference, &n) in counts[mate].iter().enumerate() {
                totals.hits[reference] += u64::from(n >= self.settings.min_hits);
                totals.histograms[reference][n] += 1;
            }
        }
        totals.units[unit.index()] += 1;
        totals.actions += u64::from(rejected);
        totals.action_not_evaluated += u64::from(!eligible && self.settings.action_enabled());
        Ok(Evidence {
            mates,
            unit,
            rejected,
            counts,
        })
    }
    pub fn report(&self) -> Value {
        let t = self.totals.lock().unwrap_or_else(|p| p.into_inner());
        let per: serde_json::Map<String, Value> = self
            .references
            .iter()
            .enumerate()
            .map(|(i, r)| (r.ids[0].clone(), json!(t.hits[i])))
            .collect();
        let resources:Vec<Value>=self.references.iter().enumerate().map(|(i,r)|json!({"resource_id":r.ids[0],"path":r.path,"sha256":r.sha256,"entry_ids":r.ids,"distinct_oriented_words":r.words.len(),"stage":if self.original_population{"original"}else{"transformed"},"population":if self.original_population{"original-input"}else{"completed-transformed"},"threshold":self.settings.min_hits,"mate_states":{"H":t.per_states[i][0],"N":t.per_states[i][1],"U":t.per_states[i][2],"NOT_EVALUATED":t.per_states[i][3]},"distinct_shared_word_count_histogram":t.histograms[i],"index_capacity_bytes":r.blob.capacity()+r.words.capacity()*std::mem::size_of::<usize>(),"reservation_bytes":r._reservation.reserved_bytes()})).collect();
        json!({"method":self.settings.method,"action":self.settings.action,"pair_rule":self.settings.pair,"unscorable_policy":if self.settings.action_enabled(){Some(self.settings.unscorable.as_str())}else{None},"population":if self.supplied.is_some()||self.original_population{"original-input"}else{"completed-transformed"},"stage":if self.supplied.is_some()||self.original_population{"original"}else{"transformed"},"mate_states":{"H":t.mates[0],"N":t.mates[1],"U":t.mates[2],"NOT_EVALUATED":t.mates[3]},"unit_states":{"H":t.units[0],"N":t.units[1],"U":t.units[2],"NOT_EVALUATED":t.units[3]},"action_failed_units":t.actions,"action_not_evaluated_units":t.action_not_evaluated,"per_reference_hits":if self.supplied.is_some(){Value::Null}else{Value::Object(per)},"k":if self.supplied.is_none(){Some(self.settings.k)}else{None},"minimum_distinct_hits":if self.supplied.is_none(){Some(self.settings.min_hits)}else{None},"resources":resources,"supplied_provenance":self.supplied.as_ref().map(|s|&s.provenance),"supplied_full_source_units":self.supplied.as_ref().map(|s|s.units),"word_scope":"distinct case-insensitive canonical A/C/G/T read words intersect each complete reference resource and its reverse complements; no cross-resource pooling","not_evaluated_scope":"native completed-transformed evidence is absent after early original fallback; supplied original evidence remains recorded, actions not evaluated","memory_scope":"persistent index/table/report aggregates remain reserved until job report is complete; query scratch charged with admitted batch"})
    }
}
impl Supplied {
    fn load(
        gp: &GlobalParams,
        e: &Arc<Execution>,
        cpu: &Arc<crate::cpu_budget::CpuBudget>,
    ) -> Result<Self, SourceError> {
        let path = gp
            .native_process
            .screen
            .hits
            .as_ref()
            .ok_or("[input] supplied hits path missing")?;
        let sidecar = format!("{path}.provenance.json");
        let size = usize::try_from(
            std::fs::metadata(&sidecar)
                .map_err(|err| format!("[input] supplied hit provenance: {err}"))?
                .len(),
        )
        .map_err(|_| SourceError::worker("[worker] supplied provenance too large"))?;
        let meta_bound = checked(size.checked_mul(1024).and_then(|n| n.checked_add(65536)))?;
        let meta_ticket = e.reserve_persistent_feature(meta_bound)?;
        let sidecar_hash = sha256(&sidecar)?;
        let bytes = read_exact_file(&sidecar, size)?;
        let meta: Value = decode_provenance(&bytes)?;
        if meta["version"] != "processing-supplied-hits/1"
            || meta["population"] != "original-input"
            || meta["source_ordinal_origin"].as_u64() != Some(0)
            || meta["input_phred"].as_i64() != Some(gp.quality_phred as i64)
        {
            return Err(
                "[input] supplied hit provenance version/population/ordinal/Phred mismatch".into(),
            );
        }
        let paired = gp.paired();
        let mates = if paired { 2 } else { 1 };
        let layout = if paired { "PE" } else { "SE" };
        if meta["layout"] != layout {
            return Err("[input] supplied hit provenance layout mismatch".into());
        }
        if gp.native_interleaved && meta["interleaved"].as_bool() != Some(true) {
            return Err(
                "[input] supplied hit interleaved association must be explicitly bound".into(),
            );
        }
        if !gp.native_interleaved && meta["interleaved"].as_bool() == Some(true) {
            return Err("[input] supplied hit contradictory interleaved association".into());
        }
        if meta["aggregation"] != "one boolean per original mate" {
            return Err(
                "[input] supplied hit aggregation must be one boolean per original mate".into(),
            );
        }
        if meta["provider"]["name"]
            .as_str()
            .is_none_or(|s| s.is_empty())
            || meta["provider"]["version"]
                .as_str()
                .is_none_or(|s| s.is_empty())
            || meta["provider"]["arguments"]
                .as_array()
                .is_none_or(|a| a.iter().any(|v| !v.is_string()))
        {
            return Err("[input] supplied hit provider name/version/arguments required".into());
        }
        let units = meta["source_units"]
            .as_u64()
            .ok_or("[input] supplied hit source_units must be an unsigned integer")?;
        let full_slots = usize::try_from(units)
            .ok()
            .and_then(|n| n.checked_mul(mates))
            .ok_or_else(|| SourceError::worker("[worker] supplied hit table capacity overflow"))?;
        let table_ticket = e.reserve_persistent_feature(full_slots)?;
        let mut values = vec![0u8; full_slots];
        if values.capacity() != full_slots {
            return Err(SourceError::worker("[worker] supplied hit table capacity exceeded reservation"));
        }
        let mut originals: Vec<(String, usize, Option<String>)> = Vec::new();
        if let Some(lanes) = &gp.native_lane_manifest {
            for line in lanes.lines().skip(1) {
                let mut fields = line.split('\t');
                let id = fields.next().unwrap();
                for (mate, label) in fields.enumerate() {
                    if !label.is_empty() {
                        originals.push((
                            std::path::Path::new(&gp.native_lane_parent)
                                .join(label)
                                .to_string_lossy()
                                .into_owned(),
                            mate + 1,
                            Some(id.into()),
                        ));
                    }
                }
            }
        } else {
            originals.push((
                gp.fq1_path.clone(),
                if gp.native_interleaved { 0 } else { 1 },
                None,
            ));
            if paired && !gp.native_interleaved {
                originals.push((gp.fq2_path.clone(), 2, None));
            }
        }
        let inputs = meta["inputs"]
            .as_array()
            .ok_or("[input] supplied hit inputs binding missing")?;
        if inputs.len() != originals.len() {
            return Err("[input] supplied hit input association/count mismatch".into());
        }
        let mut derived = Vec::with_capacity(originals.len());
        for ((actual, mate, lane), bound) in originals.iter().zip(inputs) {
            if actual == "-" || !std::fs::metadata(actual).is_ok_and(|m| m.is_file()) {
                return Err("[input] supplied hits require regular seekable originals".into());
            }
            if bound["path"].as_str().is_none_or(|p| p.is_empty()) {
                return Err("[input] supplied hit provenance path label missing".into());
            }
            if (*mate == 0 && bound["mate"] != "interleaved")
                || (*mate != 0 && bound["mate"].as_u64() != Some(*mate as u64))
            {
                return Err("[input] supplied hit mate association mismatch".into());
            }
            if let Some(lane) = lane {
                if bound["lane"] != lane.as_str() {
                    return Err(
                        "[input] supplied hit lane order/association missing or mismatched".into(),
                    );
                }
            }
            let hash = sha256(actual)?;
            if bound["sha256"] != hash {
                return Err("[input] supplied hit provenance input hash mismatch".into());
            }
            let mut magic = [0u8; 2];
            let read = File::open(actual)
                .and_then(|mut f| f.read(&mut magic))
                .map_err(|err| format!("[input] supplied hit input: {err}"))?;
            let codec = if read == 2 && magic == [31, 139] {
                "gzip"
            } else {
                "fastq"
            };
            if bound.get("codec").is_some_and(|v| v != codec) {
                return Err("[input] supplied hit codec assertion mismatch".into());
            }
            derived.push(json!({"actual_path":actual,"provenance_label":bound["path"],"sha256":hash,"codec":codec,"mate":if *mate==0{json!("interleaved")}else{json!(mate)},"lane":lane,"association_source":"validated CLI input layout and ordered sidecar entries; labels are not paths to open"}));
        }
        let refs = meta["references"]
            .as_array()
            .ok_or("[input] supplied hit reference binding missing")?;
        for reference in refs {
            let identity = ["id", "resource_id", "name", "path"].iter().any(|key| {
                reference
                    .get(key)
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty())
            });
            if !identity
                || reference["sha256"]
                    .as_str()
                    .is_none_or(|s| s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                return Err("[input] supplied hit reference identity/hash invalid".into());
            }
        }
        if !gp.native_process.screen.references.is_empty()
            && refs.len() != gp.native_process.screen.references.len()
        {
            return Err("[input] supplied hit reference argument identities mismatch".into());
        }
        for (path, bound) in gp.native_process.screen.references.iter().zip(refs) {
            if bound["sha256"] != sha256(path)? {
                return Err("[input] supplied hit reference hash mismatch".into());
            }
        }
        let mut full = gp.clone();
        full.native_limit_units = None;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut source = crate::source::Source::open_layout(&full, stop, cpu.clone(), e)?;
        let count_result = (|| -> Result<u64, SourceError> {
            let mut count = 0u64;
            while let Some(batch) = source.next_batch()? {
                let raw = checked(
                    batch
                        .reads1
                        .len()
                        .checked_add(batch.reads2.as_ref().map_or(0, Vec::len)),
                )?;
                let capacity = checked(
                    batch
                        .reads1
                        .capacity()
                        .checked_add(batch.reads2.as_ref().map_or(0, Vec::capacity)),
                )?;
                let _ticket = e.reserve_persistent_feature(
                    Execution::reservation_with_capacity(raw, batch.record_count, capacity)?,
                )?;
                let _permit = cpu.acquire(crate::cpu_budget::Stage::Source);
                let first = crate::pipeline::parse_chunk_records_with_capacity(
                    &batch.reads1,
                    "read1",
                    batch.first_record_index,
                    true,
                    batch.record_count,
                )?;
                let second = batch
                    .reads2
                    .as_ref()
                    .map(|bytes| {
                        crate::pipeline::parse_chunk_records_with_capacity(
                            bytes,
                            "read2",
                            batch.first_record_index,
                            true,
                            batch.record_count,
                        )
                    })
                    .transpose()?;
                if first.len() != batch.record_count
                    || second.as_ref().is_some_and(|r| r.len() != first.len())
                {
                    return Err("[input] supplied provenance original mate count mismatch".into());
                }
                for (i, r1) in first.iter().enumerate() {
                    crate::native_kernel::validate(r1, gp.quality_phred)?;
                    if let Some(second) = &second {
                        crate::native_kernel::validate(&second[i], gp.quality_phred)?;
                        if !crate::record::pair_ids_match(&r1.seq_id, &second[i].seq_id) {
                            return Err(
                                "[input] supplied provenance original mate identifiers mismatch"
                                    .into(),
                            );
                        }
                    }
                }
                count = count
                    .checked_add(first.len() as u64)
                    .ok_or("[input] supplied hit source count overflow")?;
            }
            Ok(count)
        })();
        let close = source.cancel();
        drop(source);
        let count = count_result?;
        close?;
        if count != units {
            return Err("[input] supplied hit full source_units count mismatch".into());
        }
        for ((actual, _, _), bound) in originals.iter().zip(inputs) {
            if bound["sha256"] != sha256(actual)? {
                return Err("[input] supplied original changed during validation".into());
            }
        }
        let tsv_size = usize::try_from(
            std::fs::metadata(path)
                .map_err(|err| format!("[input] supplied hits: {err}"))?
                .len(),
        )
        .map_err(|_| SourceError::worker("[worker] supplied hit file too large"))?;
        let _tsv_ticket = e.reserve_persistent_feature(checked(
            tsv_size.checked_mul(3).and_then(|n| n.checked_add(4096)),
        )?)?;
        let hits_hash = sha256(path)?;
        let tsv = read_exact_file(path, tsv_size)?;
        let text =
            std::str::from_utf8(&tsv).map_err(|_| "[input] supplied hit TSV is not UTF-8")?;
        let mut lines = text.lines();
        if lines.next() != Some("source_ordinal\tmate\thit") {
            return Err("[input] supplied hit TSV header invalid".into());
        }
        for line in lines {
            let mut cells = line.split('\t');
            let ordinal = cells
                .next()
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or("[input] supplied hit source ordinal invalid")?;
            let mate = cells
                .next()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|m| (1..=mates).contains(m))
                .ok_or("[input] supplied hit mate invalid")?;
            let hit = match cells.next() {
                Some("true") => 2,
                Some("false") => 1,
                _ => return Err("[input] supplied hit boolean invalid".into()),
            };
            if cells.next().is_some() || ordinal >= units {
                return Err("[input] supplied hit ordinal/column out of range".into());
            }
            let at = ordinal as usize * mates + mate - 1;
            if values[at] != 0 {
                return Err("[input] duplicate supplied hit row".into());
            }
            values[at] = hit;
        }
        if values.contains(&0) {
            return Err("[input] missing supplied hit row for original mate".into());
        }
        if sha256(&sidecar)? != sidecar_hash || sha256(path)? != hits_hash {
            return Err("[input] supplied hit provenance/table changed during validation".into());
        }
        let provenance = json!({"sidecar_path":sidecar,"sidecar_sha256":sidecar_hash,"hits_path":path,"hits_sha256":hits_hash,"provider_assertions":meta,"derived_inputs":derived,"derived_full_source_units":count,"derived_layout":layout,"derived_interleaved":gp.native_interleaved,"validated_all_rows_before_processing":true,"provider_thresholds":meta.get("thresholds"),"provider_per_reference_detail":meta.get("per_reference"),"unavailable_reason":"absent provider threshold/per-reference fields are unavailable, not native defaults; codec/count/order are derived from full-hash-bound originals","path_policy":"sidecar paths are provenance labels; CLI-selected originals are opened; no sidecar-relative rebasing","processing_prefix_units":gp.native_limit_units.map(|n|n.min(count))});
        Ok(Self {
            values,
            units,
            mates,
            provenance,
            _reservations: vec![meta_ticket, table_ticket],
        })
    }
}
struct UniqueValue(Value);
impl<'de> serde::Deserialize<'de> for UniqueValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueValue;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("provenance JSON without duplicate keys")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(json!(v)))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(json!(v)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(json!(v)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Ok(UniqueValue(json!(v)))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(json!(v)))
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = a.next_element::<UniqueValue>()? {
                    out.push(v.0);
                }
                Ok(UniqueValue(Value::Array(out)))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut a: A,
            ) -> Result<Self::Value, A::Error> {
                let mut out = serde_json::Map::new();
                while let Some(k) = a.next_key::<String>()? {
                    if out.contains_key(&k) {
                        return Err(serde::de::Error::custom("duplicate provenance key"));
                    }
                    out.insert(k, a.next_value::<UniqueValue>()?.0);
                }
                Ok(UniqueValue(Value::Object(out)))
            }
        }
        d.deserialize_any(Visitor)
    }
}
fn decode_provenance(bytes: &[u8]) -> Result<Value, String> {
    serde_json::from_slice::<UniqueValue>(bytes)
        .map(|v| v.0)
        .map_err(|e| format!("[input] supplied hit provenance JSON: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    fn execution() -> Arc<Execution> {
        Execution::with_statistics_budget(2, 1, Arc::new(AtomicBool::new(false)), None, 0).unwrap()
    }
    fn dir() -> std::path::PathBuf {
        let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../.scratch/m7-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
    #[test]
    fn m7_exact_words_orientations_resource_grouping_and_release() {
        let d = dir();
        let p = d.join("one.fa");
        std::fs::write(&p, ">refA\nACG\n>entry2\nAAA\n").unwrap();
        let e = execution();
        let before = e.facts()["memory_budget"]["current_feature_reserved_bytes"]
            .as_u64()
            .unwrap();
        {
            let r = Resource::load(p.to_str().unwrap(), 3, &e).unwrap();
            assert_eq!(r.ids, vec!["refA", "entry2"]);
            assert_eq!(r.count(&[b"ACG"], 3), 1);
            assert_eq!(r.count(&[b"ACG", b"CGT"], 3), 2);
            assert_eq!(r.count(&[b"AAA"], 3), 1);
            assert_eq!(r.count(&[b"NNN"], 3), 0);
            assert!(r._reservation.reserved_bytes() >= r.blob.capacity() + r.words.capacity() * 8);
        }
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"]
                .as_u64()
                .unwrap(),
            before
        );
        std::fs::write(&p, ">refA\nNNR\n").unwrap();
        assert!(Resource::load(p.to_str().unwrap(), 3, &e).is_err());
        std::fs::write(&p, ">refA\n\n").unwrap();
        assert!(Resource::load(p.to_str().unwrap(), 3, &e).is_err());
        std::fs::write(&p, ">refA\nACG\n>refA\nTTT\n").unwrap();
        assert!(Resource::load(p.to_str().unwrap(), 3, &e).is_err());
        std::fs::write(&p, format!(">largek\n{}\n", "A".repeat(40))).unwrap();
        assert!(Resource::load(p.to_str().unwrap(), 40, &e).is_ok());
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"]
                .as_u64()
                .unwrap(),
            before
        );
    }
    #[test]
    fn m7_hnu_nine_table_actions_and_native_fallback() {
        let states = [State::H, State::N, State::U];
        let any = [
            State::H,
            State::H,
            State::H,
            State::H,
            State::N,
            State::U,
            State::H,
            State::U,
            State::U,
        ];
        let both = [
            State::H,
            State::N,
            State::U,
            State::N,
            State::N,
            State::N,
            State::U,
            State::N,
            State::U,
        ];
        for (i, a) in states.iter().enumerate() {
            for (j, b) in states.iter().enumerate() {
                assert_eq!(aggregate(&[*a, *b], "any"), any[i * 3 + j]);
                assert_eq!(aggregate(&[*a, *b], "both"), both[i * 3 + j]);
            }
        }
        for action in ["inspect", "remove", "retain"] {
            for policy in ["keep", "reject"] {
                let s = Settings {
                    method: "kmer-exact".into(),
                    action: action.into(),
                    unscorable: policy.into(),
                    ..Settings::default()
                };
                assert!(!veto(State::NotEvaluated, &s));
                assert_eq!(
                    veto(State::U, &s),
                    action != "inspect" && policy == "reject"
                );
                assert_eq!(veto(State::H, &s), action == "remove");
                assert_eq!(veto(State::N, &s), action == "retain");
            }
        }
        assert_eq!(
            aggregate(&[State::H, State::NotEvaluated], "any"),
            State::NotEvaluated
        );
    }
    #[test]
    fn m7_supplied_full_original_binding_prefix_rows_and_release() {
        let d = dir();
        let input = d.join("raw.fastq");
        let hits = d.join("hits.tsv");
        let sidecar = d.join("hits.tsv.provenance.json");
        std::fs::write(&input, "@u0\nACG\n+\nIII\n@u1\nTTT\n+\nIII\n").unwrap();
        std::fs::write(
            &hits,
            "source_ordinal\tmate\thit\n0\t1\ttrue\n1\t1\tfalse\n",
        )
        .unwrap();
        let meta = json!({"version":"processing-supplied-hits/1","population":"original-input","source_ordinal_origin":0,"layout":"SE","input_phred":33,"source_units":2,"inputs":[{"path":"not-opened-relative-to-sidecar.fastq","sha256":sha256(input.to_str().unwrap()).unwrap(),"mate":1}],"references":[],"provider":{"name":"independent-test-provider","version":"literal2","arguments":[]},"aggregation":"one boolean per original mate"});
        std::fs::write(&sidecar, meta.to_string()).unwrap();
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.fq1_path = input.to_string_lossy().into_owned();
        gp.threads_num = 2;
        gp.quality_phred = 33;
        gp.output_quality_phred = 33;
        gp.native_limit_units = Some(1);
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.screen = Settings {
            method: "supplied-hits".into(),
            action: "remove".into(),
            hits: Some(hits.to_string_lossy().into_owned()),
            ..Settings::default()
        };
        gp.native_process = Arc::new(settings);
        let e = execution();
        let cpu = crate::cpu_budget::CpuBudget::new(2);
        {
            let screen = Screen::prepare(&gp, &e, &cpu).unwrap();
            let r = FastqRecord::new(1);
            assert!(screen.evaluate(0, &[&r], true).unwrap().rejected);
            let fallback = screen.evaluate(1, &[&r], false).unwrap();
            assert_eq!(fallback.unit, State::N);
            assert!(!fallback.rejected);
            assert_eq!(screen.report()["supplied_full_source_units"], 2);
            assert!(screen.report()["supplied_provenance"]["provider_thresholds"].is_null());
        }
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        let mut provider_refs = meta.clone();
        provider_refs["references"] = json!([{"id":"provider-resource","sha256":"f".repeat(64)}]);
        std::fs::write(&sidecar, provider_refs.to_string()).unwrap();
        assert!(Screen::prepare(&gp, &e, &cpu).is_ok());
        std::fs::write(&sidecar, meta.to_string()).unwrap();
        std::fs::write(&hits, "source_ordinal\tmate\thit\n0\t1\ttrue\n").unwrap();
        assert!(
            Screen::prepare(&gp, &e, &cpu)
                .err()
                .unwrap()
                .contains("missing supplied hit")
        );
        std::fs::write(
            &hits,
            "source_ordinal\tmate\thit\n0\t1\ttrue\n0\t1\tfalse\n",
        )
        .unwrap();
        assert!(
            Screen::prepare(&gp, &e, &cpu)
                .err()
                .unwrap()
                .contains("duplicate supplied hit")
        );
        let mut bad = meta.clone();
        bad["inputs"][0]["sha256"] = json!("0".repeat(64));
        std::fs::write(&sidecar, bad.to_string()).unwrap();
        assert!(
            Screen::prepare(&gp, &e, &cpu)
                .err()
                .unwrap()
                .contains("hash mismatch")
        );
        let mut bad = meta.clone();
        bad["source_units"] = json!(u64::MAX);
        std::fs::write(&sidecar, bad.to_string()).unwrap();
        assert!(Screen::prepare(&gp, &e, &cpu).is_err());
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        assert!(decode_provenance(br#"{"layout":"PE","layout":"SE"}"#).is_err());
    }
    #[test]
    fn m7_thresholds_never_pool_independent_resources() {
        let d = dir();
        let a = d.join("a.fa");
        let b = d.join("b.fa");
        std::fs::write(&a, ">a\nAAA\n").unwrap();
        std::fs::write(&b, ">b\nCCC\n").unwrap();
        let mut gp = GlobalParams::new();
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.screen = Settings {
            method: "kmer-exact".into(),
            references: vec![
                a.to_string_lossy().into_owned(),
                b.to_string_lossy().into_owned(),
            ],
            k: 3,
            min_hits: 2,
            ..Settings::default()
        };
        gp.native_process = Arc::new(settings);
        let e = execution();
        let screen = Screen::prepare(&gp, &e, &crate::cpu_budget::CpuBudget::new(2)).unwrap();
        let mut read = FastqRecord::new(1);
        read.sequence = "AAACCCAAA".into();
        let evidence = screen.evaluate(0, &[&read], true).unwrap();
        assert_eq!(evidence.counts[0], vec![1, 1]);
        assert_eq!(evidence.unit, State::N);
        let skipped = screen.evaluate(1, &[&read], false).unwrap();
        assert_eq!(skipped.mates[0], State::NotEvaluated);
        assert_eq!(screen.report()["mate_states"]["NOT_EVALUATED"], 1);
    }
}

#[cfg(test)]
mod memory_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[test]
    fn m7_index_impossible_fit_cancel_and_t32_statistics_query_bounds() {
        let stop = Arc::new(AtomicBool::new(false));
        let e = Execution::with_statistics_budget(32, 2, stop.clone(), None, 0).unwrap();
        let d = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../.scratch/m7-memory-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        let file = d.join("too-large.fa");
        File::create(&file).unwrap().set_len(8 << 20).unwrap();
        assert!(
            Resource::load(file.to_str().unwrap(), 31, &e)
                .err()
                .unwrap()
                .contains("cannot fit")
        );
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        stop.store(true, Ordering::Relaxed);
        assert!(
            e.reserve_persistent_feature(1)
                .err()
                .unwrap()
                .contains("cancelled")
        );
        let mut settings = crate::native_processing::ProcessSettings::default();
        settings.screen.method = "kmer-exact".into();
        settings.screen.references = vec!["a.fa".into()];
        settings.overlap.merge = true;
        assert!(
            Execution::with_statistics_budget(
                32,
                2,
                Arc::new(AtomicBool::new(false)),
                None,
                settings.additional_statistics(true)
            )
            .is_ok()
        );
        let bound = settings
            .feature_chunk_bound(2500, 1 << 20, true, true)
            .unwrap();
        assert!(bound >= 32 * (1 << 20));
        let mut off = crate::native_processing::ProcessSettings::default();
        assert_eq!(
            off.feature_chunk_bound(2500, 1 << 20, false, false)
                .unwrap(),
            0
        );
        off.screen.k = 1001;
        assert_eq!(
            off.feature_chunk_bound(2500, 1 << 20, false, false)
                .unwrap(),
            0
        );
    }
}

#[cfg(test)]
mod r93_case_tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    #[test]
    fn reference_query_case_strand_ambiguity_capacity_and_drop() {
        let d=std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.scratch/r93-screen");std::fs::create_dir_all(&d).unwrap();
        let p=d.join("case.fa");
        let e=Execution::with_statistics_budget(2,1,Arc::new(AtomicBool::new(false)),None,0).unwrap();
        for seq in ["ACGTTT","acgttt","ACGttt"] {
            let bytes=format!(">ref\n{seq}\n");std::fs::write(&p,&bytes).unwrap();
            let mut settings=crate::native_processing::ProcessSettings::default();settings.screen=Settings{method:"kmer-exact".into(),references:vec![p.to_str().unwrap().into()],k:3,min_hits:1,..Default::default()};
            let mut gp=GlobalParams::new();gp.native_process=Arc::new(settings);
            {
                let screen=Screen::prepare(&gp,&e,&crate::cpu_budget::CpuBudget::new(2)).unwrap();
                for(query,hits,state)in [("TTT",1,State::H),("ttt",1,State::H),("AAA",1,State::H),("aAa",1,State::H),("TTTttt",1,State::H),("NNN",0,State::U),("ACN",0,State::U),("TT",0,State::U)] {
                    let r=FastqRecord{sequence:query.into(),..Default::default()};let v=screen.evaluate(0,&[&r],true).unwrap();assert_eq!((v.counts[0][0],v.unit),(hits,state));
                }
                let r=&screen.references[0];assert_eq!(r.sha256,sha256(p.to_str().unwrap()).unwrap());assert_eq!(std::fs::read(&p).unwrap(),bytes.as_bytes());
                assert!(r._reservation.reserved_bytes()>=r.blob.capacity()+r.words.capacity()*std::mem::size_of::<usize>());
            }
            assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
        }
        std::fs::write(&p,">ref\nacgNRYttt\n").unwrap();
        {let r=Resource::load(p.to_str().unwrap(),3,&e).unwrap();assert_eq!(r.count(&[b"ACG",b"cgt",b"TTT",b"aaa",b"NNN"],3),4);}
        assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
    }
}
