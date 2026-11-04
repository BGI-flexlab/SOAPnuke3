use crate::failure::Failure;
use crate::{native_qc::Qc, job_api::*, record::FastqRecord};
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};
#[derive(Clone, Debug)]
pub(crate) struct Channel {
    pub logical: String,
    pub io: String,
    pub path: PathBuf,
    pub codec: String,
    pub phred: i32,
    pub duplicate: bool,
    pub mate: usize,
}
pub(crate) struct Unit {
    pub rows: Vec<FastqRecord>,
    pub error: Option<crate::failure::UnitError>,
    pub lane: Option<usize>,
}
pub(crate) struct State {
    pub input: Qc,
    pub accepted: Qc,
    pub written: Qc,
    pub channels: Vec<Qc>,
    pub channel_units: Vec<u64>,
    pub input_units: u64,
    pub representatives: Option<u64>,
    pub duplicates: Option<u64>,
    pub written_units: u64,
    pub error: Option<crate::failure::UnitError>,
    pub error_lane: Option<usize>,
    collector: Option<crate::dedup_grouping::Collector>,
    spool: Option<File>,
}
impl State {
 pub(crate) fn unstarted()->Self{Self{input:Qc::default(),accepted:Qc::default(),written:Qc::default(),channels:Vec::new(),channel_units:Vec::new(),input_units:0,representatives:None,duplicates:None,written_units:0,error:None,error_lane:None,collector:None,spool:None}}
}
pub(crate) struct Runtime {
    pub directory: PathBuf,
    pub channels: Vec<Channel>,
    pub state: Mutex<State>,
    _metadata: crate::native_execution::FeatureTicket,
}
impl Runtime {
    pub(crate) fn statistics_bound(n: usize) -> Result<usize, String> {
        n.checked_add(3)
            .and_then(|n| n.checked_mul(Qc::maximum_heap_bytes() + std::mem::size_of::<Qc>()))
            .ok_or("dedup statistics overflow".into())
    }
    pub(crate) fn feature_bound(raw: usize, n: usize) -> Result<usize, String> {
        raw.checked_mul(4)
            .and_then(|v| {
                n.checked_mul(
                    4 * std::mem::size_of::<FastqRecord>() + std::mem::size_of::<Unit>() + 128,
                )
                .and_then(|n| v.checked_add(n))
            })
            .and_then(|v| v.checked_add(65536))
            .ok_or("dedup chunk bound overflow".into())
    }
    pub(crate) fn new(
        directory: &PathBuf,
        channels: &[Channel],
        e: &Arc<crate::native_execution::Execution>,
    ) -> Result<Self, String> {
        let dynamic = channels
            .iter()
            .try_fold(0usize, |n, c| {
                n.checked_add(c.logical.capacity())?
                    .checked_add(c.io.capacity())?
                    .checked_add(c.path.capacity())?
                    .checked_add(c.codec.capacity())?
                    .checked_add(std::mem::size_of::<Channel>())
            })
            .ok_or("dedup metadata overflow")?;
        let bound = directory
            .capacity()
            .checked_add(dynamic)
            .and_then(|n| n.checked_mul(8))
            .and_then(|n| n.checked_add(65536))
            .ok_or("dedup metadata overflow")?;
        let metadata = e.reserve_preparation_feature(bound).map_err(|e| e.to_string())?;
        Ok(Self {
            directory: directory.clone(),
            channels: channels.to_vec(),
            state: Mutex::new(State {
                input: Qc::default(),
                accepted: Qc::default(),
                written: Qc::default(),
                channels: (0..channels.len()).map(|_| Qc::default()).collect(),
                channel_units: vec![0; channels.len()],
                input_units: 0,
                representatives: None,
                duplicates: None,
                written_units: 0,
                error: None,
                error_lane: None,
                collector: None,
                spool: None,
            }),
            _metadata: metadata,
        })
    }
    pub(crate) fn prepare(
        &self,
        e: &Arc<crate::native_execution::Execution>,
    ) -> Result<(), String> {
        let mut s = self.state.lock().unwrap();
        s.collector = Some(
            crate::dedup_grouping::Collector::new(1 << 20, &self.directory, e)
                .map_err(|e| format!("[worker] grouping admission: {e:?}"))?,
        );
        s.spool = Some(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.directory.join("payload.bin"))
                .map_err(|e| format!("[write] payload: {e}"))?,
        );
        Ok(())
    }
    pub(crate) fn process(
        first: Vec<FastqRecord>,
        mut second: Option<Vec<FastqRecord>>,
        terminal: Option<crate::failure::UnitError>,
        lane: Option<usize>,
    ) -> Vec<Unit> {
        let mut out = Vec::with_capacity(first.len() + 1);
        for (i, r) in first.into_iter().enumerate() {
            let ordinal = r.source_index;
            let mut rows = vec![r];
            if let Some(v) = &mut second {
                rows.push(std::mem::take(&mut v[i]));
            }
            let mut error = None;
            if let Err(message) = rows
                .iter_mut()
                .try_for_each(|r| crate::native_kernel::validate_normalize(r, 33))
            {
                rows.clear();
                error = Some(crate::failure::UnitError {
                    ordinal,
                    stage: "input",
                    message,
                    code: 3,
                });
            } else if rows.len() == 2
                && !crate::record::pair_ids_match(&rows[0].seq_id, &rows[1].seq_id)
            {
                error = Some(crate::failure::UnitError {
                    ordinal,
                    stage: "pair-association",
                    message: "input association mismatch: paired identifiers differ".into(),
                    code: 3,
                });
            } else if terminal.as_ref().is_some_and(|e| e.ordinal == ordinal) {
                error = terminal.clone();
            }
            let failed = error.is_some();
            out.push(Unit { rows, error, lane });
            if failed {
                break;
            }
        }
        if let Some(e) = terminal {
            if out.last().is_none_or(|u| {
                u.rows.first().is_none_or(|r| r.source_index < e.ordinal)
                    && u.error.as_ref().is_none_or(|r| r.ordinal < e.ordinal)
            }) {
                out.push(Unit {
                    rows: vec![],
                    error: Some(e),
                    lane,
                });
            }
        }
        out
    }
    pub(crate) fn append(
        &self,
        gp: &crate::params::GlobalParams,
        units: &mut Vec<Unit>,
        cpu: &Arc<crate::cpu_budget::CpuBudget>,
    ) -> Result<(), Failure> {
        let mut s = self.state.lock().unwrap();
        for unit in units.drain(..) {
            let _permit = cpu.acquire(crate::cpu_budget::Stage::Sink);
            if !unit.rows.is_empty() {
                for r in &unit.rows {
                    s.input
                        .observe_validated(r.sequence.as_bytes(), r.qual_seq.as_bytes(), 33);
                }
                s.input_units += 1;
                gp.native_runtime.as_ref().unwrap().processed(1, 0);
            }
            if let Some(e) = unit.error {
                s.error_lane = unit.lane;
                s.error = Some(e.clone());
                return Err(e.failure(format!(
                    "[input] {} ordinal {}: {}",
                    e.stage, e.ordinal, e.message
                )));
            }
            let references = [
                unit.rows[0].sequence.as_str(),
                unit.rows.get(1).map_or("", |r| r.sequence.as_str()),
            ];
            s.collector
                .as_mut()
                .unwrap()
                .push(&references[..unit.rows.len()])
                .map_err(|e| Failure::worker(format!("[worker] grouping: {e:?}")))?;
            let f = s.spool.as_mut().unwrap();
            write_unit(f, &unit.rows, unit.lane.unwrap_or(0))
                .map_err(|e| Failure::write(format!("[write] payload: {e}")))?;
        }
        Ok(())
    }
    pub(crate) fn finish_input(&self) -> Result<(), String> {
        let mut s = self.state.lock().unwrap();
        if let Some(mut f) = s.spool.take() {
            f.flush().map_err(|e| e.to_string())?;
            f.sync_all().map_err(|e| e.to_string())?;
            crate::checked_io::close_file(f)?;
        }
        Ok(())
    }
    pub(crate) fn abort_grouping(&self) {
        let mut s = self.state.lock().unwrap();
        drop(s.collector.take());
        drop(s.spool.take());
    }
    pub(crate) fn classify(
        &self,
        io: &Arc<crate::native_io::OwnedIo>,
    ) -> Result<crate::dedup_grouping::Facts, String> {
        if io.cancel.is_cancelled() {
            return Err("API cancelled".into());
        }
        let collector = self
            .state
            .lock()
            .unwrap()
            .collector
            .take()
            .ok_or("grouping state absent")?;
        let _permit = io.execution.acquire_post_input_cpu().map_err(|e| e.to_string())?;
        let facts = collector
            .finish_cancellable(|_, _| {}, || io.cancel.is_cancelled())
            .map_err(|e| format!("grouping: {e:?}"))?;
        if io.cancel.is_cancelled() {
            return Err("API cancelled".into());
        }
        let _record = io
            .execution
            .reserve_finalization_feature(4 * crate::native_execution::RECORD_BYTES + 65536).map_err(|e| e.to_string())?;
        let mut data = File::open(self.directory.join("payload.bin")).map_err(|e| e.to_string())?;
        let mut flags =
            File::open(self.directory.join("classification.bin")).map_err(|e| e.to_string())?;
        let mut ordinal = 0;
        let mut reps = 0;
        let mut duplicates = 0;
        let mut q = Qc::default();
        loop {
            if io.cancel.is_cancelled() {
                return Err("API cancelled".into());
            }
            let Some((rows, _)) = read_unit(&mut data).map_err(|e| e.to_string())? else {
                break;
            };
            let mut flag = [0];
            flags.read_exact(&mut flag).map_err(|e| e.to_string())?;
            match flag[0] {
                1 => {
                    reps += 1;
                    for r in &rows {
                        q.observe_validated(r.sequence.as_bytes(), r.qual_seq.as_bytes(), 33);
                    }
                }
                2 => duplicates += 1,
                _ => return Err("invalid ordinal classification".into()),
            }
            ordinal += 1;
        }
        if ordinal != facts.units {
            return Err("payload/classification count mismatch".into());
        }
        let mut extra = [0];
        if flags.read(&mut extra).map_err(|e| e.to_string())? != 0 {
            return Err("extra ordinal classification".into());
        }
        let mut s = self.state.lock().unwrap();
        s.accepted = q;
        s.representatives = Some(reps);
        s.duplicates = Some(duplicates);
        Ok(facts)
    }
    pub(crate) fn emit(&self, gp: &crate::params::GlobalParams) -> Result<(), Failure> {
        let io = gp.native_io.as_ref().unwrap();
        let names = self
            .channels
            .iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    c.io.clone(),
                    format!(
                        "channel-{i:02}.{}",
                        if c.codec == "gzip" { "fq.gz" } else { "fq" }
                    ),
                )
            })
            .collect::<Vec<_>>();
        io.prepare(&self.directory, &names)
            .map_err(|e| format!("[write] prepare: {e}")).map_err(Failure::write)?;
        let _record = io
            .execution
            .reserve_finalization_feature(8 * crate::native_execution::RECORD_BYTES + 65536).map_err(|e: crate::failure::BudgetError| { let mut f=e.0; f.category=crate::failure::Category::Write; f })?;
        let mut data = File::open(self.directory.join("payload.bin")).map_err(|e| e.to_string()).map_err(Failure::write)?;
        let mut flags =
            File::open(self.directory.join("classification.bin")).map_err(|e| e.to_string()).map_err(Failure::write)?;
        let comp = crate::ffi::Compressor::new(gp.native_gzip_level).map_err(Failure::write)?;
        loop {
            if io.cancel.is_cancelled() {
                return Err(Failure::write("API cancelled"));
            }
            let Some((rows, lane)) = read_unit(&mut data).map_err(|e| e.to_string()).map_err(Failure::write)? else {
                break;
            };
            let mut flag = [0];
            flags.read_exact(&mut flag).map_err(|e| e.to_string()).map_err(Failure::write)?;
            let duplicate = flag[0] == 2;
            let mut written = false;
            for (i, c) in self.channels.iter().enumerate() {
                if c.duplicate != duplicate {
                    continue;
                }
                let mut channel_written = false;
                let mates = if c.mate == usize::MAX {
                    0..rows.len()
                } else {
                    c.mate..c.mate + 1
                };
                for mate in mates {
                    let _permit = io.execution.acquire_post_input_cpu().map_err(|e: crate::failure::BudgetError| { let mut f=e.0; f.category=crate::failure::Category::Write; f })?;
                    let r = &rows[mate];
                    let mut raw = Vec::with_capacity(
                        r.seq_id.len() + r.sequence.len() + r.qual_seq.len() + 8,
                    );
                    if let Err(message) =
                        crate::output::serialize_native_record(r, false, gp, &c.io, &mut raw)
                    {
                        let mut s = self.state.lock().unwrap();
                        s.error_lane = Some(lane);
                        s.error = Some(crate::failure::UnitError {
                            ordinal: r.source_index,
                            stage: "quality-output",
                            message: message.clone(),
                            code: 3,
                        });
                        return Err(Failure::input(format!("[input] {message}")).at(r.source_index));
                    }
                    let payload = if c.codec == "gzip" {
                        comp.gzip_compress_alloc(&raw)
                    } else {
                        raw
                    };
                    io.write(&c.io, &payload)
                        .map_err(|e| format!("[write] {e}")).map_err(Failure::write)?;
                    let mut s = self.state.lock().unwrap();
                    s.channels[i].observe_validated(
                        r.sequence.as_bytes(),
                        r.qual_seq.as_bytes(),
                        33,
                    );
                    s.written
                        .observe_validated(r.sequence.as_bytes(), r.qual_seq.as_bytes(), 33);
                    if !channel_written {
                        s.channel_units[i] += 1;
                        channel_written = true;
                    }
                    if !written {
                        s.written_units += 1;
                        written = true;
                    }
                }
            }
        }
        io.finish_completed(gp.native_gzip_level, false)
            .map_err(|e| Failure::write(format!("[write] close: {e}")))
    }
}
fn write_unit(f: &mut File, rows: &[FastqRecord], lane: usize) -> std::io::Result<()> {
    f.write_all(&rows[0].source_index.to_le_bytes())?;
    f.write_all(&(lane as u64).to_le_bytes())?;
    f.write_all(&[rows.len() as u8])?;
    for r in rows {
        for text in [&r.seq_id, &r.sequence, &r.qual_seq] {
            f.write_all(&(text.len() as u64).to_le_bytes())?;
            f.write_all(text.as_bytes())?;
        }
    }
    Ok(())
}
fn number(f: &mut File) -> std::io::Result<u64> {
    let mut b = [0; 8];
    f.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}
fn read_unit(f: &mut File) -> std::io::Result<Option<(Vec<FastqRecord>, usize)>> {
    let mut b = [0; 8];
    if f.read(&mut b[..1])? == 0 {
        return Ok(None);
    }
    f.read_exact(&mut b[1..])?;
    let ordinal = u64::from_le_bytes(b);
    let lane =
        usize::try_from(number(f)?).map_err(|_| std::io::Error::other("payload lane overflow"))?;
    let mut mate = [0];
    f.read_exact(&mut mate)?;
    if !(1..=2).contains(&mate[0]) {
        return Err(std::io::Error::other("payload mate count"));
    }
    let mut rows = Vec::with_capacity(mate[0] as usize);
    for i in 0..mate[0] {
        let mut fields = Vec::with_capacity(3);
        for _ in 0..3 {
            let n = usize::try_from(number(f)?)
                .map_err(|_| std::io::Error::other("payload length overflow"))?;
            if n > crate::native_execution::RECORD_BYTES {
                return Err(std::io::Error::other("payload field bound"));
            }
            let mut bytes = vec![0; n];
            f.read_exact(&mut bytes)?;
            fields
                .push(String::from_utf8(bytes).map_err(|_| std::io::Error::other("payload UTF8"))?);
        }
        let mut r = FastqRecord::new(i as i32 + 1);
        r.seq_id = fields.remove(0);
        r.sequence = fields.remove(0);
        r.qual_seq = fields.remove(0);
        r.source_index = ordinal;
        r.input_phred = Some(33);
        rows.push(r);
    }
    Ok(Some((rows, lane)))
}
#[cfg(test)]
mod lifetime_tests {
    use super::*;
    fn job(name: &str, bad: bool) {
        let directory = crate::test_support::workspace("direct").join(name);
        std::fs::create_dir(&directory).unwrap();
        let input = directory.join("input.fastq");
        let output = directory.join("output.fastq");
        let bytes = if bad {
            &b"@u\nAC\n+\n\x01\x01\n"[..]
        } else {
            &b"@u full\nACGTCC\n+\nIIIIII\n"[..]
        };
        std::fs::write(&input, bytes).unwrap();
        let spec = JobSpec {
            configured: std::collections::BTreeMap::new(),
            supplied: std::collections::BTreeMap::from([
                (
                    "spill-dir".into(),
                    Setting::Text(directory.join("spill").to_string_lossy().into_owned()),
                ),
                ("threads".into(), Setting::Int(1)),
            ]),
            layout: Layout::Se,
            lanes: vec![LaneDecl {
                id: "source".into(),
                inputs: vec![InputDecl {
                    channel: "R1".into(),
                    location: Location::Path(input),
                    format: Format::Fastq,
                    phred: 33,
                    seekable: true,
                }],
            }],
            outputs: vec![OutputDecl {
                channel: "R1".into(),
                location: Location::Path(output.clone()),
                format: Format::Fastq,
                phred: 33,
            }],
            json_report: None,
        };
        let plan = crate::dedup_app::compile_api(&spec).unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let e = crate::native_execution::Execution::with_statistics_budget(
            1,
            1,
            stop.clone(),
            None,
            Runtime::statistics_bound(1).unwrap(),
        )
        .unwrap();
        let bytes = crate::job_api::metadata_bound(&spec).unwrap() * 8;
        let ticket = e.reserve_preparation_feature(bytes).unwrap();
        let owned = Arc::new(crate::native_io::OwnedIo::new(
            spec,
            JobIo {
                inputs: std::collections::BTreeMap::new(),
                outputs: std::collections::BTreeMap::new(),
            },
            CancellationToken::default(),
            e.clone(),
            stop,
            ticket,
        ));
        println!("R84_NATIVE_DEDUP_RUN_ENTER");
        let (code, report, _) = crate::dedup_app::run(&plan, Some(owned.clone()));
        assert_eq!(code, if bad { 3 } else { 0 });
        assert_eq!(report.input_units, if bad { 0 } else { 1 });
        assert_eq!(report.accepted_units, if bad { None } else { Some(1) });
        assert_eq!(report.written_units, Some(if bad { 0 } else { 1 }));
        if bad {
            assert!(!output.exists());
        } else {
            assert_eq!(
                std::fs::read(&output).unwrap(),
                b"@u full\nACGTCC\n+\nIIIIII\n"
            );
        }
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            bytes
        );
        assert_eq!(e.facts()["active"], 0);
        assert_eq!(e.facts()["ready"], 0);
        assert_eq!(e.facts()["active_chunk_bytes"], 0);
        assert_eq!(e.facts()["admitted"], e.facts()["retired"]);
        assert_eq!(e.facts()["compute"]["active"], 0);
        drop(owned);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        assert_eq!(
            e.facts()["memory_budget"]["current_persistent_feature_reserved_bytes"],
            0
        );
        assert_eq!(
            std::fs::read_dir(directory.join("spill")).unwrap().count(),
            0
        );
        let open = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|d| std::fs::read_link(d.ok()?.path()).ok())
            .filter(|p| p.starts_with(&directory))
            .count();
        assert_eq!(open, 0);
        std::fs::write(directory.join("returned.json"), report.json).unwrap();
    }
    #[test]
    fn success_joined_source_replay_owners_retire() {
        job("native-success", false)
    }
    #[test]
    fn failed_source_owners_retire_without_classification() {
        job("native-failure", true)
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    #[test]
    fn reserved_before_clones_and_field_drop_order() {
        let e = crate::native_execution::Execution::new(
            1,
            1,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .unwrap();
        let caller = e.reserve_preparation_feature(4096).unwrap();
        let directory = crate::test_support::workspace("direct");
        let channels = vec![Channel {
            logical: "representative.R1".into(),
            io: "R1".into(),
            path: directory.join("unused.fastq"),
            codec: "plain".into(),
            phred: 33,
            duplicate: false,
            mate: 0,
        }];
        let r = Arc::new(Runtime::new(&directory, &channels, &e).unwrap());
        let s = r.state.lock().unwrap();
        let actual = std::mem::size_of::<Runtime>()
            + 2 * std::mem::size_of::<usize>()
            + r.directory.capacity()
            + r.channels.capacity() * std::mem::size_of::<Channel>()
            + r.channels
                .iter()
                .map(|c| {
                    c.logical.capacity() + c.io.capacity() + c.path.capacity() + c.codec.capacity()
                })
                .sum::<usize>()
            + s.channels.capacity() * std::mem::size_of::<Qc>()
            + s.channel_units.capacity() * std::mem::size_of::<u64>();
        let reserved = r._metadata.reserved_bytes();
        assert!(actual <= reserved);
        drop(s);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            4096 + reserved
        );
        drop(r);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            4096
        );
        drop(channels);
        drop(directory);
        drop(caller);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        assert!(Runtime::statistics_bound(usize::MAX).is_err());
        assert!(Runtime::feature_bound(usize::MAX, 1).is_err());
        println!("R84_METADATA_PROBE actual={} reserved={}", actual, reserved);
    }
}
