use crate::failure::{Failure, Category, Reason};
// Parallel executor: producer (Source) -> worker threads (filter+stat+shard) ->
// ordered sink (concatenation) -> merged global stats. Mirrors v2's
// fastq_executor.cpp semantics (batch0 single-worker gate, strictly ordered
// consumption).
use crate::cpu_budget::{CpuBudget, Stage};
use crate::ffi::Compressor;
use crate::filter::{filter_pe_fqs, stat_pe_fqs};
use crate::output::{self, Sink};
use crate::params::GlobalParams;
use crate::record::FastqRecord;
use crate::source::Source;
use crate::stats::{GlobalStats, PeLocalStats, merge_pe_stat, merge_se_stat};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};

struct BatchWork {
    demux:Option<crate::source::DemuxInput>,
    lane_index: Option<usize>,
    ticket: Option<crate::native_execution::Ticket>,
    id: u64,
    first_record_index: u64,
    record_count: usize,
    reads1: Vec<u8>,
    reads2: Option<Vec<u8>>,
}

// Compressed gzip members produced by a worker, delivered to the sink over the
// order channel (P1: no intermediate file materialization; the old design wrote
// every member to tmp_dir and re-read it in the sink).
pub(crate) struct Member {
    pub bytes: Vec<u8>,
    pub recs: u64,
    pub gzip: bool,
}

#[derive(Default)]
pub(crate) struct MemberSet {
    pub(crate) ticket: Option<crate::native_execution::Ticket>,
    pub native_dedup: Option<Vec<crate::dedup_runtime::Unit>>,
    pub native_demux: Option<Vec<crate::demux_runtime::Unit>>,
    pub native_split_records: Option<[Vec<FastqRecord>; 3]>,
    pub native_selected_ordinals: Vec<u64>,
    pub native_merged: Option<Member>,
    pub native_merged_qc: Option<Box<crate::native_qc::Qc>>,
    pub native_written: [Option<Box<crate::native_qc::Qc>>; 2],
    pub native_aux: [Option<Member>; 6],
    pub native_diagnostic_qc: [Option<Box<crate::native_qc::Qc>>; 10],
    pub native_manifest: Vec<u8>,
    pub native_matches: Vec<crate::native_processing::MatchedUnit>,
    pub clean1: Option<Vec<Member>>,
    pub clean2: Option<Vec<Member>>,
    pub trim1: Option<Vec<Member>>,
    pub trim2: Option<Vec<Member>>,
    // Streaming mode carries the already-filtered records and the statistics
    // for this batch. These fields are empty for ordinary file output.
    pub stream1: Option<Vec<FastqRecord>>,
    pub stream2: Option<Vec<FastqRecord>>,
    pub stream_stats: Option<Box<PeLocalStats>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobMembers { Process, Demux, Dedup }
impl MemberSet {
    fn require_job(&self, expected: JobMembers) -> Result<(), Failure> {
        let process = self.native_split_records.is_some() || !self.native_selected_ordinals.is_empty()
            || self.native_merged.is_some() || self.native_merged_qc.is_some()
            || self.native_written.iter().any(Option::is_some) || self.native_aux.iter().any(Option::is_some)
            || self.native_diagnostic_qc.iter().any(Option::is_some) || !self.native_manifest.is_empty()
            || !self.native_matches.is_empty() || self.clean1.is_some() || self.clean2.is_some()
            || self.trim1.is_some() || self.trim2.is_some() || self.stream1.is_some()
            || self.stream2.is_some() || self.stream_stats.is_some();
        let actual = match (self.native_demux.is_some(), self.native_dedup.is_some(), process) {
            (false, false, _) => JobMembers::Process,
            (true, false, false) => JobMembers::Demux,
            (false, true, false) => JobMembers::Dedup,
            _ => return Err(Failure::worker("[worker] mixed completed-job payload")),
        };
        if actual != expected { return Err(Failure::worker("[worker] completed-job payload does not match sink")); }
        Ok(())
    }
}

struct Shared {
    stop: Arc<AtomicBool>,
    err: Mutex<Option<Failure>>,
    gp: Arc<GlobalParams>,
    cpu: Arc<CpuBudget>,
    execution: Option<Arc<crate::native_execution::Execution>>,
}

struct MetricsGuard {
    budget: Arc<CpuBudget>,
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        write_cpu_metrics(&self.budget);
    }
}

impl Shared {
    fn set_err(&self, e: Failure) {
        // Cancellation can close a lane or queue before the thread that
        // caused it publishes its real error.  Do not let that synthetic
        // cancellation symptom become the pipeline result.  Real source,
        // worker, and sink errors still win and set the shared stop flag.
        let cancellation = e.reason == Reason::SyntheticCancellation;
        if cancellation && self.stop.load(Ordering::Relaxed) {
            return;
        }
        let mut g = self.err.lock().unwrap_or_else(|p| p.into_inner());
        if g.is_none() {
            *g = Some(e);
        }
        self.stop.store(true, Ordering::Relaxed);
        if let Some(e) = &self.execution {
            e.cancel();
        }
        self.cpu.wake_cancelled();
    }
    fn compute(&self, stage: Stage) -> Result<crate::cpu_budget::Permit, Failure> {
        if self.gp.native_m1 {
            self.cpu.acquire_cancellable(stage, &self.stop).map_err(Failure::from)
        } else {
            Ok(self.cpu.acquire(stage))
        }
    }
    fn take_err(&self) -> Option<Failure> {
        self.err.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

fn signal_batch0(done: &Arc<(Mutex<bool>, Condvar)>) {
    let mut completed = done.0.lock().unwrap_or_else(|p| p.into_inner());
    *completed = true;
    done.1.notify_all();
}

fn send_with_backpressure<T>(
    tx: &SyncSender<T>,
    mut message: T,
    stop: &AtomicBool,
    label: &str,
) -> Result<(), Failure> {
    loop {
        if stop.load(Ordering::Relaxed) {
            return Err(Failure::synthetic_cancel("FASTQ pipeline cancelled"));
        }
        match tx.try_send(message) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(returned)) => {
                message = returned;
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
            Err(TrySendError::Disconnected(_)) => {
                stop.store(true, Ordering::Relaxed);
                return Err(Failure::write(format!("{label} channel closed")));
            }
        }
    }
}

enum OrderedSink {
    Dedup,
    Demux,
    Owned,
    File(Sink),
    Streaming,
    NativeStdout,
    NativeSplit(output::NativeSplit),
}

impl OrderedSink {
    fn append(
        &mut self,
        gp: &GlobalParams,
        members: &mut MemberSet,
        cpu: &Arc<CpuBudget>,
    ) -> Result<(), Failure> {
        members.require_job(match self { Self::Dedup=>JobMembers::Dedup, Self::Demux=>JobMembers::Demux, _=>JobMembers::Process })?;
        let result = match self {
            OrderedSink::Dedup => gp.native_dedup.as_ref().unwrap().append(gp,members.native_dedup.as_mut().unwrap(),cpu),
            OrderedSink::Demux => gp.native_demux.as_ref().unwrap().append(gp,members.native_demux.as_mut().unwrap(),cpu),
            OrderedSink::Owned => {
                let io=gp.native_io.as_ref().unwrap();
                for (index,channel,members) in [(0,"R1",members.clean1.as_ref()),(1,"R2",members.clean2.as_ref())] {
                    if let Some(parts)=members { for part in parts {io.write(channel,&part.bytes).map_err(|e|format!("owned output write: {e}")).map_err(Failure::write)?;} }
                    let _=index;
                }
                for index in 0..2 {if let Some(qc)=&members.native_written[index] {gp.native_runtime.as_ref().unwrap().appended(index,qc);}}
                Ok(())
            }
            OrderedSink::NativeSplit(sink) => sink.append(gp, members, cpu).map_err(Failure::from),
            OrderedSink::File(sink) => sink.append_members(gp, members, cpu).map_err(Failure::write),
            OrderedSink::NativeStdout => {
                use std::io::Write;
                let mut output = std::io::stdout().lock();
                for member in members.clean1.iter().flatten() {
                    output
                        .write_all(&member.bytes)
                        .map_err(|e| format!("stdout write: {e}")).map_err(Failure::write)?;
                }
                output.flush().map_err(|e| format!("stdout flush: {e}")).map_err(Failure::write)?;
                if let (Some(runtime), Some(qc)) = (&gp.native_runtime, &members.native_written[0])
                {
                    let _permit = cpu.acquire(Stage::Sink);
                    runtime.appended(0, qc);
                    if gp.native_out_interleaved {
                        if let Some(qc) = &members.native_written[1] {
                            runtime.appended(1, qc);
                        }
                    }
                }
                Ok(())
            }
            OrderedSink::Streaming => write_stream_batch(members, gp, cpu).map_err(Failure::write),
        };
        result?;
        append_native_diagnostics(gp, members, cpu)
    }

    fn abort(&mut self) {
        if let OrderedSink::File(sink) = self {
            sink.abort();
        }
    }

    fn finish(&mut self, gp: &GlobalParams, cpu: &Arc<CpuBudget>) -> Result<(), Failure> {
        match self {
            OrderedSink::Dedup => gp.native_dedup.as_ref().unwrap().finish_input()
                .map_err(|e|Failure::write(format!("[write] payload close: {e}")))?,
            OrderedSink::Demux => gp.native_io.as_ref().unwrap().finish_completed(gp.native_gzip_level,false)
                .map_err(|e|Failure::write(format!("[write] demux finalization: {e}")))?,
            OrderedSink::Owned => gp.native_io.as_ref().unwrap().finish_completed(gp.native_gzip_level,false)
                .map_err(|e|Failure::write(format!("owned output finalization: {e}")))?,
            OrderedSink::File(sink) => sink.finish(gp,cpu).map_err(Failure::write)?,
            OrderedSink::NativeSplit(sink) => {
                sink.finish(gp,cpu).map_err(Failure::from)?;
                if let Some(io)=&gp.native_io {io.finish_completed(gp.native_gzip_level,true)
                    .map_err(|e|Failure::write(format!("[write] owned auxiliary finalization: {e}")))?;}
            }
            OrderedSink::Streaming | OrderedSink::NativeStdout => {}
        }
        finish_native_diagnostics(gp,cpu)
    }
}

fn append_native_diagnostics(
    gp: &GlobalParams,
    members: &MemberSet,
    cpu: &Arc<CpuBudget>,
) -> Result<(), Failure> {
    use std::io::Write;
    if gp.native_process.split_units == 0 {
        if let Some(member) = &members.native_merged {
            if let Some(io)=&gp.native_io {
                io.write("merged-out",&member.bytes).map_err(|e|format!("[write] owned merged append: {e}")).map_err(Failure::write)?;
            } else {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(std::path::Path::new(&gp.output_dir).join(&gp.native_merged))
                .map_err(|e| format!("[write] merged channel open: {e}")).map_err(Failure::write)?;
            file.write_all(&member.bytes)
                .map_err(|e| format!("[write] merged channel append: {e}")).map_err(Failure::write)?;
            }
            if let (Some(runtime), Some(qc)) = (&gp.native_runtime, &members.native_merged_qc) {
                let _permit = cpu.acquire(Stage::Sink);
                runtime.appended_merged(qc);
            }
        }
    }
    for i in 0..6 {
        if let Some(member) = &members.native_aux[i] {
            if let Some(io)=&gp.native_io {
                io.write(["reject1","reject2","transformed1","transformed2","singleton1","singleton2"][i],&member.bytes).map_err(|e|format!("[write] owned optional append: {e}")).map_err(Failure::write)?;
            } else {
            let path = std::path::Path::new(&gp.output_dir).join(&gp.native_aux[i]);
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|e| format!("[write] optional channel open: {e}")).map_err(Failure::write)?;
            file.write_all(&member.bytes)
                .map_err(|e| format!("[write] optional channel append: {e}")).map_err(Failure::write)?;
            }
            if let (Some(runtime), Some(qc)) =
                (&gp.native_runtime, &members.native_diagnostic_qc[i])
            {
                let _permit = cpu.acquire(Stage::Sink);
                runtime.appended_auxiliary(i, qc);
            }
        }
    }
    if !gp.native_record_manifest.is_empty() && !members.native_manifest.is_empty() {
        let path = std::path::Path::new(&gp.output_dir).join(&gp.native_record_manifest);
        let first = !path.exists();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("[write] record manifest open: {e}")).map_err(Failure::write)?;
        if first {
            file.write_all(b"unit\trecord_stage\treasons\tevaluation\tchannels\n")
                .map_err(|e| format!("[write] record manifest header: {e}")).map_err(Failure::write)?;
        }
        file.write_all(&members.native_manifest)
            .map_err(|e| format!("[write] record manifest append: {e}")).map_err(Failure::write)?;
    }
    if !gp.native_match_audit.is_empty() && !members.native_matches.is_empty() {
        let runtime = gp.native_runtime.as_ref().unwrap();
        let (payload, recorded, omitted) = {
            let _permit = cpu.acquire(Stage::Sink);
            let counts = runtime
                .match_counts
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let mut recorded = 0u64;
            let mut omitted = 0u64;
            let mut payload = Vec::new();
            for unit in &members.native_matches {
                if counts[0] + recorded < gp.native_match_limit as u64 {
                    recorded += 1;
                    for hit in &unit.hits {
                        payload.extend_from_slice(
                            format!(
                                "{}\tR{}\tadapters\t{}\t{}\t{}\t{}\t{}\n",
                                unit.ordinal,
                                hit.mate + 1,
                                hit.adapter,
                                hit.start,
                                hit.end,
                                hit.mismatches,
                                if gp.native_process.adapter_discard {
                                    "discard"
                                } else {
                                    "trim"
                                }
                            )
                            .as_bytes(),
                        );
                    }
                } else {
                    omitted += 1;
                }
            }
            (payload, recorded, omitted)
        };
        let path = std::path::Path::new(&gp.output_dir).join(&gp.native_match_audit);
        let first = !path.exists();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("[write] match report open: {e}")).map_err(Failure::write)?;
        if first {
            file.write_all(b"unit\tmate\tstage\tadapter\tstart\tend\tmismatches\taction\n")
                .map_err(|e| format!("[write] match report header: {e}")).map_err(Failure::write)?;
        }
        file.write_all(&payload)
            .map_err(|e| format!("[write] match report append: {e}")).map_err(Failure::write)?;
        let _permit = cpu.acquire(Stage::Sink);
        let mut counts = runtime
            .match_counts
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        counts[0] += recorded;
        counts[1] += omitted;
    }
    if let Some(runtime) = &gp.native_runtime {
        let _permit = cpu.acquire(Stage::Sink);
        for i in 6..10 {
            if let Some(qc) = &members.native_diagnostic_qc[i] {
                runtime.appended_auxiliary(i, qc);
            }
        }
    }
    Ok(())
}
fn finish_native_diagnostics(gp: &GlobalParams, cpu: &Arc<CpuBudget>) -> Result<(), Failure> {
    use std::io::Write;
    for name in gp
        .native_aux
        .iter()
        .filter(|name| !name.is_empty())
        .chain(
            std::iter::once(&gp.native_merged)
                .filter(|name| !name.is_empty() && gp.native_process.split_units == 0),
        )
        .chain(std::iter::once(&gp.native_record_manifest).filter(|name| !name.is_empty()))
        .chain(std::iter::once(&gp.native_match_audit).filter(|name| !name.is_empty()))
    {
        if gp.native_io.is_some() && (gp.native_aux.iter().any(|n|n==name) || name==&gp.native_merged) { continue; }
        let path = std::path::Path::new(&gp.output_dir).join(name);
        let first = !path.exists();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("[write] optional finalization open: {e}")).map_err(Failure::write)?;
        if first {
            if name == &gp.native_match_audit {
                file.write_all(b"unit\tmate\tstage\tadapter\tstart\tend\tmismatches\taction\n")
                    .map_err(|e| format!("[write] empty match report: {e}")).map_err(Failure::write)?;
            } else if name == &gp.native_record_manifest {
                file.write_all(b"unit\trecord_stage\treasons\tevaluation\tchannels\n")
                    .map_err(|e| format!("[write] record manifest empty header: {e}")).map_err(Failure::write)?;
            } else if output::wants_gzip(name) {
                let encoded = {
                    let _permit = cpu.acquire(Stage::Sink);
                    Compressor::new(gp.native_gzip_level).map_err(Failure::write)?.gzip_compress_alloc(&[])
                };
                if encoded.is_empty() {
                    return Err(Failure::write("[write] empty optional gzip failed"));
                }
                file.write_all(&encoded)
                    .map_err(|e| format!("[write] empty optional gzip: {e}")).map_err(Failure::write)?;
            }
        }
        file.flush()
            .and_then(|_| file.sync_all())
            .map_err(|e| format!("[write] optional finalization: {e}")).map_err(Failure::write)?;
    }
    Ok(())
}

fn run_sink(
    mut sink: OrderedSink,
    sink_gp: GlobalParams,
    sink_shared: Arc<Shared>,
    order_rx: Receiver<(u64, MemberSet)>,
) -> Result<(), Failure> {
    let mut expected = 0u64;
    let mut ready: std::collections::HashMap<u64, MemberSet> = std::collections::HashMap::new();
    let flush = |sink: &mut OrderedSink,
                 expected: &mut u64,
                 ready: &mut std::collections::HashMap<u64, MemberSet>|
     -> Result<(), Failure> {
        loop {
            // Reorder-map bookkeeping is bounded CPU work, but the actual
            // append may block on the filesystem. Keep the permit narrowly
            // scoped to the map operation.
            let members = {
                let _permit = sink_shared.compute(Stage::Sink)?;
                ready.remove(expected)
            };
            let Some(mut members) = members else { break };
            sink.append(&sink_gp, &mut members, &sink_shared.cpu)
                .map_err(|e| {
                    if e.category == Category::Worker { e } else {
                        let message=format!("[write] {e}"); e.with_message(message)
                    }
                })?;
            *expected += 1;
        }
        Ok(())
    };
    #[cfg(test)]
    if std::env::var_os("SOAPNUKE_TEST_PANIC_SINK").is_some() {
        panic!("injected sink panic");
    }
    loop {
        if sink_shared.stop.load(Ordering::Relaxed) {
            break;
        }
        let wait_started = std::time::Instant::now();
        let received = if let Some(runtime) = &sink_gp.native_runtime {
            match order_rx.recv_timeout(std::time::Duration::from_millis(250)) {
                Ok(value) => Ok(value),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    runtime.event("waiting_completed_chunk", false);
                    continue;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    Err(std::sync::mpsc::RecvError)
                }
            }
        } else {
            order_rx.recv()
        };
        match received {
            Ok((id, mut members)) => {
                if let Some(ticket) = &mut members.ticket {
                    ticket.mark_ready();
                }
                sink_shared
                    .cpu
                    .record_observed_queue_wait(wait_started.elapsed().as_nanos() as u64);
                {
                    let _permit = sink_shared.compute(Stage::Sink)?;
                    if sink_gp.native_m1 && (id < expected || ready.contains_key(&id)) {
                        let error = Failure::worker("[worker] duplicate completed chunk ordinal");
                        sink_shared.set_err(error.clone());
                        sink.abort();
                        return Err(error);
                    }
                    ready.insert(id, members);
                }
                if let Err(error) = flush(&mut sink, &mut expected, &mut ready) {
                    sink_shared.set_err(error.clone());
                    sink.abort();
                    return Err(error);
                }
            }
            Err(_) => {
                sink_shared
                    .cpu
                    .record_observed_queue_wait(wait_started.elapsed().as_nanos() as u64);
                break;
            }
        }
    }
    if let Some(error) = sink_shared.take_err() {
        sink.abort();
        return Err(error);
    }
    if sink_shared.stop.load(Ordering::Relaxed) {
        sink.abort();
        return Err(Failure::synthetic_cancel("FASTQ pipeline cancelled"));
    }
    if let Err(error) = flush(&mut sink, &mut expected, &mut ready) {
        sink_shared.set_err(error.clone());
        sink.abort();
        return Err(error);
    }
    if sink_gp.native_m1 && !ready.is_empty() {
        sink.abort();
        return Err(Failure::worker("[worker] missing completed chunk ordinal"));
    }
    sink.finish(&sink_gp, &sink_shared.cpu)
        .map_err(|e| {let message=format!("[write] {e}"); e.with_message(message)})
}

fn write_stream_batch(
    members: &MemberSet,
    gp: &GlobalParams,
    cpu: &Arc<CpuBudget>,
) -> Result<(), String> {
    let records1 = members.stream1.as_deref().unwrap_or(&[]);
    let paired = members.stream2.is_some();
    let records2 = members.stream2.as_deref().unwrap_or(&[]);
    let stats = members
        .stream_stats
        .as_deref()
        .ok_or_else(|| "streaming batch is missing its statistics".to_string())?;
    // Formatting is bounded CPU work. The stdout write below deliberately
    // happens after this permit is dropped because stdout can block.
    let bytes = {
        let _permit = cpu.acquire(Stage::Sink);
        crate::stream::format_batch(records1, records2, paired, stats, gp)
    };
    crate::stream::write_stdout(&bytes)
}

fn process_batch(
    work: BatchWork,
    gp: &GlobalParams,
    local: &mut PeLocalStats,
    comp: &Compressor,
    is_batch0: bool,
    ticket: Option<&mut crate::native_execution::Ticket>,
) -> Result<MemberSet, Failure> {
    let trim_enabled = gp.adapter_discard_or_trim == "trim"
        || gp.contam_discard_or_trim == "trim"
        || !gp.trim.is_empty()
        || !gp.trim_bad_head.is_empty()
        || !gp.trim_bad_tail.is_empty();
    let trim_output = !gp.trim_fq1.is_empty();
    // Parse the raw record-aligned chunks on the worker (P4: parse cost is
    // spread over the worker pool instead of sitting on the 2 lane threads).
    let mut reads1 = parse_chunk_records_with_capacity(
        &work.reads1,
        "read1",
        work.first_record_index,
        gp.native_m1,
        work.record_count,
    )
    .map_err(|e| format!("[input] {e}")).map_err(Failure::input)?;
    let mut reads2 = work
        .reads2
        .as_ref()
        .map(|bytes| {
            parse_chunk_records_with_capacity(
                bytes,
                "read2",
                work.first_record_index,
                gp.native_m1,
                work.record_count,
            )
            .map_err(|e| format!("[input] {e}"))
        })
        .transpose().map_err(Failure::input)?;
    if let Some(io)=&gp.native_io {
        let lane=&io.spec.lanes[work.lane_index.unwrap_or(0)];
        for r in &mut reads1 { r.input_phred=Some(lane.inputs[0].phred as i32); }
        if let Some(records)=&mut reads2 {for r in records {r.input_phred=Some(lane.inputs[if io.spec.layout==crate::job_api::Layout::InterleavedPe{0}else{1}].phred as i32);}}
    }
    if gp.native_m1 {
        drop(work.reads1);
        drop(work.reads2);
    }
    if reads1.len() != work.record_count
        || reads2
            .as_ref()
            .is_some_and(|records| records.len() != work.record_count)
    {
        return Err(Failure::input(format!(
            "FASTQ batch {} record count mismatch: expected {}, got read1={} read2={}",
            work.id,
            work.record_count,
            reads1.len(),
            reads2.as_ref().map_or(0, Vec::len)
        )));
    }
    if reads2
        .as_ref()
        .is_some_and(|records| reads1.len() != records.len())
    {
        return Err(Failure::input("the number of reads in fastq1 and fastq2 is not equal").because(Reason::PairCount));
    }
    if let Some(reads2) = reads2.as_ref() {
        // Pair validation moved here from the single-threaded producer.
        let base = work.first_record_index;
        for i in 0..reads1.len() {
            if !crate::record::pair_ids_match(&reads1[i].seq_id, &reads2[i].seq_id) {
                return Err(Failure::input(format!(
                    "paired FASTQ record identifiers differ at input record {}",
                    base + i as u64
                )).at(base+i as u64).because(Reason::PairIdentifiers));
            }
        }
    }

    let paired = reads2.is_some();
    if is_batch0 && !gp.native_m1 {
        // Validate quality encoding while the first parsed records are still
        // available. Filtering below moves them out of `reads1`/`reads2`.
        crate::filter::bq_check(&reads1, &local.raw1, gp).map_err(Failure::input)?;
    }

    let mut diagnostic = if gp.native_m1
        && (gp.native_process.profile == "srna"
            || gp.native_process.overlap.enabled()
            || gp.native_process.screen.enabled()
            || gp.native_process.capture.enabled()
            || gp.native_aux.iter().any(|p| !p.is_empty())
            || !gp.native_match_audit.is_empty())
    {
        let maximum = reads1
            .iter()
            .chain(reads2.iter().flatten())
            .map(|r| r.sequence.len())
            .max()
            .unwrap_or(0)
            .min(crate::params::READ_MAX_LEN);
        Some(Box::new(crate::native_kernel::DiagnosticBatch::new(
            maximum,
        )))
    } else {
        None
    };
    let (mut trim1, mut trim2, mut clean1, mut clean2) = if gp.native_m1 {
        // Inspect lengths only; later invalid records still fail in kernel order.
        local.clean1.native_qc.as_mut().unwrap().set_batch_maximum(
            reads1
                .iter()
                .map(|read| read.sequence.len())
                .max()
                .unwrap_or(0),
        );
        local.clean2.native_qc.as_mut().unwrap().set_batch_maximum(
            reads2
                .as_ref()
                .and_then(|reads| reads.iter().map(|read| read.sequence.len()).max())
                .unwrap_or(0),
        );
        let (clean1, clean2) = crate::native_kernel::apply_recorded_ticket(
            reads1,
            reads2.take(),
            local,
            gp,
            diagnostic.as_deref_mut(),
            ticket,
        )?;
        (Vec::new(), Vec::new(), clean1, clean2)
    } else if let Some(ref mut reads2) = reads2 {
        let result = filter_pe_fqs(
            &mut reads1,
            reads2,
            &mut local.fs,
            gp,
            trim_enabled,
            trim_output,
            Some(&mut local.raw1),
            Some(&mut local.raw2),
        ).map_err(Failure::write)?;
        if trim_output {
            stat_pe_fqs(
                &result.0,
                &result.1,
                &mut local.trim1,
                &mut local.trim2,
                gp.quality_phred,
                gp,
                false,
                true,
            ).map_err(Failure::write)?;
        }
        stat_pe_fqs(
            &result.2,
            &result.3,
            &mut local.clean1,
            &mut local.clean2,
            gp.quality_phred,
            gp,
            false,
            false,
        ).map_err(Failure::write)?;
        result
    } else {
        let result = crate::filter::filter_se_fqs(
            &mut reads1,
            &mut local.fs,
            gp,
            trim_enabled,
            trim_output,
            Some(&mut local.raw1),
        ).map_err(Failure::write)?;
        if trim_output {
            crate::filter::stat_se_fqs(
                &result.0,
                &mut local.trim1,
                &mut local.trim2,
                gp.quality_phred,
                gp,
                true,
            ).map_err(Failure::write)?;
        }
        crate::filter::stat_se_fqs(
            &result.1,
            &mut local.clean1,
            &mut local.clean2,
            gp.quality_phred,
            gp,
            false,
        ).map_err(Failure::write)?;
        (result.0, Vec::new(), result.1, Vec::new())
    };
    output::prepare_shared_output(&mut trim1, gp);
    output::prepare_shared_output(&mut trim2, gp);
    let mut merged = diagnostic
        .as_mut()
        .map_or_else(Vec::new, |d| std::mem::take(&mut d.merged));
    output::prepare_shared_output(&mut merged, gp);
    output::prepare_shared_output(&mut clean1, gp);
    output::prepare_shared_output(&mut clean2, gp);

    let is_fasta = gp.output_file_type == "fasta";
    let mut members = MemberSet::default();
    if gp.native_m1
        && (gp.native_process.sample_fraction != 1.0
            || gp.native_process.overlap.correct
            || gp.native_process.overlap.merge)
    {
        for (mate, records) in
            [&clean1, &clean2]
                .into_iter()
                .enumerate()
                .take(if paired { 2 } else { 1 })
        {
            let mut qc = Box::new(crate::native_qc::Qc::batch());
            qc.set_batch_maximum(records.iter().map(|r| r.sequence.len()).max().unwrap_or(0));
            for record in records {
                qc.observe_validated(
                    record.sequence.as_bytes(),
                    record.qual_seq.as_bytes(),
                    gp.output_quality_phred,
                );
            }
            members.native_written[mate] = Some(qc);
        }
    }
    if gp.native_process.split_units > 0 {
        members.native_selected_ordinals = if gp.native_process.overlap.merge {
            std::mem::take(&mut diagnostic.as_mut().unwrap().selected_ordinals)
        } else {
            clean1.iter().map(|r| r.source_index).collect()
        };
        members.native_split_records = Some([clean1, clean2, std::mem::take(&mut merged)]);
    } else if gp.is_streaming {
        members.stream1 = Some(clean1);
        if paired {
            members.stream2 = Some(clean2);
        }
    } else if !gp.clean_fq1.is_empty() {
        if gp.native_out_interleaved {
            let mut payload = Vec::new();
            for (r1, r2) in clean1.iter().zip(&clean2) {
                output::serialize_native_record(r1, false, gp, "R1", &mut payload).map_err(Failure::input)?;
                output::serialize_native_record(r2, false, gp, if gp.native_out_interleaved {"R1"}else{"R2"}, &mut payload).map_err(Failure::input)?;
            }
            members.clean1 = Some(vec![Member {
                bytes: encode_member(&payload, comp, output::wants_gzip(&gp.clean_fq1)).map_err(Failure::write)?,
                recs: 2 * clean1.len() as u64,
                gzip: output::wants_gzip(&gp.clean_fq1),
            }]);
        } else if paired {
            let mut segs =
                build_split_payloads(&clean1, &clean2, is_fasta, gp, work.first_record_index).map_err(Failure::input)?;
            segs.retain(|s| s.2 > 0);
            let (m1, m2) = compress_split(
                segs,
                comp,
                output::wants_gzip(&gp.clean_fq1),
                output::wants_gzip(&gp.clean_fq2),
            ).map_err(Failure::write)?;
            members.clean1 = Some(m1);
            members.clean2 = Some(m2);
        } else {
            let (payload, recs) =
                build_single_payload(&clean1, is_fasta, gp, work.first_record_index).map_err(Failure::input)?;
            members.clean1 = Some(vec![Member {
                bytes: encode_member(&payload, comp, output::wants_gzip(&gp.clean_fq1)).map_err(Failure::write)?,
                recs,
                gzip: output::wants_gzip(&gp.clean_fq1),
            }]);
        }
    }
    if gp.native_process.overlap.merge && gp.native_process.split_units == 0 {
        let mut qc = Box::new(crate::native_qc::Qc::merged());
        qc.set_merged_maximum(merged.iter().map(|r| r.sequence.len()).max().unwrap_or(0));
        let mut payload = Vec::new();
        for record in &merged {
            qc.observe_validated(
                record.sequence.as_bytes(),
                record.qual_seq.as_bytes(),
                gp.output_quality_phred,
            );
            output::serialize_native_record(record, false, gp, "merged-out", &mut payload).map_err(Failure::input)?;
        }
        members.native_merged = Some(Member {
            bytes: encode_member(&payload, comp, output::wants_gzip(&gp.native_merged)).map_err(Failure::write)?,
            recs: merged.len() as u64,
            gzip: output::wants_gzip(&gp.native_merged),
        });
        members.native_merged_qc = Some(qc);
    }
    if let Some(mut d) = diagnostic {
        for i in 0..6 {
            if gp.native_aux[i].is_empty() {
                continue;
            }
            let records = if i < 2 {
                &mut d.rejected[i]
            } else if i < 4 {
                &mut d.transformed[i - 2]
            } else {
                &mut d.singletons[i - 4]
            };
            output::prepare_shared_output(records, gp);
            let mut qc = crate::native_qc::Qc::batch();
            qc.set_batch_maximum(records.iter().map(|r| r.sequence.len()).max().unwrap_or(0));
            let mut payload = Vec::new();
            for record in records.iter() {
                qc.observe_validated(
                    record.sequence.as_bytes(),
                    record.qual_seq.as_bytes(),
                    gp.output_quality_phred,
                );
                output::serialize_native_record(record, false, gp, ["reject1","reject2","transformed1","transformed2","singleton1","singleton2"][i], &mut payload).map_err(Failure::input)?;
            }
            members.native_aux[i] = Some(Member {
                bytes: encode_member(&payload, comp, output::wants_gzip(&gp.native_aux[i])).map_err(Failure::write)?,
                recs: records.len() as u64,
                gzip: output::wants_gzip(&gp.native_aux[i]),
            });
            members.native_diagnostic_qc[i] = Some(Box::new(qc));
        }
        for (i, qc) in d.stages.into_iter().enumerate() {
            members.native_diagnostic_qc[6 + i] = Some(Box::new(qc));
        }
        members.native_manifest = d.manifest;
        members.native_matches = d.matches;
    }
    if trim_output {
        let mut p1 = Vec::with_capacity(trim1.len() * 200);
        for r in &trim1 {
            output::serialize_native_record(r, is_fasta, gp, "R1", &mut p1).map_err(Failure::input)?;
        }
        let mut p2 = Vec::with_capacity(trim2.len() * 200);
        for r in &trim2 {
            output::serialize_native_record(r, is_fasta, gp, "R2", &mut p2).map_err(Failure::input)?;
        }
        let g1 = encode_member(&p1, comp, output::wants_gzip(&gp.trim_fq1)).map_err(Failure::write)?;
        members.trim1 = Some(vec![Member {
            bytes: g1,
            recs: trim1.len() as u64,
            gzip: output::wants_gzip(&gp.trim_fq1),
        }]);
        if paired {
            let g2 = encode_member(&p2, comp, output::wants_gzip(&gp.trim_fq2)).map_err(Failure::write)?;
            members.trim2 = Some(vec![Member {
                bytes: g2,
                recs: trim2.len() as u64,
                gzip: output::wants_gzip(&gp.trim_fq2),
            }]);
        }
    }
    Ok(members)
}

fn choose_demux_error(current:&mut Option<crate::failure::UnitError>,next:Option<crate::failure::UnitError>){if let Some(e)=next{if current.as_ref().is_none_or(|old|e.ordinal<old.ordinal){*current=Some(e);}}}
fn parse_demux_chunk(bytes:&[u8],label:&str,start:u64,capacity:usize)->(Vec<FastqRecord>,Option<crate::failure::UnitError>){
    let mut records=Vec::with_capacity(capacity);let mut pos=0;
    while pos<bytes.len(){let mut count=0;let mut end=pos;for b in &bytes[pos..]{end+=1;if *b==b'\n'{count+=1;if count==4{break;}}}
        let result=if count==4{parse_chunk_records_with_capacity(&bytes[pos..end],label,start+records.len()as u64,true,1)}else{Err("truncated FASTQ record".into())};
        match result{Ok(mut v)=>records.append(&mut v),Err(message)=>{let ordinal=start+records.len()as u64;return(records,Some(crate::failure::UnitError{ordinal,stage:if label=="index"{"index-association"}else{"input"},message,code:3}));}}pos=end;
    }(records,None)
}
fn process_worker_batch(
    mut work: BatchWork,
    gp: &GlobalParams,
    local: &mut PeLocalStats,
    comp: &Compressor,
    is_batch0: bool,
) -> Result<MemberSet, Failure> {
    let mut ticket = work.ticket.take();
    if gp.native_dedup.is_some(){let(mut first,mut terminal)=parse_demux_chunk(&work.reads1,"read1",work.first_record_index,work.record_count);let mut second=work.reads2.as_ref().map(|b|{let(records,error)=parse_demux_chunk(b,"read2",work.first_record_index,work.record_count);choose_demux_error(&mut terminal,error);records});let lane=&gp.native_io.as_ref().unwrap().spec.lanes[work.lane_index.unwrap_or(0)];for r in &mut first{r.input_phred=Some(lane.inputs.iter().find(|d|d.channel=="R1").unwrap().phred as i32);}if let Some(v)=&mut second{for r in v.iter_mut(){r.input_phred=Some(lane.inputs.iter().find(|d|d.channel==if gp.native_io.as_ref().unwrap().spec.layout==crate::job_api::Layout::InterleavedPe{"R1"}else{"R2"}).unwrap().phred as i32);}first.truncate(v.len());v.truncate(first.len());}if let Some(raw)=work.demux.take(){choose_demux_error(&mut terminal,raw.error);}let units=crate::dedup_runtime::Runtime::process(first,second,terminal,work.lane_index);return Ok(MemberSet{ticket,native_dedup:Some(units),..MemberSet::default()});}
    if let Some(demux)=&gp.native_demux {
        let (mut first,mut terminal)=parse_demux_chunk(&work.reads1,"read1",work.first_record_index,work.record_count);
        let mut second=work.reads2.as_ref().map(|b|{let(records,error)=parse_demux_chunk(b,"read2",work.first_record_index,work.record_count);choose_demux_error(&mut terminal,error);records});
        let lane=&gp.native_io.as_ref().unwrap().spec.lanes[work.lane_index.unwrap_or(0)];
        for r in &mut first{r.input_phred=Some(lane.inputs.iter().find(|d|d.channel=="R1").unwrap().phred as i32);}
        if let Some(second)=&mut second{for r in second.iter_mut(){r.input_phred=Some(lane.inputs.iter().find(|d|d.channel==if gp.native_io.as_ref().unwrap().spec.layout==crate::job_api::Layout::InterleavedPe{"R1"}else{"R2"}).unwrap().phred as i32);}first.truncate(second.len());second.truncate(first.len());}
        let mut indices=Vec::new();
        if let Some(raw)=work.demux.take(){
            choose_demux_error(&mut terminal,raw.error);
            for(i,bytes)in raw.indices.iter().enumerate(){let(mut records,error)=parse_demux_chunk(bytes,"index",work.first_record_index,work.record_count);choose_demux_error(&mut terminal,error);let decl=&gp.native_io.as_ref().unwrap().spec.lanes[0].inputs.iter().filter(|d|d.channel=="I1"||d.channel=="I2").nth(i).unwrap();for r in &mut records{r.input_phred=Some(decl.phred as i32);}indices.push(records);}
        }
        let mut units=demux.process(first,second,indices,terminal,gp,comp);for unit in &mut units{unit.lane_index=work.lane_index;}return Ok(MemberSet{ticket,native_demux:Some(units),..MemberSet::default()});
    }
    if gp.native_m1 {
        let before = if !gp.paired() {
            local.raw1.gs.reads_number
        } else {
            local.raw1.gs.reads_number.min(local.raw2.gs.reads_number)
        };
        // Keep only a small per-batch native QC object. Another full PeLocalStats
        // value here would add the inherited large fixed matrices to worker stacks.
        let mut prior1 = local
            .clean1
            .native_qc
            .replace(Box::new(crate::native_qc::Qc::batch()))
            .unwrap();
        let mut prior2 = local
            .clean2
            .native_qc
            .replace(Box::new(crate::native_qc::Qc::batch()))
            .unwrap();
        let lane_index = work.lane_index;
        let result = process_batch(work, gp, local, comp, is_batch0, ticket.as_mut()).map_err(|e| {
            if let Some(index) = lane_index {
                let lane = gp.native_io.as_ref().and_then(|io|io.spec.lanes.get(index)).map(|l|l.id.as_str()).or_else(|| gp
                    .native_lane_manifest
                    .as_ref()
                    .and_then(|text| text.lines().skip(1).nth(index))
                    .and_then(|line| line.split('\t').next()))
                    .unwrap_or("input");
                let message=format!("{e}; lane_id={lane}; lane_index={index}");
                let mut e=e.with_message(message);e.lane=Some(index);e
            } else {
                e
            }
        });
        let batch1 = local.clean1.native_qc.take().unwrap();
        let batch2 = local.clean2.native_qc.take().unwrap();
        prior1.merge(&batch1);
        prior2.merge(&batch2);
        local.clean1.gs = prior1.general();
        local.clean2.gs = prior2.general();
        local.clean1.native_qc = Some(prior1);
        local.clean2.native_qc = Some(prior2);
        if let Some(runtime) = &gp.native_runtime {
            let after = if !gp.paired() {
                local.raw1.gs.reads_number
            } else {
                local.raw1.gs.reads_number.min(local.raw2.gs.reads_number)
            };
            runtime.processed(after - before, batch1.reads);
        }
        let mut members = result?;
        if members.native_written[0].is_none() {
            members.native_written = [Some(batch1), Some(batch2)];
        }
        members.ticket = ticket;
        return Ok(members);
    }
    if !gp.is_streaming {
        return process_batch(work, gp, local, comp, is_batch0, ticket.as_mut());
    }

    // Streaming messages must carry a self-contained, batch-local snapshot.
    // Keep the worker accumulator separate so the final report still covers
    // the complete run. The snapshot is moved to the ordered sink after the
    // batch has been accepted; no references into the batch survive.
    let mut batch_stats = PeLocalStats::new(gp);
    let mut members = process_batch(work, gp, &mut batch_stats, comp, is_batch0, ticket.as_mut())?;
    crate::stats::merge_pe_local(local, &batch_stats, gp, false);
    members.stream_stats = Some(Box::new(batch_stats));
    Ok(members)
}

#[cfg(test)]
fn test_completion_control(id: u64) {
    let delayed_id = std::env::var("SOAPNUKE_TEST_DELAY_BATCH_ID")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    if delayed_id == Some(id) {
        let delay_ms = std::env::var("SOAPNUKE_TEST_DELAY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(100);
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
    }
    if let Ok(path) = std::env::var("SOAPNUKE_TEST_COMPLETION_TRACE") {
        let mut trace = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("completion trace must be writable");
        std::io::Write::write_all(&mut trace, format!("{id}\n").as_bytes())
            .expect("completion trace must be writable");
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchWork, process_batch, process_worker_batch};
    use crate::ffi::Compressor;
    use crate::params::GlobalParams;
    use crate::stats::PeLocalStats;

    fn one_record_with_quality(id: &str, base: u8, quality: &[u8]) -> Vec<u8> {
        let sequence = [base; 4];
        let mut out = format!("@{}\n", id).into_bytes();
        out.extend_from_slice(&sequence);
        out.extend_from_slice(b"\n+\n");
        out.extend_from_slice(quality);
        out.push(b'\n');
        out
    }

    fn one_record(id: &str, base: u8) -> Vec<u8> {
        one_record_with_quality(id, base, b"IIII")
    }

    fn batch(id: u64, name: &str, base: u8) -> BatchWork {
        BatchWork {
            demux:None,
            lane_index: None,
            ticket: None,
            id,
            first_record_index: 0,
            record_count: 1,
            reads1: one_record(name, base),
            reads2: Some(one_record(name, base)),
        }
    }

    fn batch_with_quality(id: u64, name: &str, base: u8, quality: &[u8]) -> BatchWork {
        BatchWork {
            demux:None,
            lane_index: None,
            ticket: None,
            id,
            first_record_index: 0,
            record_count: 1,
            reads1: one_record_with_quality(name, base, quality),
            reads2: Some(one_record_with_quality(name, base, quality)),
        }
    }

    fn record_with_sequence(id: &str, sequence: &str) -> Vec<u8> {
        let quality = "I".repeat(sequence.len());
        format!("@{}\n{}\n+\n{}\n", id, sequence, quality).into_bytes()
    }

    fn paired_batch(id: u64, name: &str, sequence1: &str, sequence2: &str) -> BatchWork {
        BatchWork {
            demux:None,
            lane_index: None,
            ticket: None,
            id,
            first_record_index: 0,
            record_count: 1,
            reads1: record_with_sequence(name, sequence1),
            reads2: Some(record_with_sequence(name, sequence2)),
        }
    }

    #[test]
    fn batch_qc_capacity_follows_parsed_lengths_without_restricting_persistent_qc() {
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.fq2_path = "r2.fq".into();
        gp.clean_fq1 = "r1.fq.gz".into();
        gp.clean_fq2 = "r2.fq.gz".into();
        gp.native_cuts = [[1, 0], [1, 0]];
        let comp = Compressor::new(2).unwrap();
        let mut local = PeLocalStats::new(&gp);
        for (id, maximum) in [(0, 150usize), (1, 1000usize)] {
            let mut work = paired_batch(id, "first", &"A".repeat(75), &"C".repeat(75));
            work.reads1
                .extend(record_with_sequence("second", &"A".repeat(maximum)));
            work.reads2
                .as_mut()
                .unwrap()
                .extend(record_with_sequence("second", &"C".repeat(maximum)));
            work.record_count = 2;
            let members = process_worker_batch(work, &gp, &mut local, &comp, false).unwrap();
            for qc in members.native_written.iter().flatten() {
                assert_eq!(qc.cycles.capacity(), maximum);
                assert_eq!(qc.lengths.capacity(), maximum + 1);
                assert_eq!(qc.reads, 2);
                assert!(qc.allocated_bytes() <= crate::native_execution::QC_BYTES);
            }
            assert_eq!(
                local.raw1.native_qc.as_ref().unwrap().cycles.capacity(),
                1000
            );
            assert_eq!(
                local.clean1.native_qc.as_ref().unwrap().cycles.capacity(),
                1000
            );
            assert_eq!(local.clean1.native_qc.as_ref().unwrap().reads, 2 * (id + 1));
        }
    }

    #[test]
    fn batch_qc_length_scan_preserves_valid_prefix_before_later_input_errors() {
        for overlength in [true, false] {
            let mut gp = GlobalParams::new();
            gp.native_m1 = true;
            gp.fq2_path = "r2.fq".into();
            gp.native_cuts = [[1, 0], [1, 0]];
            let comp = Compressor::new(2).unwrap();
            let mut local = PeLocalStats::new(&gp);
            let mut work = paired_batch(0, "first", &"A".repeat(1000), &"C".repeat(1000));
            work.reads1
                .extend(record_with_sequence("second", &"A".repeat(150)));
            let mut invalid =
                record_with_sequence("second", &"C".repeat(if overlength { 1001 } else { 150 }));
            if !overlength {
                let position = invalid.len() - 2;
                invalid[position] = b' ';
            }
            work.reads2.as_mut().unwrap().extend(invalid);
            work.record_count = 2;
            let error = match process_worker_batch(work, &gp, &mut local, &comp, false) {
                Ok(_) => panic!("invalid later record unexpectedly succeeded"),
                Err(error) => error,
            };
            assert!(
                error.contains(if overlength {
                    "supported limit"
                } else {
                    "quality.encoding"
                }),
                "{error}"
            );
            for stat in [&local.raw1, &local.raw2, &local.clean1, &local.clean2] {
                assert_eq!(stat.native_qc.as_ref().unwrap().reads, 1);
            }
            assert_eq!(
                local.raw1.native_qc.as_ref().unwrap().general().base_number,
                1000
            );
            assert_eq!(
                local
                    .clean1
                    .native_qc
                    .as_ref()
                    .unwrap()
                    .general()
                    .base_number,
                999
            );
            assert_eq!(local.fs.native.rejected, 0);
        }
    }

    #[test]
    fn batch_qc_se_and_empty_handoffs_keep_only_their_own_capacity() {
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.clean_fq1 = "r1.fq.gz".into();
        let comp = Compressor::new(2).unwrap();
        let mut local = PeLocalStats::new(&gp);
        for (id, maximum) in [(0, 0usize), (1, 150usize)] {
            let mut work = batch(id, "empty", b'A');
            work.reads2 = None;
            work.reads1 = record_with_sequence("read", &"A".repeat(maximum));
            let members = process_worker_batch(work, &gp, &mut local, &comp, false).unwrap();
            let first = members.native_written[0].as_ref().unwrap();
            assert_eq!(first.reads, u64::from(maximum > 0));
            assert_eq!(first.cycles.capacity(), maximum);
            let second = members.native_written[1].as_ref().unwrap();
            assert_eq!(second.reads, 0);
            assert_eq!(second.cycles.capacity(), 0);
            assert_eq!(second.lengths.capacity(), 0);
        }
    }

    #[test]
    fn clean_statistics_do_not_copy_filtered_raw_batches() {
        let mut gp = GlobalParams::new();
        gp.min_read_length = -1;
        gp.clean_fq1 = "clean.R1.fq.gz".to_string();
        gp.clean_fq2 = "clean.R2.fq.gz".to_string();
        let mut local = PeLocalStats::new(&gp);
        let comp = Compressor::new(1).unwrap();

        // NNNN is filtered by the default N-ratio rule; AAAA is retained.
        process_batch(batch(0, "read0", b'N'), &gp, &mut local, &comp, false, None).unwrap();
        process_batch(batch(1, "read1", b'A'), &gp, &mut local, &comp, false, None).unwrap();

        assert_eq!(local.raw1.gs.reads_number, 2);
        assert_eq!(local.raw1.gs.a_number, 4);
        assert_eq!(local.raw1.gs.n_number, 4);
        assert_eq!(local.clean1.gs.reads_number, 1);
        assert_eq!(local.clean1.gs.base_number, 4);
        assert_eq!(local.clean1.gs.a_number, 4);
        assert_eq!(local.clean1.gs.n_number, 0);
    }

    #[test]
    fn process_batch_discards_overlapped_pair() {
        let mut gp = GlobalParams::new();
        gp.min_read_length = -1;
        gp.overlap_length = 4;
        gp.pe_mismatch_ratio = 0.0;
        gp.clean_fq1 = "clean.R1.fq.gz".to_string();
        gp.clean_fq2 = "clean.R2.fq.gz".to_string();
        let mut local = PeLocalStats::new(&gp);
        let comp = Compressor::new(1).unwrap();

        let members = process_batch(
            paired_batch(0, "overlap", "AAAACCCC", "TTTTGGGG"),
            &gp,
            &mut local,
            &comp,
            false,
            None,
        )
        .unwrap();

        assert_eq!(local.fs.over_lapped_num, 1);
        assert_eq!(local.clean1.gs.reads_number, 0);
        assert!(members.clean1.as_ref().unwrap().is_empty());
        assert!(members.clean2.as_ref().unwrap().is_empty());
    }

    #[test]
    fn first_batch_quality_check_runs_before_records_are_moved() {
        let mut gp = GlobalParams::new();
        gp.min_read_length = -1;
        gp.clean_fq1 = "clean.R1.fq.gz".to_string();
        gp.clean_fq2 = "clean.R2.fq.gz".to_string();
        let mut local = PeLocalStats::new(&gp);
        let comp = Compressor::new(1).unwrap();

        let result = process_batch(
            batch_with_quality(0, "quality-ok", b'A', b"IIII"),
            &gp,
            &mut local,
            &comp,
            true,
            None,
        );
        assert!(
            result.is_ok(),
            "valid first batch rejected: {:?}",
            result.err()
        );
    }

    #[test]
    fn invalid_first_batch_quality_is_rejected_by_quality_check() {
        let mut gp = GlobalParams::new();
        gp.quality_phred = 64;
        gp.min_read_length = -1;
        gp.clean_fq1 = "clean.R1.fq.gz".to_string();
        gp.clean_fq2 = "clean.R2.fq.gz".to_string();
        let mut local = PeLocalStats::new(&gp);
        let comp = Compressor::new(1).unwrap();

        let error = match process_batch(
            batch_with_quality(0, "quality-bad", b'A', b"!!!!"),
            &gp,
            &mut local,
            &comp,
            true,
            None,
        ) {
            Ok(_) => panic!("bad quality encoding must stop before output"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "base quality seems abnormal,please check the quality system parameter or fastq file"
        );
    }
}

// Build the serialized (mate1, mate2) payload segments for one batch.
// - Head mode (totalReadsNum with "head" suffix) is applied by the ordered
//   sink, after filtering, so the limit counts retained clean records.
// - Split mode (cleanOutSplit > 0) is handled entirely by the sink (it
//   decompresses the member and re-slices at record boundaries across
//   batches); the worker emits one whole-batch segment here.
fn build_split_payloads(
    clean1: &[FastqRecord],
    clean2: &[FastqRecord],
    is_fasta: bool,
    gp: &GlobalParams,
    _first_record_index: u64,
) -> Result<Vec<(Vec<u8>, Vec<u8>, u64)>, String> {
    let mut p1 = Vec::with_capacity(clean1.len() * 200);
    let mut p2 = Vec::with_capacity(clean2.len() * 200);
    let mut n: u64 = 0;
    for (r1, r2) in clean1.iter().zip(clean2.iter()) {
        output::serialize_native_record(r1, is_fasta, gp, "R1", &mut p1)?;
        output::serialize_native_record(r2, is_fasta, gp, "R2", &mut p2)?;
        n += 1;
    }
    Ok(vec![(p1, p2, n)])
}

fn build_single_payload(
    clean: &[FastqRecord],
    is_fasta: bool,
    gp: &GlobalParams,
    _first_record_index: u64,
) -> Result<(Vec<u8>, u64),String> {
    let mut payload = Vec::with_capacity(clean.len() * 200);
    let mut recs = 0u64;
    for record in clean {
        output::serialize_native_record(record, is_fasta, gp, "R1", &mut payload)?;
        recs += 1;
    }
    Ok((payload, recs))
}

// Compress each (mate1, mate2) payload with the worker-owned compressor.
fn compress_split(
    segs: Vec<(Vec<u8>, Vec<u8>, u64)>,
    comp: &Compressor,
    gzip1: bool,
    gzip2: bool,
) -> Result<(Vec<Member>, Vec<Member>), String> {
    let mut out1 = Vec::with_capacity(segs.len());
    let mut out2 = Vec::with_capacity(segs.len());
    for (p1, p2, recs) in segs {
        let g1 = encode_member(&p1, comp, gzip1)?;
        let g2 = encode_member(&p2, comp, gzip2)?;
        out1.push(Member {
            bytes: g1,
            recs,
            gzip: gzip1,
        });
        out2.push(Member {
            bytes: g2,
            recs,
            gzip: gzip2,
        });
    }
    Ok((out1, out2))
}

// Parse a raw record-aligned chunk (whole records, produced by the lane's
// 4-newline split) into FastqRecords. Validations match the v2 reader.
fn parse_chunk_records(
    chunk: &[u8],
    label: &str,
    first_record_index: u64,
) -> Result<Vec<FastqRecord>, String> {
    parse_chunk_records_mode(chunk, label, first_record_index, false)
}

fn parse_chunk_records_mode(
    chunk: &[u8],
    label: &str,
    first_record_index: u64,
    allow_empty: bool,
) -> Result<Vec<FastqRecord>, String> {
    parse_chunk_records_with_capacity(
        chunk,
        label,
        first_record_index,
        allow_empty,
        crate::source::LANE_CHUNK,
    )
}
pub(crate) fn parse_chunk_records_with_capacity(
    chunk: &[u8],
    label: &str,
    first_record_index: u64,
    allow_empty: bool,
    capacity: usize,
) -> Result<Vec<FastqRecord>, String> {
    let mut out = Vec::with_capacity(capacity);
    let mut pos = 0usize;
    while pos < chunk.len() {
        let start = pos;
        let mut found = 0usize;
        let mut nl = [0usize; 4];
        let mut i = start;
        while i < chunk.len() {
            if chunk[i] == b'\n' {
                nl[found] = i;
                found += 1;
                if found == 4 {
                    break;
                }
            }
            i += 1;
        }
        if found < 4 {
            return Err(format!("truncated FASTQ record in {} input", label));
        }
        let n1 = nl[0];
        let n2 = nl[1];
        let n3 = nl[2];
        let n4 = nl[3];
        match build_record(
            &chunk[start..n1],
            &chunk[n1 + 1..n2],
            &chunk[n2 + 1..n3],
            &chunk[n3 + 1..n4],
            label,
            allow_empty,
        ) {
            Ok(Some(mut r)) => {
                r.source_index = first_record_index + out.len() as u64;
                out.push(r);
            }
            Ok(None) => return Err(format!("failed to parse FASTQ record in {} input", label)),
            Err(e) => return Err(e),
        }
        pos = n4 + 1;
    }
    Ok(out)
}

fn build_record(
    id_line: &[u8],
    seq_line: &[u8],
    sep_line: &[u8],
    qual_line: &[u8],
    label: &str,
    allow_empty: bool,
) -> Result<Option<FastqRecord>, String> {
    // FASTQ permits CRLF line endings.  The lane framing keeps the raw bytes,
    // so remove exactly one terminal CR from each logical line before
    // validation and storing the record fields.
    let id_line = without_terminal_cr(id_line);
    let seq_line = without_terminal_cr(seq_line);
    let sep_line = without_terminal_cr(sep_line);
    let qual_line = without_terminal_cr(qual_line);
    let id = fast_string(id_line);
    let seq = fast_string(seq_line);
    let qual = fast_string(qual_line);
    let pair_position = if label == "read2" { 2 } else { 1 };
    let mut record = FastqRecord::new(pair_position);
    if id.is_empty() || id.as_bytes()[0] != b'@' {
        return Err(format!(
            "FASTQ record identifier does not start with '@' in {} input file",
            label
        ));
    }
    if sep_line.is_empty() || sep_line[0] != b'+' {
        return Err(format!(
            "FASTQ record separator does not start with '+' in {} input file",
            label
        ));
    }
    if (!allow_empty && seq.is_empty()) || seq.len() != qual.len() {
        return Err(format!(
            "FASTQ record sequence and quality lengths differ in {} input file (seq={} qual={} id={:?} seqhead={:?} qualhead={:?})",
            label,
            seq.len(),
            qual.len(),
            &id[..id.len().min(40)],
            &seq[..seq.len().min(20)],
            &qual[..qual.len().min(20)],
        ));
    }
    record.seq_id = id;
    record.sequence = seq;
    record.qual_seq = qual;
    Ok(Some(record))
}

fn without_terminal_cr(line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\r') {
        &line[..line.len() - 1]
    } else {
        line
    }
}

fn fast_string(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

fn encode_member(payload: &[u8], comp: &Compressor, gzip: bool) -> Result<Vec<u8>, String> {
    if gzip {
        let gz = comp.gzip_compress_alloc(payload);
        #[cfg(test)]
        let gz = if std::env::var_os("SOAPNUKE_TEST_FAIL_COMPRESSION").is_some() {
            Vec::new()
        } else {
            gz
        };
        if gz.is_empty() {
            return Err("libdeflate gzip compress failed".into());
        }
        Ok(gz)
    } else {
        Ok(payload.to_vec())
    }
}

pub fn run_filter(gp: &GlobalParams, gv: &mut GlobalStats) -> Result<(), String> {
    run_filter_typed(gp, gv).map_err(String::from)
}
pub(crate) fn run_filter_typed(gp: &GlobalParams, gv: &mut GlobalStats) -> Result<(), Failure> {
    run_filter_with_sampling_typed(gp, gv, None)
}

pub fn run_filter_with_sampling(
    gp: &GlobalParams,
    gv: &mut GlobalStats,
    sample_target: Option<(u64, u64)>,
) -> Result<(), String> {
    run_filter_with_sampling_typed(gp, gv, sample_target).map_err(String::from)
}
fn run_filter_with_sampling_typed(
    gp: &GlobalParams,
    gv: &mut GlobalStats,
    sample_target: Option<(u64, u64)>,
) -> Result<(), Failure> {
    if gp.native_m1 && gp.native_process.qc_only {return run_inspection(gp,gv);}
    let tmp_dir = if gp.is_streaming {
        String::new()
    } else {
        output::make_tmp_dir(gp).map_err(Failure::write)?
    };
    let result = run_filter_inner(gp, gv, &tmp_dir, sample_target);
    if result.is_err() && !tmp_dir.is_empty() {
        let _ = output::remove_tmp_dir(gp, &tmp_dir);
    }
    result
}

struct ReplayBatch {
    batch: crate::source::InputBatch,
    raw_reservation: crate::native_execution::FeatureTicket,
}
fn parse_inference_record(
    bytes: &[u8],
    cursor: &mut usize,
    label: &str,
    ordinal: u64,
) -> Result<FastqRecord, Failure> {
    let start = *cursor;
    for _ in 0..4 {
        let offset = memchr::memchr(b'\n', &bytes[*cursor..])
            .ok_or("[input] incomplete inference FASTQ record").map_err(Failure::input)?;
        *cursor += offset + 1;
    }
    let mut records =
        parse_chunk_records_with_capacity(&bytes[start..*cursor], label, ordinal, true, 1)
            .map_err(|e| format!("[input] {e}")).map_err(Failure::input)?;
    if records.len() != 1 {
        return Err(Failure::input("[input] inference record count mismatch"));
    }
    Ok(records.pop().unwrap())
}
fn prepare_inference(
    source: &mut Source,
    gp: &mut GlobalParams,
    gv: &mut GlobalStats,
    execution: &Arc<crate::native_execution::Execution>,
    cpu: &Arc<CpuBudget>,
) -> Result<
    (
        Vec<ReplayBatch>,
        Option<crate::native_execution::FeatureTicket>,
    ),
    Failure,
> {
    if gp.native_process.infer_mode == "off" {
        let _permit = cpu.acquire(Stage::Source);
        Arc::get_mut(&mut gp.native_process).unwrap().compile().map_err(Failure::worker)?;
        return Ok((Vec::new(), None));
    }
    let mode = gp.native_process.infer_mode.clone();
    let requested = gp.native_process.infer_units as u64;
    let target = requested.min(gp.native_limit_units.unwrap_or(u64::MAX));
    let minimum = gp.native_process.infer_min_support;
    let selected_mates = [
        mode == "r1" || mode == "both",
        mode == "r2" || mode == "both",
    ];
    let runtime = gp.native_runtime.as_ref().unwrap().clone();
    let mut headroom: Option<crate::native_execution::FeatureTicket> = None;
    let mut replay_headroom = 0usize;
    let mut index_reservation: Option<crate::native_execution::FeatureTicket> = None;
    let mut batches: Vec<ReplayBatch> = Vec::new();
    let mut key_reservations: [Option<crate::native_execution::FeatureTicket>; 2] = [None, None];
    let mut keys: [Vec<u128>; 2] = Default::default();
    let mut sampled = 0u64;
    let mut returned = 0u64;
    let mut end_reason = "error";
    let old_patch = source.exchange_native_patch_size((target as usize).min(gp.patch_size).max(1));
    let result = (|| -> Result<[Option<(String, u64)>; 2], Failure> {
        while sampled < target {
            source.exchange_native_patch_size(
                ((target - sampled) as usize).min(gp.patch_size).max(1),
            );
            let Some(batch) = source.next_batch()? else {
                end_reason = "eof";
                break;
            };
            returned += batch.record_count as u64;
            let raw = batch
                .reads1
                .len()
                .checked_add(batch.reads2.as_ref().map_or(0, Vec::len))
                .ok_or("[worker] inference raw length overflow").map_err(Failure::worker)?;
            let complete = crate::native_execution::Execution::reservation_with_capacity(
                raw,
                batch.record_count,
                raw,
            ).map_err(Failure::from)?
            .checked_add(gp.native_process.feature_chunk_bound(
                batch.record_count,
                raw,
                gp.native_aux.iter().any(|s| !s.is_empty()),
                !gp.native_match_audit.is_empty(),
            ).map_err(Failure::worker)?)
            .ok_or("[worker] inference replay bound overflow").map_err(Failure::worker)?;
            if complete > replay_headroom {
                drop(headroom.take());
                headroom = Some(execution.reserve_persistent_feature(complete).map_err(Failure::from)?);
                replay_headroom = complete;
            }
            if batches.len() == batches.capacity() {
                let capacity = batches
                    .capacity()
                    .checked_mul(2)
                    .unwrap_or(usize::MAX)
                    .max(batches.len() + 1)
                    .max(4);
                let bytes = capacity
                    .checked_mul(std::mem::size_of::<ReplayBatch>())
                    .ok_or("[worker] inference index overflow").map_err(Failure::worker)?;
                let next = execution.reserve_persistent_feature(bytes).map_err(Failure::from)?;
                batches
                    .try_reserve_exact(capacity - batches.len())
                    .map_err(|e| format!("[worker] inference index allocation: {e}")).map_err(Failure::worker)?;
                if batches.capacity() > capacity {
                    return Err(Failure::worker("[worker] inference index capacity exceeded reservation"));
                }
                index_reservation = Some(next);
            }
            let raw_reservation = execution.reserve_persistent_feature(raw).map_err(Failure::from)?;
            let compact = crate::source::InputBatch {
                demux:None,
                lane_index: batch.lane_index,
                id: batch.id,
                first_record_index: batch.first_record_index,
                record_count: batch.record_count,
                reads1: batch.reads1.to_vec(),
                reads2: batch.reads2.as_ref().map(|v| v.to_vec()),
            };
            drop(batch);
            if compact.reads1.capacity() + compact.reads2.as_ref().map_or(0, Vec::capacity) > raw {
                return Err(Failure::worker("[worker] inference raw capacity exceeded reservation"));
            }
            let _permit = cpu.acquire(Stage::Source);
            let mut positions = [0usize; 2];
            for offset in 0..compact.record_count {
                let ordinal = compact.first_record_index + offset as u64;
                if ordinal != sampled {
                    return Err(Failure::worker("[worker] inference original ordinal discontinuity"));
                }
                let mut r1 =
                    parse_inference_record(&compact.reads1, &mut positions[0], "read1", ordinal)?;
                let mut r2 = compact
                    .reads2
                    .as_ref()
                    .map(|b| parse_inference_record(b, &mut positions[1], "read2", ordinal))
                    .transpose()?;
                if r2
                    .as_ref()
                    .is_some_and(|r| !crate::record::pair_ids_match(&r1.seq_id, &r.seq_id))
                {
                    return Err(Failure::input(format!(
                        "[input] paired FASTQ record identifiers differ at input record {ordinal}"
                    )).at(ordinal).because(Reason::PairIdentifiers));
                }
                if let Some(io)=&gp.native_io {let lane=&io.spec.lanes[compact.lane_index.unwrap_or(0)];r1.input_phred=Some(lane.inputs[0].phred as i32);if let Some(r)=&mut r2 {r.input_phred=Some(lane.inputs[if gp.native_interleaved{0}else{1}].phred as i32);}}
                crate::native_kernel::validate_normalize(&mut r1, gp.quality_phred).map_err(Failure::input)?;
                if let Some(r) = &mut r2 { crate::native_kernel::validate_normalize(r,gp.quality_phred).map_err(Failure::input)?; }
                crate::native_kernel::observe(&r1, &mut gv.raw1, gp.quality_phred);
                if let Some(r) = &r2 {
                    crate::native_kernel::observe(r, &mut gv.raw2, gp.quality_phred);
                }
                sampled += 1;
                runtime.preparation_observed_input();
                    if let Some(estimator)=runtime.repetition.lock().unwrap_or_else(|p|p.into_inner()).as_ref(){estimator.observe(&r1,r2.as_ref(),None).map_err(Failure::from)?;}
                for (mate, read) in std::iter::once(&r1).chain(r2.as_ref()).enumerate() {
                    if !selected_mates[mate] {
                        continue;
                    }
                    for length in 6..=30.min(read.sequence.len()) {
                        if let Some(key) = crate::native_processing::suffix_key(
                            &read.sequence.as_bytes()[read.sequence.len() - length..],
                        ) {
                            if keys[mate].len() == keys[mate].capacity() {
                                let capacity = keys[mate]
                                    .capacity()
                                    .checked_mul(2)
                                    .unwrap_or(usize::MAX)
                                    .max(keys[mate].len() + 1)
                                    .max(4);
                                let bytes = capacity
                                    .checked_mul(std::mem::size_of::<u128>())
                                    .ok_or("[worker] inference key capacity overflow").map_err(Failure::worker)?;
                                let next = execution.reserve_persistent_feature(bytes).map_err(Failure::from)?;
                                let additional = capacity - keys[mate].len();
                                keys[mate].try_reserve_exact(additional).map_err(|e| {
                                    format!("[worker] inference key allocation: {e}")
                                }).map_err(Failure::worker)?;
                                if keys[mate].capacity() > capacity {
                                    return Err(
                                        Failure::worker("[worker] inference key capacity exceeded reservation"),
                                    );
                                }
                                key_reservations[mate] = Some(next);
                            }
                            keys[mate].push(key);
                        }
                    }
                }
            }
            if positions[0] != compact.reads1.len()
                || compact
                    .reads2
                    .as_ref()
                    .is_some_and(|b| positions[1] != b.len())
            {
                return Err(Failure::input("[input] inference batch framing count mismatch"));
            }
            batches.push(ReplayBatch {
                batch: compact,
                raw_reservation,
            });
            runtime.event("inference_preparation", false);
        }
        if sampled == target {
            end_reason = if gp.native_limit_units.is_some_and(|n| n <= requested) {
                "job-prefix"
            } else {
                "target"
            };
        }
        let _permit = cpu.acquire(Stage::Source);
        let mut selected: [Option<(String, u64)>; 2] = [None, None];
        for mate in 0..2 {
            if selected_mates[mate] {
                selected[mate] =
                    crate::native_processing::select_inferred(&mut keys[mate], sampled, minimum);
                if selected[mate].is_none() {
                    return Err(Failure::worker(format!(
                        "[worker] inference has no eligible motif for R{}",
                        mate + 1
                    )).because(Reason::NoEligibleMotif));
                }
            }
        }
        let process = Arc::get_mut(&mut gp.native_process).unwrap();
        for mate in 0..2 {
            if let Some((motif, _)) = &selected[mate] {
                process.adapters[mate] = vec![motif.clone()];
                process.adapter_names[mate] = vec![format!("inferred.r{}", mate + 1)];
            }
        }
        process.compile().map_err(Failure::worker)?;
        gp.native_inference_observed = sampled;
        Ok(selected)
    })();
    source.exchange_native_patch_size(old_patch);
    gv.raw1.gs = gv.raw1.native_qc.as_ref().unwrap().general();
    gv.raw2.gs = gv.raw2.native_qc.as_ref().unwrap().general();
    let status = match &result {
        Ok(_) => "selected",
        Err(e) if e.reason == Reason::NoEligibleMotif => "failed-no-eligible",
        Err(e) if e.category == Category::Input => {
            "failed-input"
        }
        Err(_) => "failed-resource",
    };
    let selected = result.as_ref().ok();
    let motifs: [Option<&str>; 2] = std::array::from_fn(|i| {
        selected.and_then(|s| s[i].as_ref().map(|(motif, _)| motif.as_str()))
    });
    let counts: [Option<u64>; 2] =
        std::array::from_fn(|i| selected.and_then(|s| s[i].as_ref().map(|(_, n)| *n)));
    *runtime.inference.lock().unwrap_or_else(|p| p.into_inner()) = serde_json::json!({"status":status,"requested_units":requested,"effective_target_units":target,"sampled_units":sampled,"returned_units":returned,"sample_end":if status=="failed-input"||status=="failed-resource"{"error"}else{end_reason},"selected_motif":if mode=="both"{None}else{motifs[if mode=="r2"{1}else{0}]},"selected_motifs":motifs,"support_counts":counts,"support_denominator":sampled,"minimum_support":minimum,"raw_qc_cutoff":if result.is_ok(){sampled}else{0},"raw_qc_scope":"complete original units returned and validated during preparation; counted once; unread/read-ahead/invalid units excluded","processing_priority":"preparation input errors precede downstream transforms only when inference enabled","reserved_replay_headroom_peak_bytes":replay_headroom,"buffered_raw_capacity_bytes":batches.iter().map(|b|b.batch.reads1.capacity()+b.batch.reads2.as_ref().map_or(0,Vec::capacity)).sum::<usize>(),"index_capacity_bytes":batches.capacity()*std::mem::size_of::<ReplayBatch>(),"key_capacity_bytes":keys.iter().map(|v|v.capacity()*std::mem::size_of::<u128>()).sum::<usize>(),"memory_scope":"simultaneous reservations include growth overlap and reusable preparation/replay headroom; headroom is not allocated payload"});
    result?;
    Ok((batches, index_reservation))
}

fn run_filter_inner(
    gp: &GlobalParams,
    gv: &mut GlobalStats,
    tmp_dir: &str,
    sample_target: Option<(u64, u64)>,
) -> Result<(), Failure> {
    if gp.threads_num == 0 {
        return Err(Failure::write("thread count must be greater than zero"));
    }
    let threads = gp.threads_num;
    let choice = if let Some(io)=&gp.native_io{io.choice(threads)}else{crate::source::decoder_choice(
        threads,
        &gp.fq1_path,
        if !gp.paired() {
            None
        } else {
            Some(&gp.fq2_path)
        },
    ).map_err(Failure::write)?
    };
    eprintln!(
        "input_decoder={:?}\tdecoder_threads_per_lane={}\treserved_cpu_credits={}",
        choice.backend, choice.threads_per_lane, choice.reserved_credits
    );
    let stop = gp.native_io.as_ref().map_or_else(||Arc::new(AtomicBool::new(false)),|io|io.stop.clone());
    let execution = if let Some(io)=&gp.native_io {Some(io.execution.clone())} else if gp.native_m1 {
        Some(crate::native_execution::Execution::with_statistics_budget(
            threads,
            if !gp.paired() { 1 } else { 2 },
            stop.clone(),
            gp.native_memory_total,
            gp.native_process.additional_statistics(
                gp.native_process.capture.enabled()
                    || gp.native_aux.iter().any(|p| !p.is_empty())
                    || !gp.native_match_audit.is_empty(),
            ),
        ).map_err(Failure::from)?)
    } else {
        None
    };
    if let Some(runtime) = &gp.native_runtime {
        if gp.native_process.screen.enabled()
            || gp.native_process.profile == "srna"
            || gp.native_process.overlap.enabled()
            || gp.native_process.screen.enabled()
            || gp.native_process.capture.enabled()
            || gp.native_aux.iter().any(|p| !p.is_empty())
            || !gp.native_match_audit.is_empty()
        {
            runtime.enable_auxiliary();
        }
        runtime.set_execution(execution.clone());
    }
    let mut settings_reservation = None;
    let mut shared = Arc::new(Shared {
        stop,
        execution,
        err: Mutex::new(None),
        gp: Arc::new(gp.clone()),
        cpu: CpuBudget::new_with_reserved(threads, choice.reserved_credits),
    });
    if let Some(e) = &shared.execution {
        if gp.native_dedup.is_some(){e.record_dedup_runtime(shared.cpu.clone(),crate::decoder::Snapshot::empty("Source not opened yet"));}else{        e.record_runtime(
            shared.cpu.clone(),
            serde_json::json!({"lanes":[],"scope":"Source not opened yet"}),
        );}
    }
    let _metrics_guard = MetricsGuard {
        budget: shared.cpu.clone(),
    };

    if gp.native_process.html_report {*gp.native_runtime.as_ref().unwrap().html_buffer.lock().unwrap_or_else(|p|p.into_inner())=Some(shared.execution.as_ref().unwrap().reserve_preparation_feature(8192).map_err(Failure::from)?);}
    if gp.native_process.repetition {
        *gp.native_runtime.as_ref().unwrap().repetition.lock().unwrap_or_else(|p|p.into_inner())=Some(crate::native_repetition::Estimator::new(gp,shared.execution.as_ref().unwrap()).map_err(Failure::from)?);
    }
    if let Some(dedup)=&gp.native_dedup {dedup.prepare(shared.execution.as_ref().unwrap()).map_err(Failure::worker)?;}
    if let Some(demux)=&gp.native_demux {demux.prepare(shared.execution.as_ref().unwrap(),gp.native_io.as_ref().unwrap()).map_err(Failure::worker)?;}
    // Build the file sink before starting lane threads. Streaming has no file
    // destination, so it uses the same ordered-result seam with a stdout sink.
    let sink = if gp.native_dedup.is_some(){OrderedSink::Dedup} else if gp.native_demux.is_some(){OrderedSink::Demux} else if gp.native_io.is_some() && gp.native_process.split_units==0 {
        OrderedSink::Owned
    } else if gp.native_process.split_units > 0 {
        OrderedSink::NativeSplit(output::NativeSplit::new(gp)?)
    } else if gp.native_stdout {
        OrderedSink::NativeStdout
    } else if gp.is_streaming {
        OrderedSink::Streaming
    } else {
        OrderedSink::File(
            Sink::new_with_sample(gp, tmp_dir, sample_target)
                .map_err(|e| format!("[write] {e}")).map_err(Failure::write)?,
        )
    };
    if gp.native_process.screen.enabled() {
        let screen = crate::native_screen::Screen::prepare(
            gp,
            shared.execution.as_ref().unwrap(),
            &shared.cpu,
        ).map_err(Failure::from)?;
        *gp.native_runtime
            .as_ref()
            .unwrap()
            .screen
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(screen);
    }
    let mut source = if gp.native_m1
        && (gp.native_io.is_some()
            || gp.native_interleaved
            || gp.native_limit_units.is_some()
            || gp.native_lane_manifest.is_some()
            || gp.native_process.infer_mode != "off")
    {
        Source::open_layout(
            gp,
            shared.stop.clone(),
            shared.cpu.clone(),
            shared.execution.as_ref().unwrap(),
        )?
    } else if let Some(gzip) = gp.native_stdin_gzip {
        Source::open_native_stream(gzip, gp.patch_size, shared.stop.clone(), shared.cpu.clone())?
    } else if gp.native_m1 {
        Source::open_native(
            &gp.fq1_path,
            if !gp.paired() {
                None
            } else {
                Some(&gp.fq2_path)
            },
            gp.patch_size,
            shared.stop.clone(),
            shared.cpu.clone(),
        )?
    } else if !gp.paired() {
        Source::open_single_with_budget(
            &gp.fq1_path,
            gp.patch_size,
            shared.stop.clone(),
            shared.cpu.clone(),
        )?
    } else {
        Source::open_with_budget(
            &gp.fq1_path,
            &gp.fq2_path,
            gp.patch_size,
            shared.stop.clone(),
            shared.cpu.clone(),
        )?
    };
    let mut replay_batches = Vec::new();
    let mut replay_index = None;
    if gp.native_m1
        && (gp.native_process.infer_mode != "off"
            || gp.native_process.primers.iter().any(|v| !v.is_empty()))
    {
        let execution = shared.execution.as_ref().unwrap().clone();
        let cpu = shared.cpu.clone();
        let prepared = (|| -> Result<_, Failure> {
            settings_reservation = Some(
                execution
                    .reserve_persistent_feature(gp.native_process.runtime_settings_bound().map_err(Failure::worker)?).map_err(Failure::from)?,
            );
            let working = Arc::get_mut(&mut Arc::get_mut(&mut shared).unwrap().gp).unwrap();
            Arc::make_mut(&mut working.native_process);
            prepare_inference(&mut source, working, gv, &execution, &cpu)
        })();
        match prepared {
            Ok((batches, index)) => {
                replay_batches = batches;
                replay_index = index;
            }
            Err(e) => {
                if gp.native_process.infer_mode != "off" {
                    let runtime = gp.native_runtime.as_ref().unwrap();
                    let mut state = runtime.inference.lock().unwrap_or_else(|p| p.into_inner());
                    if state.is_null() {
                        *state = serde_json::json!({"status":"failed-resource","requested_units":gp.native_process.infer_units,"effective_target_units":(gp.native_process.infer_units as u64).min(gp.native_limit_units.unwrap_or(u64::MAX)),"sampled_units":0,"returned_units":0,"sample_end":"error","selected_motif":null,"selected_motifs":[null,null],"support_denominator":0,"raw_qc_cutoff":0});
                    }
                }
                record_source_runtime(gp,&execution,cpu.clone(),&source);
                let _ = source.cancel();
                return Err(e);
            }
        }
    }
    let _replay_index = replay_index;
    let mut replay_batches = replay_batches.into_iter();
    let gp = shared.gp.as_ref();
    if let Some(e) = &shared.execution {
        record_source_runtime(gp,e,shared.cpu.clone(),&source);
    }
    let (batch0_tx, batch0_rx) = sync_channel::<BatchWork>(1);
    let (batch_tx, batch_rx) = sync_channel::<BatchWork>(threads * 2);
    let batch_rx = Arc::new(Mutex::new(batch_rx));
    let (order_tx_main, order_rx) = sync_channel::<(u64, MemberSet)>(threads);
    // P5 (record降维): PeLocalStats is ~0.5MB; send a heap pointer over the
    // channel instead of copying the struct by value (which overflowed the
    // 2MiB worker stacks and wasted bandwidth).
    let (stat_tx_main, stat_rx) = sync_channel::<(usize, Box<PeLocalStats>)>(threads);
    let b0_done = Arc::new((Mutex::new(false), Condvar::new()));

    // Sink thread: append shards in strict batch order. Workers complete out
    // of order, so hold a reorder buffer and emit only the expected id.
    let sink_gp = gp.clone();
    let sink_shared = shared.clone();
    let sink_panic_shared = sink_shared.clone();
    let sink_handle = std::thread::Builder::new()
        .name("sink".into())
        .spawn(move || -> Result<(), Failure> {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_sink(sink, sink_gp, sink_shared, order_rx)
            })) {
                Ok(result) => result,
                Err(_) => {
                    let error = Failure::worker("sink thread panicked");
                    sink_panic_shared.set_err(error.clone());
                    Err(error)
                }
            }
        })
        .map_err(|e| format!("[worker] sink spawn: {e}")).map_err(Failure::worker)?;

    // Worker threads. Worker 0 owns batch0_rx; the rest have None.
    let mut handles: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(threads);
    let mut b0_rx_opt = Some(batch0_rx);
    for i in 0..threads {
        let shared = shared.clone();
        let batch0_rx: Option<Receiver<BatchWork>> = if i == 0 { b0_rx_opt.take() } else { None };
        let worker_batch_rx = batch_rx.clone();
        let worker_order_tx = order_tx_main.clone();
        let worker_stat_tx = stat_tx_main.clone();
        let worker_b0_done = b0_done.clone();
        let worker_gp = shared.gp.clone();
        let panic_shared = shared.clone();
        let panic_b0_done = worker_b0_done.clone();
        #[cfg(test)]
        let force_spawn_failure =
            i == 1 && std::env::var_os("SOAPNUKE_TEST_FAIL_WORKER_SPAWN").is_some();
        #[cfg(not(test))]
        let force_spawn_failure = false;
        if force_spawn_failure {
            let error = Failure::worker("injected worker thread creation failure");
            shared.set_err(error.clone());
            signal_batch0(&b0_done);
            drop(batch0_tx);
            drop(batch_tx);
            drop(order_tx_main);
            drop(stat_tx_main);
            drop(worker_order_tx);
            drop(worker_stat_tx);
            let _ = source.cancel();
            for handle in handles {
                let _ = handle.join();
            }
            let _ = sink_handle.join();
            return Err(error);
        }
        let spawn_shared = shared.clone();
        let handle = match std::thread::Builder::new()
            .name(format!("worker{}", i))
            .spawn(move || {
                let worker_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // Statistics matrices and the compressor allocate
                    // substantial state. Account for both initializations so
                    // they cannot overlap with source or sink compute under
                    // the user's total `-T` budget.
                    let mut local = {
                        let _permit = match shared.compute(Stage::Worker) {
                            Ok(p) => p,
                            Err(_) => {
                                signal_batch0(&worker_b0_done);
                                return;
                            }
                        };
                        Box::new(PeLocalStats::new(&worker_gp))
                    };
                    // One compressor belongs to this persistent worker. It
                    // is reused sequentially for all outputs in a batch;
                    // batch-level workers provide the parallelism.
                    let comp = {
                        let _permit = match shared.compute(Stage::Worker) {
                            Ok(p) => p,
                            Err(_) => {
                                signal_batch0(&worker_b0_done);
                                return;
                            }
                        };
                        match Compressor::new(if worker_gp.native_m1 {
                            worker_gp.native_gzip_level
                        } else {
                            2
                        }) {
                            Ok(c) => c,
                            Err(e) => {
                                shared.set_err(Failure::write(e));
                                signal_batch0(&worker_b0_done);
                                return;
                            }
                        }
                    };
                    #[cfg(test)]
                    if i == 0 && std::env::var_os("SOAPNUKE_TEST_PANIC_WORKER").is_some() {
                        panic!("injected worker panic");
                    }
                    if let Some(rx) = batch0_rx {
                        match rx.recv() {
                            Ok(work) => {
                                let id = work.id;
                                let result = {
                                    match shared.compute(Stage::Worker) {
                                        Ok(_permit) => process_worker_batch(
                                            work, &worker_gp, &mut local, &comp, true,
                                        ),
                                        Err(e) => Err(e),
                                    }
                                };
                                match result {
                                    Ok(members) => {
                                        #[cfg(test)]
                                        test_completion_control(id);
                                        let wait_started = std::time::Instant::now();
                                        if let Err(error) = send_with_backpressure(
                                            &worker_order_tx,
                                            (id, members),
                                            &shared.stop,
                                            "order",
                                        ) {
                                            if !shared.stop.load(Ordering::Relaxed) {
                                                shared.set_err(error);
                                            }
                                        }
                                        shared.cpu.record_observed_queue_wait(
                                            wait_started.elapsed().as_nanos() as u64,
                                        );
                                    }
                                    Err(e) => shared.set_err(e),
                                }
                            }
                            Err(_) => {}
                        }
                        // Only worker 0 owns the first-batch gate. Other
                        // workers must not release one another before batch 0
                        // has completed.
                        signal_batch0(&worker_b0_done);
                    }
                    if i != 0 {
                        let (lock, cv) = &*worker_b0_done;
                        let mut done = lock.lock().unwrap_or_else(|p| p.into_inner());
                        while !*done {
                            if shared.stop.load(Ordering::Relaxed) {
                                break;
                            }
                            done = cv.wait(done).unwrap_or_else(|p| p.into_inner());
                        }
                    }
                    loop {
                        if shared.stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let input_wait_started = std::time::Instant::now();
                        let received = {
                            let rx = match worker_batch_rx.lock() {
                                Ok(g) => g,
                                Err(p) => p.into_inner(),
                            };
                            rx.recv()
                        };
                        shared.cpu.record_worker_input_wait(
                            input_wait_started.elapsed().as_nanos() as u64,
                        );
                        match received {
                            Ok(work) => {
                                let id = work.id;
                                let result = {
                                    match shared.compute(Stage::Worker) {
                                        Ok(_permit) => process_worker_batch(
                                            work, &worker_gp, &mut local, &comp, false,
                                        ),
                                        Err(e) => Err(e),
                                    }
                                };
                                match result {
                                    Ok(members) => {
                                        #[cfg(test)]
                                        test_completion_control(id);
                                        let wait_started = std::time::Instant::now();
                                        if let Err(error) = send_with_backpressure(
                                            &worker_order_tx,
                                            (id, members),
                                            &shared.stop,
                                            "order",
                                        ) {
                                            if !shared.stop.load(Ordering::Relaxed) {
                                                shared.set_err(error);
                                            }
                                            break;
                                        }
                                        shared.cpu.record_observed_queue_wait(
                                            wait_started.elapsed().as_nanos() as u64,
                                        );
                                    }
                                    Err(e) => {
                                        shared.set_err(e);
                                        break;
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    let _ = worker_stat_tx.send((i, local));
                }));
                if worker_result.is_err() {
                    let error = Failure::worker(format!("worker {i} thread panicked"));
                    panic_shared.set_err(error);
                    signal_batch0(&panic_b0_done);
                }
                // Keep the sender alive until panic reporting has set the
                // shared error. Otherwise the sink can observe a closed
                // order channel and publish empty outputs before the
                // worker's catch_unwind branch records cancellation.
                drop(worker_order_tx);
            }) {
            Ok(handle) => handle,
            Err(error) => {
                let error = Failure::worker(format!("cannot create worker thread {}: {}", i, error));
                spawn_shared.set_err(error.clone());
                signal_batch0(&b0_done);
                drop(batch0_tx);
                drop(batch_tx);
                drop(order_tx_main);
                drop(stat_tx_main);
                let _ = source.cancel();
                for handle in handles {
                    let _ = handle.join();
                }
                let _ = sink_handle.join();
                return Err(error);
            }
        };
        handles.push(handle);
    }
    drop(batch_rx);
    drop(stat_tx_main);
    drop(order_tx_main);

    // Producer on the calling thread.
    let mut first = true;
    loop {
        if shared.stop.load(Ordering::Relaxed) {
            break;
        }
        let retained = replay_batches.next();
        let (next, replay_raw) = match retained {
            Some(ReplayBatch {
                batch,
                raw_reservation,
            }) => (Ok(Some(batch)), Some(raw_reservation)),
            None => (source.next_batch(), None),
        };
        if gp.native_m1
            && (gp.native_io.is_some()
                || gp.native_interleaved
                || gp.native_limit_units.is_some()
                || gp.native_lane_manifest.is_some())
        {
            record_source_runtime(gp,shared.execution.as_ref().unwrap(),shared.cpu.clone(),&source);
        }
        match next {
            Ok(Some(batch)) => {
                let Some(raw_capacity) = batch
                    .reads1
                    .capacity()
                    .checked_add(batch.reads2.as_ref().map_or(0, Vec::capacity)).and_then(|n|n.checked_add(batch.demux.as_ref().map_or(0,|d|d.indices.iter().map(Vec::capacity).sum())))
                else {
                    shared.set_err(Failure::worker("[worker] raw buffer capacity overflow"));
                    break;
                };
                let feature = match if gp.native_dedup.is_some(){crate::dedup_runtime::Runtime::feature_bound(batch.reads1.len()+batch.reads2.as_ref().map_or(0,Vec::len),batch.record_count)}else if gp.native_demux.is_some(){crate::demux_runtime::Runtime::feature_bound(batch.reads1.len()+batch.reads2.as_ref().map_or(0,Vec::len)+batch.demux.as_ref().map_or(0,|d|d.indices.iter().map(Vec::len).sum()),batch.record_count)}else{gp.native_process.feature_chunk_bound(
                    batch.record_count,
                    batch.reads1.len() + batch.reads2.as_ref().map_or(0, Vec::len) + batch.demux.as_ref().map_or(0,|d|d.indices.iter().map(Vec::len).sum()),
                    gp.native_aux.iter().any(|p| !p.is_empty()),
                    !gp.native_match_audit.is_empty(),
                )} {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        shared.set_err(Failure::worker(error));
                        break;
                    }
                };
                let ticket = match &shared.execution {
                    Some(e) => match e.admit_with_feature(
                        batch.reads1.len() + batch.reads2.as_ref().map_or(0, Vec::len) + batch.demux.as_ref().map_or(0,|d|d.indices.iter().map(Vec::len).sum()),
                        batch.record_count,
                        raw_capacity,
                        feature,
                    ) {
                        Ok(t) => Some(t),
                        Err(error) => {
                            shared.set_err(Failure::from(error));
                            break;
                        }
                    },
                    None => None,
                };
                drop(replay_raw);
                let work = BatchWork {
                    demux:batch.demux,
            lane_index: batch.lane_index,
                    ticket,
                    id: batch.id,
                    first_record_index: batch.first_record_index,
                    record_count: batch.record_count,
                    reads1: batch.reads1,
                    reads2: batch.reads2,
                };
                if first {
                    let wait_started = std::time::Instant::now();
                    if let Err(error) =
                        send_with_backpressure(&batch0_tx, work, &shared.stop, "batch0")
                    {
                        if !shared.stop.load(Ordering::Relaxed) {
                            shared.set_err(error);
                        }
                        break;
                    }
                    shared
                        .cpu
                        .record_observed_queue_wait(wait_started.elapsed().as_nanos() as u64);
                    first = false;
                } else {
                    let wait_started = std::time::Instant::now();
                    if let Err(error) =
                        send_with_backpressure(&batch_tx, work, &shared.stop, "batch")
                    {
                        if !shared.stop.load(Ordering::Relaxed) {
                            shared.set_err(error);
                        }
                        break;
                    }
                    shared
                        .cpu
                        .record_observed_queue_wait(wait_started.elapsed().as_nanos() as u64);
                }
            }
            Ok(None) => break,
            Err(e) => {
                shared.set_err(e.into());
                break;
            }
        }
    }
    drop(batch0_tx);
    drop(batch_tx);

    if let Err(error) = source.finish() {
        shared.set_err(error.into());
    }

    for h in handles {
        if h.join().is_err() {
            shared.set_err(Failure::worker("worker thread panicked"));
            signal_batch0(&b0_done);
        }
    }
    match sink_handle.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => shared.set_err(error),
        Err(_) => shared.set_err(Failure::worker("sink thread panicked")),
    }

    // Merge worker stats into gv (addition/max are order independent).
    let trim_merge = !gp.trim_fq1.is_empty();
    for (_, local) in stat_rx.iter() {
        let _permit = if gp.native_m1 {
            None
        } else {
            Some(shared.cpu.acquire(Stage::Worker))
        };
        if !gp.paired() {
            merge_se_stat(gv, &local, gp, trim_merge);
        } else {
            merge_pe_stat(gv, &local, gp, trim_merge);
        }
    }
    if let Some(e) = shared.take_err() {
        return Err(e);
    }
    // Clean up the file-output staging directory. Streaming has no staging
    // directory because stdout is non-transactional.
    if !tmp_dir.is_empty() {
        let _ = output::remove_tmp_dir(gp, tmp_dir);
    }
    Ok(())
}

fn write_cpu_metrics(budget: &Arc<CpuBudget>) {
    let path = match std::env::var("SOAPNUKE_CPU_METRICS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let metrics = budget.snapshot();
    let mut text = format!(
        "limit\t{}\nreserved\t{}\nactive\t{}\npeak_active\t{}\npermit_wait_events\t{}\npermit_wait_nanos\t{}\nobserved_queue_wait_events\t{}\nobserved_queue_wait_nanos\t{}\nworker_input_wait_events\t{}\nworker_input_wait_nanos\t{}\n",
        metrics.limit,
        metrics.reserved,
        metrics.active,
        metrics.peak_active,
        metrics.permit_wait_events,
        metrics.permit_wait_nanos,
        metrics.observed_queue_wait_events,
        metrics.observed_queue_wait_nanos,
        metrics.worker_input_wait_events,
        metrics.worker_input_wait_nanos,
    );
    for stage in Stage::ALL {
        text.push_str(&format!(
            "stage_cpu_nanos_{}\t{}\n",
            stage.name(),
            metrics.active_cpu_nanos[stage as usize]
        ));
        text.push_str(&format!(
            "stage_wall_nanos_{}\t{}\n",
            stage.name(),
            metrics.active_wall_nanos[stage as usize]
        ));
    }
    let _ = std::fs::write(path, text);
}

#[cfg(test)]
mod cpu_budget_tests {
    use super::{parse_chunk_records, run_filter, run_filter_with_sampling};
    use crate::ffi::Compressor;
    use crate::params::GlobalParams;
    use crate::stats::GlobalStats;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static METRICS_ENV_LOCK: Mutex<()> = Mutex::new(());
    #[test]
    fn audit_rows_follow_source_order_after_out_of_order_completion_and_keep_whole_units() {
        use std::sync::Arc;
        let _lock = METRICS_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _decoder = TestEnvGuard::set("SOAPNUKE_INPUT_DECODER", "zlib");
        let _delay = TestEnvGuard::set("SOAPNUKE_TEST_DELAY_BATCH_ID", "1");
        let _ms = TestEnvGuard::set("SOAPNUKE_TEST_DELAY_MS", "100");
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../.scratch/audit-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let trace = dir.join("completion.txt");
        let _trace = TestEnvGuard::set("SOAPNUKE_TEST_COMPLETION_TRACE", trace.to_str().unwrap());
        let input = dir.join("input.fastq");
        fs::write(
            &input,
            "@u0\nGGGAC\n+\nIIIII\n@u1\nGGGCC\n+\nIIIII\n@u2\nGGGTT\n+\nIIIII\n",
        )
        .unwrap();
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.threads_num = 4;
        gp.patch_size = 1;
        gp.quality_phred = 33;
        gp.output_quality_phred = 33;
        gp.output_file_type = "fastq".into();
        gp.native_gzip_level = 2;
        gp.fq1_path = input.to_string_lossy().into_owned();
        gp.output_dir = dir.to_string_lossy().into_owned();
        gp.clean_fq1 = "out.fastq".into();
        gp.log = dir.join("legacy.log").to_string_lossy().into_owned();
        gp.native_match_audit = "matches.tsv".into();
        gp.native_match_limit = 2;
        gp.native_runtime = Some(Arc::new(crate::native_runtime::Runtime::new(
            0,
            "audit".into(),
        )));
        let mut process = crate::native_processing::ProcessSettings::default();
        process.primers[0] = vec!["GGG".into()];
        process.primer_names[0] = vec!["p5".into()];
        process.max_error = 0.0;
        process.compile_three_prime().unwrap();
        gp.native_process = Arc::new(process);
        let mut stats = GlobalStats::new(&gp);
        run_filter(&gp, &mut stats).unwrap();
        assert_eq!(
            fs::read_to_string(dir.join("matches.tsv")).unwrap(),
            "unit\tmate\tstage\tadapter\tstart\tend\tmismatches\taction\n0\tR1\tadapters\tp5\t0\t3\t0\ttrim\n1\tR1\tadapters\tp5\t0\t3\t0\ttrim\n"
        );
        assert_eq!(
            *gp.native_runtime
                .as_ref()
                .unwrap()
                .match_counts
                .lock()
                .unwrap(),
            [2, 1]
        );
        let order = fs::read_to_string(trace).unwrap();
        assert!(order.find("2\n").unwrap() < order.find("1\n").unwrap());
        assert_eq!(
            gp.native_runtime.as_ref().unwrap().execution_facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        // The same unit's selected mates/ends remain together under a one-unit limit.
        let d = dir.join("whole-unit");
        fs::create_dir(&d).unwrap();
        gp.output_dir = d.to_string_lossy().into_owned();
        gp.native_match_limit = 1;
        gp.native_runtime = Some(Arc::new(crate::native_runtime::Runtime::new(
            0,
            "whole".into(),
        )));
        let mut members = super::MemberSet::default();
        members.native_matches = vec![
            crate::native_processing::MatchedUnit {
                ordinal: 7,
                hits: vec![
                    crate::native_processing::SelectedAdapter {
                        mate: 0,
                        adapter: "p5".into(),
                        start: 0,
                        end: 3,
                        mismatches: 0,
                    },
                    crate::native_processing::SelectedAdapter {
                        mate: 1,
                        adapter: "p3".into(),
                        start: 2,
                        end: 5,
                        mismatches: 0,
                    },
                ],
            },
            crate::native_processing::MatchedUnit {
                ordinal: 8,
                hits: vec![crate::native_processing::SelectedAdapter {
                    mate: 0,
                    adapter: "p5".into(),
                    start: 0,
                    end: 3,
                    mismatches: 0,
                }],
            },
        ];
        super::append_native_diagnostics(
            &gp,
            &members,
            &crate::cpu_budget::CpuBudget::new_with_reserved(1, 0),
        )
        .unwrap();
        let text = fs::read_to_string(d.join("matches.tsv")).unwrap();
        assert!(text.contains("7\tR1") && text.contains("7\tR2") && !text.contains("8\tR1"));
        assert_eq!(
            *gp.native_runtime
                .as_ref()
                .unwrap()
                .match_counts
                .lock()
                .unwrap(),
            [1, 1]
        );
        let blocked = dir.join("blocked");
        fs::create_dir(&blocked).unwrap();
        fs::create_dir(blocked.join("matches.tsv")).unwrap();
        gp.output_dir = blocked.to_string_lossy().into_owned();
        assert!(
            super::append_native_diagnostics(
                &gp,
                &members,
                &crate::cpu_budget::CpuBudget::new_with_reserved(1, 0)
            )
            .unwrap_err()
            .contains("match report open")
        );
    }

    #[test]
    fn inference_pe_qc_cutoff_and_budget_rejection_are_unit_atomic() {
        use std::sync::{Arc, atomic::AtomicBool};
        let _lock = METRICS_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _decoder = TestEnvGuard::set("SOAPNUKE_INPUT_DECODER", "zlib");
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../.scratch/inference-pe-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let a = dir.join("a.fastq");
        let b = dir.join("b.fastq");
        fs::write(
            &a,
            "@u0/1\nTTAGATCG\n+\nIIIIIIII\n@u1/1\nCCAGATCG\n+\nIIIIIIII\n",
        )
        .unwrap();
        fs::write(
            &b,
            "@u0/2\nAACTAGCT\n+\nIIIIIIII\n@u1/2\nGGCTAGCT\n+\nIIIIIIII\n",
        )
        .unwrap();
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.threads_num = 1;
        gp.quality_phred = 33;
        gp.output_quality_phred = 33;
        gp.fq1_path = a.to_string_lossy().into_owned();
        gp.fq2_path = b.to_string_lossy().into_owned();
        gp.patch_size = 2;
        gp.native_runtime = Some(Arc::new(crate::native_runtime::Runtime::new(
            0,
            "pe".into(),
        )));
        let mut process = crate::native_processing::ProcessSettings::default();
        process.infer_mode = "both".into();
        process.infer_units = 2;
        process.infer_min_support = 1.0;
        gp.native_process = Arc::new(process);
        let mut stats = GlobalStats::new(&gp);
        let stop = Arc::new(AtomicBool::new(false));
        let e = crate::native_execution::Execution::new(1, 2, stop.clone()).unwrap();
        let cpu = crate::cpu_budget::CpuBudget::new_with_reserved(1, 0);
        let settings = e
            .reserve_persistent_feature(gp.native_process.runtime_settings_bound().unwrap())
            .unwrap();
        let mut source = crate::source::Source::open_layout(&gp, stop, cpu.clone(), &e).unwrap();
        let (batches, index) =
            super::prepare_inference(&mut source, &mut gp, &mut stats, &e, &cpu).unwrap();
        assert_eq!(stats.raw1.native_qc.as_ref().unwrap().reads, 2);
        assert_eq!(stats.raw2.native_qc.as_ref().unwrap().reads, 2);
        let mut local = crate::stats::PeLocalStats::new(&gp);
        for super::ReplayBatch {
            batch,
            raw_reservation,
        } in batches
        {
            let raw = batch.reads1.len() + batch.reads2.as_ref().unwrap().len();
            let ticket = e
                .admit_with_feature(
                    raw,
                    batch.record_count,
                    raw,
                    gp.native_process
                        .feature_chunk_bound(batch.record_count, raw, false, false)
                        .unwrap(),
                )
                .unwrap();
            drop(raw_reservation);
            let r1 = super::parse_chunk_records_with_capacity(
                &batch.reads1,
                "read1",
                batch.first_record_index,
                true,
                batch.record_count,
            )
            .unwrap();
            let r2 = super::parse_chunk_records_with_capacity(
                batch.reads2.as_ref().unwrap(),
                "read2",
                batch.first_record_index,
                true,
                batch.record_count,
            )
            .unwrap();
            let result = crate::native_kernel::apply(r1, Some(r2), &mut local, &gp).unwrap();
            assert_eq!(
                result
                    .0
                    .iter()
                    .map(|r| r.sequence.as_str())
                    .collect::<Vec<_>>(),
                vec!["TT", "CC"]
            );
            assert_eq!(
                result
                    .1
                    .iter()
                    .map(|r| r.sequence.as_str())
                    .collect::<Vec<_>>(),
                vec!["AA", "GG"]
            );
            drop(ticket);
        }
        assert_eq!(local.raw1.native_qc.as_ref().unwrap().reads, 0);
        assert_eq!(local.raw2.native_qc.as_ref().unwrap().reads, 0);
        assert_eq!(
            gp.native_runtime.as_ref().unwrap().inference_report()["replayed_units"],
            2
        );
        drop(index);
        drop(source);
        drop(settings);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        let input = dir.join("long-header.fastq");
        fs::write(
            &input,
            format!("@{}\nAGATCG\n+\nIIIIII\n", "x".repeat(10000)),
        )
        .unwrap();
        gp.fq1_path = input.to_string_lossy().into_owned();
        gp.fq2_path.clear();
        gp.native_inference_observed = 0;
        let mut process = crate::native_processing::ProcessSettings::default();
        process.infer_mode = "r1".into();
        process.infer_units = 1;
        gp.native_process = Arc::new(process);
        gp.native_runtime = Some(Arc::new(crate::native_runtime::Runtime::new(
            0,
            "budget".into(),
        )));
        let mut stats = GlobalStats::new(&gp);
        let stop = Arc::new(AtomicBool::new(false));
        let base = crate::native_execution::Execution::statistics_reservation(1, 1).unwrap();
        let total = base
            + crate::native_execution::PREFETCH_PER_LANE
            + crate::native_execution::ASSEMBLY_BYTES
            + crate::native_execution::Execution::reservation(0, 0).unwrap()
            + 65536;
        let e = crate::native_execution::Execution::with_budget(1, 1, stop.clone(), Some(total))
            .unwrap();
        let settings = e
            .reserve_persistent_feature(gp.native_process.runtime_settings_bound().unwrap())
            .unwrap();
        let mut source = crate::source::Source::open_layout(&gp, stop, cpu.clone(), &e).unwrap();
        assert!(
            super::prepare_inference(&mut source, &mut gp, &mut stats, &e, &cpu)
                .err()
                .unwrap()
                .contains("fit")
        );
        assert_eq!(stats.raw1.native_qc.as_ref().unwrap().reads, 0);
        assert_eq!(
            gp.native_runtime.as_ref().unwrap().inference_report()["status"],
            "failed-resource"
        );
        source.cancel().unwrap();
        drop(source);
        drop(settings);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
    }

    #[test]
    fn inference_preparation_boundary_raw_qc_replay_and_owned_capacities() {
        use std::sync::{Arc, atomic::AtomicBool};
        let _lock = METRICS_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _decoder = TestEnvGuard::set("SOAPNUKE_INPUT_DECODER", "zlib");
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../.scratch/inference-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let setup = |label: &str, text: &str, units: usize, patch: usize| {
            let dir = root.join(label);
            fs::create_dir(&dir).unwrap();
            let input = dir.join("in.fastq");
            fs::write(&input, text).unwrap();
            let mut gp = GlobalParams::new();
            gp.native_m1 = true;
            gp.threads_num = 1;
            gp.quality_phred = 33;
            gp.output_quality_phred = 33;
            gp.patch_size = patch;
            gp.fq1_path = input.to_string_lossy().into_owned();
            gp.native_runtime = Some(Arc::new(crate::native_runtime::Runtime::new(
                0,
                label.into(),
            )));
            let mut settings = crate::native_processing::ProcessSettings::default();
            settings.infer_mode = "r1".into();
            settings.infer_units = units;
            settings.infer_min_support = 1.0;
            if label.starts_with("tag") {
                settings.capture.labels =
                    vec![crate::native_processing::LabelSpec::parse("U@r1:1:2").unwrap()];
            }
            gp.native_process = Arc::new(settings);
            let stats = GlobalStats::new(&gp);
            let stop = Arc::new(AtomicBool::new(false));
            let e = crate::native_execution::Execution::new(1, 1, stop.clone()).unwrap();
            let cpu = crate::cpu_budget::CpuBudget::new_with_reserved(1, 0);
            let source = crate::source::Source::open_layout(&gp, stop, cpu.clone(), &e).unwrap();
            (gp, stats, source, e, cpu)
        };
        let text = "@u0\nTTAGATCG\n+\nIIIIIIII\n@u1\nCCAGATCG\n+\nIIIIIIII\n";
        let (mut gp, mut stats, mut source, e, cpu) = setup("valid", text, 4, 2);
        let settings = e
            .reserve_persistent_feature(gp.native_process.runtime_settings_bound().unwrap())
            .unwrap();
        let (batches, index) =
            super::prepare_inference(&mut source, &mut gp, &mut stats, &e, &cpu).unwrap();
        assert_eq!(stats.raw1.native_qc.as_ref().unwrap().reads, 2);
        assert_eq!(gp.native_inference_observed, 2);
        assert_eq!(gp.native_process.adapters[0], vec!["AGATCG"]);
        assert_eq!(source.backend_facts()["opened_lane_groups"], 1);
        let concatenated: Vec<u8> = batches
            .iter()
            .flat_map(|b| b.batch.reads1.iter().copied())
            .collect();
        assert_eq!(concatenated, text.as_bytes());
        let mut local = crate::stats::PeLocalStats::new(&gp);
        let mut cleaned = Vec::new();
        for super::ReplayBatch {
            batch,
            raw_reservation,
        } in batches
        {
            assert_eq!(batch.reads1.capacity(), batch.reads1.len());
            let ticket = e
                .admit_with_feature(
                    batch.reads1.len(),
                    batch.record_count,
                    batch.reads1.capacity(),
                    gp.native_process
                        .feature_chunk_bound(batch.record_count, batch.reads1.len(), false, false)
                        .unwrap(),
                )
                .unwrap();
            drop(raw_reservation);
            let reads = super::parse_chunk_records_with_capacity(
                &batch.reads1,
                "read1",
                batch.first_record_index,
                true,
                batch.record_count,
            )
            .unwrap();
            cleaned.extend(
                crate::native_kernel::apply(reads, None, &mut local, &gp)
                    .unwrap()
                    .0,
            );
            drop(ticket);
        }
        assert_eq!(
            cleaned
                .iter()
                .map(|r| r.sequence.as_str())
                .collect::<Vec<_>>(),
            vec!["TT", "CC"]
        );
        assert_eq!(local.raw1.native_qc.as_ref().unwrap().reads, 0);
        assert_eq!(local.clean1.native_qc.as_ref().unwrap().reads, 2);
        assert_eq!(
            gp.native_runtime.as_ref().unwrap().inference_report()["replayed_units"],
            2
        );
        assert!(source.next_batch().unwrap().is_none());
        drop(index);
        drop(source);
        drop(settings);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        let tagged = "@u0 RX:Z:U=TT\nAAAGATCG\n+\nIIIIIIII\n";
        let invalid = "@u1\nCCAGATCG\n+\n IIIIIII\n";
        let valid = "@u1\nCCAGATCG\n+\nIIIIIIII\n";
        for (label, text, units, patch, count, status) in [
            (
                "tag-later-invalid",
                format!("{tagged}{invalid}"),
                2,
                2,
                1,
                "failed-input",
            ),
            (
                "tag-reverse",
                format!("{invalid}{tagged}"),
                2,
                2,
                0,
                "failed-input",
            ),
            (
                "tag-cross-batch",
                format!("{tagged}{valid}@u2\nGGAGATCG\n+\n"),
                3,
                2,
                2,
                "failed-input",
            ),
            (
                "no-motif",
                "@x\nAAAAAA\n+\nIIIIII\n@y\nCCCCCC\n+\nIIIIII\n".into(),
                2,
                2,
                2,
                "failed-no-eligible",
            ),
        ] {
            let (mut gp, mut stats, mut source, e, cpu) = setup(label, &text, units, patch);
            let settings = e
                .reserve_persistent_feature(gp.native_process.runtime_settings_bound().unwrap())
                .unwrap();
            let error = super::prepare_inference(&mut source, &mut gp, &mut stats, &e, &cpu)
                .err()
                .unwrap();
            if status == "failed-input" {
                assert!(!error.contains("tag conflict"));
            } else {
                assert!(error.contains("no eligible motif"));
            }
            let state = gp.native_runtime.as_ref().unwrap().inference_report();
            assert_eq!(state["status"], status);
            assert_eq!(state["sampled_units"], count);
            assert_eq!(state["replayed_units"], 0);
            assert_eq!(state["selected_motif"], serde_json::Value::Null);
            assert_eq!(stats.raw1.native_qc.as_ref().unwrap().reads, count);
            assert_eq!(stats.clean1.native_qc.as_ref().unwrap().reads, 0);
            assert_eq!(stats.fs.native.rejected, 0);
            assert_eq!(stats.fs.native.transformed, 0);
            assert_eq!(stats.fs.native.fallback, 0);
            source.cancel().unwrap();
            drop(source);
            drop(settings);
            assert_eq!(
                e.facts()["memory_budget"]["current_feature_reserved_bytes"],
                0
            );
        }
        // Without inference preparation, the earlier native tag error remains first.
        let (mut gp, _, mut source, e, _) = setup("tag-off", &format!("{tagged}{invalid}"), 2, 2);
        gp.native_inference_observed = 0;
        let batch = source.next_batch().unwrap().unwrap();
        let reads =
            super::parse_chunk_records_with_capacity(&batch.reads1, "read1", 0, true, 2).unwrap();
        let mut local = crate::stats::PeLocalStats::new(&gp);
        assert!(
            crate::native_kernel::apply(reads, None, &mut local, &gp)
                .err()
                .unwrap()
                .contains("tag conflict")
        );
        assert_eq!(local.raw1.native_qc.as_ref().unwrap().reads, 1);
        source.cancel().unwrap();
        drop(source);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
    }

    struct TestEnvGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl TestEnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    fn fixture_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.scratch/tests");
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!(
            "soapnuke-pipeline-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn read_gzip(path: &PathBuf) -> Vec<u8> {
        let mut reader = crate::zlib_reader::ZlibReader::open(path.to_str().unwrap()).unwrap();
        let mut output = Vec::new();
        loop {
            let bytes = reader.available().unwrap().to_vec();
            if bytes.is_empty() {
                break;
            }
            let length = bytes.len();
            output.extend_from_slice(&bytes);
            reader.consume(length);
        }
        output
    }

    fn metric(path: &PathBuf, key: &str) -> u64 {
        let text = fs::read_to_string(path).unwrap();
        text.lines()
            .find_map(|line| {
                let (name, value) = line.split_once('\t')?;
                (name == key).then(|| value.parse().unwrap())
            })
            .unwrap()
    }

    fn fastq_fixture(records: usize, mate: u8) -> Vec<u8> {
        let mut bytes = Vec::new();
        for id in 0..records {
            bytes.extend_from_slice(format!("@read{}/{}\nACGT\n+\nIIII\n", id, mate).as_bytes());
        }
        bytes
    }

    #[test]
    fn parsed_records_preserve_r1_and_r2_pair_positions() {
        let r1 = parse_chunk_records(b"@read/1\nACGT\n+\nIIII\n", "read1", 0).unwrap();
        let r2 = parse_chunk_records(b"@read/2\nACGT\n+\nIIII\n", "read2", 0).unwrap();
        assert_eq!(r1[0].pair_pos, 1);
        assert_eq!(r2[0].pair_pos, 2);
    }

    fn streaming_params(dir: &PathBuf, threads: usize, paired: bool) -> GlobalParams {
        let mut gp = GlobalParams::new();
        gp.fq1_path = dir.join("input-r1.fastq").to_string_lossy().into_owned();
        gp.fq2_path = if paired {
            dir.join("input-r2.fastq").to_string_lossy().into_owned()
        } else {
            String::new()
        };
        gp.clean_fq1 = "stream-r1.fastq".to_string();
        gp.clean_fq2 = if paired {
            "stream-r2.fastq".to_string()
        } else {
            String::new()
        };
        gp.output_dir = dir.to_string_lossy().into_owned();
        gp.patch_size = 1;
        gp.threads_num = threads;
        gp.min_read_length = 1;
        gp.is_streaming = true;
        gp
    }

    fn streaming_pe_fixture() -> (Vec<u8>, Vec<u8>) {
        let mut r1 = Vec::new();
        let mut r2 = Vec::new();
        for id in 0..6 {
            let sequence = if id % 2 == 0 { "ACGT" } else { "NNNN" };
            let quality = if id == 0 { "KKKK" } else { "IIII" };
            r1.extend_from_slice(format!("@read{}/1\n{sequence}\n+\n{quality}\n", id).as_bytes());
            r2.extend_from_slice(format!("@read{}/2\n{sequence}\n+\n{quality}\n", id).as_bytes());
        }
        (r1, r2)
    }

    #[test]
    fn streaming_pipeline_emits_ordered_batches_and_correct_max_quality_stats() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        for threads in [1usize, 8] {
            let dir = fixture_dir(&format!("streaming-success-{threads}"));
            fs::create_dir_all(&dir).unwrap();
            let (r1, r2) = streaming_pe_fixture();
            fs::write(dir.join("input-r1.fastq"), r1).unwrap();
            fs::write(dir.join("input-r2.fastq"), r2).unwrap();
            let capture = dir.join("stdout.txt");
            std::env::set_var("SOAPNUKE_TEST_STREAM_CAPTURE", &capture);
            let gp = streaming_params(&dir, threads, true);
            let mut stats = GlobalStats::new(&gp);
            run_filter(&gp, &mut stats).unwrap();
            std::env::remove_var("SOAPNUKE_TEST_STREAM_CAPTURE");

            let text = fs::read_to_string(&capture).unwrap();
            assert_eq!(text.matches("#Total_statistical_information\n").count(), 6);
            assert!(text.contains(">+\tread0/1\t1\tACGT\tKKKK\n"));
            assert!(text.contains(">+\tread0/2\t2\tACGT\tKKKK\n"));
            assert!(!text.contains(">+\tread1/1\t1\tNNNN\tIIII\n"));
            let first_r1 = text.find(">+\tread0/1\t1\tACGT\tKKKK\n").unwrap();
            let first_r2 = text.find(">+\tread0/2\t2\tACGT\tKKKK\n").unwrap();
            let first_stats = text.find("#Total_statistical_information\n").unwrap();
            assert!(first_r1 < first_r2 && first_r2 < first_stats);

            let quality_marker = "#Raw_Base_quality_value_distribution_by_read_position\n";
            let quality_start = text.find(quality_marker).unwrap() + quality_marker.len();
            let first_quality_row = text[quality_start..].lines().next().unwrap();
            let quality_values: Vec<u64> = first_quality_row
                .split_whitespace()
                .map(|value| value.parse().unwrap())
                .collect();
            assert_eq!(quality_values.len(), gp.quality_bins());
            assert_eq!(quality_values[42], 1);
            let fq2_start = text.find("#Fq2_statistical_information\n").unwrap();
            let fq2_quality_start =
                fq2_start + text[fq2_start..].find(quality_marker).unwrap() + quality_marker.len();
            let fq2_quality_rows: Vec<Vec<u64>> = text[fq2_quality_start..]
                .lines()
                .take(2)
                .map(|row| {
                    row.split_whitespace()
                        .map(|value| value.parse().unwrap())
                        .collect()
                })
                .collect();
            assert_eq!(fq2_quality_rows.len(), 2);
            for row in fq2_quality_rows {
                assert_eq!(row.len(), gp.quality_bins());
                assert_eq!(row[42], 1);
            }
            assert!(!dir.join("stream-r1.fastq").exists());
            assert!(!dir.join("stream-r2.fastq").exists());
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn streaming_completion_can_be_out_of_order_but_stdout_is_monotonic() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let dir = fixture_dir("streaming-completion-skew");
        fs::create_dir_all(&dir).unwrap();
        let (r1, r2) = streaming_pe_fixture();
        fs::write(dir.join("input-r1.fastq"), r1).unwrap();
        fs::write(dir.join("input-r2.fastq"), r2).unwrap();
        let capture = dir.join("stdout.txt");
        let trace = dir.join("completion-order.txt");
        std::env::set_var("SOAPNUKE_TEST_STREAM_CAPTURE", &capture);
        std::env::set_var("SOAPNUKE_TEST_COMPLETION_TRACE", &trace);
        std::env::set_var("SOAPNUKE_TEST_DELAY_BATCH_ID", "1");
        std::env::set_var("SOAPNUKE_TEST_DELAY_MS", "100");
        let gp = streaming_params(&dir, 8, true);
        let mut stats = GlobalStats::new(&gp);
        run_filter(&gp, &mut stats).unwrap();
        std::env::remove_var("SOAPNUKE_TEST_STREAM_CAPTURE");
        std::env::remove_var("SOAPNUKE_TEST_COMPLETION_TRACE");
        std::env::remove_var("SOAPNUKE_TEST_DELAY_BATCH_ID");
        std::env::remove_var("SOAPNUKE_TEST_DELAY_MS");

        let completion_ids: Vec<u64> = fs::read_to_string(&trace)
            .unwrap()
            .lines()
            .map(|line| line.parse().unwrap())
            .collect();
        assert_eq!(completion_ids.len(), 6);
        let batch_one = completion_ids.iter().position(|id| *id == 1).unwrap();
        let batch_two = completion_ids.iter().position(|id| *id == 2).unwrap();
        assert!(
            batch_two < batch_one,
            "completion order was not skewed: {completion_ids:?}"
        );

        let output = fs::read_to_string(&capture).unwrap();
        let record_ids: Vec<&str> = output
            .lines()
            .filter(|line| line.starts_with(">+\t"))
            .map(|line| line.split('\t').nth(1).unwrap())
            .collect();
        assert_eq!(
            record_ids,
            vec![
                "read0/1", "read0/2", "read2/1", "read2/2", "read4/1", "read4/2"
            ]
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn streaming_write_failure_cancels_full_pipeline_without_final_files() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let dir = fixture_dir("streaming-write-failure");
        fs::create_dir_all(&dir).unwrap();
        let (r1, r2) = streaming_pe_fixture();
        fs::write(dir.join("input-r1.fastq"), r1).unwrap();
        fs::write(dir.join("input-r2.fastq"), r2).unwrap();
        let gp = streaming_params(&dir, 8, true);
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        std::env::set_var("SOAPNUKE_TEST_STREAM_WRITE_FAIL", "1");
        let worker_gp = gp.clone();
        let handle = std::thread::spawn(move || {
            let mut stats = GlobalStats::new(&worker_gp);
            result_tx.send(run_filter(&worker_gp, &mut stats)).unwrap();
        });
        let result = result_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("streaming write failure did not cancel the pipeline");
        std::env::remove_var("SOAPNUKE_TEST_STREAM_WRITE_FAIL");
        handle.join().unwrap();
        let error = result.expect_err("injected streaming write failure succeeded");
        assert!(error.contains("injected streaming write failure"));
        assert!(!dir.join("stream-r1.fastq").exists());
        assert!(!dir.join("stream-r2.fastq").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn single_end_plain_and_gzip_inputs_write_correct_fasta_headers() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let compressor = Compressor::new(2).unwrap();
        for (label, input_gzip, output_gzip) in [("plain", false, false), ("gzip", true, true)] {
            let dir = fixture_dir(&format!("se-fasta-{label}"));
            fs::create_dir_all(&dir).unwrap();
            let input = dir.join(if input_gzip {
                "input.fastq.gz"
            } else {
                "input.fastq"
            });
            let output = dir.join(if output_gzip {
                "clean.fasta.gz"
            } else {
                "clean.fasta"
            });
            let fastq = b"@read0\nACGT\n+\nIIII\n@read1\nTGCA\n+\nJJJJ\n";
            fs::write(
                &input,
                if input_gzip {
                    compressor.gzip_compress_alloc(fastq)
                } else {
                    fastq.to_vec()
                },
            )
            .unwrap();
            let mut gp = GlobalParams::new();
            gp.fq1_path = input.to_string_lossy().into_owned();
            gp.clean_fq1 = output.file_name().unwrap().to_string_lossy().into_owned();
            gp.output_dir = dir.to_string_lossy().into_owned();
            gp.output_file_type = "fasta".to_string();
            gp.patch_size = 1;
            gp.threads_num = 1;
            gp.min_read_length = 1;
            let mut stats = GlobalStats::new(&gp);
            run_filter(&gp, &mut stats).unwrap();
            let bytes = if output_gzip {
                read_gzip(&output)
            } else {
                fs::read(&output).unwrap()
            };
            assert_eq!(bytes, b">read0\nACGT\n>read1\nTGCA\n");
            fs::remove_dir_all(dir).unwrap();
        }
    }

    fn single_end_output_fixture() -> (PathBuf, PathBuf) {
        let dir = fixture_dir("se-output-modes");
        fs::create_dir_all(&dir).unwrap();
        let input = dir.join("input.fastq");
        fs::write(
            &input,
            b"@filtered\nNNNN\n+\nIIII\n@kept0\nACGT\n+\nIIII\n@kept1\nTGCA\n+\nIIII\n@kept2\nCCCC\n+\nIIII\n",
        )
        .unwrap();

        (dir, input)
    }

    #[test]
    fn single_end_trim_and_split_use_clean_record_boundaries() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let (dir, input) = single_end_output_fixture();
        let split_dir = dir.join("split");
        fs::create_dir_all(&split_dir).unwrap();
        let mut split_gp = GlobalParams::new();
        split_gp.fq1_path = input.to_string_lossy().into_owned();
        split_gp.clean_fq1 = "clean.fastq".to_string();
        split_gp.trim_fq1 = "trim.fastq".to_string();
        split_gp.adapter_discard_or_trim = "trim".to_string();
        split_gp.output_dir = split_dir.to_string_lossy().into_owned();
        split_gp.clean_out_split = 2;
        split_gp.patch_size = 1;
        split_gp.threads_num = 1;
        split_gp.min_read_length = 1;
        let mut split_stats = Box::new(GlobalStats::new(&split_gp));
        run_filter(&split_gp, &mut split_stats).unwrap();
        let split0 = fs::read(split_dir.join("split.0.clean.fastq")).unwrap();
        let split1 = fs::read(split_dir.join("split.1.clean.fastq")).unwrap();
        assert_eq!(split0.iter().filter(|byte| **byte == b'@').count(), 2);
        assert_eq!(split1.iter().filter(|byte| **byte == b'@').count(), 1);
        assert_eq!(
            fs::read(split_dir.join("trim.fastq"))
                .unwrap()
                .iter()
                .filter(|byte| **byte == b'@')
                .count(),
            4
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn single_end_head_uses_clean_record_boundaries() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let (dir, input) = single_end_output_fixture();
        let head_dir = dir.join("head");
        fs::create_dir_all(&head_dir).unwrap();
        let mut head_gp = GlobalParams::new();
        head_gp.fq1_path = input.to_string_lossy().into_owned();
        head_gp.clean_fq1 = "clean.fastq".to_string();
        head_gp.output_dir = head_dir.to_string_lossy().into_owned();
        head_gp.total_reads_num = 2.0;
        head_gp.l_total_reads_num = 2;
        head_gp.total_reads_num_random = false;
        head_gp.patch_size = 1;
        head_gp.threads_num = 1;
        head_gp.min_read_length = 1;
        let mut head_stats = Box::new(GlobalStats::new(&head_gp));
        run_filter(&head_gp, &mut head_stats).unwrap();
        let head = fs::read(head_dir.join("clean.fastq")).unwrap();
        assert_eq!(head, b"@kept0\nACGT\n+\nIIII\n@kept1\nTGCA\n+\nIIII\n");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn exact_sampling_preserves_order_and_paired_mates_for_plain_and_gzip_outputs() {
        std::thread::Builder::new()
            .name("exact-sampling-fixture".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
                let compressor = Compressor::new(2).unwrap();
                for (input_gzip, output_gzip) in [(false, false), (true, true)] {
                    let dir = fixture_dir(if input_gzip {
                        "exact-sampling-gzip"
                    } else {
                        "exact-sampling-plain"
                    });
                    fs::create_dir_all(&dir).unwrap();
                    let r1 = dir.join(if input_gzip {
                        "input-r1.fastq.gz"
                    } else {
                        "input-r1.fastq"
                    });
                    let r2 = dir.join(if input_gzip {
                        "input-r2.fastq.gz"
                    } else {
                        "input-r2.fastq"
                    });
                    let full1 = dir.join(if output_gzip {
                        "total-clean-r1.fastq.gz"
                    } else {
                        "total-clean-r1.fastq"
                    });
                    let full2 = dir.join(if output_gzip {
                        "total-clean-r2.fastq.gz"
                    } else {
                        "total-clean-r2.fastq"
                    });
                    let requested1 = dir.join(if output_gzip {
                        "clean-r1.fastq.gz"
                    } else {
                        "clean-r1.fastq"
                    });
                    let requested2 = dir.join(if output_gzip {
                        "clean-r2.fastq.gz"
                    } else {
                        "clean-r2.fastq"
                    });
                    let mut fastq1 = Vec::new();
                    let mut fastq2 = Vec::new();
                    for id in 0..10 {
                        fastq1.extend_from_slice(
                            format!("@read{}/1\nACGT\n+\nIIII\n", id).as_bytes(),
                        );
                        fastq2.extend_from_slice(
                            format!("@read{}/2\nTGCA\n+\nIIII\n", id).as_bytes(),
                        );
                    }
                    if input_gzip {
                        fs::write(&r1, compressor.gzip_compress_alloc(&fastq1)).unwrap();
                        fs::write(&r2, compressor.gzip_compress_alloc(&fastq2)).unwrap();
                    } else {
                        fs::write(&r1, &fastq1).unwrap();
                        fs::write(&r2, &fastq2).unwrap();
                    }

                    let mut total_gp = GlobalParams::new();
                    total_gp.fq1_path = r1.to_string_lossy().into_owned();
                    total_gp.fq2_path = r2.to_string_lossy().into_owned();
                    total_gp.clean_fq1 = full1.file_name().unwrap().to_string_lossy().into_owned();
                    total_gp.clean_fq2 = full2.file_name().unwrap().to_string_lossy().into_owned();
                    total_gp.output_dir = dir.to_string_lossy().into_owned();
                    total_gp.patch_size = 2;
                    total_gp.threads_num = 8;
                    total_gp.min_read_length = 1;
                    total_gp.max_read_length = 4;
                    let mut total_stats = Box::new(GlobalStats::new(&total_gp));
                    run_filter(&total_gp, &mut total_stats).unwrap();
                    assert_eq!(total_stats.clean1.gs.reads_number, 10);

                    let mut sample_gp = total_gp.clone();
                    sample_gp.clean_fq1 = requested1
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned();
                    sample_gp.clean_fq2 = requested2
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned();
                    let mut sample_stats = Box::new(GlobalStats::new(&sample_gp));
                    run_filter_with_sampling(&sample_gp, &mut sample_stats, Some((3, 10))).unwrap();

                    let read = |path: &PathBuf| {
                        if output_gzip {
                            read_gzip(path)
                        } else {
                            fs::read(path).unwrap()
                        }
                    };
                    assert_eq!(read(&full1), fastq1);
                    assert_eq!(read(&full2), fastq2);
                    let selected1 = read(&requested1);
                    let selected2 = read(&requested2);
                    assert_eq!(selected1.iter().filter(|byte| **byte == b'@').count(), 3);
                    assert_eq!(selected2.iter().filter(|byte| **byte == b'@').count(), 3);
                    let ids1: Vec<&[u8]> = selected1
                        .split(|byte| *byte == b'\n')
                        .filter(|line| !line.is_empty())
                        .step_by(4)
                        .collect();
                    let ids2: Vec<&[u8]> = selected2
                        .split(|byte| *byte == b'\n')
                        .filter(|line| !line.is_empty())
                        .step_by(4)
                        .collect();
                    assert_eq!(ids1.len(), 3);
                    assert_eq!(ids1.len(), ids2.len());
                    for (id1, id2) in ids1.iter().zip(ids2.iter()) {
                        assert_eq!(id1.strip_suffix(b"/1"), id2.strip_suffix(b"/2"));
                    }

                    let sample_t1_1 = dir.join(if output_gzip {
                        "clean-t1-r1.fastq.gz"
                    } else {
                        "clean-t1-r1.fastq"
                    });
                    let sample_t1_2 = dir.join(if output_gzip {
                        "clean-t1-r2.fastq.gz"
                    } else {
                        "clean-t1-r2.fastq"
                    });
                    let mut sample_t1_gp = sample_gp.clone();
                    sample_t1_gp.clean_fq1 = sample_t1_1
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned();
                    sample_t1_gp.clean_fq2 = sample_t1_2
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned();
                    sample_t1_gp.threads_num = 1;
                    let mut sample_t1_stats = Box::new(GlobalStats::new(&sample_t1_gp));
                    run_filter_with_sampling(&sample_t1_gp, &mut sample_t1_stats, Some((3, 10)))
                        .unwrap();
                    assert_eq!(read(&sample_t1_1), selected1);
                    assert_eq!(read(&sample_t1_2), selected2);

                    let zero1 = dir.join(if output_gzip {
                        "zero-r1.fastq.gz"
                    } else {
                        "zero-r1.fastq"
                    });
                    let zero2 = dir.join(if output_gzip {
                        "zero-r2.fastq.gz"
                    } else {
                        "zero-r2.fastq"
                    });
                    let mut zero_gp = sample_gp.clone();
                    zero_gp.clean_fq1 = zero1.file_name().unwrap().to_string_lossy().into_owned();
                    zero_gp.clean_fq2 = zero2.file_name().unwrap().to_string_lossy().into_owned();
                    let mut zero_stats = Box::new(GlobalStats::new(&zero_gp));
                    run_filter_with_sampling(&zero_gp, &mut zero_stats, Some((0, 10))).unwrap();
                    assert!(read(&zero1).is_empty());
                    assert!(read(&zero2).is_empty());

                    let fewer1 = dir.join(if output_gzip {
                        "fewer-r1.fastq.gz"
                    } else {
                        "fewer-r1.fastq"
                    });
                    let fewer2 = dir.join(if output_gzip {
                        "fewer-r2.fastq.gz"
                    } else {
                        "fewer-r2.fastq"
                    });
                    let mut fewer_gp = sample_gp.clone();
                    fewer_gp.clean_fq1 = fewer1.file_name().unwrap().to_string_lossy().into_owned();
                    fewer_gp.clean_fq2 = fewer2.file_name().unwrap().to_string_lossy().into_owned();
                    let mut fewer_stats = Box::new(GlobalStats::new(&fewer_gp));
                    let fewer_error =
                        run_filter_with_sampling(&fewer_gp, &mut fewer_stats, Some((3, 11)))
                            .unwrap_err();
                    assert!(fewer_error.contains("unaccounted"));
                    assert!(!fewer1.exists());
                    assert!(!fewer2.exists());

                    let more1 = dir.join(if output_gzip {
                        "more-r1.fastq.gz"
                    } else {
                        "more-r1.fastq"
                    });
                    let more2 = dir.join(if output_gzip {
                        "more-r2.fastq.gz"
                    } else {
                        "more-r2.fastq"
                    });
                    let mut more_gp = sample_gp.clone();
                    more_gp.clean_fq1 = more1.file_name().unwrap().to_string_lossy().into_owned();
                    more_gp.clean_fq2 = more2.file_name().unwrap().to_string_lossy().into_owned();
                    let mut more_stats = Box::new(GlobalStats::new(&more_gp));
                    let more_error =
                        run_filter_with_sampling(&more_gp, &mut more_stats, Some((3, 9)))
                            .unwrap_err();
                    assert!(more_error.contains("more retained records"));
                    assert!(!more1.exists());
                    assert!(!more2.exists());

                    fs::remove_dir_all(dir).unwrap();
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn exact_sampling_supports_single_end_and_keeps_full_report() {
        std::thread::Builder::new()
            .name("exact-sampling-se-fixture".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
                let compressor = Compressor::new(2).unwrap();
                for input_gzip in [false, true] {
                    let dir = fixture_dir(if input_gzip {
                        "exact-sampling-se-gzip"
                    } else {
                        "exact-sampling-se-plain"
                    });
                    fs::create_dir_all(&dir).unwrap();
                    let input = dir.join(if input_gzip {
                        "input.fastq.gz"
                    } else {
                        "input.fastq"
                    });
                    let total = dir.join(if input_gzip {
                        "total.clean.fastq.gz"
                    } else {
                        "total.clean.fastq"
                    });
                    let requested = dir.join(if input_gzip {
                        "clean.fastq.gz"
                    } else {
                        "clean.fastq"
                    });
                    let mut fastq = Vec::new();
                    for id in 0..9 {
                        fastq.extend_from_slice(
                            format!("@single{}\nACGT\n+\nIIII\n", id).as_bytes(),
                        );
                    }
                    if input_gzip {
                        fs::write(&input, compressor.gzip_compress_alloc(&fastq)).unwrap();
                    } else {
                        fs::write(&input, &fastq).unwrap();
                    }
                    let mut total_gp = GlobalParams::new();
                    total_gp.fq1_path = input.to_string_lossy().into_owned();
                    total_gp.clean_fq1 = total.file_name().unwrap().to_string_lossy().into_owned();
                    total_gp.output_dir = dir.to_string_lossy().into_owned();
                    total_gp.patch_size = 2;
                    total_gp.threads_num = 8;
                    total_gp.min_read_length = 1;
                    total_gp.max_read_length = 4;
                    let mut total_stats = Box::new(GlobalStats::new(&total_gp));
                    run_filter(&total_gp, &mut total_stats).unwrap();
                    assert_eq!(total_stats.clean1.gs.reads_number, 9);

                    let mut sample_gp = total_gp.clone();
                    sample_gp.clean_fq1 = requested
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned();
                    let mut sample_stats = Box::new(GlobalStats::new(&sample_gp));
                    run_filter_with_sampling(&sample_gp, &mut sample_stats, Some((4, 9))).unwrap();
                    let selected = if input_gzip {
                        read_gzip(&requested)
                    } else {
                        fs::read(&requested).unwrap()
                    };
                    assert_eq!(selected.iter().filter(|byte| **byte == b'@').count(), 4);
                    assert_eq!(
                        if input_gzip {
                            read_gzip(&total)
                        } else {
                            fs::read(&total).unwrap()
                        },
                        fastq
                    );
                    assert_eq!(total_stats.raw1.gs.reads_number, 9);
                    assert_eq!(total_stats.clean1.gs.reads_number, 9);
                    fs::remove_dir_all(dir).unwrap();
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn pipeline_cpu_budget_is_bounded_and_observable_on_success_and_failure() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let compressor = Compressor::new(2).unwrap();
        for threads in [1usize, 2, 8] {
            let dir = fixture_dir(&format!("budget-success-{threads}"));
            fs::create_dir_all(&dir).unwrap();
            let r1 = dir.join("input-r1.fastq.gz");
            let r2 = dir.join("input-r2.fastq.gz");
            let clean1 = dir.join("clean-r1.fastq.gz");
            let clean2 = dir.join("clean-r2.fastq.gz");
            let metrics = dir.join("metrics.tsv");
            fs::write(&r1, compressor.gzip_compress_alloc(&fastq_fixture(32, 1))).unwrap();
            fs::write(&r2, compressor.gzip_compress_alloc(&fastq_fixture(32, 2))).unwrap();

            let mut gp = GlobalParams::new();
            gp.fq1_path = r1.to_string_lossy().into_owned();
            gp.fq2_path = r2.to_string_lossy().into_owned();
            gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
            gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
            gp.output_dir = dir.to_string_lossy().into_owned();
            gp.patch_size = 1;
            gp.threads_num = threads;
            gp.min_read_length = 1;
            std::env::set_var("SOAPNUKE_CPU_METRICS", &metrics);
            let mut stats = GlobalStats::new(&gp);
            run_filter(&gp, &mut stats).unwrap();
            std::env::remove_var("SOAPNUKE_CPU_METRICS");

            assert_eq!(metric(&metrics, "limit"), threads as u64);
            assert_eq!(metric(&metrics, "active"), 0);
            assert!(metric(&metrics, "peak_active") <= threads as u64);
            assert!(metric(&metrics, "stage_cpu_nanos_source") > 0);
            assert!(metric(&metrics, "stage_cpu_nanos_worker") > 0);
            assert!(metric(&metrics, "observed_queue_wait_events") > 0);
            assert_eq!(read_gzip(&clean1), read_gzip(&r1));
            assert_eq!(read_gzip(&clean2), read_gzip(&r2));
            fs::remove_dir_all(dir).unwrap();

            let dir = fixture_dir(&format!("budget-failure-{threads}"));
            fs::create_dir_all(&dir).unwrap();
            let r1 = dir.join("input-r1.fastq.gz");
            let r2 = dir.join("input-r2.fastq.gz");
            let clean1 = dir.join("clean-r1.fastq.gz");
            let clean2 = dir.join("clean-r2.fastq.gz");
            let metrics = dir.join("metrics.tsv");
            fs::write(&r1, compressor.gzip_compress_alloc(&fastq_fixture(32, 1))).unwrap();
            let mut bad_r2 = compressor.gzip_compress_alloc(&fastq_fixture(32, 2));
            bad_r2.truncate(bad_r2.len() - 1);
            fs::write(&r2, bad_r2).unwrap();

            let mut gp = GlobalParams::new();
            gp.fq1_path = r1.to_string_lossy().into_owned();
            gp.fq2_path = r2.to_string_lossy().into_owned();
            gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
            gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
            gp.output_dir = dir.to_string_lossy().into_owned();
            gp.patch_size = 1;
            gp.threads_num = threads;
            gp.min_read_length = 1;
            std::env::set_var("SOAPNUKE_CPU_METRICS", &metrics);
            let mut stats = GlobalStats::new(&gp);
            assert!(run_filter(&gp, &mut stats).is_err());
            std::env::remove_var("SOAPNUKE_CPU_METRICS");

            assert_eq!(metric(&metrics, "limit"), threads as u64);
            assert_eq!(metric(&metrics, "active"), 0);
            assert!(metric(&metrics, "peak_active") <= threads as u64);
            assert!(!clean1.exists());
            assert!(!clean2.exists());
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn accepts_complete_final_fastq_record_without_newline_for_pe_and_se() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        for paired in [true, false] {
            let dir = fixture_dir(if paired {
                "final-record-without-newline-pe"
            } else {
                "final-record-without-newline-se"
            });
            fs::create_dir_all(&dir).unwrap();
            let r1 = dir.join("input-r1.fastq");
            let r2 = dir.join("input-r2.fastq");
            let clean1 = dir.join("clean-r1.fastq.gz");
            let clean2 = dir.join("clean-r2.fastq.gz");
            fs::write(&r1, b"@read0/1\nACGT\n+\nIIII").unwrap();
            if paired {
                fs::write(&r2, b"@read0/2\nACGT\n+\nIIII").unwrap();
            }

            let mut gp = GlobalParams::new();
            gp.fq1_path = r1.to_string_lossy().into_owned();
            gp.fq2_path = if paired {
                r2.to_string_lossy().into_owned()
            } else {
                String::new()
            };
            gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
            gp.clean_fq2 = if paired {
                clean2.file_name().unwrap().to_string_lossy().into_owned()
            } else {
                String::new()
            };
            gp.output_dir = dir.to_string_lossy().into_owned();
            gp.patch_size = 1;
            gp.threads_num = 1;
            gp.min_read_length = 1;
            let mut stats = GlobalStats::new(&gp);
            run_filter(&gp, &mut stats).unwrap();
            assert_eq!(read_gzip(&clean1), b"@read0/1\nACGT\n+\nIIII\n");
            if paired {
                assert_eq!(read_gzip(&clean2), b"@read0/2\nACGT\n+\nIIII\n");
            }
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn worker_thread_creation_failure_cancels_without_final_output() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let compressor = Compressor::new(2).unwrap();
        let dir = fixture_dir("worker-thread-creation-failure");
        fs::create_dir_all(&dir).unwrap();
        let r1 = dir.join("input-r1.fastq.gz");
        let r2 = dir.join("input-r2.fastq.gz");
        let clean1 = dir.join("clean-r1.fastq.gz");
        let clean2 = dir.join("clean-r2.fastq.gz");
        fs::write(&r1, compressor.gzip_compress_alloc(&fastq_fixture(64, 1))).unwrap();
        fs::write(&r2, compressor.gzip_compress_alloc(&fastq_fixture(64, 2))).unwrap();
        let mut gp = GlobalParams::new();
        gp.fq1_path = r1.to_string_lossy().into_owned();
        gp.fq2_path = r2.to_string_lossy().into_owned();
        gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
        gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
        gp.output_dir = dir.to_string_lossy().into_owned();
        gp.patch_size = 1;
        gp.threads_num = 2;
        gp.min_read_length = 1;
        std::env::set_var("SOAPNUKE_TEST_FAIL_WORKER_SPAWN", "1");
        let mut stats = GlobalStats::new(&gp);
        let error = run_filter(&gp, &mut stats)
            .expect_err("thread creation failure unexpectedly succeeded");
        std::env::remove_var("SOAPNUKE_TEST_FAIL_WORKER_SPAWN");
        assert!(
            error.contains("worker thread creation failure"),
            "unexpected error: {error}"
        );
        assert!(!clean1.exists());
        assert!(!clean2.exists());
        let residue = fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().starts_with("TMP-"));
        assert!(
            !residue,
            "temporary output residue remained after spawn failure"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn worker_panic_is_cancelled_and_reported_at_t1_t2_t8() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let compressor = Compressor::new(2).unwrap();
        for threads in [1usize, 2, 8] {
            let dir = fixture_dir(&format!("worker-panic-{threads}"));
            fs::create_dir_all(&dir).unwrap();
            let r1 = dir.join("input-r1.fastq.gz");
            let r2 = dir.join("input-r2.fastq.gz");
            let clean1 = dir.join("clean-r1.fastq.gz");
            let clean2 = dir.join("clean-r2.fastq.gz");
            let metrics = dir.join("metrics.tsv");
            fs::write(&r1, compressor.gzip_compress_alloc(&fastq_fixture(64, 1))).unwrap();
            fs::write(&r2, compressor.gzip_compress_alloc(&fastq_fixture(64, 2))).unwrap();
            let mut gp = GlobalParams::new();
            gp.fq1_path = r1.to_string_lossy().into_owned();
            gp.fq2_path = r2.to_string_lossy().into_owned();
            gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
            gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
            gp.output_dir = dir.to_string_lossy().into_owned();
            gp.patch_size = 1;
            gp.threads_num = threads;
            gp.min_read_length = 1;
            std::env::set_var("SOAPNUKE_TEST_PANIC_WORKER", "1");
            std::env::set_var("SOAPNUKE_CPU_METRICS", &metrics);
            let mut stats = GlobalStats::new(&gp);
            let error =
                run_filter(&gp, &mut stats).expect_err("worker panic unexpectedly succeeded");
            std::env::remove_var("SOAPNUKE_TEST_PANIC_WORKER");
            std::env::remove_var("SOAPNUKE_CPU_METRICS");
            assert!(
                error.contains("worker 0 thread panicked"),
                "unexpected error: {error}"
            );
            assert_eq!(metric(&metrics, "active"), 0);
            assert!(metric(&metrics, "peak_active") <= threads as u64);
            assert!(!clean1.exists());
            assert!(!clean2.exists());
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn sink_panic_is_cancelled_and_reported_at_t1_t2_t8() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let compressor = Compressor::new(2).unwrap();
        for threads in [1usize, 2, 8] {
            let dir = fixture_dir(&format!("sink-panic-{threads}"));
            fs::create_dir_all(&dir).unwrap();
            let r1 = dir.join("input-r1.fastq.gz");
            let r2 = dir.join("input-r2.fastq.gz");
            let clean1 = dir.join("clean-r1.fastq.gz");
            let clean2 = dir.join("clean-r2.fastq.gz");
            let metrics = dir.join("metrics.tsv");
            fs::write(&r1, compressor.gzip_compress_alloc(&fastq_fixture(64, 1))).unwrap();
            fs::write(&r2, compressor.gzip_compress_alloc(&fastq_fixture(64, 2))).unwrap();
            let mut gp = GlobalParams::new();
            gp.fq1_path = r1.to_string_lossy().into_owned();
            gp.fq2_path = r2.to_string_lossy().into_owned();
            gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
            gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
            gp.output_dir = dir.to_string_lossy().into_owned();
            gp.patch_size = 1;
            gp.threads_num = threads;
            gp.min_read_length = 1;
            let error = {
                let _sink_panic_env = TestEnvGuard::set("SOAPNUKE_TEST_PANIC_SINK", "1");
                let _metrics_env = TestEnvGuard::set(
                    "SOAPNUKE_CPU_METRICS",
                    metrics.to_str().expect("metrics path is valid UTF-8"),
                );
                let mut stats = GlobalStats::new(&gp);
                run_filter(&gp, &mut stats).expect_err("sink panic unexpectedly succeeded")
            };
            assert!(
                error.contains("sink thread panicked"),
                "unexpected error: {error}"
            );
            assert_eq!(metric(&metrics, "active"), 0);
            assert!(metric(&metrics, "peak_active") <= threads as u64);
            assert!(!clean1.exists());
            assert!(!clean2.exists());
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn worker_validation_failure_releases_all_compute_permits() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        for threads in [1usize, 2, 8] {
            let dir = fixture_dir(&format!("worker-failure-{threads}"));
            fs::create_dir_all(&dir).unwrap();
            let r1 = dir.join("input-r1.fastq");
            let r2 = dir.join("input-r2.fastq");
            let clean1 = dir.join("clean-r1.fastq.gz");
            let clean2 = dir.join("clean-r2.fastq.gz");
            let metrics = dir.join("metrics.tsv");
            let malformed = b"@read0/1\nACGT\n!\nIIII\n";
            let malformed2 = b"@read0/2\nACGT\n!\nIIII\n";
            fs::write(&r1, malformed).unwrap();
            fs::write(&r2, malformed2).unwrap();

            let mut gp = GlobalParams::new();
            gp.fq1_path = r1.to_string_lossy().into_owned();
            gp.fq2_path = r2.to_string_lossy().into_owned();
            gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
            gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
            gp.output_dir = dir.to_string_lossy().into_owned();
            gp.patch_size = 1;
            gp.threads_num = threads;
            gp.min_read_length = 1;
            std::env::set_var("SOAPNUKE_CPU_METRICS", &metrics);
            let mut stats = GlobalStats::new(&gp);
            let error = run_filter(&gp, &mut stats).expect_err("malformed worker input succeeded");
            std::env::remove_var("SOAPNUKE_CPU_METRICS");

            assert!(error.contains("separator"), "unexpected error: {error}");
            assert_eq!(metric(&metrics, "active"), 0);
            assert!(metric(&metrics, "peak_active") <= threads as u64);
            assert!(!clean1.exists());
            assert!(!clean2.exists());
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn successful_run_promotes_staged_outputs() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let dir = fixture_dir("success");
        fs::create_dir_all(&dir).unwrap();
        let r1 = dir.join("input-r1.fastq");
        let r2 = dir.join("input-r2.fastq");
        let clean1 = dir.join("clean-r1.fastq.gz");
        let clean2 = dir.join("clean-r2.fastq.gz");
        fs::write(&r1, b"@read0/1\nACGT\n+\nIIII\n").unwrap();
        fs::write(&r2, b"@read0/2\nACGT\n+\nIIII\n").unwrap();

        let mut gp = GlobalParams::new();
        gp.fq1_path = r1.to_string_lossy().into_owned();
        gp.fq2_path = r2.to_string_lossy().into_owned();
        gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
        gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
        gp.output_dir = dir.to_string_lossy().into_owned();
        gp.patch_size = 1;
        gp.threads_num = 1;
        gp.min_read_length = 1;

        let mut stats = GlobalStats::new(&gp);
        run_filter(&gp, &mut stats).unwrap();
        assert!(clean1.exists());
        assert!(clean2.exists());
        assert_eq!(read_gzip(&clean1), fs::read(&r1).unwrap());
        assert_eq!(read_gzip(&clean2), fs::read(&r2).unwrap());
        assert!(!fs::read_dir(&dir).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("TMP")
        }));

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn pe_crlf_input_preserves_ids_and_final_record() {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let dir = fixture_dir("pe-crlf");
        fs::create_dir_all(&dir).unwrap();
        let r1 = dir.join("input-r1.fastq");
        let r2 = dir.join("input-r2.fastq");
        let clean1 = dir.join("clean-r1.fastq.gz");
        let clean2 = dir.join("clean-r2.fastq.gz");
        fs::write(
            &r1,
            b"@read0/1\r\nACGT\r\n+\r\nIIII\r\n@read1/1\r\nTGCA\r\n+\r\nJJJJ\r\n",
        )
        .unwrap();
        fs::write(
            &r2,
            b"@read0/2\r\nACGT\r\n+\r\nIIII\r\n@read1/2\r\nTGCA\r\n+\r\nJJJJ\r\n",
        )
        .unwrap();

        let mut gp = GlobalParams::new();
        gp.fq1_path = r1.to_string_lossy().into_owned();
        gp.fq2_path = r2.to_string_lossy().into_owned();
        gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
        gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
        gp.output_dir = dir.to_string_lossy().into_owned();
        gp.patch_size = 1;
        gp.threads_num = 2;
        gp.min_read_length = 1;

        let mut stats = GlobalStats::new(&gp);
        run_filter(&gp, &mut stats).unwrap();
        assert_eq!(
            read_gzip(&clean1),
            b"@read0/1\nACGT\n+\nIIII\n@read1/1\nTGCA\n+\nJJJJ\n"
        );
        assert_eq!(
            read_gzip(&clean2),
            b"@read0/2\nACGT\n+\nIIII\n@read1/2\nTGCA\n+\nJJJJ\n"
        );
        assert_eq!(stats.raw1.gs.reads_number, 2);
        assert_eq!(stats.raw2.gs.reads_number, 2);
        fs::remove_dir_all(dir).unwrap();
    }

    fn malformed_lane_run(bad_r1: bool) {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let dir = fixture_dir(if bad_r1 { "bad-r1" } else { "bad-r2" });
        fs::create_dir_all(&dir).unwrap();
        let r1 = dir.join("input-r1.fastq");
        let r2 = dir.join("input-r2.fastq");
        let clean1 = dir.join("clean-r1.fastq.gz");
        let clean2 = dir.join("clean-r2.fastq.gz");
        let valid1 = b"@read0/1\nACGT\n+\nIIII\n";
        let valid2 = b"@read0/2\nACGT\n+\nIIII\n";
        let malformed = b"@read0/1\nACGT\n+\nIII";
        let input1: &[u8] = if bad_r1 { malformed } else { valid1 };
        let input2: &[u8] = if bad_r1 { valid2 } else { malformed };
        fs::write(&r1, input1).unwrap();
        fs::write(&r2, input2).unwrap();

        let mut gp = GlobalParams::new();
        gp.fq1_path = r1.to_string_lossy().into_owned();
        gp.fq2_path = r2.to_string_lossy().into_owned();
        gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
        gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
        gp.output_dir = dir.to_string_lossy().into_owned();
        gp.patch_size = 1;
        gp.threads_num = 1;
        gp.min_read_length = 1;

        let mut stats = GlobalStats::new(&gp);
        let result = run_filter(&gp, &mut stats);
        assert!(result.is_err(), "malformed input unexpectedly succeeded");
        assert!(
            !clean1.exists(),
            "R1 final output was published after failure"
        );
        assert!(
            !clean2.exists(),
            "R2 final output was published after failure"
        );

        fs::remove_dir_all(dir).unwrap();
    }

    fn malformed_gzip_lane_run(bad_r1: bool) {
        let _metrics_env_guard = METRICS_ENV_LOCK.lock().unwrap();
        let dir = fixture_dir(if bad_r1 { "bad-gzip-r1" } else { "bad-gzip-r2" });
        fs::create_dir_all(&dir).unwrap();
        let r1 = dir.join("input-r1.fastq.gz");
        let r2 = dir.join("input-r2.fastq.gz");
        let clean1 = dir.join("clean-r1.fastq.gz");
        let clean2 = dir.join("clean-r2.fastq.gz");
        let valid1 = b"@read0/1\nACGT\n+\nIIII\n";
        let valid2 = b"@read0/2\nACGT\n+\nIIII\n";
        let compressor = Compressor::new(2).unwrap();
        let valid_gzip1 = compressor.gzip_compress_alloc(valid1);
        let valid_gzip2 = compressor.gzip_compress_alloc(valid2);
        let mut corrupt_gzip = if bad_r1 {
            valid_gzip1.clone()
        } else {
            valid_gzip2.clone()
        };
        let trailer = corrupt_gzip.len() - 8;
        corrupt_gzip[trailer] ^= 1;
        fs::write(&r1, if bad_r1 { &corrupt_gzip } else { &valid_gzip1 }).unwrap();
        fs::write(&r2, if bad_r1 { &valid_gzip2 } else { &corrupt_gzip }).unwrap();

        let mut gp = GlobalParams::new();
        gp.fq1_path = r1.to_string_lossy().into_owned();
        gp.fq2_path = r2.to_string_lossy().into_owned();
        gp.clean_fq1 = clean1.file_name().unwrap().to_string_lossy().into_owned();
        gp.clean_fq2 = clean2.file_name().unwrap().to_string_lossy().into_owned();
        gp.output_dir = dir.to_string_lossy().into_owned();
        gp.patch_size = 1;
        gp.threads_num = 1;
        gp.min_read_length = 1;

        let mut stats = GlobalStats::new(&gp);
        let result = run_filter(&gp, &mut stats);
        assert!(result.is_err(), "corrupt gzip input unexpectedly succeeded");
        assert!(!clean1.exists());
        assert!(!clean2.exists());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn malformed_r1_does_not_finalize_clean_outputs() {
        malformed_lane_run(true);
    }

    #[test]
    fn malformed_r2_does_not_finalize_clean_outputs() {
        malformed_lane_run(false);
    }

    #[test]
    fn malformed_gzip_r1_does_not_finalize_clean_outputs() {
        malformed_gzip_lane_run(true);
    }

    #[test]
    fn malformed_gzip_r2_does_not_finalize_clean_outputs() {
        malformed_gzip_lane_run(false);
    }
}

#[cfg(test)]
pub(crate) fn qualification_parse_at(
    chunk: &[u8],
    label: &str,
    first: u64,
    count: usize,
) -> Result<Vec<FastqRecord>, String> {
    parse_chunk_records_with_capacity(chunk, label, first, true, count)
}

#[cfg(test)]
pub(crate) fn qualification_parse(chunk: &[u8]) -> Result<Vec<FastqRecord>, String> {
    parse_chunk_records_with_capacity(chunk, "qualification", 0, true, 1)
}

fn run_inspection(gp:&GlobalParams, gv:&mut GlobalStats)->Result<(),Failure>{
    let stop=gp.native_io.as_ref().map_or_else(||Arc::new(AtomicBool::new(false)),|io|io.stop.clone());let choice=if let Some(io)=&gp.native_io{io.choice(gp.threads_num)}else{crate::source::decoder_choice(gp.threads_num,&gp.fq1_path,if gp.paired(){Some(&gp.fq2_path)}else{None}).map_err(Failure::write)?};
    let cpu=CpuBudget::new_with_reserved(gp.threads_num,choice.reserved_credits);let execution=if let Some(io)=&gp.native_io{io.execution.clone()}else{crate::native_execution::Execution::with_statistics_budget(gp.threads_num,if gp.paired(){2}else{1},stop.clone(),gp.native_memory_total,gp.native_process.additional_statistics(false)).map_err(Failure::from)?};
    let runtime=gp.native_runtime.as_ref().unwrap();runtime.set_execution(Some(execution.clone()));execution.record_runtime(cpu.clone(),serde_json::json!({"lanes":[],"scope":"inspection source not opened"}));
    if gp.native_process.html_report{*runtime.html_buffer.lock().unwrap_or_else(|p|p.into_inner())=Some(execution.reserve_preparation_feature(8192).map_err(Failure::from)?);}
    if gp.native_process.repetition{*runtime.repetition.lock().unwrap_or_else(|p|p.into_inner())=Some(crate::native_repetition::Estimator::new(gp,&execution).map_err(Failure::from)?);}
    if gp.native_process.screen.enabled(){*runtime.screen.lock().unwrap_or_else(|p|p.into_inner())=Some(crate::native_screen::Screen::prepare(gp,&execution,&cpu).map_err(Failure::from)?);}
    let screen=runtime.screen.lock().unwrap_or_else(|p|p.into_inner()).clone();let mut source=Source::open_layout(gp,stop,cpu.clone(),&execution)?;
    let result=(||->Result<(),Failure>{while let Some(batch)=source.next_batch()?{
        let raw=batch.reads1.len().checked_add(batch.reads2.as_ref().map_or(0,Vec::len)).ok_or_else(||Failure::worker("[worker] inspection raw overflow"))?;
        let capacity=batch.reads1.capacity().checked_add(batch.reads2.as_ref().map_or(0,Vec::capacity)).ok_or_else(||Failure::worker("[worker] inspection capacity overflow"))?;
        let feature=gp.native_process.feature_chunk_bound(batch.record_count,raw,false,false).map_err(Failure::worker)?;let mut ticket=execution.admit_with_feature(raw,batch.record_count,capacity,feature).map_err(Failure::from)?;let _permit=cpu.acquire(Stage::Worker);
        let mut first=parse_chunk_records_with_capacity(&batch.reads1,"read1",batch.first_record_index,true,batch.record_count).map_err(Failure::input)?;
        let mut second=batch.reads2.as_ref().map(|b|parse_chunk_records_with_capacity(b,"read2",batch.first_record_index,true,batch.record_count)).transpose().map_err(Failure::input)?;
        if first.len()!=batch.record_count||second.as_ref().is_some_and(|v|v.len()!=first.len()){return Err(Failure::input("input: paired read counts differ").because(Reason::PairCount));}
        if let Some(second)=&second {for(a,b)in first.iter().zip(second){if !crate::record::pair_ids_match(&a.seq_id,&b.seq_id){return Err(Failure::input("input: paired identifiers differ").because(Reason::PairIdentifiers));}}}
        if let Some(io)=&gp.native_io {let lane=&io.spec.lanes[batch.lane_index.unwrap_or(0)];for r in &mut first{r.input_phred=Some(lane.inputs[0].phred as i32);}if let Some(records)=&mut second{for r in records{r.input_phred=Some(lane.inputs[if gp.native_interleaved{0}else{1}].phred as i32);}}}
        for(i,r1)in first.iter_mut().enumerate(){crate::native_kernel::validate_normalize(r1,gp.quality_phred).map_err(Failure::input)?;let mut r2=second.as_mut().map(|s|&mut s[i]);if let Some(r)=r2.as_deref_mut(){crate::native_kernel::validate_normalize(r,gp.quality_phred).map_err(Failure::input)?;}let r1=&*r1;let r2=r2.as_deref();crate::native_kernel::observe(r1,&mut gv.raw1,gp.quality_phred);if let Some(r)=r2{crate::native_kernel::observe(r,&mut gv.raw2,gp.quality_phred);}if let Some(estimator)=runtime.repetition.lock().unwrap_or_else(|p|p.into_inner()).as_ref(){estimator.observe(r1,r2,Some(&mut ticket)).map_err(Failure::from)?;}if let Some(screen)=&screen {let reads=if let Some(r)=r2{vec![r1,r]}else{vec![r1]};screen.evaluate(r1.source_index,&reads,true).map_err(Failure::input)?;}runtime.processed(1,0);}
    }Ok(())})();let close=source.cancel();execution.record_runtime(cpu,source.backend_facts());gv.raw1.gs=gv.raw1.native_qc.as_ref().unwrap().general();gv.raw2.gs=gv.raw2.native_qc.as_ref().unwrap().general();result.and(close.map_err(Failure::from))
}

fn record_source_runtime(gp:&GlobalParams,e:&Arc<crate::native_execution::Execution>,cpu:Arc<CpuBudget>,source:&Source){if gp.native_dedup.is_some(){e.record_dedup_runtime(cpu,source.dedup_backend_facts());}else{e.record_runtime(cpu,source.backend_facts());}}

#[cfg(test)]
mod typed_cancellation_tests {
    use super::*;
    #[test]
    fn only_typed_synthetic_cancellation_is_suppressed() {
        let shared=Shared{stop:Arc::new(AtomicBool::new(true)),err:Mutex::new(None),
            gp:Arc::new(GlobalParams::new()),cpu:CpuBudget::new(1),execution:None};
        shared.set_err(Failure::synthetic_cancel("new opaque cancellation wording"));
        assert!(shared.take_err().is_none());
        shared.set_err(Failure::input("FASTQ source cancelled"));
        let actual=shared.take_err().unwrap();
        assert_eq!(actual.category,Category::Input);
        assert_eq!(actual.reason,Reason::Ordinary);
        assert_eq!(actual.message,"FASTQ source cancelled");
        shared.set_err(Failure::worker("later failure"));
        assert_eq!(shared.take_err().unwrap(),actual);
    }
}

#[cfg(test)]
mod job_payload_tests {
    use super::*;
    #[test]
    fn completed_job_payload_rejects_mixed_and_wrong_sink_before_output() {
        let mut members=MemberSet::default();
        members.require_job(JobMembers::Process).unwrap();
        assert!(members.require_job(JobMembers::Demux).is_err());
        members.native_demux=Some(Vec::new());members.require_job(JobMembers::Demux).unwrap();
        members.native_dedup=Some(Vec::new());assert!(members.require_job(JobMembers::Demux).is_err());
        members.native_demux=None;members.require_job(JobMembers::Dedup).unwrap();
        members.clean1=Some(Vec::new());assert!(members.require_job(JobMembers::Dedup).is_err());
        let mut sink=OrderedSink::Demux;
        let error=sink.append(&GlobalParams::new(),&mut members,&CpuBudget::new(1)).unwrap_err();
        assert_eq!(error.category,Category::Worker);
        assert_eq!(error.message,"[worker] mixed completed-job payload");
    }
}
