use crate::failure::BudgetError;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub const MIB: usize = 1 << 20;
pub const RECORD_BYTES: usize = MIB;
pub const TARGET_BYTES: usize = MIB;
pub const LANE_TARGET: usize = TARGET_BYTES / 2;
pub const LANE_BYTES: usize = LANE_TARGET + RECORD_BYTES;
pub const CHUNK_UNITS: usize = 4096;
pub const PIPELINE_BYTES: usize = 512 * MIB;
pub const STATS_BYTES: usize = 128 * MIB;
// Per lane: largest Rust reader, framing record, building/queued/carried chunks.
pub const PREFETCH_PER_LANE: usize = 8 * MIB + RECORD_BYTES + 3 * LANE_BYTES;
pub const ASSEMBLY_BYTES: usize = 2 * RECORD_BYTES;
pub const QC_BYTES: usize = MIB;

#[derive(Default)]
struct State {
    active: usize,
    bytes: usize,
    peak_active: usize,
    peak_bytes: usize,
    feature_bytes: usize,
    persistent_feature_bytes: usize,
    peak_feature_bytes: usize,
    peak_total_bytes: usize,
    ready: usize,
    peak_ready: usize,
    admitted: u64,
    retired: u64,
    #[cfg(test)]
    byte_waits: usize,
}
pub struct Execution {
    window: usize,
    available_bytes: usize,
    prefetch: usize,
    stats: usize,
    additional_stats: usize,
    requested_total: Option<usize>,
    pipeline_limit: usize,
    cpu: Mutex<Option<Arc<crate::cpu_budget::CpuBudget>>>,
    backends: Mutex<Value>,
    dedup_backends:Mutex<Option<Box<crate::decoder::Snapshot>>>,
    state: Mutex<State>,
    wake: Condvar,
    stop: Arc<AtomicBool>,
}
pub struct Ticket {
    owner: Arc<Execution>,
    bytes: usize,
    feature_bytes: usize,
    ready: bool,
}
pub struct FeatureTicket {
    owner: Arc<Execution>,
    bytes: usize,
    persistent: bool,
}
impl FeatureTicket {
    pub fn reserved_bytes(&self) -> usize {
        self.bytes
    }
}
impl Drop for FeatureTicket {
    fn drop(&mut self) {
        let mut s = self.owner.state.lock().unwrap_or_else(|p| p.into_inner());
        s.feature_bytes -= self.bytes;
        if self.persistent {
            s.persistent_feature_bytes -= self.bytes;
        }
        self.owner.wake.notify_all();
    }
}
impl Execution {
    pub fn new(threads: usize, lanes: usize, stop: Arc<AtomicBool>) -> Result<Arc<Self>, BudgetError> {
        Self::with_budget(threads, lanes, stop, None)
    }
    pub fn validate_budget(
        threads: usize,
        lanes: usize,
        total: Option<usize>,
    ) -> Result<(usize, usize, usize), BudgetError> {
        Self::validate_budget_with_statistics(threads, lanes, total, 0)
    }
    pub fn validate_budget_with_statistics(
        threads: usize,
        lanes: usize,
        total: Option<usize>,
        additional: usize,
    ) -> Result<(usize, usize, usize), BudgetError> {
        let stats = Self::statistics_reservation(threads, lanes)?
            .checked_add(additional)
            .ok_or("[worker] statistics capacity overflow")?;
        if stats > STATS_BYTES {
            return Err("[worker] additional statistics exceed128MiB".into());
        }
        let prefetch = lanes
            .checked_mul(PREFETCH_PER_LANE)
            .and_then(|n| n.checked_add(ASSEMBLY_BYTES))
            .ok_or("[worker] memory prefetch reservation overflow")?;
        let pipeline = match total {
            Some(b) => b
                .checked_sub(stats)
                .ok_or("[worker] memory budget cannot reserve statistics")?
                .min(PIPELINE_BYTES),
            None => PIPELINE_BYTES,
        };
        let available = pipeline
            .checked_sub(prefetch)
            .ok_or("[worker] memory budget cannot reserve readers")?;
        if available < Self::reservation(0, 0)? {
            return Err("[worker] memory budget cannot reserve a complete chunk".into());
        }
        Ok((stats, prefetch, pipeline))
    }
    pub fn with_budget(
        threads: usize,
        lanes: usize,
        stop: Arc<AtomicBool>,
        total: Option<usize>,
    ) -> Result<Arc<Self>, BudgetError> {
        Self::with_statistics_budget(threads, lanes, stop, total, 0)
    }
    pub fn with_statistics_budget(
        threads: usize,
        lanes: usize,
        stop: Arc<AtomicBool>,
        total: Option<usize>,
        additional: usize,
    ) -> Result<Arc<Self>, BudgetError> {
        let (stats, prefetch, pipeline_limit) =
            Self::validate_budget_with_statistics(threads, lanes, total, additional)?;
        Ok(Arc::new(Self {
            window: threads.saturating_mul(2).max(1),
            available_bytes: pipeline_limit - prefetch,
            prefetch,
            stats,
            additional_stats: additional,
            requested_total: total,
            pipeline_limit,
            cpu: Mutex::new(None),
            dedup_backends:Mutex::new(None),backends: Mutex::new(Value::Null),
            state: Mutex::new(State {
                peak_total_bytes: stats + prefetch,
                ..State::default()
            }),
            wake: Condvar::new(),
            stop,
        }))
    }
    fn update_peaks(&self, s: &mut State) {
        s.peak_feature_bytes = s.peak_feature_bytes.max(s.feature_bytes);
        s.peak_total_bytes = s
            .peak_total_bytes
            .max(self.stats + self.prefetch + s.bytes + s.feature_bytes);
    }
    pub fn reserve_preparation_feature(self:&Arc<Self>,bytes:usize)->Result<FeatureTicket,BudgetError>{self.reserve_idle_feature(bytes,false)}
    // D calls this only after run_filter returned: producer is this caller,
    // every spawned worker/sink was joined and Source/decoder ownership dropped.
    // Counter checks are an additional ownership check, not a lifecycle proof.
    pub(crate) fn reserve_finalization_feature(self:&Arc<Self>,bytes:usize)->Result<FeatureTicket,BudgetError>{self.reserve_idle_feature(bytes,true)}
    fn reserve_idle_feature(self:&Arc<Self>,bytes:usize,finalization:bool)->Result<FeatureTicket,BudgetError>{
        let mut s=self.state.lock().unwrap_or_else(|p|p.into_inner());
        if !finalization && self.stop.load(Ordering::Relaxed){return Err(BudgetError::cancelled("FASTQ pipeline cancelled"));}
        if s.active!=0 || (finalization && (s.ready!=0 || s.bytes!=0)){return Err("[worker] idle feature reservation requires retired work; no blocking reserve permitted".into());}
        let next=s.feature_bytes.checked_add(bytes).ok_or("[worker] preparation feature overflow")?;
        if next.checked_add(s.bytes).is_none_or(|n|n>self.available_bytes){return Err("[worker] repetition preparation cannot fit managed budget".into());}
        s.feature_bytes=next;s.persistent_feature_bytes+=bytes;self.update_peaks(&mut s);
        Ok(FeatureTicket{owner:self.clone(),bytes,persistent:true})
    }
    pub fn reserve_persistent_feature(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<FeatureTicket, BudgetError> {
        self.reserve_feature_kind(bytes, true)
    }
    pub fn reserve_feature(self: &Arc<Self>, bytes: usize) -> Result<FeatureTicket, BudgetError> {
        self.reserve_feature_kind(bytes, false)
    }
    fn reserve_feature_kind(
        self: &Arc<Self>,
        bytes: usize,
        persistent: bool,
    ) -> Result<FeatureTicket, BudgetError> {
        if bytes > self.available_bytes {
            return Err("[worker] feature cannot fit managed budget".into());
        }
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if bytes > self.available_bytes - s.persistent_feature_bytes {
            return Err("[worker] feature cannot fit with persistent reservations".into());
        }
        while s.bytes + s.feature_bytes + bytes > self.available_bytes {
            if bytes > self.available_bytes - s.persistent_feature_bytes {
                return Err("[worker] feature cannot fit with persistent reservations".into());
            }
            #[cfg(test)]
            {
                s.byte_waits += 1;
            }
            if self.stop.load(Ordering::Relaxed) {
                return Err(BudgetError::cancelled("FASTQ pipeline cancelled"));
            }
            s = self
                .wake
                .wait_timeout(s, Duration::from_millis(10))
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        if self.stop.load(Ordering::Relaxed) {
            return Err(BudgetError::cancelled("FASTQ pipeline cancelled"));
        }
        s.feature_bytes += bytes;
        if persistent {
            s.persistent_feature_bytes += bytes;
        }
        self.wake.notify_all();
        self.update_peaks(&mut s);
        Ok(FeatureTicket {
            owner: self.clone(),
            bytes,
            persistent,
        })
    }
    pub fn statistics_reservation(threads: usize, lanes: usize) -> Result<usize, BudgetError> {
        if threads == 0 || !(1..=2).contains(&lanes) {
            return Err("[worker] invalid native statistics dimensions".into());
        }
        let overflow = || "[worker] statistics reservation overflow".to_string();
        let qc = std::mem::size_of::<crate::native_qc::Qc>();
        let heap = crate::native_qc::Qc::maximum_heap_bytes();
        // Six boxed QC headers remain in each bundle; only raw/accepted lanes
        // grow. Worker bundles remain owned through final global reduction.
        let bundle = (2 * lanes)
            .checked_mul(heap)
            .and_then(|n| n.checked_add(6 * qc))
            .and_then(|n| n.checked_add(std::mem::size_of::<crate::stats::PeLocalStats>()))
            .ok_or_else(overflow)?;
        let stats = threads
            .checked_add(1)
            .and_then(|n| n.checked_mul(bundle))
            .and_then(|n| n.checked_add(lanes * heap + 2 * qc))
            // Population finalization holds one aggregate QC at a time.
            .and_then(|n| n.checked_add(heap + qc))
            .ok_or_else(overflow)?;
        if stats > STATS_BYTES {
            return Err(format!(
                "[worker] statistics reservation {stats} exceeds {STATS_BYTES}"
            ).into());
        }
        Ok(stats)
    }
    pub fn reservation(raw: usize, units: usize) -> Result<usize, BudgetError> {
        if raw > ASSEMBLY_BYTES || units > CHUNK_UNITS {
            return Err("[worker] native chunk exceeds declared framing limits".into());
        }
        // Includes lossy UTF-8 expansion in the inherited String parser, geometric
        // serialization capacities, gzip bound, record vectors and batch QC.
        raw.checked_mul(24)
            .and_then(|n| {
                n.checked_add(4 * units * std::mem::size_of::<crate::record::FastqRecord>())
            })
            .and_then(|n| n.checked_add(2 * QC_BYTES))
            .ok_or_else(|| "[worker] chunk reservation overflow".into())
    }
    pub fn reservation_with_capacity(
        raw: usize,
        units: usize,
        capacity: usize,
    ) -> Result<usize, BudgetError> {
        let excess = capacity
            .checked_sub(raw)
            .ok_or("[worker] raw capacity below length")?;
        Self::reservation(raw, units)?
            .checked_add(excess)
            .ok_or_else(|| "[worker] raw capacity reservation overflow".into())
    }
    pub fn admit(
        self: &Arc<Self>,
        raw: usize,
        units: usize,
        capacity: usize,
    ) -> Result<Ticket, BudgetError> {
        self.admit_with_feature(raw, units, capacity, 0)
    }
    pub fn admit_with_feature(
        self: &Arc<Self>,
        raw: usize,
        units: usize,
        capacity: usize,
        feature: usize,
    ) -> Result<Ticket, BudgetError> {
        let bytes = Self::reservation_with_capacity(raw, units, capacity)?;
        let total = bytes
            .checked_add(feature)
            .ok_or("[worker] complete feature chunk overflow")?;
        if total > self.available_bytes {
            return Err("[worker] complete chunk cannot fit managed budget".into());
        }
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if total > self.available_bytes - s.persistent_feature_bytes {
            return Err("[worker] complete chunk cannot fit with persistent reservations".into());
        }
        while s.active >= self.window || s.bytes + s.feature_bytes + total > self.available_bytes {
            if total > self.available_bytes - s.persistent_feature_bytes {
                return Err(
                    "[worker] complete chunk cannot fit with persistent reservations".into(),
                );
            }
            #[cfg(test)]
            if s.bytes + s.feature_bytes + total > self.available_bytes {
                s.byte_waits += 1;
            }
            if self.stop.load(Ordering::Relaxed) {
                return Err(BudgetError::cancelled("FASTQ pipeline cancelled"));
            }
            s = self
                .wake
                .wait_timeout(s, Duration::from_millis(10))
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        if self.stop.load(Ordering::Relaxed) {
            return Err(BudgetError::cancelled("FASTQ pipeline cancelled"));
        }
        s.active += 1;
        s.bytes += bytes;
        s.feature_bytes += feature;
        s.admitted += 1;
        s.peak_active = s.peak_active.max(s.active);
        s.peak_bytes = s.peak_bytes.max(s.bytes);
        self.update_peaks(&mut s);
        Ok(Ticket {
            owner: Arc::clone(self),
            bytes,
            feature_bytes: feature,
            ready: false,
        })
    }
    pub fn cancel(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.wake.notify_all();
    }
    pub fn compute(
        &self,
        stage: crate::cpu_budget::Stage,
    ) -> Result<Option<crate::cpu_budget::Permit>, BudgetError> {
        let cpu = self.cpu.lock().unwrap_or_else(|p| p.into_inner()).clone();
        cpu.map(|v| v.acquire_cancellable(stage, &self.stop))
            .transpose()
    }
    pub(crate) fn acquire_post_input_cpu(&self)->Result<Option<crate::cpu_budget::Permit>,BudgetError>{let state=self.state.lock().unwrap();if state.active!=0||state.ready!=0||state.bytes!=0{return Err("post-input work requires joined/retired pipeline".into());}drop(state);Ok(self.cpu.lock().unwrap().as_ref().map(|c|c.acquire(crate::cpu_budget::Stage::Worker)))}
    pub(crate) fn enable_dedup_backends(&self){let mut v=self.dedup_backends.lock().unwrap_or_else(|p|p.into_inner());assert!(v.is_none());*v=Some(Box::new(crate::decoder::Snapshot::empty("Source not opened yet")));}
    pub(crate) fn clear_dedup_backends(&self){self.dedup_backends.lock().unwrap_or_else(|p|p.into_inner()).take();}
    pub(crate) fn record_dedup_runtime(&self,cpu:Arc<crate::cpu_budget::CpuBudget>,facts:crate::decoder::Snapshot){*self.cpu.lock().unwrap_or_else(|p|p.into_inner())=Some(cpu);**self.dedup_backends.lock().unwrap_or_else(|p|p.into_inner()).as_mut().expect("Dedup metadata pre-admitted")=facts;}
    pub fn record_runtime(&self, cpu: Arc<crate::cpu_budget::CpuBudget>, backends: Value) {
        *self.cpu.lock().unwrap_or_else(|p| p.into_inner()) = Some(cpu);
        *self.backends.lock().unwrap_or_else(|p| p.into_inner()) = backends;
    }
    pub fn facts(&self) -> Value {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let cpu=self.cpu.lock().unwrap_or_else(|p|p.into_inner()).as_ref().map(|b|{let v=b.snapshot();json!({"limit":v.limit,"reserved":v.reserved,"active":v.active,"peak_active":v.peak_active,"scope":"application compute permits; OS thread count/activity separately observed; not exclusive allocation"})});
        json!({"compute":cpu,"decoders":*self.backends.lock().unwrap_or_else(|p|p.into_inner()),"window_limit":self.window,"admitted":s.admitted,"retired":s.retired,"active":s.active,"peak_active":s.peak_active,
            "managed_pipeline_limit_bytes":self.pipeline_limit,"memory_budget":{"requested_total_bytes":self.requested_total,"effective_pipeline_limit_bytes":self.pipeline_limit,"current_statistics_reserved_bytes":self.stats,"current_core_pipeline_reserved_bytes":self.prefetch+s.bytes,"current_feature_reserved_bytes":s.feature_bytes,"current_persistent_feature_reserved_bytes":s.persistent_feature_bytes,"current_managed_pipeline_reserved_bytes":self.prefetch+s.bytes+s.feature_bytes,"current_total_reserved_bytes":self.stats+self.prefetch+s.bytes+s.feature_bytes,"peak_feature_reserved_bytes":s.peak_feature_bytes,"peak_total_reserved_bytes":s.peak_total_bytes,"scope":"single-lock conservative managed processing reservations; not whole RSS; configuration/report metadata, opaque native caches, stacks and allocator overhead excluded"},"prefetch_reserved_bytes":self.prefetch,"chunk_capacity_bytes":self.available_bytes,"active_chunk_bytes":s.bytes,"peak_chunk_bytes":s.peak_bytes,
            "statistics_reserved_bytes":self.stats,"additional_statistics_reserved_bytes":self.additional_stats,"statistics_limit_bytes":STATS_BYTES,"statistics_formula":"(T+1)*(sizeof(PeLocalStats)+6*sizeof(Qc)+2*lanes*QcMaxHeap)+2*sizeof(Qc)+lanes*QcMaxHeap+sizeof(Qc)+QcMaxHeap; batch QC separately charged to live tickets; additional_statistics_reserved_bytes includes enabled optional-channel/T-F cumulative QC","ready":s.ready,"peak_ready":s.peak_ready,"max_record_bytes":RECORD_BYTES,"target_chunk_bytes":TARGET_BYTES,"max_chunk_units":CHUNK_UNITS,
            "reservation_formula":"24*raw_bytes + (raw_capacity-raw_bytes) + 4*source_units*sizeof(FastqRecord) + 2*QC_BYTES; carried vectors stay in prefetch slots before admission; cumulative stats separately reserved",
            "scope":"managed Rust capacities/reservations; not whole RSS, thread stacks, allocator overhead or opaque native codec caches",
            "native_cache_bound":null,"native_cache_bound_unavailable_reason":"opaque codec allocator/cache bytes not exported; external RSS and native worker observations required"})
    }
}
impl Ticket {
    // The ordered sink transfers already-admitted metadata capacity into the
    // persistent split index. No second reservation/wait occurs under a chunk.
    pub fn retain_feature(&mut self, bytes: usize) -> Result<FeatureTicket, BudgetError> {
        if bytes > self.feature_bytes {
            return Err("[worker] split metadata exceeds admitted feature capacity".into());
        }
        let mut s = self.owner.state.lock().unwrap_or_else(|p| p.into_inner());
        self.feature_bytes -= bytes;
        s.persistent_feature_bytes += bytes;
        Ok(FeatureTicket {
            owner: self.owner.clone(),
            bytes,
            persistent: true,
        })
    }
    pub fn mark_ready(&mut self) {
        if !self.ready {
            let mut s = self.owner.state.lock().unwrap_or_else(|p| p.into_inner());
            s.ready += 1;
            s.peak_ready = s.peak_ready.max(s.ready);
            self.ready = true;
        }
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        let mut s = self.owner.state.lock().unwrap_or_else(|p| p.into_inner());
        s.active -= 1;
        s.bytes -= self.bytes;
        s.feature_bytes -= self.feature_bytes;
        s.retired += 1;
        if self.ready {
            s.ready -= 1;
        }
        self.owner.wake.notify_all();
    }
}

#[cfg(test)]
mod split_transfer_tests {
    use super::*;
    #[test]
    fn split_ticket_transfer_preserves_one_epoch_and_releases_each_owner_once() {
        let e = Execution::new(1, 1, Arc::new(AtomicBool::new(false))).unwrap();
        let mut chunk = e.admit_with_feature(16, 1, 16, 256).unwrap();
        let before = e.facts();
        let retained = chunk.retain_feature(64).unwrap();
        let during = e.facts();
        assert_eq!(
            before["memory_budget"]["current_total_reserved_bytes"],
            during["memory_budget"]["current_total_reserved_bytes"]
        );
        assert_eq!(
            during["memory_budget"]["current_persistent_feature_reserved_bytes"],
            64
        );
        assert!(chunk.retain_feature(193).is_err());
        drop(chunk);
        let remaining = e.facts();
        assert_eq!(remaining["active"], 0);
        assert_eq!(
            remaining["memory_budget"]["current_feature_reserved_bytes"],
            64
        );
        drop(retained);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_budget_failure_keeps_cancellation_distinct_from_nonfit() {
        use crate::failure::{Category, Reason};
        let stop = Arc::new(AtomicBool::new(false));
        let e = Execution::new(2, 1, stop.clone()).unwrap();
        let ticket = e.reserve_preparation_feature(32).unwrap();
        drop(ticket);
        stop.store(true, Ordering::Relaxed);
        let cancelled = e.reserve_preparation_feature(1).err().expect("cancelled").0;
        assert_eq!(cancelled.reason, Reason::SyntheticCancellation);
        assert_eq!(cancelled.category, Category::Input);
        let nonfit = e.reserve_finalization_feature(usize::MAX).err().expect("nonfit").0;
        assert_eq!(nonfit.reason, Reason::Ordinary);
        assert_eq!(nonfit.category, Category::Worker);
        let ticket = e.reserve_finalization_feature(32).unwrap();
        drop(ticket);
        assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"], 0);
        assert_eq!(e.facts()["active_chunk_bytes"], 0);
        let cpu = crate::cpu_budget::CpuBudget::new(1);
        let cancelled = cpu.acquire_cancellable(crate::cpu_budget::Stage::Worker, &stop).err().expect("cancelled").0;
        assert_eq!(cancelled.reason, Reason::SyntheticCancellation);
    }
    #[test]
    fn core_and_chunk_feature_admission_is_atomic_and_releases_together() {
        let e = Execution::new(2, 1, Arc::new(AtomicBool::new(false))).unwrap();
        let core = Execution::reservation_with_capacity(100, 1, 100).unwrap();
        assert!(e
            .admit_with_feature(100, 1, 100, e.available_bytes - core + 1)
            .is_err());
        let ticket = e.admit_with_feature(100, 1, 100, 4096).unwrap();
        let f = e.facts();
        assert_eq!(f["memory_budget"]["current_feature_reserved_bytes"], 4096);
        assert_eq!(f["active_chunk_bytes"], core);
        drop(ticket);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        assert_eq!(e.facts()["active_chunk_bytes"], 0);
    }
    #[test]
    fn persistent_index_makes_impossible_chunk_fail_without_waiting() {
        let e = Execution::new(2, 1, Arc::new(AtomicBool::new(false))).unwrap();
        let bytes = Execution::reservation(100, 1).unwrap();
        let index = e
            .reserve_persistent_feature(e.available_bytes - bytes + 1)
            .unwrap();
        let child = e.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let h = std::thread::spawn(move || {
            let result = child.admit(100, 1, 100).map(|_| ());
            tx.send(result).unwrap();
        });
        let received = rx.recv_timeout(Duration::from_millis(200));
        if received.is_err() {
            e.cancel();
        }
        let waited = e.state.lock().unwrap().byte_waits;
        drop(index);
        h.join().unwrap();
        assert!(
            received.is_ok(),
            "permanently impossible admission waited for its own persistent index; waits={waited}"
        );
        let error = received.unwrap().err().unwrap();
        assert!(error.contains("cannot fit"), "{error}");
        assert_eq!(waited, 0);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        let ticket = e.admit(100, 1, 100).unwrap();
        drop(ticket);
        assert_eq!(
            e.facts()["memory_budget"]["current_persistent_feature_reserved_bytes"],
            0
        );
    }
    #[test]
    fn explicit_budget_preserves_ceilings_and_checked_startup() {
        for lanes in [1, 2] {
            for threads in [1, 2, 16, 32] {
                let stats = Execution::statistics_reservation(threads, lanes).unwrap();
                for total in [None, Some(512 * MIB), Some(1024 * MIB)] {
                    let e = Execution::with_budget(
                        threads,
                        lanes,
                        Arc::new(AtomicBool::new(false)),
                        total,
                    )
                    .unwrap();
                    let f = e.facts();
                    assert_eq!(e.stats, stats);
                    assert!(e.stats <= STATS_BYTES && e.pipeline_limit <= PIPELINE_BYTES);
                    if let Some(b) = total {
                        assert!(e.stats + e.pipeline_limit <= b);
                    }
                    assert_eq!(
                        f["memory_budget"]["current_total_reserved_bytes"],
                        stats + e.prefetch
                    );
                    assert_eq!(
                        f["memory_budget"]["peak_total_reserved_bytes"],
                        stats + e.prefetch
                    );
                }
                let fixed = stats + lanes * PREFETCH_PER_LANE + ASSEMBLY_BYTES;
                let minimum = Execution::reservation(0, 0).unwrap();
                assert!(
                    Execution::validate_budget(threads, lanes, Some(fixed + minimum - 1)).is_err()
                );
                assert!(Execution::validate_budget(threads, lanes, Some(fixed + minimum)).is_ok());
            }
        }
        assert!(Execution::validate_budget(1, 1, Some(MIB)).is_err());
        assert!(Execution::validate_budget(usize::MAX, 2, Some(usize::MAX)).is_err());
    }
    #[test]
    fn joint_peaks_measure_one_epoch_not_sum_of_separate_peaks() {
        let e = Execution::new(2, 1, Arc::new(AtomicBool::new(false))).unwrap();
        let bytes = Execution::reservation(100, 1).unwrap();
        let feature = e.reserve_feature(2 * bytes).unwrap();
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            2 * bytes
        );
        drop(feature);
        let mut core = e.admit(100, 1, 100).unwrap();
        core.mark_ready();
        let facts = e.facts();
        let b = &facts["memory_budget"];
        assert_eq!(
            b["current_core_pipeline_reserved_bytes"],
            e.prefetch + bytes
        );
        assert_eq!(
            b["current_total_reserved_bytes"],
            e.stats + e.prefetch + bytes
        );
        assert_eq!(
            b["peak_total_reserved_bytes"],
            e.stats + e.prefetch + 2 * bytes
        );
        assert_ne!(
            b["peak_total_reserved_bytes"],
            e.stats + e.prefetch + 3 * bytes
        );
        assert_eq!(facts["ready"], 1);
        drop(core);
        let f = e.facts();
        assert_eq!(
            f["memory_budget"]["current_total_reserved_bytes"],
            e.stats + e.prefetch
        );
        assert_eq!(f["retired"], f["admitted"]);
        assert!(e.reserve_feature(usize::MAX).is_err());
    }
    #[test]
    fn feature_pressure_waits_releases_and_cancels_but_impossible_requests_fail() {
        for cancel in [false, true] {
            let e = Execution::new(2, 1, Arc::new(AtomicBool::new(false))).unwrap();
            let bytes = Execution::reservation(100, 1).unwrap();
            let feature = e.reserve_feature(e.available_bytes).unwrap();
            let child = e.clone();
            let h = std::thread::spawn(move || child.admit(100, 1, 100).map(|_| ()));
            let until = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                if e.state.lock().unwrap().byte_waits > 0 {
                    break;
                }
                assert!(std::time::Instant::now() < until);
                std::thread::yield_now();
            }
            assert_eq!(e.facts()["active"], 0);
            if cancel {
                e.cancel();
            }
            drop(feature);
            assert_eq!(h.join().unwrap().is_err(), cancel);
            assert_eq!(
                e.facts()["memory_budget"]["current_feature_reserved_bytes"],
                0
            );
            assert!(e.reserve_feature(e.available_bytes + 1).is_err());
            let total = e.stats + e.prefetch + bytes - 1;
            let small = Execution::with_budget(2, 1, Arc::new(AtomicBool::new(false)), Some(total))
                .unwrap();
            assert!(small.admit(100, 1, 100).is_err());
        }
        // A feature also waits for core ownership and is awakened by ticket retirement.
        let e = Execution::new(2, 1, Arc::new(AtomicBool::new(false))).unwrap();
        let ticket = e.admit(100, 1, 100).unwrap();
        let child = e.clone();
        let h =
            std::thread::spawn(move || child.reserve_feature(child.available_bytes).map(|_| ()));
        let until = std::time::Instant::now() + Duration::from_secs(2);
        while e.state.lock().unwrap().byte_waits == 0 {
            assert!(std::time::Instant::now() < until);
            std::thread::yield_now();
        }
        drop(ticket);
        assert!(h.join().unwrap().is_ok());
    }
    #[test]
    fn complete_reservations_cover_empty_completion_and_cancel_waiter() {
        let stop = Arc::new(AtomicBool::new(false));
        let e = Execution::new(1, 2, stop).unwrap();
        let mut a = e.admit(100, 1, 100).unwrap();
        a.mark_ready();
        let b = e.admit(100, 1, 100).unwrap();
        let child = e.clone();
        let h = std::thread::spawn(move || child.admit(100, 1, 100).map(|_| ()));
        e.cancel();
        assert!(h.join().unwrap().is_err());
        drop(a);
        drop(b);
        let f = e.facts();
        assert_eq!(f["active"], 0);
        assert_eq!(f["active_chunk_bytes"], 0);
        assert_eq!(f["retired"], 2);
        assert_eq!(f["peak_ready"], 1);
    }
    #[test]
    fn byte_capacity_wait_releases_and_cancels_with_window_room() {
        for cancel in [false, true] {
            let stop = Arc::new(AtomicBool::new(false));
            let mut e = Execution::new(2, 1, stop).unwrap();
            Arc::get_mut(&mut e).unwrap().available_bytes = Execution::reservation(100, 1).unwrap();
            let first = e.admit(100, 1, 100).unwrap();
            let child = e.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            let h = std::thread::spawn(move || {
                let result = child.admit(100, 1, 100);
                let valid = result.is_ok();
                drop(result);
                tx.send(valid).unwrap();
            });
            let until = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                {
                    let state = e.state.lock().unwrap();
                    assert_eq!(state.active, 1);
                    assert!(state.active < e.window);
                    if state.byte_waits > 0 {
                        break;
                    }
                }
                assert!(
                    std::time::Instant::now() < until,
                    "second admission never reached byte wait"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(
                rx.try_recv().is_err(),
                "byte-pressure waiter admitted early"
            );
            if cancel {
                e.cancel();
                assert!(!rx.recv_timeout(Duration::from_secs(2)).unwrap());
                drop(first);
            } else {
                drop(first);
                assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap());
            }
            h.join().unwrap();
            let facts = e.facts();
            assert_eq!(facts["active"], 0);
            assert_eq!(facts["active_chunk_bytes"], 0);
            assert_eq!(facts["retired"], facts["admitted"]);
        }
    }
    #[test]
    fn raw_capacity_is_charged_for_the_whole_ticket_lifetime() {
        let raw = 100;
        let capacity = 2 * LANE_BYTES;
        let base = Execution::reservation(raw, 1).unwrap();
        assert_eq!(
            Execution::reservation_with_capacity(raw, 1, capacity).unwrap(),
            base + capacity - raw
        );
        assert!(Execution::reservation_with_capacity(raw, 1, raw - 1).is_err());
        assert!(Execution::reservation_with_capacity(raw, 1, usize::MAX).is_err());
        let e = Execution::new(4, 2, Arc::new(AtomicBool::new(false))).unwrap();
        let mut ticket = e.admit(raw, 1, capacity).unwrap();
        ticket.mark_ready();
        assert_eq!(e.facts()["active_chunk_bytes"], base + capacity - raw);
        drop(ticket);
        assert_eq!(e.facts()["active_chunk_bytes"], 0);
        assert_eq!(e.facts()["retired"], 1);
    }
    #[test]
    fn t32_actual_statistics_capacities_cover_retained_workers_and_reduction() {
        use crate::native_qc::Qc;
        use crate::params::GlobalParams;
        use crate::stats::{GlobalStats, PeLocalStats};
        let bytes = |local: &PeLocalStats| {
            std::mem::size_of::<PeLocalStats>()
                + [
                    &local.raw1,
                    &local.raw2,
                    &local.trim1,
                    &local.trim2,
                    &local.clean1,
                    &local.clean2,
                ]
                .iter()
                .map(|stat| {
                    assert_eq!(stat.qs.position_qual.capacity(), 0);
                    assert_eq!(stat.bs.position_acgt_content.capacity(), 0);
                    for bins in [
                        &stat.ts.ht,
                        &stat.ts.hlq,
                        &stat.ts.tt,
                        &stat.ts.tlq,
                        &stat.ts.ta,
                    ] {
                        assert_eq!(bins.capacity(), 0);
                    }
                    stat.native_qc.as_ref().unwrap().allocated_bytes()
                })
                .sum::<usize>()
        };
        for lanes in [1, 2] {
            let mut gp = GlobalParams::new();
            gp.native_m1 = true;
            let mut workers = Vec::new();
            let sequence = vec![b'A'; 1000];
            let quality: Vec<_> = (0..1000).map(|i| 33 + (i % 94) as u8).collect();
            for _ in 0..32 {
                let mut worker = Box::new(PeLocalStats::new(&gp));
                for stat in [&mut worker.raw1, &mut worker.clean1] {
                    stat.native_qc
                        .as_mut()
                        .unwrap()
                        .observe_validated(&sequence, &quality, 33);
                    stat.gs = stat.native_qc.as_ref().unwrap().general();
                }
                if lanes == 2 {
                    for stat in [&mut worker.raw2, &mut worker.clean2] {
                        stat.native_qc
                            .as_mut()
                            .unwrap()
                            .observe_validated(&sequence, &quality, 33);
                        stat.gs = stat.native_qc.as_ref().unwrap().general();
                    }
                }
                worker.fs.native.rejected = 1;
                workers.push(worker);
            }
            let worker_bytes: usize = workers.iter().map(|worker| bytes(worker)).sum();
            let mut global = GlobalStats::new(&gp);
            for worker in &workers {
                if lanes == 2 {
                    crate::stats::merge_pe_stat(&mut global, worker, &gp, false);
                } else {
                    crate::stats::merge_se_stat(&mut global, worker, &gp, false);
                }
            }
            assert!(std::mem::size_of::<GlobalStats>() <= std::mem::size_of::<PeLocalStats>());
            let global_bytes = std::mem::size_of::<GlobalStats>()
                + [
                    &global.raw1,
                    &global.raw2,
                    &global.trim1,
                    &global.trim2,
                    &global.clean1,
                    &global.clean2,
                ]
                .iter()
                .map(|stat| stat.native_qc.as_ref().unwrap().allocated_bytes())
                .sum::<usize>();
            assert_eq!(global.raw1.native_qc.as_ref().unwrap().reads, 32);
            assert_eq!(global.clean1.native_qc.as_ref().unwrap().reads, 32);
            assert_eq!(global.fs.native.rejected, 32);
            let mut written = [Qc::default(), Qc::default()];
            let mut aggregate = Qc::default();
            for qc in written.iter_mut().take(lanes) {
                qc.observe_validated(&sequence, &quality, 33);
                aggregate.merge(qc);
            }
            let actual = worker_bytes
                + global_bytes
                + written.iter().map(Qc::allocated_bytes).sum::<usize>()
                + aggregate.allocated_bytes();
            let bound = Execution::statistics_reservation(32, lanes).unwrap();
            assert!(actual <= bound && bound <= STATS_BYTES);
            let mut batch = Qc::default();
            batch.observe_validated(&sequence, &quality, 33);
            assert!(batch.allocated_bytes() <= QC_BYTES);
            println!("CAPACITY lanes={lanes} retained_workers={worker_bytes} global={global_bytes} actual_with_written_and_scratch={actual} reserved={bound} stats_limit={STATS_BYTES} batch_qc={}", batch.allocated_bytes());
        }
        assert!(Execution::statistics_reservation(usize::MAX, 2)
            .unwrap_err()
            .contains("overflow"));
        assert!(Execution::statistics_reservation(1000, 2)
            .unwrap_err()
            .contains("exceeds"));
        assert!(Execution::statistics_reservation(64, 2).is_err());
        assert!(Execution::statistics_reservation(0, 2).is_err());
        assert!(Execution::statistics_reservation(32, 0).is_err());
        let legacy = PeLocalStats::new(&GlobalParams::new());
        assert_eq!(legacy.raw1.qs.position_qual.len(), 43000);
        assert!(legacy.raw1.native_qc.is_none());
    }

    #[test]
    fn framing_and_stats_limits_are_checked() {
        assert!(Execution::reservation(ASSEMBLY_BYTES + 1, 1).is_err());
        assert!(Execution::reservation(1, CHUNK_UNITS + 1).is_err());
        assert!(Execution::new(16, 2, Arc::new(AtomicBool::new(false))).is_ok());
        assert!(Execution::new(1000, 2, Arc::new(AtomicBool::new(false))).is_err());
    }
}

#[cfg(test)]mod demux_finalization_tests{use super::*;
#[test]fn stopped_finalization_is_nonblocking_bounded_and_releases(){
 let stop=Arc::new(AtomicBool::new(false));let e=Execution::new(1,1,stop.clone()).unwrap();let active=e.admit(0,0,0).unwrap();stop.store(true,Ordering::Relaxed);assert!(e.reserve_preparation_feature(1).is_err());assert!(e.reserve_finalization_feature(1).is_err());drop(active);
 {let mut s=e.state.lock().unwrap();s.ready=1;}assert!(e.reserve_finalization_feature(1).is_err());{let mut s=e.state.lock().unwrap();s.ready=0;}
 let held=e.reserve_finalization_feature(64).unwrap();assert!(stop.load(Ordering::Relaxed));assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],64);assert!(e.reserve_finalization_feature(usize::MAX).is_err());assert!(e.reserve_finalization_feature(e.available_bytes+1).is_err());assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],64);drop(held);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);assert!(e.reserve_preparation_feature(1).is_err());
}
}

pub(crate) struct DedupFacts<'a>{pub execution:&'a Execution,pub spec:&'a crate::job_api::JobSpec}
impl serde::Serialize for DedupFacts<'_>{fn serialize<S:serde::Serializer>(&self,ser:S)->Result<S::Ok,S::Error>{
 use serde::ser::SerializeMap;
 struct Compute<'a>(&'a crate::cpu_budget::MetricsSnapshot);impl serde::Serialize for Compute<'_>{fn serialize<S:serde::Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let mut m=s.serialize_map(Some(5))?;m.serialize_entry("limit",&self.0.limit)?;m.serialize_entry("reserved",&self.0.reserved)?;m.serialize_entry("active",&self.0.active)?;m.serialize_entry("peak_active",&self.0.peak_active)?;m.serialize_entry("scope","application compute permits; OS thread count/activity separately observed; not exclusive allocation")?;m.end()}}
 struct Memory<'a>{e:&'a Execution,s:&'a State}
 impl serde::Serialize for Memory<'_>{fn serialize<S:serde::Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let e=self.e;let v=self.s;let mut m=s.serialize_map(Some(12))?;
 m.serialize_entry("requested_total_bytes",&e.requested_total)?;m.serialize_entry("effective_pipeline_limit_bytes",&e.pipeline_limit)?;m.serialize_entry("current_statistics_reserved_bytes",&e.stats)?;m.serialize_entry("current_core_pipeline_reserved_bytes",&(e.prefetch+v.bytes))?;m.serialize_entry("current_feature_reserved_bytes",&v.feature_bytes)?;m.serialize_entry("current_persistent_feature_reserved_bytes",&v.persistent_feature_bytes)?;m.serialize_entry("current_managed_pipeline_reserved_bytes",&(e.prefetch+v.bytes+v.feature_bytes))?;m.serialize_entry("current_total_reserved_bytes",&(e.stats+e.prefetch+v.bytes+v.feature_bytes))?;m.serialize_entry("peak_feature_reserved_bytes",&v.peak_feature_bytes)?;m.serialize_entry("peak_total_reserved_bytes",&v.peak_total_bytes)?;m.serialize_entry("scope","single-lock conservative managed processing reservations including Dedup report/backend snapshots; not whole RSS; caller-owned configuration, opaque native caches, stacks and allocator overhead excluded")?;m.end()}}
 let e=self.execution;let state=e.state.lock().unwrap_or_else(|p|p.into_inner());let cpu=e.cpu.lock().unwrap_or_else(|p|p.into_inner()).as_ref().map(|v|v.snapshot());let snapshot=**e.dedup_backends.lock().unwrap_or_else(|p|p.into_inner()).as_ref().expect("Dedup metadata remains admitted through serialization");
 let mut m=ser.serialize_map(None)?;if let Some(v)=cpu.as_ref(){m.serialize_entry("compute",&Compute(v))?;}else{m.serialize_entry("compute",&Option::<u64>::None)?;}
 m.serialize_entry("decoders",&crate::dedup_backend::View{snapshot,spec:self.spec})?;
 m.serialize_entry("window_limit",&e.window)?;m.serialize_entry("admitted",&state.admitted)?;m.serialize_entry("retired",&state.retired)?;m.serialize_entry("active",&state.active)?;m.serialize_entry("peak_active",&state.peak_active)?;m.serialize_entry("managed_pipeline_limit_bytes",&e.pipeline_limit)?;m.serialize_entry("memory_budget",&Memory{e,s:&state})?;m.serialize_entry("prefetch_reserved_bytes",&e.prefetch)?;m.serialize_entry("chunk_capacity_bytes",&e.available_bytes)?;m.serialize_entry("active_chunk_bytes",&state.bytes)?;m.serialize_entry("peak_chunk_bytes",&state.peak_bytes)?;m.serialize_entry("statistics_reserved_bytes",&e.stats)?;m.serialize_entry("additional_statistics_reserved_bytes",&e.additional_stats)?;m.serialize_entry("statistics_limit_bytes",&STATS_BYTES)?;
 m.serialize_entry("statistics_formula","(T+1)*(sizeof(PeLocalStats)+6*sizeof(Qc)+2*lanes*QcMaxHeap)+2*sizeof(Qc)+lanes*QcMaxHeap+sizeof(Qc)+QcMaxHeap; batch QC separately charged to live tickets; additional_statistics_reserved_bytes includes enabled optional-channel/T-F cumulative QC")?;
 m.serialize_entry("ready",&state.ready)?;m.serialize_entry("peak_ready",&state.peak_ready)?;m.serialize_entry("max_record_bytes",&RECORD_BYTES)?;m.serialize_entry("target_chunk_bytes",&TARGET_BYTES)?;m.serialize_entry("max_chunk_units",&CHUNK_UNITS)?;m.serialize_entry("reservation_formula","24*raw_bytes + (raw_capacity-raw_bytes) + 4*source_units*sizeof(FastqRecord) + 2*QC_BYTES; carried vectors stay in prefetch slots before admission; cumulative stats separately reserved")?;
 m.serialize_entry("scope","managed Rust capacities/reservations including Dedup report metadata; not whole RSS, thread stacks, allocator overhead or opaque native codec caches")?;m.serialize_entry("native_cache_bound",&Option::<u64>::None)?;m.serialize_entry("native_cache_bound_unavailable_reason","opaque codec allocator/cache bytes not exported; external RSS and native worker observations required")?;m.end()
}}
