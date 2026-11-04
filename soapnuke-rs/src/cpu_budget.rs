//! A small process-wide compute budget for the FASTQ pipeline.
//!
//! `-T N` is a limit on simultaneously active application compute, not a
//! request to create N plus an unbounded number of helper threads.  Pipeline
//! code acquires a permit only for bounded CPU work and drops it before any
//! channel, condition-variable, or thread wait.

use crate::failure::BudgetError;
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

const STAGE_COUNT: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Stage {
    Source = 0,
    Producer = 1,
    Worker = 2,
    Sink = 3,
}

impl Stage {
    pub const ALL: [Stage; STAGE_COUNT] =
        [Stage::Source, Stage::Producer, Stage::Worker, Stage::Sink];

    pub fn name(self) -> &'static str {
        match self {
            Stage::Source => "source",
            Stage::Producer => "producer",
            Stage::Worker => "worker",
            Stage::Sink => "sink",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MetricsSnapshot {
    pub limit: usize,
    pub reserved: usize,
    pub active: usize,
    pub peak_active: usize,
    pub active_cpu_nanos: [u64; STAGE_COUNT],
    pub active_wall_nanos: [u64; STAGE_COUNT],
    pub permit_wait_nanos: u64,
    pub permit_wait_events: u64,
    pub observed_queue_wait_nanos: u64,
    pub observed_queue_wait_events: u64,
    pub worker_input_wait_nanos: u64,
    pub worker_input_wait_events: u64,
}

impl MetricsSnapshot {
    pub fn active_cpu_millis(&self, stage: Stage) -> f64 {
        self.active_cpu_nanos[stage as usize] as f64 / 1_000_000.0
    }
}

#[derive(Default)]
struct State {
    available: usize,
    active: usize,
    peak_active: usize,
    next_ticket: u64,
    serving_ticket: u64,
    withdrawn: std::collections::BTreeSet<u64>,
    active_cpu_nanos: [u64; STAGE_COUNT],
    active_wall_nanos: [u64; STAGE_COUNT],
    permit_wait_nanos: u64,
    permit_wait_events: u64,
    observed_queue_wait_nanos: u64,
    observed_queue_wait_events: u64,
    worker_input_wait_nanos: u64,
    worker_input_wait_events: u64,
}

pub struct CpuBudget {
    limit: usize,
    reserved: usize,
    state: Mutex<State>,
    wake: Condvar,
}

pub struct Permit {
    budget: Arc<CpuBudget>,
    stage: Stage,
    started_wall: Instant,
    started_cpu_nanos: u64,
    // A permit is a thread-local capability.  Keeping this marker makes an
    // accidental cross-thread move a compile-time error, which is important
    // because the nested-permit guard below is thread-local.
    _not_send: PhantomData<Rc<()>>,
}

thread_local! {
    static ACTIVE_PERMITS: Cell<usize> = const { Cell::new(0) };
}

impl CpuBudget {
    pub fn new(limit: usize) -> Arc<CpuBudget> {
        Self::new_with_reserved(limit, 0)
    }

    pub fn new_with_reserved(limit: usize, reserved: usize) -> Arc<CpuBudget> {
        assert!(limit > 0, "CPU budget must be greater than zero");
        assert!(
            reserved < limit,
            "reserved CPU credits must be below the budget"
        );
        Arc::new(CpuBudget {
            limit,
            reserved,
            state: Mutex::new(State {
                available: limit - reserved,
                ..State::default()
            }),
            wake: Condvar::new(),
        })
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
    pub fn current_thread_has_permit() -> bool {
        ACTIVE_PERMITS.with(|v| v.get() > 0)
    }

    pub fn acquire(self: &Arc<CpuBudget>, stage: Stage) -> Permit {
        self.acquire_inner(stage, None)
            .expect("uncancellable compute acquisition")
    }
    pub fn acquire_cancellable(
        self: &Arc<Self>,
        stage: Stage,
        stop: &AtomicBool,
    ) -> Result<Permit, BudgetError> {
        self.acquire_inner(stage, Some(stop))
    }
    pub fn wake_cancelled(&self) {
        let _state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        self.wake.notify_all();
    }
    fn acquire_inner(
        self: &Arc<Self>,
        stage: Stage,
        stop: Option<&AtomicBool>,
    ) -> Result<Permit, BudgetError> {
        ACTIVE_PERMITS.with(|depth| {
            assert_eq!(
                depth.get(),
                0,
                "nested CPU permits are forbidden; drop the outer permit before acquiring another"
            );
        });
        let wait_started = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if stop.is_some_and(|v| v.load(Ordering::Relaxed)) {
            return Err(BudgetError::cancelled("FASTQ pipeline cancelled"));
        }
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        let mut waited = false;
        while state.available == 0 || ticket != state.serving_ticket {
            if stop.is_some_and(|v| v.load(Ordering::Relaxed)) {
                state.withdrawn.insert(ticket);
                loop {
                    let next = state.serving_ticket;
                    if !state.withdrawn.remove(&next) {
                        break;
                    }
                    state.serving_ticket = state.serving_ticket.wrapping_add(1);
                }
                self.wake.notify_all();
                return Err(BudgetError::cancelled("FASTQ pipeline cancelled"));
            }
            waited = true;
            state = self.wake.wait(state).unwrap_or_else(|p| p.into_inner());
        }
        state.serving_ticket = state.serving_ticket.wrapping_add(1);
        loop {
            let next = state.serving_ticket;
            if !state.withdrawn.remove(&next) {
                break;
            }
            state.serving_ticket = state.serving_ticket.wrapping_add(1);
        }
        self.wake.notify_all();
        if waited {
            state.permit_wait_events += 1;
            state.permit_wait_nanos = state
                .permit_wait_nanos
                .saturating_add(wait_started.elapsed().as_nanos() as u64);
        }
        state.available -= 1;
        state.active += 1;
        state.peak_active = state.peak_active.max(state.active);
        ACTIVE_PERMITS.with(|depth| {
            assert_eq!(depth.get(), 0);
            depth.set(1);
        });
        Ok(Permit {
            budget: Arc::clone(self),
            stage,
            started_wall: Instant::now(),
            started_cpu_nanos: thread_cpu_nanos(),
            _not_send: PhantomData,
        })
    }

    /// Record one observed bounded-queue wait. This is a selected-instrument
    /// measure, not an exhaustive wait-time accounting. Callers must invoke
    /// this outside a compute permit so waiting cannot occupy a `-T` credit.
    pub fn record_observed_queue_wait(&self, elapsed_nanos: u64) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.observed_queue_wait_events += 1;
        state.observed_queue_wait_nanos = state
            .observed_queue_wait_nanos
            .saturating_add(elapsed_nanos);
    }

    /// Record time spent waiting for the next QC batch, including receiver
    /// lock contention. This is kept separate from producer backpressure and
    /// sink waits and is called without a compute permit.
    pub fn record_worker_input_wait(&self, elapsed_nanos: u64) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.worker_input_wait_events += 1;
        state.worker_input_wait_nanos = state.worker_input_wait_nanos.saturating_add(elapsed_nanos);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        MetricsSnapshot {
            limit: self.limit,
            reserved: self.reserved,
            active: state.active,
            peak_active: state.peak_active,
            active_cpu_nanos: state.active_cpu_nanos,
            active_wall_nanos: state.active_wall_nanos,
            permit_wait_nanos: state.permit_wait_nanos,
            permit_wait_events: state.permit_wait_events,
            observed_queue_wait_nanos: state.observed_queue_wait_nanos,
            observed_queue_wait_events: state.observed_queue_wait_events,
            worker_input_wait_nanos: state.worker_input_wait_nanos,
            worker_input_wait_events: state.worker_input_wait_events,
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        ACTIVE_PERMITS.with(|depth| {
            assert_eq!(
                depth.get(),
                1,
                "CPU permit was dropped on a different thread or was already released"
            );
            depth.set(0);
        });
        let mut state = self.budget.state.lock().unwrap_or_else(|p| p.into_inner());
        state.active = state.active.saturating_sub(1);
        state.available += 1;
        let slot = &mut state.active_cpu_nanos[self.stage as usize];
        *slot = slot.saturating_add(thread_cpu_nanos().saturating_sub(self.started_cpu_nanos));
        let wall_slot = &mut state.active_wall_nanos[self.stage as usize];
        *wall_slot = wall_slot.saturating_add(self.started_wall.elapsed().as_nanos() as u64);
        // Wake all waiters because only the thread holding the next ticket
        // may proceed. `notify_one` could wake a later ticket and leave the
        // rightful waiter asleep indefinitely.
        self.budget.wake.notify_all();
    }
}

#[cfg(target_os = "linux")]
fn thread_cpu_nanos() -> u64 {
    #[repr(C)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: i64,
    }
    extern "C" {
        fn clock_gettime(clock_id: i32, tp: *mut Timespec) -> i32;
    }
    let mut ts = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // Linux CLOCK_THREAD_CPUTIME_ID. A failure is treated as zero rather
    // than converting wall time into a misleading CPU measurement.
    if unsafe { clock_gettime(3, &mut ts) } == 0 {
        (ts.tv_sec.max(0) as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec.max(0) as u64)
    } else {
        0
    }
}

#[cfg(not(target_os = "linux"))]
fn thread_cpu_nanos() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::{CpuBudget, Stage};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    };
    use std::thread;
    use std::time::Instant;

    #[test]
    fn cancellation_wakes_a_waiting_compute_ticket_without_extra_credit() {
        let b = CpuBudget::new(1);
        let held = b.acquire(Stage::Worker);
        let stop = Arc::new(AtomicBool::new(false));
        let x = b.clone();
        let y = stop.clone();
        let h = thread::spawn(move || x.acquire_cancellable(Stage::Source, &y).map(|_| ()));
        stop.store(true, Ordering::Relaxed);
        b.wake_cancelled();
        assert!(h.join().unwrap().is_err());
        drop(held);
        {
            let _observation = b.acquire(Stage::Sink);
        }
        assert_eq!(b.snapshot().active, 0);
        assert_eq!(b.snapshot().peak_active, 1);
    }
    #[test]
    fn permits_bound_simultaneous_active_work() {
        let budget = CpuBudget::new(1);
        let held = budget.acquire(Stage::Source);
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let budget = Arc::clone(&budget);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let permit = budget.acquire(Stage::Worker);
                for _ in 0..50_000 {
                    std::hint::spin_loop();
                }
                drop(permit);
            }));
        }
        barrier.wait();
        // Both workers must queue before the sole permit becomes available.
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        while budget.state.lock().unwrap().next_ticket < 3 {
            assert!(Instant::now() < deadline, "workers did not queue for permits");
            thread::yield_now();
        }
        drop(held);
        for handle in handles {
            handle.join().unwrap();
        }
        let metrics = budget.snapshot();
        assert_eq!(metrics.limit, 1);
        assert_eq!(metrics.peak_active, 1);
        assert!(metrics.permit_wait_events > 0);
        assert!(metrics.active_cpu_nanos[Stage::Worker as usize] > 0);
        assert!(metrics.active_wall_nanos[Stage::Worker as usize] > 0);
    }

    #[test]
    fn observed_queue_waits_are_reported_separately() {
        let budget = CpuBudget::new(2);
        budget.record_observed_queue_wait(123);
        budget.record_observed_queue_wait(456);
        let metrics = budget.snapshot();
        assert_eq!(metrics.observed_queue_wait_events, 2);
        assert_eq!(metrics.observed_queue_wait_nanos, 579);
        assert_eq!(metrics.permit_wait_events, 0);
    }

    #[test]
    fn bounded_source_worker_sink_stress_releases_before_queue_waits() {
        for limit in [1usize, 2, 8] {
            let budget = CpuBudget::new(limit);
            let cancelled = Arc::new(AtomicBool::new(false));
            let (source_tx, worker_rx) = std::sync::mpsc::sync_channel::<u64>(2);
            let (worker_tx, sink_rx) = std::sync::mpsc::sync_channel::<u64>(2);

            let source_budget = Arc::clone(&budget);
            let source_cancelled = Arc::clone(&cancelled);
            let source = thread::spawn(move || {
                for value in 0..128u64 {
                    if source_cancelled.load(Ordering::Relaxed) {
                        break;
                    }
                    {
                        let _permit = source_budget.acquire(Stage::Source);
                        for _ in 0..1_000 {
                            std::hint::spin_loop();
                        }
                    }
                    let started = Instant::now();
                    if source_tx.send(value).is_err() {
                        break;
                    }
                    source_budget.record_observed_queue_wait(started.elapsed().as_nanos() as u64);
                }
            });

            let worker_budget = Arc::clone(&budget);
            let worker_cancelled = Arc::clone(&cancelled);
            let worker = thread::spawn(move || {
                for (processed, value) in worker_rx.into_iter().enumerate() {
                    {
                        let _permit = worker_budget.acquire(Stage::Worker);
                        for _ in 0..1_000 {
                            std::hint::spin_loop();
                        }
                    }
                    let started = Instant::now();
                    if worker_tx.send(value).is_err() {
                        break;
                    }
                    worker_budget.record_observed_queue_wait(started.elapsed().as_nanos() as u64);
                    if processed == 15 {
                        worker_cancelled.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            });

            let sink_budget = Arc::clone(&budget);
            let sink = thread::spawn(move || {
                for _ in sink_rx {
                    {
                        let _permit = sink_budget.acquire(Stage::Sink);
                        for _ in 0..1_000 {
                            std::hint::spin_loop();
                        }
                    }
                    for _ in 0..2_000 {
                        std::hint::spin_loop();
                    }
                }
            });

            source.join().unwrap();
            worker.join().unwrap();
            sink.join().unwrap();
            let metrics = budget.snapshot();
            assert_eq!(metrics.active, 0, "permits leaked at T={limit}");
            assert!(
                metrics.peak_active <= limit,
                "peak {} exceeded T={limit}",
                metrics.peak_active
            );
            assert!(metrics.observed_queue_wait_events > 0);
        }
    }

    #[test]
    fn permit_is_returned_when_owner_panics_or_cancels() {
        let budget = CpuBudget::new(1);
        let worker_budget = Arc::clone(&budget);
        let handle = thread::spawn(move || {
            let _permit = worker_budget.acquire(Stage::Worker);
            panic!("injected worker cancellation");
        });
        assert!(handle.join().is_err());
        let permit = budget.acquire(Stage::Sink);
        drop(permit);
        let metrics = budget.snapshot();
        assert_eq!(metrics.active, 0);
        assert!(metrics.peak_active <= 1);
    }

    #[test]
    fn nested_permit_acquisition_panics_before_consuming_capacity() {
        let budget = CpuBudget::new(1);
        let outer = budget.acquire(Stage::Worker);
        let nested = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _inner = budget.acquire(Stage::Sink);
        }));
        assert!(nested.is_err());
        drop(outer);
        let metrics = budget.snapshot();
        assert_eq!(metrics.active, 0);
        assert_eq!(metrics.peak_active, 1);
    }

    #[test]
    fn zero_cpu_budget_is_rejected_instead_of_clamped() {
        let result = std::panic::catch_unwind(|| CpuBudget::new(0));
        assert!(result.is_err());
    }
}
