use crate::failure::BudgetError;
use crate::native_qc::Qc;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub struct Runtime {
    execution: Mutex<Option<Arc<crate::native_execution::Execution>>>,
    pub repetition: Mutex<Option<Arc<crate::native_repetition::Estimator>>>,
    pub html_buffer: Mutex<Option<crate::native_execution::FeatureTicket>>,
    pub screen: Mutex<Option<Arc<crate::native_screen::Screen>>>,
    pub written: Mutex<[Qc; 2]>,
    pub merged: Mutex<Option<Box<Qc>>>,
    pub inference: Mutex<Value>,
    pub split: Mutex<Value>,
    pub replayed: AtomicU64,
    pub auxiliary: Mutex<Option<Box<[Qc; 10]>>>,
    pub match_counts: Mutex<[u64; 2]>,
    pub qc_only: std::sync::atomic::AtomicBool,
    input: AtomicU64,
    accepted: AtomicU64,
    written_records: [AtomicU64; 2],
    started: Instant,
    next: AtomicU64,
    interval: u64,
    sample: String,
}
impl Runtime {
    pub fn new(interval: u64, sample: String) -> Self {
        Self {
            execution: Mutex::new(None),
            repetition:Mutex::new(None),
            html_buffer:Mutex::new(None),
            screen: Mutex::new(None),
            written: Mutex::new([Qc::default(), Qc::default()]),
            merged: Mutex::new(None),
            inference: Mutex::new(Value::Null),
            split: Mutex::new(Value::Null),
            replayed: AtomicU64::new(0),
            auxiliary: Mutex::new(None),
            match_counts: Mutex::new([0; 2]),
            qc_only: std::sync::atomic::AtomicBool::new(false),
            input: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            written_records: [AtomicU64::new(0), AtomicU64::new(0)],
            started: Instant::now(),
            next: AtomicU64::new(interval),
            interval,
            sample,
        }
    }
    pub fn enable_auxiliary(&self) {
        *self.auxiliary.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(Box::new(std::array::from_fn(|_| Qc::default())));
    }
    pub fn appended_auxiliary(&self, index: usize, qc: &Qc) {
        self.auxiliary
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_mut()
            .unwrap()[index]
            .merge(qc);
    }
    pub fn set_execution(&self, value: Option<Arc<crate::native_execution::Execution>>) {
        *self.execution.lock().unwrap_or_else(|p| p.into_inner()) = value;
    }
    pub fn reserve_split_storage(
        &self,
        bytes: usize,
    ) -> Result<crate::native_execution::FeatureTicket, BudgetError> {
        let execution = self
            .execution
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or("[worker] split execution budget unavailable")?;
        execution.reserve_persistent_feature(bytes)
    }
    pub fn execution_facts(&self) -> serde_json::Value {
        self.execution
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map_or(serde_json::Value::Null, |e| e.facts())
    }
    pub fn preparation_observed_input(&self) {
        self.input.fetch_add(1, Ordering::Relaxed);
    }
    pub fn replayed_input(&self) {
        self.replayed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inference_report(&self) -> Value {
        let mut value = self
            .inference
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if value.is_object() {
            value["replayed_units"] = json!(self.replayed.load(Ordering::Relaxed));
        }
        value
    }
    pub fn processed(&self, input: u64, accepted: u64) {
        self.input.fetch_add(input, Ordering::Relaxed);
        self.accepted.fetch_add(accepted, Ordering::Relaxed);
        self.event("processed", false);
    }
    pub(crate) fn appended_records(&self,channel:usize,records:u64){self.written_records[channel].fetch_add(records,Ordering::Relaxed);}
    pub fn appended_merged(&self, qc: &Qc) {
        self.merged
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_or_insert_with(|| Box::new(Qc::merged()))
            .merge(qc);
    }
    pub fn appended(&self, channel: usize, qc: &Qc) {
        self.written.lock().unwrap_or_else(|p| p.into_inner())[channel].merge(qc);
        self.written_records[channel].fetch_add(qc.reads, Ordering::Relaxed);
    }
    pub fn event(&self, stage: &str, force: bool) {
        let elapsed = self.started.elapsed();
        let next = self.next.load(Ordering::Relaxed);
        if !force
            && (self.interval == 0
                || elapsed.as_secs() < next
                || self
                    .next
                    .compare_exchange(
                        next,
                        elapsed.as_secs().saturating_add(self.interval),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_err())
        {
            return;
        }
        let credit = if !force && !crate::cpu_budget::CpuBudget::current_thread_has_permit() {
            let execution = self
                .execution
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            match execution {
                Some(e) => match e.compute(crate::cpu_budget::Stage::Sink) {
                    Ok(p) => p,
                    Err(_) => return,
                },
                None => None,
            }
        } else {
            None
        };
        let mut event=json!({"event":"progress","sample":self.sample,"stage":stage,"source_units_observed":self.input.load(Ordering::Relaxed),"accepted_units_observed":self.accepted.load(Ordering::Relaxed),"written_channel_records_observed":[self.written_records[0].load(Ordering::Relaxed),self.written_records[1].load(Ordering::Relaxed)],"elapsed_seconds":elapsed.as_secs_f64(),"scope":"observed complete units/chunk appends; not completion or durable counts"});
        if self.qc_only.load(Ordering::Relaxed){event["accepted_units_observed"]=Value::Null;event["written_channel_records_observed"]=Value::Null;event["not_applicable_reason"]=json!("QC-only: no acceptance or FASTQ writers");}
        let line=event.to_string();
        drop(credit);
        use std::io::Write;
        let _ = writeln!(std::io::stderr(), "{line}");
    }
}
#[cfg(target_os = "linux")]
pub fn process_cpu_seconds() -> Option<f64> {
    #[repr(C)]
    struct Timespec {
        sec: std::os::raw::c_long,
        nsec: std::os::raw::c_long,
    }
    unsafe extern "C" {
        fn clock_gettime(clock: std::os::raw::c_int, value: *mut Timespec) -> std::os::raw::c_int;
    }
    let mut value = Timespec { sec: 0, nsec: 0 };
    // Linux CLOCK_PROCESS_CPUTIME_ID observes all threads of this process.
    if unsafe { clock_gettime(2, &mut value) } == 0 {
        Some(value.sec as f64 + value.nsec as f64 * 1e-9)
    } else {
        None
    }
}
#[cfg(not(target_os = "linux"))]
pub fn process_cpu_seconds() -> Option<f64> {
    None
}
pub fn metrics(
    enabled: bool,
    started: Instant,
    cpu_start: Option<f64>,
    output_bytes: Option<u64>,
) -> Value {
    let cpu = if enabled {
        process_cpu_seconds()
            .zip(cpu_start)
            .map(|(end, start)| (end - start).max(0.0))
    } else {
        None
    };
    let rss = if enabled {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|text| {
                text.lines().find_map(|line| {
                    line.strip_prefix("VmHWM:")
                        .and_then(|v| v.split_whitespace().next())
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(|v| v * 1024)
                })
            })
    } else {
        None
    };
    let reason = if enabled {
        "platform observation unavailable"
    } else {
        "disabled by --metrics=false"
    };
    json!({"enabled":enabled,"wall_seconds":if enabled{Some(started.elapsed().as_secs_f64())}else{None},"wall_seconds_unavailable_reason":if enabled{None}else{Some(reason)},"cpu_seconds":cpu,"cpu_seconds_unavailable_reason":if cpu.is_none(){Some(reason)}else{None},"peak_rss_bytes":rss,"peak_rss_bytes_unavailable_reason":if rss.is_none(){Some(reason)}else{None},"output_bytes":output_bytes,"output_bytes_unavailable_reason":if output_bytes.is_none(){Some("one or more output byte observations unavailable")}else{None},"method":"wall Instant and Linux process CPU from execution start to report preparation; peak RSS Linux /proc/self/status VmHWM; output bytes final-or-staged artifact metadata, possibly partial on failure; excludes report serialization/publication; external time harness measures whole command"})
}
