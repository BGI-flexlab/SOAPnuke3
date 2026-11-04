use crate::{
    job_api::*,
    dedup_runtime::{Channel, Runtime},
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
#[derive(Clone, Debug)]
pub(crate) struct Plan {
    pub common: crate::native_config::RunPlan,
    pub spec: JobSpec,
    pub channels: Vec<Channel>,
    pub spill: PathBuf,
    pub values: BTreeMap<String, String>,
    pub configured: Vec<String>,
    pub supplied: Vec<String>,
    pub catalogue: Option<CatalogueMetadata>,
}
#[derive(Clone, Debug)]
pub(crate) struct CatalogueMetadata {
    pub schema: String,
    pub catalogue_version: Option<String>,
    pub sha256: String,
    pub path: PathBuf,
}
impl CatalogueMetadata {
    pub(crate) fn from_catalogue(c: &crate::resources_app::Catalogue) -> Self {
        Self {
            schema: c.value["schema"].as_str().unwrap().into(),
            catalogue_version: c
                .value
                .get("catalogue_version")
                .and_then(Value::as_str)
                .map(str::to_owned),
            sha256: c.sha256.clone(),
            path: c.path.clone(),
        }
    }
    pub(crate) fn capacity(&self) -> Option<usize> {
        std::mem::size_of::<Self>()
            .checked_add(self.schema.capacity())?
            .checked_add(self.catalogue_version.as_ref().map_or(0, String::capacity))?
            .checked_add(self.sha256.capacity())?
            .checked_add(self.path.capacity())
    }
}
fn required(v: &BTreeMap<String, String>, k: &str) -> Result<String, String> {
    v.get(k)
        .filter(|s| !s.is_empty())
        .cloned()
        .ok_or_else(|| format!("{k} required"))
}
fn boolean(v: &BTreeMap<String, String>, k: &str) -> Result<bool, String> {
    match v.get(k).map(String::as_str) {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        _ => Err(format!("{k} requires boolean")),
    }
}
fn format(v: &BTreeMap<String, String>, k: &str) -> Result<Format, String> {
    match v.get(k).map_or("auto", String::as_str) {
        "auto" => Ok(Format::Auto),
        "fastq" => Ok(Format::Fastq),
        "gzip" => Ok(Format::Gzip),
        _ => Err(format!("invalid {k}")),
    }
}
fn phred(v: &BTreeMap<String, String>, k: &str) -> Result<u8, String> {
    let n = v
        .get(k)
        .map_or("33", String::as_str)
        .parse::<u64>()
        .map_err(|_| format!("invalid {k}"))?;
    if ![33, 64].contains(&n) {
        return Err(format!("invalid {k}: requires33 or64"));
    }
    Ok(n as u8)
}
pub(crate) fn config_value(k: &str, v: &Value) -> Result<String, String> {
    if !crate::option_metadata::lookup(crate::option_metadata::Job::Dedup,k).is_some() || k == "config" {
        return Err(format!("unknown dedup config option {k}"));
    }
    if k == "key" && v.as_str() != Some("full-sequence") {
        return Err("config key must be full-sequence".into());
    }
    if ["key", "duplicates1", "duplicates2", "spill-dir"].contains(&k) {
        return v
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("config {k} requires string"));
    }
    crate::native_config::config_arguments(k, v)?;
    Ok(match v {
        Value::String(s) => s.clone(),
        _ => v.to_string(),
    })
}
pub(crate) fn parse(args: &[String]) -> Result<Plan, String> {
    let args = crate::native_cli::normalize_demux(args)?;
    let mut v = BTreeMap::new();
    for token in args {
        let (k, value) = token
            .strip_prefix("--")
            .and_then(|s| s.split_once('='))
            .ok_or("named dedup option required")?;
        if !crate::option_metadata::lookup(crate::option_metadata::Job::Dedup,k).is_some() {
            return Err(format!("dedup forbids option {k}"));
        }
        if v.insert(k.into(), value.into()).is_some() {
            return Err(format!("duplicate dedup option {k}"));
        }
    }
    let supplied = v.keys().cloned().collect::<Vec<_>>();
    let mut configured = vec![];
    if let Some(path) = v.get("config") {
        let text = fs::read_to_string(path).map_err(|e| e.to_string())?;
        for (k, value) in crate::native_config::decode_job_config(&text, "dedup")? {
            let value = config_value(&k, &value)?;
            configured.push(k.clone());
            v.entry(k).or_insert(value);
        }
    }
    compile(v, supplied, configured, None)
}
fn position(name: &str) -> Option<(bool, usize)> {
    match name {
        "R1" | "r1" | "out1" | "representative.R1" => Some((false, 0)),
        "R2" | "r2" | "out2" | "representative.R2" => Some((false, 1)),
        "out-interleaved" | "representative.interleaved" => Some((false, usize::MAX)),
        "duplicates1" | "duplicate.R1" => Some((true, 0)),
        "duplicates2" | "duplicate.R2" => Some((true, 1)),
        _ => None,
    }
}
fn path_id(p: &Path) -> Result<PathBuf, String> {
    crate::job_api::path_identity(p)
}
fn compile(
    v: BTreeMap<String, String>,
    supplied: Vec<String>,
    configured: Vec<String>,
    declared: Option<&JobSpec>,
) -> Result<Plan, String> {
    boolean(&v, "help")?;
    boolean(&v, "version")?;
    if v.get("key").is_some_and(|s| s != "full-sequence") {
        return Err("key must be full-sequence".into());
    }
    let spill = PathBuf::from(required(&v, "spill-dir")?);
    if spill.as_os_str() == "-" || spill.is_file() {
        return Err("spill-dir requires a workspace directory".into());
    }
    let spill = path_id(&spill)?;
    let report = if let Some(s) = declared {
        s.json_report.clone()
    } else {
        Some(PathBuf::from(required(&v, "report")?))
    };
    if report.as_ref().is_some_and(|p| p.as_os_str() == "-") {
        return Err("report cannot be stdout".into());
    }
    let input_phred = phred(&v, "input-phred")?;
    let output_phred = phred(&v, "output-phred")?;
    let input_format = format(&v, "input-format")?;
    let output_format = format(&v, "output-format")?;
    let (mut layout, mut lanes) = if let Some(s) = declared {
        (s.layout.clone(), s.lanes.clone())
    } else if let Some(path) = v.get("lanes") {
        if ["r1", "r2", "interleaved"]
            .iter()
            .any(|k| v.contains_key(*k))
        {
            return Err("lanes excludes r1/r2/interleaved".into());
        }
        if !v.contains_key("sample-id") {
            return Err("lanes requires sample-id".into());
        }
        let (text, parent, _, _) = crate::native_config::inspect_lanes(path)?;
        let mut lanes = Vec::new();
        for line in text.lines().skip(1) {
            let f = line.split('\t').collect::<Vec<_>>();
            let mut inputs = vec![];
            for (i, key) in ["R1", "R2"].iter().enumerate() {
                if !f[i + 1].is_empty() {
                    let path = PathBuf::from(f[i + 1]);
                    let path = if path.is_absolute() {
                        path
                    } else {
                        parent.join(path)
                    };
                    inputs.push(InputDecl {
                        channel: (*key).into(),
                        location: Location::Path(path),
                        format: input_format.clone(),
                        phred: input_phred,
                        seekable: true,
                    });
                }
            }
            lanes.push(LaneDecl {
                id: f[0].into(),
                inputs,
            });
        }
        let layout = if lanes[0].inputs.len() == 2 {
            Layout::SplitPe
        } else {
            Layout::Se
        };
        (layout, lanes)
    } else {
        let interleaved = boolean(&v, "interleaved")?;
        if interleaved && v.contains_key("r2") {
            return Err("interleaved/r2 layout conflict".into());
        }
        let r1 = required(&v, "r1")?;
        if r1 == "-" && !v.contains_key("sample-id") {
            return Err("stdin requires sample-id".into());
        }
        let r2 = v.get("r2");
        if r2.is_some_and(|p| p == "-") {
            return Err("r2 cannot be stdin".into());
        }
        let mut inputs = vec![InputDecl {
            channel: "R1".into(),
            location: if r1 == "-" {
                Location::Handle("stdin".into())
            } else {
                Location::Path(r1.into())
            },
            format: input_format.clone(),
            phred: input_phred,
            seekable: !v.get("r1").is_some_and(|p| p == "-"),
        }];
        if let Some(r2) = r2 {
            inputs.push(InputDecl {
                channel: "R2".into(),
                location: Location::Path(r2.into()),
                format: input_format.clone(),
                phred: input_phred,
                seekable: true,
            });
        }
        (
            if interleaved {
                Layout::InterleavedPe
            } else if r2.is_some() {
                Layout::SplitPe
            } else {
                Layout::Se
            },
            vec![LaneDecl {
                id: "input".into(),
                inputs,
            }],
        )
    };
    if lanes.is_empty() {
        return Err("dedup requires R1 input".into());
    }
    let roles = if layout == Layout::SplitPe {
        vec!["R1", "R2"]
    } else {
        vec!["R1"]
    };
    let mut ids = BTreeSet::new();
    let mut handles = BTreeSet::new();
    for lane in &lanes {
        if lane.id.is_empty()
            || !ids.insert(&lane.id)
            || lane.inputs.len() != roles.len()
            || lane.inputs.iter().zip(&roles).any(|(d, k)| d.channel != *k)
        {
            return Err("dedup input layout requires exact ordered R1/R2 roles".into());
        }
        for d in &lane.inputs {
            if ![33, 64].contains(&d.phred) {
                return Err("invalid input-phred".into());
            }
            if let Location::Handle(h) = &d.location {
                if h.is_empty() || !handles.insert(h) {
                    return Err("duplicate input handle".into());
                }
            }
        }
    }
    let paired = layout != Layout::Se;
    let mut outputs = declared.map_or_else(Vec::new, |s| s.outputs.clone());
    for (k, channel) in [
        ("out1", "representative.R1"),
        ("out2", "representative.R2"),
        ("out-interleaved", "representative.interleaved"),
        ("duplicates1", "duplicate.R1"),
        ("duplicates2", "duplicate.R2"),
    ] {
        if let Some(path) = v.get(k) {
            let pos = position(channel).unwrap();
            if !outputs.iter().any(|d| position(&d.channel) == Some(pos)) {
                if path == "-" && ((pos.1 != usize::MAX && paired) || pos.1 == 1) {
                    return Err("stdout requires SE or paired interleaved destination".into());
                }
                outputs.push(OutputDecl {
                    channel: channel.into(),
                    location: if path == "-" {
                        Location::Handle("stdout".into())
                    } else {
                        Location::Path(path.into())
                    },
                    format: output_format.clone(),
                    phred: output_phred,
                });
            }
        }
    }
    let mut slots = BTreeSet::new();
    let mut out_handles = BTreeSet::new();
    let mut channels = vec![];
    for d in &outputs {
        let (duplicate, mate) = position(&d.channel).ok_or("unknown dedup output channel")?;
        if !slots.insert((duplicate, mate)) {
            return Err("duplicate output channel".into());
        }
        if !paired && mate != 0 {
            return Err("R2/interleaved/duplicates2 requires paired input".into());
        }
        if ![33, 64].contains(&d.phred) {
            return Err("invalid output-phred".into());
        }
        if let Location::Handle(h) = &d.location {
            if h.is_empty() || !out_handles.insert(h) {
                return Err("duplicate output handle".into());
            }
        }
        let logical = if mate == usize::MAX {
            "representative.interleaved".into()
        } else {
            format!(
                "{}.R{}",
                if duplicate {
                    "duplicate"
                } else {
                    "representative"
                },
                mate + 1
            )
        };
        let codec = crate::job_api::output_codec(d);
        if declared.is_none()
            && matches!(&d.location, Location::Handle(_))
            && (!matches!(d.format, Format::Fastq) || codec != "plain")
        {
            return Err("stdout requires explicit fastq output-format".into());
        }
        channels.push(Channel {
            logical,
            io: d.channel.clone(),
            path: match &d.location {
                Location::Path(p) => path_id(p)?,
                _ => PathBuf::new(),
            },
            codec,
            phred: d.phred as i32,
            duplicate,
            mate,
        });
    }
    if slots.contains(&(false, usize::MAX)) {
        if slots.contains(&(false, 0)) || slots.contains(&(false, 1)) {
            return Err("out-interleaved excludes out1/out2".into());
        }
    } else if !slots.contains(&(false, 0)) || (paired && !slots.contains(&(false, 1))) {
        return Err("out1/out2 required for input layout".into());
    }
    if paired && (slots.contains(&(true, 0)) != slots.contains(&(true, 1))) {
        return Err("duplicates1/duplicates2 require paired declarations".into());
    }
    let spec = JobSpec {
        configured: declared.map_or_else(BTreeMap::new, |s| s.configured.clone()),
        supplied: declared.map_or_else(
            || {
                v.iter()
                    .map(|(k, value)| (k.clone(), Setting::Text(value.clone())))
                    .collect()
            },
            |s| s.supplied.clone(),
        ),
        layout,
        lanes,
        outputs,
        json_report: report,
    };
    let mut input_spec = spec.clone();
    input_spec.outputs.clear();
    let mut args = vec![];
    for k in [
        "input-format",
        "input-phred",
        "output-format",
        "output-phred",
        "threads",
        "decoder",
        "memory-mib",
        "progress-seconds",
        "metrics",
        "sample-id",
        "report",
        "html-report",
    ] {
        if let Some(value) = v.get(k) {
            args.extend([format!("--{k}"), value.clone()]);
        }
    }
    let mut common = crate::native_config::parse_api(&args, &input_spec)?;
    let level = v
        .get("gzip-level")
        .map_or("4", String::as_str)
        .parse::<u32>()
        .map_err(|_| "gzip-level integer")?;
    if !(1..=9).contains(&level) {
        return Err("gzip-level must be1..9".into());
    }
    if v.contains_key("gzip-level") && !channels.iter().any(|c| c.codec == "gzip") {
        return Err("explicit gzip-level without gzip output".into());
    }
    common.gzip_level = level;
    if declared.is_some() {
        for (k, p) in [
            ("report", spec.json_report.as_ref()),
            ("html-report", common.html_report.as_ref()),
        ] {
            if p.is_some_and(|p| p.is_dir()) {
                return Err(format!("{k} destination is a directory"));
            }
        }
    }
    let extra = common.html_report.iter().cloned().collect::<Vec<_>>();
    let mut protected = spec
        .lanes
        .iter()
        .flat_map(|l| l.inputs.iter())
        .filter_map(|d| match &d.location {
            Location::Path(p) => Some(path_id(p)),
            _ => None,
        })
        .collect::<Result<Vec<_>, _>>()?;
    for k in ["config", "lanes", "resource-catalog"] {
        if let Some(p) = v.get(k) {
            protected.push(path_id(Path::new(p))?);
        }
    }
    if declared.is_none() && protected.contains(&spill) {
        return Err("spill-dir aliases an input/resource path".into());
    }
    for p in spec.json_report.iter().chain(extra.iter()) {
        if path_id(p)? == spill {
            return Err("spill-dir aliases report/html-report".into());
        }
    }
    let mut destinations = BTreeSet::new();
    for p in channels
        .iter()
        .filter(|c| !c.path.as_os_str().is_empty())
        .map(|c| &c.path)
        .chain(spec.json_report.iter())
        .chain(extra.iter())
    {
        let id = path_id(p)?;
        if (declared.is_none() && id == spill)
            || protected.contains(&id)
            || !destinations.insert(id)
            || p.symlink_metadata().is_ok()
        {
            return Err("input/output/report alias or existing output".into());
        }
    }
    crate::native_execution::Execution::validate_budget_with_statistics(
        common.threads,
        if paired { 2 } else { 1 },
        common.memory_total_bytes,
        Runtime::statistics_bound(channels.len())?,
    ).map_err(|e| e.to_string())?;
    let catalogue = if declared.is_none() {
        v.get("resource-catalog")
            .map(|p| {
                crate::resources_app::Catalogue::load(Path::new(p))
                    .map(|c| CatalogueMetadata::from_catalogue(&c))
                    .map_err(|e| format!("{}: {}", e.code, e.message))
            })
            .transpose()?
    } else {
        None
    };
    Ok(Plan {
        common,
        spec,
        channels,
        spill,
        values: v,
        configured,
        supplied,
        catalogue,
    })
}
pub(crate) fn compile_api(s: &JobSpec) -> Result<Plan, String> {
    let mut v = BTreeMap::new();
    for m in [&s.configured, &s.supplied] {
        for (k, value) in m {
            v.insert(
                k.clone(),
                config_value(k, &crate::job_api::setting_json(value))?,
            );
        }
    }
    compile(
        v,
        s.supplied.keys().cloned().collect(),
        s.configured.keys().cloned().collect(),
        Some(s),
    )
}
pub(crate) fn check_spill_bindings(plan: &Plan, io: &JobIo) -> Result<(), String> {
    let mut consumed = [false; 2];
    for d in plan.spec.lanes.iter().flat_map(|l| &l.inputs) {
        let key = match &d.location {
            Location::Handle(h) => h.as_str(),
            Location::Path(_) => d.channel.as_str(),
        };
        let role = match key {
            "R1" => Some(0),
            "R2" => Some(1),
            _ => None,
        };
        let available = io.inputs.contains_key(key) && role.is_none_or(|i| !consumed[i]);
        if available {
            if let Some(i) = role {
                consumed[i] = true;
            }
        }
        match &d.location {
            Location::Handle(_) if !available => {
                return Err("missing or previously consumed input binding".into())
            }
            Location::Path(p) if !available && path_id(p)? == plan.spill => {
                return Err("spill-dir aliases an effective input path".into())
            }
            _ => {}
        }
    }
    for d in &plan.spec.outputs {
        if let Location::Path(p) = &d.location {
            if !io.outputs.contains_key(&d.channel) && path_id(p)? == plan.spill {
                return Err("spill-dir aliases an effective output path".into());
            }
        }
    }
    Ok(())
}
struct Stdout;
impl std::io::Write for Stdout {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        std::io::stdout().write(b)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()
    }
}
impl ReportWriter for Stdout {
    fn close(&mut self) -> std::io::Result<()> {
        self.flush()
    }
}
#[cfg(test)]
thread_local! { pub(crate) static REPORT_FAULT: std::cell::Cell<u8> = const { std::cell::Cell::new(0) }; pub(crate) static R4_UNLINK:std::cell::Cell<bool>=const{std::cell::Cell::new(false)}; static LAST_EXECUTION:std::cell::RefCell<std::sync::Weak<crate::native_execution::Execution>>=std::cell::RefCell::new(std::sync::Weak::new()); }
pub(crate) fn report_ticket(e: &Arc<crate::native_execution::Execution>, bytes: usize, site: u8) -> Result<crate::native_execution::FeatureTicket,String> {
    #[cfg(test)]
    let bytes = if REPORT_FAULT.with(|v| (v.get()>0 && v.get()%10 == site) || (site == 5 && v.get() >= 10)) { usize::MAX } else { bytes };
    #[cfg(not(test))]
    let _ = site;
    e.reserve_finalization_feature(bytes).map_err(|e| e.to_string())
}
fn error_report(message: &str) -> NativeReport {
    let q = crate::native_qc::Qc::default();
    NativeReport{status:"failed",input_units:0,accepted_units:None,written_units:Some(0),json:json!({"schema":"soapnuke-v3.report/2","status":"failed","run":{"job":"dedup"},"populations":{"input":crate::native_qc::population(&[&q],"input",0,"partial",false),"accepted":null,"written":crate::native_qc::written_population(&[&q],0,"partial",false)},"api":{"execution_admitted":false},"error":{"stage":"admission","message":message},"dedup":{"dedup":{"input_units":0,"representative_units":null,"duplicate_units":null,"grouping_complete":false},"api":{"input_units":0,"accepted_units":null,"written_units":0}}}).to_string()}
}
pub(crate) fn run(
    plan: &Plan,
    provided: Option<Arc<crate::native_io::OwnedIo>>,
) -> (i32, NativeReport, Option<&'static str>) {
    let api = provided.is_some();
    let c = &plan.common;
    let started = std::time::Instant::now();
    let cpu_start = crate::native_runtime::process_cpu_seconds();
    let e = match &provided {
        Some(io) => Ok(io.execution.clone()),
        None => crate::native_execution::Execution::with_statistics_budget(
            c.threads,
            if plan.spec.layout == Layout::Se { 1 } else { 2 },
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            c.memory_total_bytes,
            Runtime::statistics_bound(plan.channels.len()).unwrap(),
        ),
    };
    let e = match e {
        Ok(e) => e,
        Err(message) => return (5, error_report(&message.to_string()), Some("memory")),
    };
    #[cfg(test)] LAST_EXECUTION.with(|v| *v.borrow_mut()=Arc::downgrade(&e));
    let mut emergency = match crate::dedup_report::Emergency::new(&e) { Ok(v)=>v, Err(err)=>return (5,error_report(&err),Some("memory")) };
    let mut publication = match plan.spec.json_report.as_deref().map(|p|crate::dedup_publication::Publication::new(p,&e)).transpose(){Ok(v)=>v,Err(err)=>return(5,error_report(&err),Some("memory"))};
    let _catalogue_input = if !api {
        match plan
            .catalogue
            .as_ref()
            .map_or(Some(0), CatalogueMetadata::capacity)
            .and_then(|n| e.reserve_preparation_feature(n).ok())
        {
            Some(t) => Some(t),
            None => {
                return (
                    5,
                    error_report("catalogue metadata does not fit managed budget"),
                    Some("memory"),
                )
            }
        }
    } else {
        None
    };
    let path_bound = match plan
        .spill
        .capacity()
        .checked_add(128)
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| n.checked_add(65536))
    {
        Some(n) => n,
        None => {
            return (
                5,
                error_report("spill path capacity overflow"),
                Some("memory"),
            )
        }
    };
    let _stage_path = match e.reserve_preparation_feature(path_bound) {
        Ok(t) => t,
        Err(err) => return (5, error_report(&err.to_string()), Some("memory")),
    };
    if let Err(err) = fs::create_dir_all(&plan.spill) {
        return (4, error_report(&err.to_string()), Some("output"));
    }
    let directory = plan.spill.join(format!(
        ".soapnuke-dedup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    if let Err(err) = fs::create_dir(&directory) {
        return (4, error_report(&err.to_string()), Some("output"));
    }
    let scratch = crate::dedup_finalize::Scratch::new(directory, _stage_path);
    let directory = &scratch.path;
    let mut transferred_report_ticket = None;
    let mut execution_admitted = false;
    let mut terminal_event = None;
    let mut result = (|| {
    let mut header_working=match crate::dedup_report::HeaderWorking::new(plan,directory,&e,provided.as_deref()){Ok(v)=>v,Err(err)=>return(5,error_report(&err),Some("memory"))};
    let runtime = match Runtime::new(&directory, &plan.channels, &e) {
        Ok(v) => Arc::new(v),
        Err(err) => return (5, error_report(&err.to_string()), Some("memory")),
    };
    let io = if let Some(io) = provided {
        io
    } else {
        let mut streams = JobIo {
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        };
        for d in plan.spec.lanes.iter().flat_map(|l| &l.inputs) {
            if let Location::Handle(h) = &d.location {
                streams
                    .inputs
                    .insert(h.clone(), InputHandle::Stream(Box::new(std::io::stdin())));
            }
        }
        for d in &plan.spec.outputs {
            if let Location::Handle(h) = &d.location {
                streams.outputs.insert(h.clone(), Box::new(Stdout));
            }
        }
        let n = crate::job_api::metadata_bound(&plan.spec)
            .and_then(|n| n.checked_mul(8))
            .unwrap_or(usize::MAX);
        let ticket = match e.reserve_preparation_feature(n) {
            Ok(t) => t,
            Err(err) => return (5, error_report(&err.to_string()), Some("memory")),
        };
        Arc::new(crate::native_io::OwnedIo::new(
            plan.spec.clone(),
            streams,
            CancellationToken::default(),
            e.clone(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ticket,
        ))
    };
    let event = Arc::new(crate::native_runtime::Runtime::new(
        c.progress_seconds,
        c.sample_id.clone(),
    ));
    terminal_event=Some(event.clone());
    event.event("starting", true);
    let mut gp = crate::params::GlobalParams::new();
    gp.native_m1 = true;
    gp.native_dedup = Some(runtime.clone());
    gp.native_io = Some(io.clone());
    gp.native_runtime = Some(event.clone());
    gp.native_interleaved = plan.spec.layout == Layout::InterleavedPe;
    gp.threads_num = c.threads;
    gp.native_memory_total = c.memory_total_bytes;
    gp.native_gzip_level = c.gzip_level as i32;
    gp.quality_phred = c.input_phred;
    gp.output_quality_phred = c.output_phred;
    gp.patch_size = 2500;
    gp.output_dir = directory.to_string_lossy().into_owned();
    gp.fq1_path = c
        .inputs
        .first()
        .map_or(String::new(), |p| p.to_string_lossy().into_owned());
    gp.fq2_path = c
        .inputs
        .get(1)
        .map_or(String::new(), |p| p.to_string_lossy().into_owned());
    let mut stats = crate::stats::GlobalStats::new(&gp);
    let mut code = 0;
    let mut failure = None;
    let mut message = None;
    let mut core = None;
    execution_admitted = true;
    match crate::pipeline::run_filter_typed(&gp, &mut stats) {
        Err(err) => {
            code = err.category.exit();
            failure = Some(if code == 3 {
                "input"
            } else if code == 5 {
                "memory"
            } else {
                "output"
            });
            message = Some(err.to_string());
            runtime.abort_grouping();
        }
        Ok(()) => {
        #[cfg(test)] if REPORT_FAULT.with(|v|v.get()==21){io.cancel.cancel();}
        match runtime.classify(&io) {
            Err(err) => {
                code = if io.cancel.is_cancelled() { 3 } else { 4 };
                failure = Some(if io.cancel.is_cancelled() {
                    "cancelled"
                } else {
                    "grouping"
                });
                message = Some(err.to_string());
                runtime.abort_grouping();
            }
            Ok(facts) => {
                core=Some(facts);
                if let Err(err) = runtime.emit(&gp) {
                    code = err.category.exit();
                    failure = Some(if io.cancel.is_cancelled() {
                        "cancelled"
                    } else if code == 3 {
                        "input"
                    } else {
                        "output"
                    });
                    message = Some(err.to_string());
                }
            }
        }},
    }
    if code != 0 {
        if let Some(err) = runtime.state.lock().unwrap().error.as_ref() {
            code = err.code;
            failure = Some(if code == 3 {
                "input"
            } else if code == 5 {
                "memory"
            } else {
                "output"
            });
        }
    }
    let completion = crate::dedup_finalize::Finalization {
        plan, execution: &e, io: &io, runtime: &runtime, working: &mut header_working,
        scratch: &scratch, emergency: &mut emergency, publication: &mut publication,
        api, started, cpu_start,
    }.finish(crate::dedup_finalize::Outcome { code, category: failure, message, core });
    transferred_report_ticket = completion.report_ticket;
    drop(stats);
    drop(gp);
    drop(runtime);
    drop(io);
    completion.result
    })();
    result = crate::dedup_finalize::after_return(result, execution_admitted, &scratch, &mut emergency, &e);
    if let Some(event)=terminal_event{event.event(if result.0==0{"complete"}else{"failed"},true);}
    result
}
pub(crate) fn entry(args: &[String]) -> i32 {
    let normalized = match crate::native_cli::normalize_demux(args) {
        Ok(v) => v,
        Err(err) => {
            eprintln!("config: {err}");
            return 2;
        }
    };
    if normalized.iter().any(|s| s == "--help=true") {
        print!("{}",include_str!("dedup_help.txt"));
        return 0;
    }
    if normalized.iter().any(|s| s == "--version=true") {
        crate::native_config::help(true);
        return 0;
    }
    for k in ["report", "html-report"] {
        if normalized
            .iter()
            .find_map(|s| s.strip_prefix(&format!("--{k}=")))
            .is_some_and(|p| Path::new(p).is_dir())
        {
            eprintln!("{k} publication destination is a directory");
            return 3;
        }
    }
    match parse(args) {
        Err(err) => {
            eprintln!("config: {err}");
            2
        }
        Ok(p) => {
            let (code, _, _) = run(&p, None);
            if code == 4 {
                3
            } else {
                code
            }
        }
    }
}

#[cfg(test)]
mod report_failure_tests {
 use super::*;
 #[test]
 fn corrected_postwork_header_denial_preserves_nonzero_prefix() {
  let root=crate::test_support::workspace("r2").join("held-header");fs::create_dir(&root).unwrap();
  let input=root.join("in.fastq");let out=root.join("out.fastq");let spill=root.join("spill");fs::write(&input,b"@u full\nACGTCC\n+\nIIIIII\n").unwrap();
  let args=vec!["--r1".into(),input.to_string_lossy().into_owned(),"--out1".into(),out.to_string_lossy().into_owned(),"--spill-dir".into(),spill.to_string_lossy().into_owned(),"--report".into(),root.join("report.json").to_string_lossy().into_owned(),"--sample-id".into(),"literal".into(),"--threads".into(),"1".into(),"--input-format".into(),"fastq".into(),"--output-format".into(),"fastq".into()];
  let plan=parse(&args).unwrap();REPORT_FAULT.with(|v|v.set(1));println!("R84_R2_NATIVE_RUN_ENTER");let(code,report,category)=run(&plan,None);REPORT_FAULT.with(|v|v.set(0));
  fs::write(root.join("returned.json"),&report.json).unwrap();let children=fs::read_dir(&spill).unwrap().map(|v|v.unwrap().path()).collect::<Vec<_>>();
  fs::write(root.join("observed.json"),json!({"code":code,"category":category,"I":report.input_units,"A":report.accepted_units,"W":report.written_units,"output_exists":out.exists(),"spill_children":children}).to_string()).unwrap();
  assert_ne!(code,0);assert_eq!((report.input_units,report.accepted_units,report.written_units),(1,Some(1),Some(1)));let v:Value=serde_json::from_str(&report.json).unwrap();assert_eq!(v["api"]["execution_admitted"],true);assert_eq!(v["dedup"]["dedup"]["grouping_complete"],true);assert_eq!(v["populations"]["input"]["read_records"],1);assert_eq!(v["populations"]["input"]["bases"],6);
  for dir in children{for file in fs::read_dir(dir).unwrap(){let file=file.unwrap();assert_eq!(file.file_name(),"channel-00.fq");assert_eq!(fs::read(file.path()).unwrap(),b"@u full\nACGTCC\n+\nIIIIII\n");}}
 }
}

#[cfg(test)]
mod r2_panel {
 use super::*;
 pub(super) fn job(name:&str,site:u8,storage:u8,bad:bool){
  let root=crate::test_support::workspace("r2").join(name);fs::create_dir(&root).unwrap();let input=root.join("in.fastq");let output=root.join("out.fastq");let spill=root.join("spill");let report=root.join("report.json");
  let mut input_bytes=b"@u full\nACGTCC\n+\nIIIIII\n".to_vec();if bad{input_bytes.extend_from_slice(b"@bad\nAAAAAA\n+\n\x01\x01\x01\x01\x01\x01\n");}fs::write(&input,&input_bytes).unwrap();
  let mut args=vec!["--r1".into(),input.to_string_lossy().into_owned(),"--out1".into(),output.to_string_lossy().into_owned(),"--spill-dir".into(),spill.to_string_lossy().into_owned(),"--report".into(),report.to_string_lossy().into_owned(),"--sample-id".into(),"literal".into(),"--threads".into(),"1".into(),"--input-format".into(),"fastq".into(),"--output-format".into(),"fastq".into()];if site%10==3{args.extend(["--html-report".into(),root.join("report.html").to_string_lossy().into_owned()]);}
  let plan=parse(&args).unwrap();REPORT_FAULT.with(|v|v.set(site));crate::dedup_grouping::STORAGE_FAULT.with(|v|v.set(storage));println!("R84_R2_NATIVE_RUN_ENTER name={name}");let(code,result,category)=run(&plan,None);REPORT_FAULT.with(|v|v.set(0));crate::dedup_grouping::STORAGE_FAULT.with(|v|v.set(0));
  fs::write(root.join("returned.json"),&result.json).unwrap();let v:Value=serde_json::from_str(&result.json).unwrap();let known=!bad&&storage==0&&site!=21;assert_eq!(result.input_units,1);assert_eq!(result.accepted_units,if known{Some(1)}else{None});assert_eq!(result.written_units,Some(if known{1}else{0}));assert_eq!(v["dedup"]["dedup"]["grouping_complete"],known);assert_eq!(v["dedup"]["api"]["input_units"],1);assert_eq!(v["dedup"]["api"]["accepted_units"],if known{json!(1)}else{Value::Null});
  if site==0&&storage==0{assert_eq!(code,0);assert_eq!(v["status"],"complete");assert_eq!(fs::read(&output).unwrap(),b"@u full\nACGTCC\n+\nIIIIII\n");}else{assert_ne!(code,0);assert_eq!(v["status"],"failed");if site>=10||site==1||site==2{assert_eq!(v["api"]["execution_admitted"],true);}}
  if bad{assert_eq!(category,Some("input"));assert_eq!(v["error"]["source_unit_ordinal"],1);}if site==21{assert_eq!(category,Some("cancelled"));}if site%10==3{assert_eq!(category,Some("output"));}if site%10==4{assert_eq!(category,Some("report-publication"));}
  let p=if v["populations"]["input"].is_null(){assert_eq!(v["report_generation"]["detailed_qc"],"unavailable_report_generation");&v["population_summaries"]["input"]}else{&v["populations"]["input"]};assert_eq!(p["read_records"],1);assert_eq!(p["bases"],6);assert_eq!(p["q20_bases"],6);assert_eq!(p["q30_bases"],6);
  for dir in fs::read_dir(&spill).unwrap(){for file in fs::read_dir(dir.unwrap().path()).unwrap(){let file=file.unwrap();assert_eq!(file.file_name(),"channel-00.fq");assert_eq!(fs::read(file.path()).unwrap(),b"@u full\nACGTCC\n+\nIIIIII\n");}}
  assert!(LAST_EXECUTION.with(|v|v.borrow().upgrade().is_none()),"Execution/tickets must have no retained strong owners after transfer");let open=fs::read_dir("/proc/self/fd").unwrap().filter_map(|v|fs::read_link(v.ok()?.path()).ok()).filter(|p|p.starts_with(&root)).count();assert_eq!(open,0);
 }
 #[test]fn body_known(){job("body",2,0,false)}
 #[test]fn header_emergency_known(){job("emergency",11,0,false)}
 #[test]fn html_reencode(){job("html",3,0,false)}
 #[test]fn json_reencode(){job("json",4,0,false)}
 #[test]fn input_prefix_emergency(){job("input",12,0,true)}
 #[test]fn cancellation_emergency(){job("cancel",21,0,false)}
 #[test]fn storage_quota(){job("quota",0,1,false)}
 #[test]fn storage_partial_write(){job("partial",0,2,false)}
 #[test]fn storage_flush(){job("flush",0,3,false)}
 #[test]fn storage_close(){job("close",0,4,false)}
 #[test]fn normal_cleanup_publication(){job("normal",0,0,false);crate::dedup_publication::proof(&crate::test_support::workspace("r2").join("publication-conditions"));}
}

#[cfg(test)]mod r4_prefix {
 use super::*;
 fn probe(name:&str,emergency:bool){
  let root=crate::test_support::workspace("r4").join(name);fs::create_dir(&root).unwrap();let input=root.join("input.fastq");fs::write(&input,b"@u full\nACGTCC\n+\nIIIIII\n@bad\nAAAAAA\n+\n\x01\x01\x01\x01\x01\x01\n").unwrap();let report=root.join("report.json");let output=root.join("out.fastq");let spill=root.join("spill");
  let args=vec!["--r1".into(),input.to_string_lossy().into_owned(),"--out1".into(),output.to_string_lossy().into_owned(),"--spill-dir".into(),spill.to_string_lossy().into_owned(),"--report".into(),report.to_string_lossy().into_owned(),"--sample-id".into(),"literal".into(),"--threads".into(),"1".into(),"--input-format".into(),"fastq".into(),"--output-format".into(),"fastq".into()];let plan=parse(&args).unwrap();R4_UNLINK.with(|v|v.set(true));REPORT_FAULT.with(|v|v.set(if emergency{31}else{0}));println!("R84_R4_NATIVE_RUN_ENTER");let(code,r,category)=run(&plan,None);REPORT_FAULT.with(|v|v.set(0));R4_UNLINK.with(|v|v.set(false));fs::write(root.join("returned.json"),&r.json).unwrap();let v:Value=serde_json::from_str(&r.json).unwrap();assert_eq!(code,3);assert_eq!(category,Some("input"));assert_eq!((r.input_units,r.accepted_units,r.written_units),(1,None,Some(0)));assert_eq!(v["error"]["stage"],"input");assert_eq!(v["error"]["source_unit_ordinal"],1);assert_eq!(v["status"],"failed");assert!(!output.exists());
  if emergency{assert_eq!(v["error"]["category"],"input");assert_eq!(v["report_generation"]["secondary_stage"],"initial-header");assert_eq!(v["report_generation"]["cleanup_stage"],"internal-file");assert_eq!(v["report_generation"]["cleanup_failed"],true);assert!(v["populations"]["input"].is_null());assert_eq!(v["population_summaries"]["input"],json!({"source_units":1,"read_records":1,"bases":6,"q20_bases":6,"q30_bases":6}));assert!(v["population_summaries"]["accepted"].is_null());assert_eq!(v["dedup"]["dedup"]["unavailable_reason"],"full-input classification and representative QC preparation not complete");assert_eq!(v["api"]["execution_admitted"],true);}else{assert_eq!(v["cleanup_stage"],"internal-file");assert_eq!(v["cleanup_failed"],true);assert_eq!(v["populations"]["input"]["read_records"],1);assert_eq!(v["populations"]["input"]["bases"],6);}
  let dirs=fs::read_dir(&spill).unwrap().map(|v|v.unwrap().path()).collect::<Vec<_>>();assert_eq!(dirs.len(),1);assert!(dirs[0].join("payload.bin").is_dir());assert!(LAST_EXECUTION.with(|v|v.borrow().upgrade().is_none()));assert_eq!(fs::read_dir("/proc/self/fd").unwrap().filter_map(|v|fs::read_link(v.ok()?.path()).ok()).filter(|p|p.starts_with(&root)).count(),0);
 }
 #[test]fn normal_primary_and_cleanup(){probe("normal-prefix",false)}
 #[test]fn emergency_three_roles(){probe("emergency-prefix",true)}
}

#[cfg(test)]mod r4_remaining{
 use super::*;
 fn contains(a:&Value,b:&Value){match b{Value::Object(m)=>for(k,v)in m{contains(&a[k],v)},Value::Array(v)=>{assert_eq!(a.as_array().unwrap().len(),v.len());for(a,b)in a.as_array().unwrap().iter().zip(v){contains(a,b)}},_=>assert_eq!(a,b)}}
 #[test]fn ownership_inspection(){crate::dedup_publication::r4_inspection_proof(&crate::test_support::workspace("r4").join("inspection"));}
 #[test]fn normal_equivalence(){let r=crate::test_support::workspace("r4");let expected:Value=serde_json::from_slice(&fs::read(r.join("normal-expected.json")).unwrap()).unwrap();let command:Value=serde_json::from_slice(&fs::read(expected["baseline_command"].as_str().unwrap()).unwrap()).unwrap();let old_report=PathBuf::from(expected["baseline_report"].as_str().unwrap());let old_root=old_report.parent().unwrap();let root=r.join("normal-equivalence");fs::create_dir(&root).unwrap();let args=command["argv"].as_array().unwrap()[2..].iter().map(|v|v.as_str().unwrap().replace(old_root.to_str().unwrap(),root.to_str().unwrap())).collect::<Vec<_>>();let p=parse(&args).unwrap();println!("R84_R4_NATIVE_RUN_ENTER");let(code,report,_)=run(&p,None);fs::write(root.join("returned.json"),&report.json).unwrap();assert_eq!(code,0);let mut v:Value=serde_json::from_str(&report.json).unwrap();contains(&v,&expected["report_subset"]);for output in expected["outputs"].as_array().unwrap(){let hex=output["bytes_hex"].as_str().unwrap();let bytes=(0..hex.len()).step_by(2).map(|i|u8::from_str_radix(&hex[i..i+2],16).unwrap()).collect::<Vec<_>>();assert_eq!(fs::read(root.join(output["path"].as_str().unwrap())).unwrap(),bytes);}
 let text=fs::read_to_string(&old_report).unwrap().replace(old_root.to_str().unwrap(),root.to_str().unwrap());let mut old:Value=serde_json::from_str(&text).unwrap();assert_eq!(v["execution"]["decoders"],old["execution"]["decoders"]);for doc in [&mut v,&mut old]{for key in ["identity","execution","measurements"]{doc.as_object_mut().unwrap().remove(key);}for row in doc["outputs"].as_array_mut().unwrap(){let path=row["staged_path"].as_str().unwrap();assert!(path.starts_with(root.join("spill").to_str().unwrap()));assert!(path.ends_with(".fq"));row.as_object_mut().unwrap().remove("staged_path");}for key in ["admitted_owner_upper_bytes","observed_named_owner_peak_bytes"]{doc["dedup"]["dedup"]["storage"].as_object_mut().unwrap().remove(key);}}
 assert_eq!(v,old);assert_eq!(fs::read_dir(root.join("spill")).unwrap().count(),0);assert!(LAST_EXECUTION.with(|v|v.borrow().upgrade().is_none()));}
 #[test]fn postwork_denial(){let root=crate::test_support::workspace("r4");super::r2_panel::job("postwork-denial",11,0,false);}
}
