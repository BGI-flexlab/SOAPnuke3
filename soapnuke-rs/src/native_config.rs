use crate::native_processing::{ProcessSettings, ProcessStage};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct RunPlan {
    pub api_spec: Option<Arc<crate::job_api::JobSpec>>,
    pub sample_id: String,
    pub progress_seconds: u64,
    pub metrics: bool,
    pub resources: Vec<serde_json::Value>,
    pub lane_manifest: Option<Arc<String>>,
    pub lane_parent: PathBuf,
    pub lane_count: usize,
    pub interleaved: bool,
    pub out_interleaved: bool,
    pub limit_units: Option<u64>,
    pub process: Arc<ProcessSettings>,
    pub adapter_files: Vec<PathBuf>,
    pub inputs: Vec<PathBuf>,
    pub outputs: Vec<PathBuf>,
    pub split_templates: Option<Arc<Vec<PathBuf>>>,
    pub auxiliary: [Option<PathBuf>; 6],
    pub auxiliary_codecs: [String; 6],
    pub record_manifest: Option<PathBuf>,
    pub match_report: Option<PathBuf>,
    pub html_report: Option<PathBuf>,
    pub match_limit: usize,
    pub report: PathBuf,
    pub output_directory: Option<PathBuf>,
    pub input_phred: i32,
    pub output_phred: i32,
    pub threads: usize,
    pub input_codecs: Vec<String>,
    pub output_codecs: Vec<String>,
    pub decoder: String,
    pub supplied: Vec<String>,
    pub memory_total_bytes: Option<usize>,
    pub config_resource: Option<serde_json::Value>,
    pub config_values: BTreeMap<String, serde_json::Value>,
    pub cli_keys: Vec<String>,
    pub cuts: [[usize; 2]; 2],
    pub quality_mode: String,
    pub quality_q: Option<u8>,
    pub quality_window: usize,
    pub tail_bases: String,
    pub tail_mask: u8,
    pub tail_min: usize,
    pub stage_order: [crate::native_processing::ProcessStage; 3],
    pub gzip_level: u32,
    pub gzip_level_active: bool,
    pub thread_budget: ThreadBudget,
    pub threads_auto: bool,
}

#[derive(Clone, Debug)]
pub struct ThreadBudget {
    pub ceiling: usize,
    pub default_threads: usize,
    pub detected_parallelism: usize,
    pub constraints: Vec<(String, usize)>,
    pub ignored_constraints: Vec<String>,
}

const SCHEDULER_CPU_VARS: [&str; 3] = ["SLURM_CPUS_PER_TASK", "PBS_NP", "NSLOTS"];

fn parse_gzip_level(value: &str) -> Result<u32, String> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err("gzip-level requires an integer in1..9".into());
    }
    let level = value
        .parse::<u32>()
        .map_err(|_| "gzip-level requires an integer in1..9")?;
    if !(1..=9).contains(&level) {
        return Err("gzip-level requires an integer in1..9".into());
    }
    Ok(level)
}

fn resolve_thread_budget(
    requested: Option<&str>,
    detected_parallelism: usize,
    scheduler_values: &[(&str, Option<&str>)],
) -> Result<(usize, ThreadBudget), String> {
    let detected = detected_parallelism.max(1);
    let mut constraints = vec![("available_parallelism".to_string(), detected)];
    let mut ignored_constraints = Vec::new();
    for (name, value) in scheduler_values {
        let Some(value) = value else { continue };
        match value.parse::<usize>() {
            Ok(n) if n > 0 => constraints.push(((*name).to_string(), n)),
            _ => ignored_constraints.push(format!(
                "{name}={value} (ignored; expected positive integer)"
            )),
        }
    }
    let ceiling = constraints.iter().map(|(_, n)| *n).min().unwrap_or(1);
    let default_threads = ceiling.min(8).max(1);
    let threads = if let Some(value) = requested {
        let n = value
            .parse::<usize>()
            .map_err(|_| "threads must be a positive integer")?;
        if n == 0 {
            return Err("threads must be positive".into());
        }
        if n > ceiling {
            return Err(format!(
                "threads {n} exceeds known CPU budget ceiling {ceiling}"
            ));
        }
        n
    } else {
        default_threads
    };
    Ok((
        threads,
        ThreadBudget {
            ceiling,
            default_threads,
            detected_parallelism: detected,
            constraints,
            ignored_constraints,
        },
    ))
}

fn validate_gzip_level_use(explicit: bool, has_gzip_output: bool) -> Result<(), String> {
    if explicit && !has_gzip_output {
        Err("explicit --gzip-level requires at least one gzip output".into())
    } else {
        Ok(())
    }
}

pub fn command_help() {
    print!("{}", include_str!("help.txt"));
}

pub fn help(version_only: bool) {
    if !version_only {
        print!("{}", include_str!("process_help.txt"));
        return;
    }
    println!("SOAPnuke {}", env!("CARGO_PKG_VERSION"));
    println!(
        "build={} source_manifest={} dependency_manifest={}",
        option_env!("SOAPNUKE_BUILD_ID").unwrap_or("unbound build"),
        option_env!("SOAPNUKE_SOURCE_SHA256").unwrap_or("unavailable"),
        option_env!("SOAPNUKE_DEPENDENCY_SHA256").unwrap_or("unavailable")
    );
    println!(
        "Release assessment: external QUALIFICATION.json bound to binary/source/dependency hashes; job completion is not release qualification."
    );
}

fn format_codec(path: &Path, format: &str) -> String {
    if format == "gzip" || (format == "auto" && path.to_string_lossy().ends_with(".gz")) {
        "gzip".into()
    } else {
        "plain".into()
    }
}

fn parse_cut(value: &str) -> Result<[usize; 2], String> {
    let (head, tail) = value.split_once(',').ok_or("cut requires HEAD,TAIL")?;
    let parse = |v: &str| {
        if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
            return Err("cut counts must be nonnegative decimal integers");
        }
        v.parse::<usize>()
            .map_err(|_| "cut count exceeds supported integer range")
    };
    Ok([parse(head)?, parse(tail)?])
}

fn parse_quality(mode: &str, value: Option<&str>) -> Result<Option<u8>, String> {
    match (mode, value) {
        ("off", None) => Ok(None),
        ("ends" | "window" | "mott", Some(value))
            if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) =>
        {
            let q = value.parse::<u8>().map_err(|_| "quality-q must be0..93")?;
            if q > 93 {
                return Err("quality-q must be0..93".into());
            }
            Ok(Some(q))
        }
        ("ends" | "window" | "mott", None) => {
            Err("non-off quality-mode requires --quality-q".into())
        }
        ("off", Some(_)) => Err("quality-q requires a non-off quality-mode".into()),
        _ => {
            Err("quality-mode must be off, ends, window or mott and quality-q must be0..93".into())
        }
    }
}

fn parse_stage_order(value: &str) -> Result<[crate::native_processing::ProcessStage; 3], String> {
    use crate::native_processing::ProcessStage;
    let mut result = crate::native_processing::DEFAULT_STAGE_ORDER;
    let mut parts = value.split(',');
    for stage in &mut result {
        *stage = match parts.next() {
            Some("tails") => ProcessStage::Tails,
            Some("quality") => ProcessStage::Quality,
            Some("adapters") => ProcessStage::Adapters,
            _ => return Err("stage-order requires a permutation of tails,quality,adapters".into()),
        };
    }
    if parts.next().is_some()
        || result[0] == result[1]
        || result[0] == result[2]
        || result[1] == result[2]
    {
        return Err("stage-order requires a permutation of tails,quality,adapters".into());
    }
    Ok(result)
}

fn parse_retain(value: &str) -> Result<Option<[usize; 2]>, String> {
    if value == "all" {
        return Ok(None);
    }
    let (start, end) = value
        .split_once(':')
        .ok_or("retain range requires start:end or all")?;
    let number = |v: &str| -> Result<usize, String> {
        if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
            return Err("retain coordinates require positive integers".into());
        }
        v.parse::<usize>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or("retain coordinates require positive integers".into())
    };
    let start = number(start)?;
    let end = number(end)?;
    if start > end {
        return Err("retain start must not exceed end".into());
    }
    Ok(Some([start - 1, end]))
}

fn read_adapters(
    paths: &[PathBuf],
    process: &mut ProcessSettings,
    resources: &mut Vec<serde_json::Value>,
) -> Result<(), String> {
    let mut names = BTreeSet::new();
    for path in paths {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read adapter FASTA {}: {e}", path.display()))?;
        use std::io::Write;
        let mut child = std::process::Command::new("sha256sum")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("resource hashing requires sha256sum: {e}"))?;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(text.as_bytes())
            .map_err(|e| format!("hash adapter snapshot: {e}"))?;
        let digest = child.wait_with_output().map_err(|e| e.to_string())?;
        if !digest.status.success() {
            return Err("adapter snapshot hashing failed".into());
        }
        let digest = String::from_utf8(digest.stdout)
            .map_err(|e| e.to_string())?
            .split_whitespace()
            .next()
            .ok_or("empty adapter hash")?
            .to_string();
        resources.push(serde_json::json!({"kind":"adapter-fasta","path":path,"sha256":digest,"hash_scope":"exact bytes read and compiled at configuration time"}));
        let mut current: Option<(usize, bool, String, String)> = None;
        let mut records = 0;
        let commit = |record: Option<(usize, bool, String, String)>,
                      process: &mut ProcessSettings|
         -> Result<(), String> {
            if let Some((mate, five, name, sequence)) = record {
                if sequence.is_empty() {
                    return Err("adapter FASTA record has an empty sequence".into());
                }
                if five {
                    process.primers[mate].push(sequence);
                    process.primer_names[mate].push(name);
                } else {
                    process.adapters[mate].push(sequence);
                    process.adapter_names[mate].push(name);
                }
            }
            Ok(())
        };
        for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
            if let Some(header) = line.strip_prefix('>') {
                commit(current.take(), process)?;
                let fields: Vec<_> = header.split(';').collect();
                if fields.len() != 3
                    || fields[0].is_empty()
                    || fields[0].chars().any(char::is_whitespace)
                    || !["end=3", "end=5"].contains(&fields[2])
                {
                    return Err(
                        "adapter FASTA header requires unique name;mate=R1|R2;end=3|5".into(),
                    );
                }
                if !names.insert(fields[0].to_string()) {
                    return Err("adapter FASTA names must be unique across files".into());
                }
                let mate = match fields[1] {
                    "mate=R1" => 0,
                    "mate=R2" => 1,
                    _ => return Err("adapter FASTA mate must be R1 or R2".into()),
                };
                current = Some((mate, fields[2] == "end=5", fields[0].into(), String::new()));
                records += 1;
            } else if let Some((_, _, _, sequence)) = current.as_mut() {
                sequence.push_str(line);
            } else {
                return Err("adapter FASTA sequence precedes header".into());
            }
        }
        commit(current, process)?;
        if records == 0 {
            return Err("adapter FASTA must contain at least one record".into());
        }
    }
    Ok(())
}

impl RunPlan {
    pub fn auxiliary_enabled(&self, i: usize) -> bool {
        self.auxiliary[i].is_some() || self.api_spec.as_ref().is_some_and(|s| s.outputs.iter().any(|d| d.channel == ["reject1","reject2","transformed1","transformed2","singleton1","singleton2"][i]))
    }
    pub fn has_auxiliary(&self) -> bool { (0..6).any(|i| self.auxiliary_enabled(i)) }

    pub fn main_channel(&self, index: usize) -> usize {
        self.process.overlap.channel(index, self.out_interleaved)
    }

    pub fn paired(&self) -> bool {
        self.api_spec.as_ref().map_or(self.interleaved || self.inputs.len() == 2, |s| s.layout != crate::job_api::Layout::Se)
    }

    pub fn provenance(&self) -> serde_json::Value {
        let p = &self.process;
        let mut values = serde_json::json!({
            "memory-mib":self.memory_total_bytes.map(|n|n/(1 << 20)),
            "config":self.config_resource.as_ref().and_then(|v|v.get("path")),
            "r1":if self.lane_manifest.is_none(){self.inputs.first()}else{None},"r2":if self.lane_manifest.is_none(){self.inputs.get(1)}else{None},
            "out1":if self.out_interleaved||p.overlap.discard_unmerged{None}else{self.outputs.first()},"out2":if self.out_interleaved||p.overlap.discard_unmerged{None}else{self.outputs.get(1)},"report":self.report,
            "sample-id":self.sample_id,"progress-seconds":self.progress_seconds,"metrics":self.metrics,
            "threads":self.threads,"input-phred":self.input_phred,"output-phred":self.output_phred,
            "input-format":self.input_codecs.first(),"output-format":self.output_codecs.first(),
            "decoder":self.decoder,"gzip-level":self.gzip_level,
            "adapter3-r1":p.adapters[0],"adapter3-r2":p.adapters[1],"adapter-file":self.adapter_files,
            "adapter-action":if p.adapter_discard {"discard"} else {"trim"},
            "adapter-min-overlap":p.min_overlap,"adapter-max-error":p.max_error,
            "min-length":p.min_length,"max-length":p.max_length,"max-n-count":p.max_n_count,
            "max-n-fraction":p.max_n_fraction,"low-q":p.low_q,"max-low-q-fraction":p.max_low_q_fraction,
            "min-mean-q":p.min_mean_q,"min-complexity":p.min_complexity,"pair-policy":p.pair_policy,
            "cut-r1":format!("{},{}",self.cuts[0][0],self.cuts[0][1]),
            "cut-r2":if self.paired() {Some(format!("{},{}",self.cuts[1][0],self.cuts[1][1]))} else {None},
            "quality-mode":self.quality_mode,"quality-q":self.quality_q,
            "quality-window":if self.quality_mode=="window" {Some(self.quality_window)} else {None},
            "tail-bases":self.tail_bases,"tail-min":if self.tail_mask!=0 {Some(self.tail_min)} else {None},
            "stage-order":self.stage_order.map(|s|s.name()).join(",")
        });
        values["adapter5-r1"] = serde_json::json!(p.primers[0]);
        if p.adapter_overlap { values["adapter-overlap"] = serde_json::json!(true); }
        values["out-dir"] = serde_json::json!(self.output_directory);
        values["adapter5-r2"] = serde_json::json!(p.primers[1]);
        values["extract-label"] = serde_json::json!(
            p.capture
                .labels
                .iter()
                .map(|l| format!(
                    "{}@r{}:{}:{}",
                    l.name,
                    l.mate + 1,
                    l.start + 1,
                    l.end - l.start
                ))
                .collect::<Vec<_>>()
        );
        values["split-units"] = serde_json::json!(if p.split_units == 0 {
            None
        } else {
            Some(p.split_units)
        });
        for (key, value) in [
            ("screen-method", serde_json::json!(p.screen.method)),
            ("screen-action", serde_json::json!(p.screen.action)),
            ("screen-reference", serde_json::json!(p.screen.references)),
            ("screen-k", serde_json::json!(p.screen.k)),
            ("screen-min-hits", serde_json::json!(p.screen.min_hits)),
            ("screen-pair", serde_json::json!(p.screen.pair)),
            ("screen-unscorable", serde_json::json!(p.screen.unscorable)),
            ("screen-hits", serde_json::json!(p.screen.hits)),
        ] {
            values[key] = value;
        }
        values["repetition"]=serde_json::json!(p.repetition);values["repetition-units"]=serde_json::json!(p.repetition_units);values["overrep-fraction"]=serde_json::json!(p.overrep_fraction);
        values["qc-only"] = serde_json::json!(p.qc_only);
        values["profile"] = serde_json::json!(p.profile);
        values["srna-min-insert"] = serde_json::json!(p.srna_min_insert);
        values["srna-reject5"] = serde_json::json!(p.srna_reject5);
        values["srna-reject-poly-a"] = serde_json::json!(p.srna_reject_poly_a);
        values["merge"] = serde_json::json!(p.overlap.merge);
        values["correct-overlap"] = serde_json::json!(p.overlap.correct);
        values["overlap-min"] = serde_json::json!(p.overlap.minimum);
        values["overlap-max-error"] = serde_json::json!(p.overlap.max_error);
        values["min-insert-length"] = serde_json::json!(p.overlap.min_insert);
        values["unmerged-mode"] = serde_json::json!(if p.overlap.discard_unmerged {
            "discard"
        } else {
            "paired"
        });
        if p.overlap.merge {
            values["merged-out"] = serde_json::json!(self.outputs.last());
        }
        values["sample-fraction"] = serde_json::json!(p.sample_fraction);
        values["sample-seed"] = serde_json::json!(p.sample_seed);
        values["label-kind"] = serde_json::json!(p.capture.kind);
        values["label-remove"] = serde_json::json!(p.capture.remove);
        values["label-min-q"] = serde_json::json!(p.capture.min_q);
        values["label-max-n"] = serde_json::json!(p.capture.max_n);
        values["retain-r1"] =
            serde_json::json!(p.capture.retain[0].map(|[a, b]| format!("{}:{b}", a + 1)));
        values["retain-r2"] =
            serde_json::json!(p.capture.retain[1].map(|[a, b]| format!("{}:{b}", a + 1)));
        for (i, key) in [
            "reject1",
            "reject2",
            "transformed1",
            "transformed2",
            "singleton1",
            "singleton2",
        ]
        .iter()
        .enumerate()
        {
            values[*key] = serde_json::json!(self.auxiliary[i]);
        }
        values["infer-adapter"] = serde_json::json!(p.infer_mode);
        values["infer-units"] = serde_json::json!(p.infer_units);
        values["infer-min-support"] = serde_json::json!(p.infer_min_support);
        values["match-report"] = serde_json::json!(self.match_report);
        values["html-report"] = serde_json::json!(self.html_report);
        values["match-limit"] = serde_json::json!(self.match_limit);
        values["interleaved"] = serde_json::json!(self.interleaved);
        values["out-interleaved"] = serde_json::json!(if self.out_interleaved {
            self.outputs.first()
        } else {
            None
        });
        values["lanes"] = self
            .resources
            .iter()
            .find(|r| r["kind"] == "lane-manifest")
            .and_then(|r| r.get("path"))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        values["limit-units"] = serde_json::json!(self.limit_units);
        let resolved: serde_json::Map<String, serde_json::Value> = values
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                let origin = if self.cli_keys.contains(key) {
                    if self.api_spec.is_some(){"api"}else{"cli"}
                } else if self.config_values.contains_key(key) {
                    "config"
                } else {
                    "default"
                };
                (
                    key.clone(),
                    serde_json::json!({"value":value,"origin":origin,"explicit":origin!="default"}),
                )
            })
            .collect();
        serde_json::json!({"version":"process-config/1","precedence":["default","config","cli"],
            "config_resource":self.config_resource,"config_options":self.config_values,
            "explicit_keys":{"config":self.config_values.keys().collect::<Vec<_>>(),"cli":self.cli_keys},
            "resolved_options":resolved,"path_policy":"inputs/resources use process working directory; relative outputs use out-dir when set; no config-parent rebasing"})
    }
}

struct ConfigOptions(BTreeMap<String, serde_json::Value>);
impl<'de> serde::Deserialize<'de> for ConfigOptions {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = ConfigOptions;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a configuration options object without duplicate keys")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut values = BTreeMap::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate config option {key}"
                        )));
                    }
                    values.insert(key, map.next_value()?);
                }
                Ok(ConfigOptions(values))
            }
        }
        d.deserialize_map(Visitor)
    }
}
struct ConfigDocument(String, ConfigOptions);
impl<'de> serde::Deserialize<'de> for ConfigDocument {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = ConfigDocument;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a process configuration with job and options")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut seen = BTreeSet::new();
                let mut job = None;
                let mut options = None;
                while let Some(key) = map.next_key::<String>()? {
                    if !seen.insert(key.clone()) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate config key {key}"
                        )));
                    }
                    match key.as_str() {
                        "job" => job = Some(map.next_value::<String>()?),
                        "options" => options = Some(map.next_value::<ConfigOptions>()?),
                        _ => {
                            return Err(serde::de::Error::custom(format!(
                                "unknown config key {key}"
                            )));
                        }
                    }
                }
                Ok(ConfigDocument(job.ok_or_else(||serde::de::Error::custom("config requires job"))?, options.ok_or_else(|| {
                    serde::de::Error::custom("config requires options")
                })?))
            }
        }
        d.deserialize_map(Visitor)
    }
}
pub(crate) fn decode_job_config(text:&str,expected_job:&str)->Result<BTreeMap<String,serde_json::Value>,String>{
    let ConfigDocument(job,ConfigOptions(options))=serde_json::from_str(text).map_err(|e|format!("invalid configuration: {e}"))?;
    if job!=expected_job{return Err(format!("config job must be {expected_job}"));} Ok(options)
}
fn decode_config(
    text: &str,
    allowed: &[&str],
) -> Result<BTreeMap<String, serde_json::Value>, String> {
    let options = decode_job_config(text, "process")?;
    for (key, value) in &options {
        if key == "correction-delta" { return Err("--correction-delta was removed: overlap correction now uses fastp's fixed Q>=30 / Q<=14 rules".into()); }
        if !allowed.contains(&key.as_str()) || key == "config" {
            return Err(format!("unknown or unimplemented config option {key}"));
        }
        config_arguments(key, value)?;
    }
    Ok(options)
}
pub(crate) fn inspect_lanes(path: &str) -> Result<(Arc<String>, PathBuf, Vec<PathBuf>, usize), String> {
    let parent = std::fs::canonicalize(
        Path::new(path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
    .map_err(|e| format!("lane parent: {e}"))?;
    let text =
        Arc::new(std::fs::read_to_string(path).map_err(|e| format!("cannot read lanes: {e}"))?);
    let mut lines = text.lines();
    if lines.next() != Some("lane_id\tr1\tr2") {
        return Err("lanes requires lane_id,r1,r2 TSV header".into());
    }
    let mut ids = BTreeSet::new();
    let mut first = Vec::new();
    let mut mates = None;
    let mut count = 0;
    for line in lines {
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 3
            || fields[0].is_empty()
            || fields[1].is_empty()
            || fields[1] == "-"
            || fields[2] == "-"
        {
            return Err("invalid lane layout/paths".into());
        }
        // Inspect metadata only: unused later FASTQ data is not opened.
        for field in fields.iter().skip(1).filter(|path| !path.is_empty()) {
            if std::fs::metadata(parent.join(field)).is_ok_and(|m| !m.is_file()) {
                return Err(
                    "lane inputs require regular seekable files; use stdin for a stream".into(),
                );
            }
        }
        if !fields[2].is_empty() {
            let a = parent.join(fields[1]);
            let b = parent.join(fields[2]);
            if a == b
                || std::fs::canonicalize(&a)
                    .ok()
                    .zip(std::fs::canonicalize(&b).ok())
                    .is_some_and(|(a, b)| a == b)
            {
                return Err("lane mate input paths must be distinct".into());
            }
        }
        if !ids.insert(fields[0]) {
            return Err("duplicate lane identifier".into());
        }
        let n = if fields[2].is_empty() { 1 } else { 2 };
        if mates.is_some_and(|prior| prior != n) {
            return Err("mixed lane layouts are invalid".into());
        }
        mates = Some(n);
        count += 1;
        if first.is_empty() {
            first.push(parent.join(fields[1]));
            if n == 2 {
                first.push(parent.join(fields[2]));
            }
        }
    }
    if count == 0 {
        return Err("lanes requires at least one lane".into());
    }
    Ok((text, parent, first, count))
}

fn inputs_stream_forbidden(values: &BTreeMap<String, String>) -> bool {
    values.contains_key("r2")
        || !values.contains_key("sample-id")
        || values.get("decoder").is_some_and(|s| s == "rapidgzip")
}
pub(crate) fn config_arguments(key: &str, value: &serde_json::Value) -> Result<Vec<String>, String> {
    use serde_json::Value;
    if key == "correction-delta" { return Err("--correction-delta was removed: overlap correction now uses fastp's fixed Q>=30 / Q<=14 rules".into()); }
    let invalid = || format!("configuration option {key} has wrong type or value");
    let kind = crate::option_metadata::known(key).map(|o|o.json_kind).unwrap_or(crate::option_metadata::JsonKind::Text);
    if kind == crate::option_metadata::JsonKind::List
    {
        return value
            .as_array()
            .ok_or_else(invalid)?
            .iter()
            .map(|v| {
                let text = v.as_str().filter(|v| !v.is_empty()).ok_or_else(invalid)?;
                if key == "extract-label" {
                    crate::native_processing::LabelSpec::parse(text)?;
                }
                if key != "screen-reference"
                    && key != "adapter-file"
                    && key != "extract-label"
                    && !text
                        .bytes()
                        .all(|b| matches!(b.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T'))
                {
                    return Err(invalid());
                }
                Ok(text.to_string())
            })
            .collect();
    }
    let result = if kind == crate::option_metadata::JsonKind::Integer {
        let n = value.as_u64().ok_or_else(invalid)?;
        usize::try_from(n).map_err(|_| invalid())?;
        if ([
            "repetition-units",
            "threads",
            "min-length",
            "max-length",
            "quality-window",
            "tail-min",
            "adapter-min-overlap",
            "memory-mib",
            "limit-units",
            "infer-units",
            "match-limit",
            "split-units",
            "overlap-min",
            "min-insert-length",
            "srna-min-insert",
            "screen-k",
            "screen-min-hits",
        ]
        .contains(&key)
            && n == 0)
            || (["low-q", "quality-q", "label-min-q"].contains(&key) && n > 93)
            || (["input-phred", "output-phred"].contains(&key) && ![33, 64].contains(&n))
            || (key == "gzip-level" && !(1..=9).contains(&n))
        {
            return Err(invalid());
        }
        if key == "memory-mib" {
            usize::try_from(n)
                .ok()
                .and_then(|n| n.checked_mul(1 << 20))
                .ok_or_else(invalid)?;
        }
        n.to_string()
    } else if kind == crate::option_metadata::JsonKind::Number {
        let n = value.as_f64().ok_or_else(invalid)?;
        let max = if key == "min-mean-q" { 93.0 } else { 1.0 };
        if !n.is_finite() || !(0.0..=max).contains(&n) {
            return Err(invalid());
        }
        n.to_string()
    } else if kind == crate::option_metadata::JsonKind::Boolean
    {
        value.as_bool().ok_or_else(invalid)?.to_string()
    } else {
        let s = value.as_str().ok_or_else(invalid)?;
        if s.is_empty()
            || (key == "sample-id" && s.trim().is_empty())
            || ([
                "r2",
                "out2",
                "report",
                "config",
                "out-dir",
                "infer-adapter",
                "infer-units",
                "infer-min-support",
                "reject1",
                "reject2",
                "transformed1",
                "transformed2",
                "singleton1",
                "singleton2",
                "merged-out",
            ]
            .contains(&key)
                && s == "-")
        {
            return Err(invalid());
        }
        if key == "match-report" && s == "-" {
            return Err(invalid());
        }
        let choices: &[&str] = match key {
            "quality-mode" => &["off", "ends", "window", "mott"],
            "tail-bases" => &["off", "A", "C", "G", "T", "N", "all"],
            "decoder" => &["auto", "zlib", "rapidgzip"],
            "input-format" | "output-format" => &["auto", "fastq", "gzip"],
            "adapter-action" => &["trim", "discard"],
            "pair-policy" => &["both-pass", "either-pass", "r1-pass"],
            "unmerged-mode" => &["paired", "discard"],
            "profile" => &["ordinary", "srna"],
            "screen-method" => &["off", "kmer-exact", "supplied-hits"],
            "screen-action" => &["inspect", "remove", "retain"],
            "screen-pair" => &["any", "both"],
            "screen-unscorable" => &["report", "keep", "reject"],
            "label-kind" => &["umi", "barcode"],
            "infer-adapter" => &["off", "r1", "r2", "both"],
            _ => &[],
        };
        if !choices.is_empty() && !choices.contains(&s) {
            return Err(invalid());
        }
        if ["cut-r1", "cut-r2"].contains(&key) {
            parse_cut(s)?;
        }
        if ["retain-r1", "retain-r2"].contains(&key) {
            parse_retain(s)?;
        }
        if key == "stage-order" {
            parse_stage_order(s)?;
        }
        s.to_string()
    };
    // Objects, arrays and null are never silently stringified into scalar options.
    if matches!(value, Value::Null | Value::Object(_) | Value::Array(_)) {
        return Err(invalid());
    }
    Ok(vec![result])
}

pub fn parse(args: &[String]) -> Result<RunPlan, String> { parse_inner(args, None) }
pub(crate) fn parse_api(args: &[String], spec: &crate::job_api::JobSpec) -> Result<RunPlan, String> {
    let mut plan=parse_inner(args,Some(spec))?;
    plan.config_values=spec.configured.iter().map(|(k,v)|(k.clone(),crate::job_api::setting_json(v))).collect();
    plan.cli_keys=spec.supplied.keys().cloned().collect();
    Ok(plan)
}
fn parse_inner(args: &[String], api: Option<&crate::job_api::JobSpec>) -> Result<RunPlan, String> {
    let args = crate::native_cli::normalize(args)?;
    let allowed = crate::option_metadata::names(crate::option_metadata::Job::Process);
    let mut values = BTreeMap::new();
    let mut repeated: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut i = 0;
    while i < args.len() {
        let flag = args[i]
            .strip_prefix("--")
            .ok_or("expected a named native option")?;
        let (key, value) = if [
            "repetition",
            "qc-only",
            "interleaved",
            "label-remove",
            "merge",
            "correct-overlap",
            "adapter-overlap",
            "srna-reject5",
            "srna-reject-poly-a",
        ]
        .contains(&flag)
            && args.get(i + 1).is_none_or(|s| s.starts_with("--"))
        {
            (flag, "true".to_string())
        } else if let Some((k, v)) = flag.split_once('=') {
            (k, v.to_string())
        } else {
            i += 1;
            (
                flag,
                args.get(i)
                    .ok_or_else(|| format!("missing value for --{flag}"))?
                    .clone(),
            )
        };
        if !allowed.contains(&key) {
            return Err(format!("unknown or unimplemented native option --{key}"));
        }
        if crate::option_metadata::known(key).is_some_and(|o|o.repeatable)
        {
            repeated.entry(key.into()).or_default().push(value.clone());
        } else if values.contains_key(key) {
            return Err(format!("duplicate option --{key}"));
        }
        if values.insert(key.to_string(), value).is_some()
            && !crate::option_metadata::known(key).is_some_and(|o|o.repeatable)
        {
            return Err(format!("duplicate option --{key}"));
        }
        i += 1;
    }
    let cli_keys: Vec<String> = values.keys().cloned().collect();
    let mut config_resource = None;
    let mut config_values = BTreeMap::new();
    if let Some(path) = values.get("config") {
        let text =
            std::fs::read_to_string(path).map_err(|e| format!("cannot read config {path}: {e}"))?;
        config_values = decode_config(&text, &allowed)?;
        use std::io::Write;
        let mut child = std::process::Command::new("sha256sum")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| e.to_string())?;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(text.as_bytes())
            .map_err(|e| e.to_string())?;
        let digest = child.wait_with_output().map_err(|e| e.to_string())?;
        if !digest.status.success() {
            return Err("config snapshot hashing failed".into());
        }
        let digest = String::from_utf8(digest.stdout)
            .map_err(|e| e.to_string())?
            .split_whitespace()
            .next()
            .ok_or("empty config digest")?
            .to_string();
        config_resource = Some(
            serde_json::json!({"kind":"process-config","path":path,"sha256":digest,"hash_scope":"exact UTF-8 bytes parsed before overrides","version":"process-config/1"}),
        );
        for (key, value) in &config_values {
            if cli_keys.contains(key) {
                continue;
            }
            let items = config_arguments(key, value)?;
            if [
                "adapter3-r1",
                "adapter3-r2",
                "adapter-file",
                "adapter5-r1",
                "adapter5-r2",
                "extract-label",
                "screen-reference",
            ]
            .contains(&key.as_str())
            {
                repeated.insert(key.clone(), items.clone());
                // An explicit empty collection still participates in provenance and exclusivity.
                values.insert(key.clone(), items.last().cloned().unwrap_or_default());
            } else {
                values.insert(key.clone(), items[0].clone());
            }
        }
    }
    let catalogue = values.get("resource-catalog").map(|path|crate::resources_app::Catalogue::load(Path::new(path))).transpose().map_err(|e|format!("{}: {}",e.code,e.message))?;
    let mut catalogue_resources = Vec::new();
    for (key,kind) in [("adapter-file","adapter"),("screen-reference","reference")] {
        if let Some(paths)=repeated.get_mut(key) { for value in paths {
            if let Some(id)=value.strip_prefix("id:") {
                let catalog=catalogue.as_ref().ok_or("resource ID requires resource-catalog")?;
                let (path,sha256)=catalog.resolve(id,kind,key).map_err(|e|format!("{}: {}",e.code,e.message))?;
                catalogue_resources.push(serde_json::json!({"kind":kind,"catalogue_id":id,"path":path,"sha256":sha256,"catalogue_sha256":catalog.sha256,"hash_scope":"exact resource file bytes hashed and verified against the catalogue during resolution"}));
                *value=path.to_string_lossy().into_owned();
            }
        }}
    }
    let output_directory = values.get("out-dir").map(|directory| {
        if directory.is_empty() || directory == "-" {
            return Err("-o/--outDir requires an existing directory".to_string());
        }
        let path = std::fs::canonicalize(directory)
            .map_err(|e| format!("output directory must exist; create it before running: {e}"))?;
        if !path.is_dir() {
            return Err("-o/--outDir requires a directory".into());
        }
        Ok(path)
    }).transpose()?;
    let implicit_report = output_directory.is_some() && !values.contains_key("report");
    if let Some(directory) = &output_directory {
        values.entry("report".into()).or_insert_with(|| "SOAPnuke.json".into());
        for key in ["out1", "out2", "out-interleaved", "report", "html-report", "match-report", "reject1", "reject2", "transformed1", "transformed2", "singleton1", "singleton2", "merged-out"] {
            if let Some(value) = values.get_mut(key) {
                if !value.is_empty() && value != "-" && Path::new(value).is_relative() {
                    *value = directory.join(&*value).to_string_lossy().into_owned();
                }
            }
        }
    }
    let get = |key: &str, default: &str| values.get(key).cloned().unwrap_or_else(|| default.into());
    let qc_only=match get("qc-only","false").as_str(){"true"=>true,"false"=>false,_=>return Err("qc-only requires boolean".into())};
    if qc_only {
        let shared=["resource-catalog","qc-only","config","out-dir","r1","r2","lanes","interleaved","limit-units","input-format","input-phred","output-phred","threads","decoder","sample-id","progress-seconds","metrics","report","html-report","memory-mib","repetition","repetition-units","overrep-fraction","screen-reference","screen-method","screen-action","screen-k","screen-min-hits","screen-pair","screen-unscorable","screen-hits"];
        if let Some(key)=values.keys().chain(config_values.keys()).find(|k|!shared.contains(&k.as_str())){return Err(format!("qc-only rejects explicit transform/profile/selection/writer option {key}"));}
        if get("screen-action","inspect")!="inspect"||["keep","reject"].contains(&get("screen-unscorable","report").as_str())||config_values.get("screen-action").and_then(serde_json::Value::as_str).is_some_and(|v|v!="inspect")||config_values.get("screen-unscorable").and_then(serde_json::Value::as_str).is_some_and(|v|v!="report"){return Err("qc-only permits reference inspection only; no action or U keep/reject".into());}
    }
    let required = |key: &str| {
        values
            .get(key)
            .filter(|v| !v.is_empty())
            .cloned()
            .ok_or_else(|| format!("--{key} requires a file path"))
    };
    let interleaved = if let Some(s) = api { s.layout == crate::job_api::Layout::InterleavedPe } else { match get("interleaved", "false").as_str() {
        "true" => true,
        "false" => false,
        _ => return Err("interleaved requires a boolean".into()),
    }
    };
    let out_interleaved = api.map_or(values.contains_key("out-interleaved"), |s| s.layout != crate::job_api::Layout::Se && s.outputs.iter().filter(|d|crate::job_api::main_channel(&d.channel).is_some()).count()==1 && s.outputs.iter().any(|d|crate::job_api::main_channel(&d.channel)==Some(0)));
    let limit_units = values
        .get("limit-units")
        .map(|s| {
            s.bytes()
                .all(|b| b.is_ascii_digit())
                .then(|| s.parse::<u64>().ok())
                .flatten()
                .filter(|n| *n > 0)
                .ok_or("limit-units requires a positive integer")
        })
        .transpose()?;
    if interleaved && limit_units.is_some_and(|n| n > u64::MAX / 2) {
        return Err("interleaved prefix record count overflow".into());
    }
    if values.contains_key("lanes")
        && (values.contains_key("r1")
            || values.contains_key("r2")
            || values.contains_key("interleaved"))
    {
        return Err("lanes excludes r1/r2/interleaved".into());
    }
    if interleaved && values.contains_key("r2") {
        return Err("interleaved excludes r2".into());
    }
    let memory_total_bytes = values
        .get("memory-mib")
        .map(|value| {
            if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err("memory-mib requires a positive decimal integer".to_string());
            }
            value
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .and_then(|n| n.checked_mul(1 << 20))
                .ok_or_else(|| {
                    "memory-mib must be positive and fit the supported integer range".to_string()
                })
        })
        .transpose()?;
    let quality_mode = get("quality-mode", "off");
    let quality_q = parse_quality(&quality_mode, values.get("quality-q").map(String::as_str))?;
    let quality_window = if quality_mode == "window" {
        let value = get("quality-window", "4");
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err("quality-window requires a positive integer".into());
        }
        let width = value
            .parse::<usize>()
            .map_err(|_| "quality-window exceeds supported integer range")?;
        if width == 0 {
            return Err("quality-window must be positive".into());
        }
        width
    } else {
        if values.contains_key("quality-window") {
            return Err("quality-window requires window mode".into());
        }
        4
    };
    let input_phred = get("input-phred", "33")
        .parse::<i32>()
        .map_err(|_| "input-phred must be33 or64")?;
    let output_phred = get("output-phred", "33")
        .parse::<i32>()
        .map_err(|_| "output-phred must be33 or64")?;
    if ![33, 64].contains(&input_phred) || ![33, 64].contains(&output_phred) {
        return Err("Phred encoding must be33 or64".into());
    }
    let detected_parallelism = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    let scheduler_values: Vec<_> = SCHEDULER_CPU_VARS
        .iter()
        .map(|name| (*name, std::env::var(name).ok()))
        .collect();
    let scheduler_refs: Vec<_> = scheduler_values
        .iter()
        .map(|(name, value)| (*name, value.as_deref()))
        .collect();
    let threads_auto = !values.contains_key("threads");
    let (threads, thread_budget) = resolve_thread_budget(
        values.get("threads").map(String::as_str),
        detected_parallelism,
        &scheduler_refs,
    )?;
    let gzip_level = parse_gzip_level(&get("gzip-level", "4"))?;
    let integer = |key: &str, value: &str| -> Result<usize, String> {
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!("{key} requires a nonnegative decimal integer"));
        }
        value
            .parse()
            .map_err(|_| format!("{key} exceeds supported integer range"))
    };
    let fraction = |key: &str, value: &str, max: f64| -> Result<f64, String> {
        let value: f64 = value
            .parse()
            .map_err(|_| format!("{key} requires a finite number in0..{max}"))?;
        if !value.is_finite() || !(0.0..=max).contains(&value) {
            return Err(format!("{key} requires a finite number in0..{max}"));
        }
        Ok(value)
    };
    let stage_order = parse_stage_order(&get("stage-order", "tails,quality,adapters"))?;
    let tail_bases = get("tail-bases", "off");
    let tail_mask = match tail_bases.as_str() {
        "off" => 0,
        "A" => 1,
        "C" => 2,
        "G" => 4,
        "T" => 8,
        "N" => 16,
        "all" => 31,
        _ => return Err("tail-bases must be off, A, C, G, T, N or all".into()),
    };
    let tail_min = integer("tail-min", &get("tail-min", "10"))?;
    if tail_min == 0 {
        return Err("tail-min must be positive".into());
    }
    if tail_mask == 0
        && get("srna-reject-poly-a", "false") != "true"
        && values.contains_key("tail-min")
    {
        return Err("tail-min requires enabled tail-bases".into());
    }
    let mut process = ProcessSettings {
        qc_only,
        min_length: integer("min-length", &get("min-length", "1"))?,
        ..ProcessSettings::default()
    };
    process.capture.labels = repeated
        .remove("extract-label")
        .unwrap_or_default()
        .iter()
        .map(|v| crate::native_processing::LabelSpec::parse(v))
        .collect::<Result<Vec<_>, _>>()?;
    let mut label_names = BTreeSet::new();
    if process
        .capture
        .labels
        .iter()
        .any(|label| !label_names.insert(&label.name))
    {
        return Err("duplicate label name".into());
    }
    process.capture.kind = get("label-kind", "umi");
    if !["umi", "barcode"].contains(&process.capture.kind.as_str()) {
        return Err("label-kind must be umi or barcode".into());
    }
    process.capture.remove = match get("label-remove", "false").as_str() {
        "true" => true,
        "false" => false,
        _ => return Err("label-remove must be boolean".into()),
    };
    process.capture.min_q = values
        .get("label-min-q")
        .map(|v| {
            integer("label-min-q", v).and_then(|n| {
                u8::try_from(n)
                    .ok()
                    .filter(|q| *q <= 93)
                    .ok_or("label-min-q must be0..93".into())
            })
        })
        .transpose()?;
    process.capture.max_n = integer("label-max-n", &get("label-max-n", "0"))?;
    process.capture.retain = [
        parse_retain(&get("retain-r1", "all"))?,
        parse_retain(&get("retain-r2", "all"))?,
    ];
    if process.capture.labels.is_empty()
        && ["label-kind", "label-remove", "label-min-q", "label-max-n"]
            .iter()
            .any(|k| values.contains_key(*k))
    {
        return Err("label controls require extract-label".into());
    }
    process.infer_mode = get("infer-adapter", "off");
    if !["off", "r1", "r2", "both"].contains(&process.infer_mode.as_str()) {
        return Err("infer-adapter must be off|r1|r2|both".into());
    }
    process.infer_units = integer("infer-units", &get("infer-units", "100000"))?;
    if process.infer_units == 0 {
        return Err("infer-units must be positive".into());
    }
    process.infer_min_support =
        fraction("infer-min-support", &get("infer-min-support", "0.01"), 1.0)?;
    if process.infer_mode == "off"
        && ["infer-units", "infer-min-support"]
            .iter()
            .any(|k| values.contains_key(*k))
    {
        return Err("inference controls require enabled infer-adapter".into());
    }
    process.min_complexity = values
        .get("min-complexity")
        .map(|v| fraction("min-complexity", v, 1.0))
        .transpose()?;
    if process.min_length == 0 {
        return Err("min-length must be at least1".into());
    }
    process.max_length = values
        .get("max-length")
        .map(|v| integer("max-length", v))
        .transpose()?;
    if process
        .max_length
        .is_some_and(|max| max < process.min_length)
    {
        return Err("max-length must be >= min-length".into());
    }
    process.max_n_count = values
        .get("max-n-count")
        .map(|v| integer("max-n-count", v))
        .transpose()?;
    process.max_n_fraction = values
        .get("max-n-fraction")
        .map(|v| fraction("max-n-fraction", v, 1.0))
        .transpose()?;
    process.low_q = values
        .get("low-q")
        .map(|v| -> Result<u8, String> {
            let q = integer("low-q", v)?;
            if q > 93 {
                Err("low-q must be0..93".into())
            } else {
                Ok(q as u8)
            }
        })
        .transpose()?;
    process.max_low_q_fraction = values
        .get("max-low-q-fraction")
        .map(|v| fraction("max-low-q-fraction", v, 1.0))
        .transpose()?;
    if process.low_q.is_some() != process.max_low_q_fraction.is_some() {
        return Err("low-q and max-low-q-fraction require each other".into());
    }
    process.min_mean_q = values
        .get("min-mean-q")
        .map(|v| fraction("min-mean-q", v, 93.0))
        .transpose()?;
    process.split_units = values
        .get("split-units")
        .map(|v| integer("split-units", v))
        .transpose()?
        .unwrap_or(0);
    if values.contains_key("split-units") && process.split_units == 0 {
        return Err("split-units must be positive".into());
    }
    process.sample_fraction = fraction("sample-fraction", &get("sample-fraction", "1"), 1.0)?;
    let seed = get("sample-seed", "0");
    process.sample_seed = if !seed.is_empty() && seed.bytes().all(|b| b.is_ascii_digit()) {
        seed.parse::<u64>()
            .map_err(|_| "sample-seed requires unsigned64")?
    } else {
        return Err("sample-seed requires unsigned64".into());
    };
    if values.contains_key("sample-seed") && process.sample_fraction == 1.0 {
        return Err("explicit sample-seed requires sample-fraction <1".into());
    }
    process.repetition=match get("repetition","false").as_str(){"true"=>true,"false"=>false,_=>return Err("repetition requires boolean".into())};
    process.repetition_units=integer("repetition-units",&get("repetition-units","100000"))?;
    if process.repetition_units==0{return Err("repetition-units must be positive".into());}
    process.overrep_fraction=values.get("overrep-fraction").map(|v|fraction("overrep-fraction",v,1.0)).transpose()?;
    if !process.repetition&&(values.contains_key("repetition-units")||process.overrep_fraction.is_some()){return Err("repetition controls require repetition=true".into());}
    process.profile = get("profile", "ordinary");
    if !["ordinary", "srna"].contains(&process.profile.as_str()) {
        return Err("profile requires ordinary|srna".into());
    }
    process.srna_min_insert = integer("srna-min-insert", &get("srna-min-insert", "1"))?;
    if process.srna_min_insert == 0 {
        return Err("srna-min-insert must be positive".into());
    }
    process.srna_reject5 = match get("srna-reject5", "false").as_str() {
        "true" => true,
        "false" => false,
        _ => return Err("srna-reject5 requires boolean".into()),
    };
    process.srna_reject_poly_a = match get("srna-reject-poly-a", "false").as_str() {
        "true" => true,
        "false" => false,
        _ => return Err("srna-reject-poly-a requires boolean".into()),
    };
    if process.profile != "srna"
        && ["srna-min-insert", "srna-reject5", "srna-reject-poly-a"]
            .iter()
            .any(|k| values.contains_key(*k))
    {
        return Err("srna controls require profile srna".into());
    }
    process.pair_policy = get("pair-policy", "both-pass");
    if !["both-pass", "either-pass", "r1-pass"].contains(&process.pair_policy.as_str()) {
        return Err("pair-policy requires both-pass|either-pass|r1-pass".into());
    }
    process.overlap.merge = match get("merge", "false").as_str() {
        "true" => true,
        "false" => false,
        _ => return Err("merge requires boolean".into()),
    };
    process.overlap.correct = match get("correct-overlap", "false").as_str() {
        "true" => true,
        "false" => false,
        _ => return Err("correct-overlap requires boolean".into()),
    };
    if process.overlap.merge {
        if values.get("correct-overlap").is_some_and(|v| v == "false") {
            return Err("merge includes overlap correction, as in fastp; omit --correct-overlap=false".into());
        }
        process.overlap.correct = true;
    }
    process.overlap.minimum = integer("overlap-min", &get("overlap-min", "20"))?;
    if process.overlap.minimum == 0 {
        return Err("overlap-min must be positive".into());
    }
    process.overlap.max_error =
        fraction("overlap-max-error", &get("overlap-max-error", "0.1"), 1.0)?;
    process.overlap.min_insert = values
        .get("min-insert-length")
        .map(|s| integer("min-insert-length", s))
        .transpose()?;
    if process.overlap.min_insert == Some(0) {
        return Err("min-insert-length must be positive".into());
    }
    process.overlap.discard_unmerged = match get("unmerged-mode", "paired").as_str() {
        "paired" => false,
        "discard" => true,
        _ => return Err("unmerged-mode requires paired|discard".into()),
    };
    if ["overlap-min", "overlap-max-error"]
        .iter()
        .any(|k| values.contains_key(*k))
        && !process.overlap.enabled()
    {
        return Err("overlap parameters require merge, correction or min-insert-length".into());
    }
    if (values.contains_key("merged-out") || values.contains_key("unmerged-mode"))
        && !process.overlap.merge
    {
        return Err("merged-out/unmerged-mode require merge".into());
    }
    if (process.overlap.merge || process.overlap.correct) && process.pair_policy != "both-pass" {
        return Err("merge/correction require both-pass pair-policy".into());
    }
    process.screen.method = get(
        "screen-method",
        if repeated.contains_key("screen-reference") {
            "kmer-exact"
        } else {
            "off"
        },
    );
    process.screen.action = get("screen-action", "inspect");
    process.screen.pair = get("screen-pair", "any");
    process.screen.unscorable = get("screen-unscorable", "report");
    process.screen.k = integer("screen-k", &get("screen-k", "31"))?;
    process.screen.min_hits = integer("screen-min-hits", &get("screen-min-hits", "2"))?;
    process.screen.references = repeated.remove("screen-reference").unwrap_or_default();
    process.screen.hits = values.get("screen-hits").cloned();
    let screen = &process.screen;
    if !["off", "kmer-exact", "supplied-hits"].contains(&screen.method.as_str())
        || !["inspect", "remove", "retain"].contains(&screen.action.as_str())
        || !["any", "both"].contains(&screen.pair.as_str())
        || !["report", "keep", "reject"].contains(&screen.unscorable.as_str())
        || screen.k == 0
        || screen.min_hits == 0
    {
        return Err(
            "invalid reference screen method/action/pair/unscorable or positive k/hits".into(),
        );
    }
    if screen.method == "off"
        && values
            .keys()
            .any(|k| k.starts_with("screen-") && k != "screen-method")
    {
        return Err("screen options require enabled screen-method".into());
    }
    if screen.action_enabled()
        && (!values.contains_key("screen-unscorable") || screen.unscorable == "report")
    {
        return Err("reference actions require explicit screen-unscorable keep|reject".into());
    }
    if screen.method == "kmer-exact" && (screen.references.is_empty() || screen.hits.is_some()) {
        return Err("kmer screening requires references and excludes screen-hits".into());
    }
    if screen.method == "supplied-hits"
        && (screen.hits.is_none()
            || ["screen-k", "screen-min-hits"]
                .iter()
                .any(|k| values.contains_key(*k)))
    {
        return Err(
            "supplied-hits requires screen-hits and excludes native k/threshold controls".into(),
        );
    }
    process.min_overlap = integer("adapter-min-overlap", &get("adapter-min-overlap", "3"))?;
    if process.min_overlap == 0 {
        return Err("adapter-min-overlap must be at least1".into());
    }
    process.max_error = fraction("adapter-max-error", &get("adapter-max-error", "0.1"), 1.0)?;
    process.adapter_discard = match get("adapter-action", "trim").as_str() {
        "trim" => false,
        "discard" => true,
        _ => return Err("adapter-action must be trim or discard".into()),
    };
    process.adapter_overlap = match get("adapter-overlap", "false").as_str() {
        "true" => true,
        "false" => false,
        _ => return Err("adapter-overlap requires boolean".into()),
    };
    process.adapters = [
        repeated.remove("adapter3-r1").unwrap_or_default(),
        repeated.remove("adapter3-r2").unwrap_or_default(),
    ];
    process.primers = [
        repeated.remove("adapter5-r1").unwrap_or_default(),
        repeated.remove("adapter5-r2").unwrap_or_default(),
    ];
    process.adapter_names = process.adapters.clone();
    process.primer_names = process.primers.clone();
    let adapter_files: Vec<PathBuf> = repeated
        .remove("adapter-file")
        .unwrap_or_default()
        .into_iter()
        .map(PathBuf::from)
        .collect();
    let mut resources = catalogue_resources;
    read_adapters(&adapter_files, &mut process, &mut resources)?;
    for motifs in process
        .adapters
        .iter_mut()
        .chain(process.primers.iter_mut())
    {
        for motif in motifs {
            *motif = motif.to_ascii_uppercase();
        }
    }
    if process.infer_mode == "off"
        && process
            .adapters
            .iter()
            .chain(process.primers.iter())
            .all(Vec::is_empty)
        && ["adapter-action", "adapter-min-overlap", "adapter-max-error"]
            .iter()
            .any(|key| values.contains_key(*key))
    {
        return Err("adapter matching controls require explicit adapter motifs".into());
    }
    for mate in 0..2 {
        if (process.infer_mode == "both"
            || process.infer_mode == if mate == 0 { "r1" } else { "r2" })
            && !process.adapters[mate].is_empty()
        {
            return Err("inference excludes explicit3prime motifs for the same mate".into());
        }
    }
    process.compile_three_prime()?;
    let input_format = get("input-format", "auto");
    let output_format = get("output-format", "auto");
    for format in [&input_format, &output_format] {
        if !["auto", "fastq", "gzip"].contains(&format.as_str()) {
            return Err("format must be auto,fastq or gzip".into());
        }
    }
    let decoder = get("decoder", "auto");
    if !["auto", "zlib", "rapidgzip"].contains(&decoder.as_str()) {
        return Err("decoder must be auto,zlib or rapidgzip".into());
    }
    let (lane_manifest, lane_parent, mut inputs, lane_count) =
        if let Some(s) = api {
            let paths = s.lanes.first().map(|l| l.inputs.iter().filter_map(|d| if let crate::job_api::Location::Path(p) = &d.location { Some(p.clone()) } else { None }).collect()).unwrap_or_default();
            (None, PathBuf::new(), paths, s.lanes.len())
        } else if let Some(path) = values.get("lanes") {
            if !values.contains_key("sample-id") {
                return Err("lanes requires explicit sample-id".into());
            }
            let (text, parent, inputs, count) = inspect_lanes(path)?;
            (Some(text), parent, inputs, count)
        } else {
            (
                None,
                PathBuf::new(),
                vec![PathBuf::from(required("r1")?)],
                0,
            )
        };
    if api.is_none() && values.contains_key("r2") {
        inputs.push(PathBuf::from(required("r2")?));
    }
    if let (Some(text), Some(path)) = (&lane_manifest, values.get("lanes")) {
        use std::io::Write;
        let mut child = std::process::Command::new("sha256sum")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| e.to_string())?;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(text.as_bytes())
            .map_err(|e| e.to_string())?;
        let result = child.wait_with_output().map_err(|e| e.to_string())?;
        if !result.status.success() {
            return Err("lane manifest snapshot hashing failed".into());
        }
        let digest = String::from_utf8(result.stdout)
            .map_err(|e| e.to_string())?
            .split_whitespace()
            .next()
            .ok_or("empty manifest digest")?
            .to_string();
        resources.push(serde_json::json!({"kind":"lane-manifest","path":path,"sha256":digest,"hash_scope":"exact configuration TSV snapshot; later lane data not opened","lane_ids":text.lines().skip(1).map(|l|l.split('\t').next().unwrap()).collect::<Vec<_>>()}));
    }
    if let Some(text) = &lane_manifest {
        for line in text.lines().skip(1) {
            for path in line.split('\t').skip(1).filter(|p| !p.is_empty()) {
                if input_format != "auto" && format_codec(Path::new(path), "auto") != input_format {
                    return Err(
                        "lane explicit input format must agree with every path suffix".into(),
                    );
                }
            }
        }
    }
    let paired = api.map_or(inputs.len() == 2 || interleaved, |s| s.layout != crate::job_api::Layout::Se);
    if process.overlap.enabled() && !paired {
        return Err("overlap controls require PE".into());
    }
    if out_interleaved && (!paired || values.contains_key("out1") || values.contains_key("out2")) {
        return Err("out-interleaved requires PE and excludes out1/out2".into());
    }

    if api.is_none() && !qc_only && paired
        && !out_interleaved
        && !process.overlap.discard_unmerged
        && !values.contains_key("out2")
    {
        return Err("PE requires --out2".into());
    }
    if !paired && values.contains_key("out2") {
        return Err("SE forbids --out2".into());
    }
    if !paired
        && (values.contains_key("adapter3-r2")
            || values.contains_key("adapter5-r2")
            || !process.adapters[1].is_empty()
            || !process.primers[1].is_empty())
    {
        return Err("SE forbids R2 adapters".into());
    }
    if !paired
        && (values.contains_key("retain-r2") || process.capture.labels.iter().any(|l| l.mate == 1))
    {
        return Err("R2 capture/retention requires paired input".into());
    }
    if !paired && ["r2", "both"].contains(&process.infer_mode.as_str()) {
        return Err("R2/both inference requires PE".into());
    }
    if !paired && values.contains_key("cut-r2") {
        return Err("SE forbids --cut-r2, including0,0".into());
    }
    let cuts = [
        parse_cut(&get("cut-r1", "0,0"))?,
        parse_cut(&get("cut-r2", "0,0"))?,
    ];
    if process.adapter_overlap {
        if !paired || qc_only || process.profile != "ordinary" || process.adapter_discard
            || process.infer_mode != "off"
            || process.adapters.iter().any(Vec::is_empty)
        {
            return Err("adapter-overlap requires ordinary PE processing, trim action, and supplied 3' adapters for both mates; inference is unsupported".into());
        }
        let adapter_stage = stage_order.iter().position(|s| *s == ProcessStage::Adapters).unwrap();
        let quality_stage = stage_order.iter().position(|s| *s == ProcessStage::Quality).unwrap();
        if cuts.iter().any(|cut| cut[0] > 0)
            || process.capture.remove && !process.capture.labels.is_empty()
            || process.capture.retain.iter().any(Option::is_some)
            || process.primers.iter().any(|v| !v.is_empty())
            || quality_stage < adapter_stage && matches!(quality_mode.as_str(), "ends" | "mott")
        {
            return Err("adapter-overlap requires unchanged read starts before its adapter stage; fixed head cuts, capture removal/retention, 5' adapters, and earlier quality ends/Mott are unsupported".into());
        }
    }
    let mut outputs = Vec::new();
    if let Some(s) = api {
        outputs = s.outputs.iter().filter(|d|crate::job_api::main_channel(&d.channel).is_some()).filter_map(|d| if let crate::job_api::Location::Path(p) = &d.location { Some(p.clone()) } else { None }).collect();
    } else if qc_only {
        // Input inspection has no FASTQ destination or inactive default writer.
    } else if process.overlap.discard_unmerged {
        if ["out1", "out2", "out-interleaved"]
            .iter()
            .any(|k| values.contains_key(*k))
        {
            return Err("unmerged-mode discard forbids ordinary main outputs".into());
        }
    } else {
        outputs.push(PathBuf::from(required(if out_interleaved {
            "out-interleaved"
        } else {
            "out1"
        })?));
        if paired && !out_interleaved {
            outputs.push(PathBuf::from(required("out2")?));
        }
    }
    if api.is_none() && process.overlap.merge {
        let path = PathBuf::from(required("merged-out")?);
        if path.as_os_str() == "-" {
            return Err("merged-out requires a named path".into());
        }
        outputs.push(path);
    }
    let mut auxiliary: [Option<PathBuf>; 6] = [
        "reject1",
        "reject2",
        "transformed1",
        "transformed2",
        "singleton1",
        "singleton2",
    ]
    .map(|key| values.get(key).map(PathBuf::from));
    if let Some(s)=api {for(i,key)in ["reject1","reject2","transformed1","transformed2","singleton1","singleton2"].iter().enumerate(){if let Some(d)=s.outputs.iter().find(|d|d.channel==*key){if let crate::job_api::Location::Path(p)=&d.location{auxiliary[i]=Some(p.clone());}}}}
    let declared_aux=|i:usize|auxiliary[i].is_some()||api.is_some_and(|s|s.outputs.iter().any(|d|d.channel==["reject1","reject2","transformed1","transformed2","singleton1","singleton2"][i]));
    for first in [0, 2, 4] {
        if !paired && declared_aux(first + 1) {
            return Err("R2 optional channel requires PE".into());
        }
        if paired && declared_aux(first) != declared_aux(first + 1) {
            return Err("paired reject/transformed channels require both mates".into());
        }
    }
    if declared_aux(4) || declared_aux(5) {
        if !paired || process.pair_policy != "both-pass" {
            return Err("singleton channels require PE and both-pass pair-policy".into());
        }
    }
    if auxiliary.iter().flatten().any(|p| p.as_os_str() == "-") {
        return Err("optional diagnostic channels must be named files".into());
    }
    let mut html_report=values.get("html-report").map(PathBuf::from);
    process.html_report=html_report.is_some();
    if html_report.as_ref().is_some_and(|p|p.as_os_str()=="-"){return Err("html-report requires a named path".into());}
    let mut match_report = values.get("match-report").map(PathBuf::from);
    let match_limit = integer("match-limit", &get("match-limit", "1000"))?;
    if match_limit == 0 {
        return Err("match-limit must be positive".into());
    }
    if match_report.is_none() && values.contains_key("match-limit") {
        return Err("match-limit requires match-report".into());
    }
    if match_report.as_ref().is_some_and(|p| p.as_os_str() == "-") {
        return Err("match-report must be a named file".into());
    }
    let mut report = if let Some(s) = api { s.json_report.clone().unwrap_or_default() } else { PathBuf::from(required("report")?) };
    let mut record_manifest = auxiliary
        .iter()
        .any(Option::is_some)
        .then(|| PathBuf::from(format!("{}.records.tsv", report.display())))
        .filter(|_|api.is_none_or(|s|s.json_report.is_some()));
    if report.as_os_str() == "-" {
        return Err("report must be a file, never stdout".into());
    }
    let mut input_codecs = Vec::new();
    let mut split_templates = None;
    if api.is_none() {
    for p in &mut inputs {
        if p.as_os_str() == "-" {
            if inputs_stream_forbidden(&values) {
                return Err("stdin requires one input stream and explicit sample-id; rapidgzip is not supported".into());
            }
            input_codecs.push(format_codec(p, &input_format));
            continue;
        }
        *p = std::fs::canonicalize(&p)
            .map_err(|e| format!("cannot identify input {}: {e}", p.display()))?;
        if !p.is_file() {
            return Err("process inputs must be regular files".into());
        }
        let inferred = format_codec(p, "auto");
        let codec = format_codec(p, &input_format);
        if inferred != codec {
            return Err("process explicit input format must agree with .gz suffix".into());
        }
        let mut magic = [0u8; 2];
        let n = File::open(&p)
            .and_then(|mut f| f.read(&mut magic))
            .map_err(|e| format!("cannot inspect input: {e}"))?;
        if (n == 2 && magic == [31, 139]) != (codec == "gzip") {
            return Err("input format/magic mismatch".into());
        }
        input_codecs.push(codec);
    }
    if inputs.len() == 2 && inputs[0] == inputs[1] {
        return Err("mate input paths must be distinct".into());
    }
    if let Some(text) = &lane_manifest {
        input_codecs = text
            .lines()
            .skip(1)
            .flat_map(|line| line.split('\t').skip(1))
            .filter(|path| !path.is_empty())
            .map(|path| format_codec(Path::new(path), &input_format))
            .collect();
    }
    if decoder == "rapidgzip" && (threads < 14 || !input_codecs.iter().any(|v| v == "gzip")) {
        return Err("rapidgzip requires gzip input and at least14 threads".into());
    }
    let mut resource_paths: Vec<PathBuf> = process
        .screen
        .references
        .iter()
        .map(PathBuf::from)
        .collect();
    if let Some(hits) = &process.screen.hits {
        resource_paths.push(PathBuf::from(hits));
        resource_paths.push(PathBuf::from(format!("{hits}.provenance.json")));
    }
    for p in &mut resource_paths {
        if let Ok(canonical) = std::fs::canonicalize(&p) {
            *p = canonical;
        } else if let Some((parent, name)) = p
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .or(Some(Path::new(".")))
            .zip(p.file_name())
        {
            if let Ok(parent) = std::fs::canonicalize(parent) {
                *p = parent.join(name);
            }
        }
    }
    let mut distinct = inputs.clone();
    distinct.extend(resource_paths.iter().cloned());
    for p in outputs
        .iter_mut()
        .chain(auxiliary.iter_mut().flatten())
        .chain(record_manifest.iter_mut())
        .chain(match_report.iter_mut())
        .chain(html_report.iter_mut())
        .chain(std::iter::once(&mut report))
    {
        if p.as_os_str() == "-" {
            if (paired && !out_interleaved)
                || output_format != "fastq"
                || !values.contains_key("output-format")
            {
                return Err("one SE stdout channel requires explicit output-format fastq".into());
            }
            continue;
        }
        let name = p
            .file_name()
            .ok_or("output path requires a filename")?
            .to_owned();
        let parent = p
            .parent()
            .filter(|v| !v.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let canonical =
            std::fs::canonicalize(parent).map_err(|e| format!("output parent must exist: {e}"))?;
        if !canonical.is_dir() {
            return Err("output parent is not a directory".into());
        }
        *p = canonical.join(name);
        let aliases_lane = lane_manifest.as_ref().is_some_and(|text| {
            text.lines()
                .skip(1)
                .flat_map(|line| line.split('\t').skip(1))
                .filter(|path| !path.is_empty())
                .any(|path| {
                    let declared = lane_parent.join(path);
                    declared
                        .parent()
                        .zip(declared.file_name())
                        .is_some_and(|(parent, name)| {
                            std::fs::canonicalize(parent)
                                .is_ok_and(|parent| parent.join(name) == *p)
                        })
                })
        });
        if p.symlink_metadata().is_ok() || distinct.contains(p) || aliases_lane {
            return Err(format!(
                "output exists or aliases another path: {}",
                p.display()
            ));
        }
        distinct.push(p.clone());
    }
    split_templates = if process.split_units > 0 {
        let mut protected = inputs.clone();
        protected.extend(resource_paths.iter().cloned());
        protected.extend(auxiliary.iter().flatten().cloned());
        protected.extend(record_manifest.iter().cloned());
        protected.extend(match_report.iter().cloned());
        protected.extend(html_report.iter().cloned());
        protected.push(report.clone());
        if let Some(text) = &lane_manifest {
            for path in text
                .lines()
                .skip(1)
                .flat_map(|l| l.split('\t').skip(1))
                .filter(|p| !p.is_empty())
            {
                let declared = lane_parent.join(path);
                if let Some((parent, name)) = declared.parent().zip(declared.file_name()) {
                    if let Ok(parent) = std::fs::canonicalize(parent) {
                        protected.push(parent.join(name));
                    }
                }
            }
        }
        Some(prepare_split_templates(&outputs,&protected,report.parent().unwrap(),&mut process)?)
    } else {
        None
    };
    #[cfg(unix)]
    {
        let report_device = std::fs::metadata(report.parent().unwrap())
            .map_err(|e| e.to_string())?
            .dev();
        for p in outputs
            .iter()
            .chain(auxiliary.iter().flatten())
            .chain(record_manifest.iter())
            .chain(match_report.iter())
            .chain(html_report.iter())
        {
            if p.as_os_str() == "-" {
                continue;
            }
            if std::fs::metadata(p.parent().unwrap())
                .map_err(|e| e.to_string())?
                .dev()
                != report_device
            {
                return Err(
                    "process output/report parents must share a filesystem for hard-link publication"
                        .into(),
                );
            }
        }
    }
    #[cfg(unix)]
    if outputs.iter().any(|p| p.as_os_str() == "-") {
        if let Ok(out) = std::fs::metadata("/dev/stdout") {
            let alias = |path: &Path| {
                std::fs::metadata(if path.as_os_str() == "-" {
                    Path::new("/dev/stdin")
                } else {
                    path
                })
                .is_ok_and(|input| input.dev() == out.dev() && input.ino() == out.ino())
            };
            if inputs.iter().chain(resource_paths.iter()).any(|p| alias(p)) {
                return Err("stdout aliases an input descriptor/file".into());
            }
            if let Some(text) = &lane_manifest {
                for line in text.lines().skip(1) {
                    for f in line.split('\t').skip(1).filter(|s| !s.is_empty()) {
                        if alias(&lane_parent.join(f)) {
                            return Err("stdout aliases a declared lane input".into());
                        }
                    }
                }
            }
        }
    }
    } else {
        let spec = api.unwrap();
        for lane in &spec.lanes { for input in &lane.inputs { input_codecs.push(crate::job_api::input_codec(input)); } }
        if decoder == "rapidgzip" && spec.lanes.iter().flat_map(|l|&l.inputs).all(|d|matches!(d.location, crate::job_api::Location::Path(_))) && (threads < 14 || !input_codecs.iter().any(|c|c=="gzip")) { return Err("rapidgzip requires seekable path gzip inputs and at least14 threads".into()); }
        crate::job_api::check_path_aliases(spec,&[],&[])?;
        if process.split_units>0 {
            if spec.outputs.iter().any(|d|crate::job_api::main_channel(&d.channel).is_some() && matches!(d.location,crate::job_api::Location::Handle(_))) {
                return Err("split requires named Path templates, not fixed owned handles".into());
            }
            for output in &mut outputs { *output=crate::job_api::path_identity(output)?; }
            let mut protected=spec.lanes.iter().flat_map(|l|&l.inputs).filter_map(|d|if let crate::job_api::Location::Path(p)=&d.location{Some(p.clone())}else{None}).collect::<Vec<_>>();
            protected.extend(spec.outputs.iter().filter(|d|crate::job_api::main_channel(&d.channel).is_none()).filter_map(|d|if let crate::job_api::Location::Path(p)=&d.location{Some(p.clone())}else{None}));
            protected.extend(spec.json_report.iter().cloned());
            protected.extend(resources.iter().filter_map(|r|r["path"].as_str().map(PathBuf::from)));
            protected.extend(values.get("resource-catalog").map(PathBuf::from));
            protected.extend(process.screen.references.iter().map(PathBuf::from));
            if let Some(hits)=&process.screen.hits {protected.extend([PathBuf::from(hits),PathBuf::from(format!("{hits}.provenance.json"))]);}
            protected.extend(match_report.iter().chain(html_report.iter()).cloned());
            for path in &mut protected {*path=crate::job_api::path_identity(path)?;}
            let parent=spec.json_report.as_ref().and_then(|p|p.parent()).map(Path::to_path_buf).unwrap_or_else(||std::env::var_os("TMPDIR").map(PathBuf::from).unwrap_or_else(||PathBuf::from(".scratch/tmp")));
            split_templates=Some(prepare_split_templates(&outputs,&protected,&parent,&mut process)?);
        }

    }
    let auxiliary_codecs = std::array::from_fn(|i| {
        auxiliary[i]
            .as_ref()
            .map_or(String::new(), |p| format_codec(p, &output_format))
    });
    let output_codecs: Vec<String> = if let Some(s) = api { s.outputs.iter().filter(|d| crate::job_api::main_channel(&d.channel).is_some()).map(crate::job_api::output_codec).collect() } else { outputs.iter().map(|p| format_codec(p, &output_format)).collect() };
    let gzip_level_active = output_codecs
        .iter()
        .chain(auxiliary_codecs.iter())
        .any(|codec| codec == "gzip");
    validate_gzip_level_use(values.contains_key("gzip-level"), gzip_level_active)?;
    let sample_id = values.get("sample-id").cloned().unwrap_or_else(|| {
        if let Some(s)=api {if inputs.is_empty(){return s.lanes.first().map(|l|l.id.clone()).unwrap_or_else(||"input".into());}}
        inputs[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned()
    });
    if sample_id.trim().is_empty() {
        return Err("sample-id must be nonempty".into());
    }
    let progress_value = get("progress-seconds", "10");
    if progress_value.is_empty() || !progress_value.bytes().all(|b| b.is_ascii_digit()) {
        return Err("progress-seconds must be a nonnegative INTEGER".into());
    }
    let progress_seconds = progress_value
        .parse::<u64>()
        .map_err(|_| "progress-seconds exceeds supported integer range")?;
    let metrics = match get("metrics", "true").as_str() {
        "true" => true,
        "false" => false,
        _ => return Err("metrics must be true or false".into()),
    };
    if process.screen.method == "supplied-hits" && inputs.iter().any(|p| p.as_os_str() == "-") {
        return Err("supplied-hits requires seekable original inputs; stdin unsupported".into());
    }
    if process.profile == "srna" {
        if process.screen.action_enabled()
            || paired
            || process.overlap.enabled()
            || process.capture.enabled()
            || cuts != [[0, 0], [0, 0]]
            || quality_mode != "off"
            || tail_mask != 0
            || stage_order != crate::native_processing::DEFAULT_STAGE_ORDER
            || process.infer_mode != "off"
            || process.adapter_discard
            || process.pair_policy != "both-pass"
        {
            return Err("srna profile excludes PE/overlap/capture/fixed cuts/quality/tails/stage-order/inference/alternate pair policy and requires adapter trim".into());
        }
        if process.adapters[0].is_empty() {
            return Err("srna profile requires explicit adapter3-r1".into());
        }
        if process.srna_reject5 && process.primers[0].is_empty() {
            return Err("srna-reject5 requires adapter5-r1".into());
        }
    }
    Ok(RunPlan {
        api_spec: api.cloned().map(Arc::new),
        sample_id,
        progress_seconds,
        metrics,
        resources,
        lane_manifest,
        lane_parent,
        lane_count,
        interleaved,
        out_interleaved,
        limit_units,
        process: Arc::new(process),
        split_templates,
        adapter_files,
        inputs,
        outputs,
        auxiliary,
        auxiliary_codecs,
        record_manifest,
        match_report,
        html_report,
        match_limit,
        report,
        output_directory,
        input_phred,
        output_phred,
        threads,
        input_codecs,
        output_codecs,
        decoder,
        supplied: values.keys().filter(|key| !implicit_report || key.as_str() != "report").cloned().collect(),
        memory_total_bytes,
        config_resource,
        config_values,
        cli_keys,
        cuts,
        quality_mode,
        quality_q,
        quality_window,
        tail_bases,
        tail_mask,
        tail_min,
        stage_order,
        gzip_level,
        gzip_level_active,
        thread_budget,
        threads_auto,
    })
}

// At most forty decimal digits are unknown, even for repeated placeholders.
// Equality classes plus the u64 formatting bounds cover unequal chunk numbers.
fn prepare_split_templates(outputs:&[PathBuf],protected:&[PathBuf],stage_parent:&Path,process:&mut ProcessSettings)->Result<Arc<Vec<PathBuf>>,String> {
        if outputs
            .iter()
            .any(|p| p.as_os_str() == "-" || !p.to_string_lossy().contains("{chunk}"))
        {
            return Err("split requires {chunk} templates and forbids stdout".into());
        }
        for i in 0..outputs.len() {
            for j in i + 1..outputs.len() {
                if split_family_collision(
                    &outputs[i].to_string_lossy(),
                    &outputs[j].to_string_lossy(),
                    true,
                )
                .is_some()
                {
                    return Err("split template families alias across chunk numbers".into());
                }
            }
        }
        for template in outputs {
            for p in protected {
                if p.as_os_str() != "-"
                    && split_family_collision(
                        &template.to_string_lossy(),
                        &p.to_string_lossy(),
                        false,
                    )
                    .is_some()
                {
                    return Err(format!(
                        "split family aliases declared input/output: {}",
                        p.display()
                    ));
                }
            }
        }
        let path_bytes = outputs
            .iter()
            .map(|p| {
                let s = p.to_string_lossy();
                s.len() + s.matches("{chunk}").count() * 20
            })
            .sum::<usize>();
        process.split_metadata_unit_bound = 8
            * std::mem::size_of::<crate::output::NativeSplitFile>()
            + 8 * path_bytes
            + 16 * (stage_parent.as_os_str().len() + 256)
            + 4096;
    Ok(Arc::new(outputs.to_vec()))
}

fn split_family_collision(a: &str, b: &str, b_template: bool) -> Option<(u64, u64)> {
    fn root(p: &mut [usize; 40], mut x: usize) -> usize {
        while p[x] != x {
            p[x] = p[p[x]];
            x = p[x];
        }
        x
    }
    let expand = |text: &str, width: usize, offset: usize, template: bool| {
        let bytes = text.as_bytes();
        let mut tokens = Vec::<u16>::new();
        let mut i = 0;
        while i < bytes.len() {
            if template && bytes[i..].starts_with(b"{chunk}") {
                tokens.extend((0..width).map(|j| 256 + (offset + j) as u16));
                i += 7;
            } else {
                tokens.push(bytes[i] as u16);
                i += 1;
            }
        }
        tokens
    };
    for na in 6..=20 {
        for nb in 6..=if b_template { 20 } else { 6 } {
            let x = expand(a, na, 0, true);
            let y = expand(b, nb, 20, b_template);
            if x.len() != y.len() {
                continue;
            }
            let mut parents = std::array::from_fn(|i| i);
            let mut good = true;
            for (&u, &v) in x.iter().zip(&y) {
                if u >= 256 && v >= 256 {
                    let r = root(&mut parents, (u - 256) as usize);
                    let s = root(&mut parents, (v - 256) as usize);
                    parents[r] = s;
                } else if u < 256 && v < 256 && u != v {
                    good = false;
                    break;
                }
            }
            if !good {
                continue;
            }
            let mut fixed = [None; 40];
            for (&u, &v) in x.iter().zip(&y) {
                if (u >= 256) != (v >= 256) {
                    let (node, ch) = if u >= 256 {
                        ((u - 256) as usize, v)
                    } else {
                        ((v - 256) as usize, u)
                    };
                    if !(48..=57).contains(&ch) {
                        good = false;
                        break;
                    }
                    let r = root(&mut parents, node);
                    let digit = (ch - 48) as u8;
                    if fixed[r].is_some_and(|n| n != digit) {
                        good = false;
                        break;
                    }
                    fixed[r] = Some(digit);
                }
            }
            if !good {
                continue;
            }
            // Six-wide numbers permit leading zeros but not the all-zero value.
            // Larger widths require a nonzero first digit. The least assignment
            // satisfying these constraints suffices to test the u64 upper bound.
            for force_a in (0..if na == 6 { 6 } else { 1 }).rev() {
                for force_b in (0..if b_template && nb == 6 { 6 } else { 1 }).rev() {
                    let mut digits = std::array::from_fn::<u8, 40, _>(|i| fixed[i].unwrap_or(0));
                    let mut possible = true;
                    for node in [
                        Some(force_a),
                        if b_template { Some(20 + force_b) } else { None },
                    ]
                    .into_iter()
                    .flatten()
                    {
                        let r = root(&mut parents, node);
                        if fixed[r] == Some(0) {
                            possible = false;
                            break;
                        }
                        digits[r] = digits[r].max(1);
                    }
                    if !possible {
                        continue;
                    }
                    let mut number = |offset: usize, width: usize| {
                        let mut n = 0u64;
                        for j in 0..width {
                            n = n
                                .checked_mul(10)?
                                .checked_add(digits[root(&mut parents, offset + j)] as u64)?;
                        }
                        if n == 0 { None } else { Some(n) }
                    };
                    if let Some(first) = number(0, na) {
                        if !b_template {
                            return Some((first, 0));
                        }
                        if let Some(second) = number(20, nb) {
                            return Some((first, second));
                        }
                    }
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    #[test]
    fn m8_configured_reference_actions_cannot_hide_behind_inspection_overrides(){
        use std::time::{SystemTime,UNIX_EPOCH};
        let d=std::path::PathBuf::from(std::env::var("TMPDIR").unwrap()).join(format!("m8-config-original-{}",SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));std::fs::create_dir(&d).unwrap();std::fs::write(d.join("input.fq"),"@unit\nACG\n+\nIII\n").unwrap();std::fs::write(d.join("reference.fa"),">literal\nACG\n").unwrap();
        for(i,(key,value,overridden,valid))in [("screen-action","remove","inspect",false),("screen-action","retain","inspect",false),("screen-unscorable","keep","report",false),("screen-unscorable","reject","report",false),("screen-action","inspect","inspect",true),("screen-unscorable","report","report",true)].into_iter().enumerate(){
            let config=d.join(format!("config-{i}.json"));std::fs::write(&config,serde_json::json!({"job":"process","options":{key:value}}).to_string()).unwrap();
            let args=vec!["--config".into(),config.to_string_lossy().into_owned(),"--qc-only".into(),"--r1".into(),d.join(if valid{"input.fq"}else{"missing.fq"}).to_string_lossy().into_owned(),"--screen-reference".into(),d.join("reference.fa").to_string_lossy().into_owned(),format!("--{key}"),overridden.into(),"--report".into(),d.join(format!("report-{i}.json")).to_string_lossy().into_owned(),"--threads".into(),"1".into()];
            let result=super::parse(&args);if valid{assert!(result.unwrap().process.qc_only);}else{assert!(result.unwrap_err().contains("qc-only permits reference inspection only"));}assert!(!d.join(format!("report-{i}.json")).exists());
        }
    }
    use super::{
        parse, parse_cut, parse_gzip_level, parse_quality, resolve_thread_budget,
        validate_gzip_level_use,
    };

    #[test]
    fn m7_typed_domains_and_all_resource_output_aliases() {
        for (key, value) in [
            ("screen-k", serde_json::json!(0)),
            ("screen-min-hits", serde_json::json!(0)),
            ("screen-method", serde_json::json!("unknown")),
            ("screen-unscorable", serde_json::json!("ignore")),
            ("screen-pair", serde_json::json!("either")),
            ("screen-reference", serde_json::json!([1])),
        ] {
            assert!(super::config_arguments(key, &value).is_err());
        }
        let d = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../.scratch/m7-alias-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        let input = d.join("raw.fq");
        std::fs::write(&input, "@x\nACG\n+\nIII\n").unwrap();
        for flag in ["--out1", "--report"] {
            for resource in ["screen-reference", "screen-hits"] {
                let report = d.join("report.json");
                let output = d.join("out.fq");
                let aliased = if flag == "--report" { &report } else { &output };
                let mut args = vec![
                    "--r1".into(),
                    input.to_string_lossy().into_owned(),
                    "--out1".into(),
                    output.to_string_lossy().into_owned(),
                    "--report".into(),
                    report.to_string_lossy().into_owned(),
                    "--threads".into(),
                    "1".into(),
                    format!("--{resource}"),
                    aliased.to_string_lossy().into_owned(),
                ];
                if resource == "screen-hits" {
                    args.extend(["--screen-method".into(), "supplied-hits".into()]);
                }
                assert!(parse(&args).unwrap_err().contains("aliases"));
                assert!(!aliased.exists());
            }
        }
    }
    #[test]
    fn m6_configured_scalar_domains_cannot_be_overridden_into_validity() {
        for (key, value) in [
            ("merge", serde_json::json!("true")),
            ("correct-overlap", serde_json::json!(1)),
            ("overlap-min", serde_json::json!(0)),
            ("overlap-max-error", serde_json::json!(1.01)),
            ("min-insert-length", serde_json::json!(0)),
            ("correction-delta", serde_json::json!(94)),
            ("unmerged-mode", serde_json::json!("other")),
            ("merged-out", serde_json::json!("-")),
        ] {
            assert!(super::config_arguments(key, &value).is_err(), "{key}");
        }
        assert_eq!(
            super::config_arguments("merge", &serde_json::json!(true)).unwrap(),
            vec!["true"]
        );
    }
    #[test]
    fn m5_config_types_domains_and_effective_relationships_fail_before_input_io() {
        use serde_json::json;
        for (key, value) in [
            ("sample-fraction", json!(true)),
            ("sample-fraction", json!(-0.1)),
            ("sample-fraction", json!(1.1)),
            ("sample-seed", json!(-1)),
            ("sample-seed", json!(1.0)),
            ("split-units", json!(0)),
            ("split-units", json!(1.0)),
            ("pair-policy", json!("other")),
            ("singleton1", json!(false)),
        ] {
            assert!(
                super::config_arguments(key, &value).is_err(),
                "{key}={value}"
            );
        }
        for (key, value) in [
            ("sample-fraction", json!(0)),
            ("sample-fraction", json!(1)),
            ("sample-seed", json!(u64::MAX)),
            ("split-units", json!(1)),
            ("pair-policy", json!("either-pass")),
            ("pair-policy", json!("r1-pass")),
            ("singleton1", json!("path.fastq")),
        ] {
            assert!(
                super::config_arguments(key, &value).is_ok(),
                "{key}={value}"
            );
        }
        let args = [
            "--r1",
            "never-opened.fastq",
            "--out1",
            "never-created.fastq",
            "--report",
            "never-created.json",
            "--sample-fraction",
            "1",
            "--sample-seed",
            "17",
        ]
        .map(str::to_string);
        assert!(parse(&args).unwrap_err().contains("explicit sample-seed"));
    }

    #[test]
    fn split_families_cover_different_numbers_repeated_placeholders_and_u64_bounds() {
        assert_eq!(
            super::split_family_collision("/x/{chunk}0", "/x/0{chunk}", true),
            Some((1, 10))
        );
        assert!(super::split_family_collision("/x/{chunk}.R1", "/x/{chunk}.R2", true).is_none());
        assert_eq!(
            super::split_family_collision("/x/{chunk}-{chunk}", "/x/000001-000001", false),
            Some((1, 0))
        );
        assert!(
            super::split_family_collision("/x/{chunk}-{chunk}", "/x/000001-000002", false)
                .is_none()
        );
        assert!(super::split_family_collision("/x/{chunk}", "/x/000000", false).is_none());
        assert_eq!(
            super::split_family_collision("/x/{chunk}", "/x/18446744073709551615", false),
            Some((u64::MAX, 0))
        );
        assert!(
            super::split_family_collision("/x/{chunk}", "/x/18446744073709551616", false).is_none()
        );
    }

    #[test]
    fn optional_channels_require_report_before_input_io_for_cli_and_config() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../.scratch/missing-report-unit-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        for key in ["reject1", "transformed1"] {
            let mut args = vec![
                "--r1".into(),
                "never-opened-input.fastq".into(),
                "--out1".into(),
                "never-created-output.fastq".into(),
                format!("--{key}"),
                "never-created-optional.fastq".into(),
            ];
            assert_eq!(parse(&args).unwrap_err(), "--report requires a file path");
            args.extend([
                "--r2".into(),
                "never-opened-mate.fastq".into(),
                "--out2".into(),
                "never-created-mate.fastq".into(),
                format!("--{}2", key.trim_end_matches('1')),
                "never-created-optional-mate.fastq".into(),
            ]);
            assert_eq!(parse(&args).unwrap_err(), "--report requires a file path");
            let config = dir.join(format!("{key}.json"));
            std::fs::write(&config, serde_json::json!({"job":"process", "options":{key:"never-created-optional.fastq"}}).to_string()).unwrap();
            let configured = vec![
                "--config".into(),
                config.to_string_lossy().into_owned(),
                "--r1".into(),
                "never-opened-input.fastq".into(),
                "--out1".into(),
                "never-created-output.fastq".into(),
            ];
            assert_eq!(
                parse(&configured).unwrap_err(),
                "--report requires a file path"
            );
        }
    }

    #[test]
    fn named_destinations_reject_all_declared_lane_aliases_without_opening_later_data() {
        let dir = crate::test_support::workspace("lane-alias-unit");
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("first.fastq"), "@first\nAC\n+\nI5\n").unwrap();
        let manifest = dir.join("lanes.tsv");
        let args = vec![
            "--lanes".into(),
            manifest.to_string_lossy().into_owned(),
            "--sample-id".into(),
            "alias".into(),
            "--out1".into(),
            dir.join("out.fastq").to_string_lossy().into_owned(),
            "--report".into(),
            dir.join("report.json").to_string_lossy().into_owned(),
            "--limit-units".into(),
            "1".into(),
            "--threads".into(),
            "1".into(),
        ];
        for later in [
            "out.fastq",
            "report.json",
            "sub/../out.fastq",
            "sub/../report.json",
        ] {
            std::fs::write(
                &manifest,
                format!("lane_id\tr1\tr2\nL0\tfirst.fastq\t\nL1\t{later}\t\n"),
            )
            .unwrap();
            assert!(parse(&args).unwrap_err().contains("aliases"));
            assert!(!dir.join(later).exists());
        }
        std::fs::write(
            &manifest,
            "lane_id\tr1\tr2\nL0\tfirst.fastq\t\nL1\tmissing-later.fastq\t\n",
        )
        .unwrap();
        assert!(parse(&args).is_ok());
        assert!(!dir.join("missing-later.fastq").exists());
        let existing = dir.join("later-existing.fastq");
        std::fs::write(&existing, "@later\nGT\n+\nII\n").unwrap();
        std::fs::write(
            &manifest,
            "lane_id\tr1\tr2\nL0\tfirst.fastq\t\nL1\tlater-existing.fastq\t\n",
        )
        .unwrap();
        let hardlink = dir.join("later-hardlink.fastq");
        std::fs::hard_link(&existing, &hardlink).unwrap();
        let mut aliases = vec![existing.clone(), hardlink];
        #[cfg(unix)]
        {
            let symlink = dir.join("later-symlink.fastq");
            std::os::unix::fs::symlink(&existing, &symlink).unwrap();
            aliases.push(symlink);
        }
        for flag in ["--out1", "--report"] {
            for alias in &aliases {
                let mut aliased = args.clone();
                let index = aliased.iter().position(|arg| arg == flag).unwrap() + 1;
                aliased[index] = alias.to_string_lossy().into_owned();
                assert!(parse(&aliased).unwrap_err().contains("aliases"));
                assert_eq!(std::fs::read(&existing).unwrap(), b"@later\nGT\n+\nII\n");
            }
        }
    }
    #[test]
    fn lane_codec_declarations_cover_ordered_future_paths_without_opening_them() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../.scratch/lane-codec-unit");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a1.fastq"), "@p/1\nAC\n+\nII\n").unwrap();
        std::fs::write(dir.join("a2.fastq"), "@p/2\nGT\n+\nII\n").unwrap();
        let manifest = dir.join("lanes.tsv");
        std::fs::write(
            &manifest,
            "lane_id\tr1\tr2\nL0\ta1.fastq\ta2.fastq\nL1\tmissing1.fastq.gz\tmissing2.fastq.gz\n",
        )
        .unwrap();
        let args = vec![
            "--lanes".into(),
            manifest.to_string_lossy().into_owned(),
            "--sample-id".into(),
            "lane-codec".into(),
            "--out1".into(),
            dir.join("out1.fq").to_string_lossy().into_owned(),
            "--out2".into(),
            dir.join("out2.fq").to_string_lossy().into_owned(),
            "--report".into(),
            dir.join("report.json").to_string_lossy().into_owned(),
            "--limit-units".into(),
            "1".into(),
            "--threads".into(),
            "1".into(),
        ];
        let plan = parse(&args).unwrap();
        assert_eq!(plan.input_codecs, ["plain", "plain", "gzip", "gzip"]);
        assert!(!dir.join("missing1.fastq.gz").exists());
        std::fs::write(
            &manifest,
            "lane_id\tr1\tr2\nL0\ta1.fastq\ta2.fastq\nL1\tx.fastq\tx.fastq\n",
        )
        .unwrap();
        assert!(parse(&args).unwrap_err().contains("distinct"));
    }
    #[test]
    fn stdin_codec_reports_existing_plain_gzip_enum_without_reading() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../.scratch/stdin-codec-unit");
        std::fs::create_dir_all(&dir).unwrap();
        for (format, codec) in [("auto", "plain"), ("fastq", "plain"), ("gzip", "gzip")] {
            let args = vec![
                "--r1".into(),
                "-".into(),
                "--sample-id".into(),
                "codec".into(),
                "--input-format".into(),
                format.into(),
                "--out1".into(),
                dir.join(format!("out-{format}-{}.fq", std::process::id()))
                    .to_string_lossy()
                    .into_owned(),
                "--report".into(),
                dir.join(format!("report-{format}-{}.json", std::process::id()))
                    .to_string_lossy()
                    .into_owned(),
                "--threads".into(),
                "1".into(),
            ];
            let plan = parse(&args).unwrap();
            assert_eq!(plan.input_codecs, [codec]);
        }
    }
    #[test]
    fn stream_layout_controls_are_typed_and_fail_before_consumption() {
        for flags in [
            vec!["--r1", "-", "--decoder", "rapidgzip"],
            vec!["--r1", "-"],
            vec!["--limit-units", "0"],
            vec!["--limit-units", "+1"],
            vec!["--limit-units", "18446744073709551616"],
            vec!["--interleaved", "--r2", "missing"],
        ] {
            let args = flags.into_iter().map(str::to_string).collect::<Vec<_>>();
            assert!(parse(&args).is_err());
        }
        assert!(super::config_arguments("interleaved", &serde_json::json!(true)).is_ok());
        assert!(super::config_arguments("interleaved", &serde_json::json!("true")).is_err());
        assert!(super::config_arguments("limit-units", &serde_json::json!(0)).is_err());
    }
    #[test]
    fn config_scalar_domains_are_validated_even_before_cli_overrides() {
        use serde_json::json;
        for (key, value) in [
            ("min-length", json!(0)),
            ("max-length", json!(0)),
            ("memory-mib", json!(0)),
            ("memory-mib", json!(u64::MAX)),
            ("quality-q", json!(94)),
            ("input-phred", json!(34)),
            ("metrics", json!(1)),
            ("max-n-fraction", json!(1.1)),
            ("adapter3-r1", json!(["AX"])),
            ("stage-order", json!("tails,tails,adapters")),
            ("sample-id", json!(" ")),
        ] {
            assert!(
                super::config_arguments(key, &value).is_err(),
                "{key}={value}"
            );
        }
        assert_eq!(
            super::config_arguments("memory-mib", &json!(512)).unwrap(),
            ["512"]
        );
        assert_eq!(
            super::config_arguments("metrics", &json!(false)).unwrap(),
            ["false"]
        );
        assert_eq!(
            super::config_arguments("max-n-fraction", &json!(0.5)).unwrap(),
            ["0.5"]
        );
    }
    #[test]
    fn json_config_rejects_duplicates_types_and_invalid_overridden_values() {
        let keys = ["min-length", "metrics", "adapter3-r1", "quality-mode"];
        for text in [
            r#"{"job":"process","job":"process","options":{}}"#,
            r#"{"job":"process","options":{"min-length":1,"min-length":2}}"#,
            r#"{"job":"process","options":{"min-length":"1"}}"#,
            r#"{"job":"process","options":{"min-length":1.0}}"#,
            r#"{"job":"process","options":{"metrics":"true"}}"#,
            r#"{"job":"process","options":{"adapter3-r1":"ACGT"}}"#,
            r#"{"job":"process","options":{"quality-mode":"unknown"}}"#,
            r#"{"job":"other","options":{}}"#,
            r#"{"job":"process","options":{"min-length":null}}"#,
            r#"{"job":"process","options":{"unknown":1}}"#,
        ] {
            assert!(super::decode_config(text, &keys).is_err(), "{text}");
        }
        let parsed = super::decode_config(
            r#"{"job":"process","options":{"min-length":1,"metrics":false,"adapter3-r1":[]}}"#,
            &keys,
        )
        .unwrap();
        assert_eq!(parsed["min-length"], serde_json::json!(1));
        assert_eq!(
            super::config_arguments("adapter3-r1", &parsed["adapter3-r1"])
                .unwrap()
                .len(),
            0
        );
    }
    #[test]
    fn gzip_level_range_and_plain_only_explicitness() {
        for (value, expected) in [("1", 1), ("4", 4), ("9", 9)] {
            assert_eq!(parse_gzip_level(value).unwrap(), expected);
        }
        for value in ["0", "10", "-1", "+1", "1.0", "x", ""] {
            assert!(parse_gzip_level(value).is_err(), "{value}");
        }
        assert!(validate_gzip_level_use(false, false).is_ok());
        assert!(validate_gzip_level_use(false, true).is_ok());
        assert!(validate_gzip_level_use(true, true).is_ok());
        assert!(validate_gzip_level_use(true, false).is_err());
    }

    #[test]
    fn thread_budget_uses_detected_and_scheduler_constraints() {
        let (threads, budget) = resolve_thread_budget(None, 16, &[]).unwrap();
        assert_eq!(threads, 8);
        assert_eq!(budget.ceiling, 16);
        assert_eq!(budget.default_threads, 8);
        assert_eq!(
            budget.constraints,
            vec![("available_parallelism".into(), 16)]
        );

        let (threads, budget) = resolve_thread_budget(
            None,
            16,
            &[("SLURM_CPUS_PER_TASK", Some("6")), ("PBS_NP", Some("12"))],
        )
        .unwrap();
        assert_eq!(threads, 6);
        assert_eq!(budget.ceiling, 6);
        assert_eq!(budget.default_threads, 6);
        assert_eq!(budget.constraints.len(), 3);

        let (threads, _) = resolve_thread_budget(Some("14"), 32, &[]).unwrap();
        assert_eq!(threads, 14);
        assert!(resolve_thread_budget(Some("9"), 8, &[]).is_err());
        assert!(resolve_thread_budget(Some("7"), 16, &[("NSLOTS", Some("4"))]).is_err());
        assert!(resolve_thread_budget(Some("0"), 16, &[]).is_err());
        assert!(resolve_thread_budget(Some("bad"), 16, &[]).is_err());

        let (threads, budget) = resolve_thread_budget(
            None,
            0,
            &[("SLURM_CPUS_PER_TASK", Some("bad")), ("NSLOTS", Some("0"))],
        )
        .unwrap();
        assert_eq!(threads, 1);
        assert_eq!(budget.ceiling, 1);
        assert_eq!(budget.ignored_constraints.len(), 2);
    }
    #[test]
    fn m1_option_dependencies_reject_invalid_controls_before_opening_inputs() {
        for (flags, reason) in [
            (
                vec![
                    "--quality-mode",
                    "window",
                    "--quality-q",
                    "20",
                    "--quality-window",
                    "0",
                ],
                "quality-window",
            ),
            (
                vec![
                    "--quality-mode",
                    "mott",
                    "--quality-q",
                    "20",
                    "--quality-window",
                    "4",
                ],
                "quality-window",
            ),
            (vec!["--tail-bases", "off", "--tail-min", "10"], "tail-min"),
            (vec!["--tail-bases", "all", "--tail-min", "0"], "tail-min"),
            (vec!["--tail-bases", "GG"], "tail-bases"),
            (vec!["--min-complexity", "NaN"], "min-complexity"),
        ] {
            let args = flags.into_iter().map(str::to_string).collect::<Vec<_>>();
            let error = parse(&args).unwrap_err();
            assert!(error.contains(reason), "{error}");
        }
    }
    #[test]
    fn stage_order_accepts_exactly_six_permutations() {
        for text in [
            "tails,quality,adapters",
            "tails,adapters,quality",
            "quality,tails,adapters",
            "quality,adapters,tails",
            "adapters,tails,quality",
            "adapters,quality,tails",
        ] {
            let order = super::parse_stage_order(text).unwrap();
            assert_eq!(order.map(|s| s.name()).join(","), text);
        }
        for text in [
            "",
            "tails,quality",
            "tails,quality,adapters,tails",
            "tails,tails,adapters",
            "Tails,quality,adapters",
            "tails, quality,adapters",
        ] {
            assert!(super::parse_stage_order(text).is_err());
        }
    }
    #[test]
    fn quality_window_contract() {
        assert_eq!(parse_quality("window", Some("20")).unwrap(), Some(20));
    }
    #[test]
    fn quality_mode_dependencies_and_endpoints() {
        assert_eq!(parse_quality("off", None).unwrap(), None);
        assert_eq!(parse_quality("ends", Some("0")).unwrap(), Some(0));
        assert_eq!(parse_quality("ends", Some("93")).unwrap(), Some(93));
        for (mode, q) in [
            ("ends", None),
            ("off", Some("20")),
            ("unknown", Some("20")),
            ("ends", Some("94")),
            ("ends", Some("-1")),
            ("ends", Some("+1")),
            ("ends", Some("1.5")),
        ] {
            assert!(parse_quality(mode, q).is_err());
        }
    }
    #[test]
    fn cut_values_are_strict_checked_integers() {
        assert_eq!(parse_cut("0,17").unwrap(), [0, 17]);
        for value in [
            "",
            "1",
            "1,2,3",
            "-1,0",
            "+1,0",
            "1.0,0",
            "0,",
            "184467440737095516160,0",
        ] {
            assert!(parse_cut(value).is_err(), "{value}");
        }
    }
    #[test]
    fn invalid_native_settings_fail_before_paths() {
        for words in [
            vec!["--threads", "one"],
            vec!["--threads", "0"],
            vec!["--min-length", "0"],
            vec!["--input-phred", "1"],
            vec!["--input-format", "bam"],
            vec!["--threads", "1", "--threads", "2"],
            vec!["--adapter1", "A"],
        ] {
            assert!(parse(&words.into_iter().map(str::to_string).collect::<Vec<_>>()).is_err());
        }
    }
}

#[cfg(test)]
mod api_interleaved_mapping_tests {
    use crate::job_api::*;
    #[test]
    fn main_channel_spelling_and_auxiliaries_preserve_interleaved_mapping() {
        for name in ["R1","out-interleaved"] {
            let s=JobSpec{configured:Default::default(),supplied:Default::default(),layout:Layout::SplitPe,lanes:vec![LaneDecl{id:"lane".into(),inputs:vec![InputDecl{channel:"R1".into(),location:Location::Handle("one".into()),format:Format::Fastq,phred:33,seekable:false},InputDecl{channel:"R2".into(),location:Location::Handle("two".into()),format:Format::Fastq,phred:33,seekable:false}]}],outputs:vec![OutputDecl{channel:name.into(),location:Location::Handle("main".into()),format:Format::Fastq,phred:33},OutputDecl{channel:"reject1".into(),location:Location::Handle("reject1".into()),format:Format::Fastq,phred:33},OutputDecl{channel:"reject2".into(),location:Location::Handle("reject2".into()),format:Format::Fastq,phred:33}],json_report:None};
            let p=super::parse_api(&[],&s).unwrap();assert!(p.out_interleaved);assert!(p.auxiliary_enabled(0)&&p.auxiliary_enabled(1));assert!(p.inputs.is_empty()&&p.outputs.is_empty());
        }
    }
}

#[cfg(test)]
mod catalogue_report_scope_tests {
    fn check(kind: &str, id: &str, flag: &str, filename: &str, digest: &str) {
        let pack = crate::test_support::fixtures().join("pack");
        let root = crate::test_support::workspace("catalogue-report").join(kind);
        std::fs::create_dir(&root).unwrap();
        let resource = pack.join("fixtures/catalogue").join(filename).canonicalize().unwrap();
        for (i, value) in [format!("id:{id}"), resource.to_string_lossy().into_owned()].into_iter().enumerate() {
            let mut args = vec!["--r1".into(), pack.join(format!("fixtures/{kind_case}/input.R1.fastq", kind_case=if kind=="adapter"{"adapter-resource-id"}else{"reference-resource-id"})).to_string_lossy().into_owned(), "--out1".into(), root.join(format!("{i}.fastq")).to_string_lossy().into_owned(), "--report".into(), root.join(format!("{i}.json")).to_string_lossy().into_owned(), "--threads".into(), "1".into(), flag.into(), value];
            if i==0 { args.extend(["--resource-catalog".into(), pack.join("fixtures/catalogue/resources.json").to_string_lossy().into_owned()]); }
            if kind=="reference" {args.extend(["--screen-k".into(), "3".into(), "--screen-min-hits".into(), "1".into()]);}
            let plan = super::parse(&args).unwrap();
            let rows: Vec<_> = plan.resources.iter().filter(|r|r.get("catalogue_id").is_some()).collect();
            if i==0 {
                assert_eq!(rows.len(),1); let row=rows[0];
                assert_eq!(row["kind"],kind); assert_eq!(row["catalogue_id"],id);
                assert_eq!(row["path"],serde_json::json!(resource)); assert_eq!(row["sha256"],digest);
                assert_eq!(row["catalogue_sha256"],"fd3968d7fe7cb233e0082e1276594e7317871825d38a957ebb083774cd0713a9");
                assert_eq!(row["hash_scope"],"exact resource file bytes hashed and verified against the catalogue during resolution");
            } else {assert!(rows.is_empty());}
            if kind=="adapter" {let row=plan.resources.iter().find(|r|r["kind"]=="adapter-fasta").unwrap();assert_eq!(row["sha256"],digest);assert_eq!(row["hash_scope"],"exact bytes read and compiled at configuration time");}
            assert!(!root.join(format!("{i}.fastq")).exists()); assert!(!root.join(format!("{i}.json")).exists());
        }
    }
    #[test] fn adapter_catalogue_report_scope_and_direct_digest() {check("adapter","adapter-demo","--adapter-file","adapters.fa","347a12d63d2125e1e76df4fe1d2cd4298dedb14f7f587dcaafafaf5bad250bfc");}
    #[test] fn reference_catalogue_report_scope_and_direct_digest() {check("reference","reference-demo","--screen-reference","reference.fa","01fc86a57f692aa26feb67c742349415ef5cde9161bd1e1157f96d16fdd09c34");}
}
