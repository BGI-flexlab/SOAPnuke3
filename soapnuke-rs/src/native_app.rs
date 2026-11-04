use crate::failure::Failure;
use crate::native_config::{self, RunPlan};
use crate::params::GlobalParams;
use crate::stats::GlobalStats;
use serde_json::{Value, json};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
enum Fault {
    None,
    #[cfg(test)]
    Writer,
    #[cfg(test)]
    Summary,
    #[cfg(test)]
    R2Append,
    #[cfg(test)]
    SplitPublish,
    #[cfg(test)]
    Close,
    #[cfg(test)]
    Worker,
    #[cfg(test)]
    Cancel,
    #[cfg(test)]
    Report,
    #[cfg(test)]
    Delay,
    #[cfg(test)]
    Lane1,
    #[cfg(test)]
    Lane2,
    #[cfg(test)]
    ReportLink,
    #[cfg(test)]
    Compression,
    #[cfg(test)]
    HtmlWrite,
    #[cfg(test)]
    HtmlClose,
    #[cfg(test)]
    HtmlLink,
}

pub fn entry(args: &[String]) -> i32 {
    if args.is_empty()
        || (args.len() == 1 && matches!(args[0].as_str(), "-h" | "--help" | "-v" | "--version"))
    {
        native_config::help(args.first().is_some_and(|arg| matches!(arg.as_str(), "-v" | "--version")));
        return 0;
    }
    match native_config::parse(args) {
        Ok(plan) => run(&plan, Fault::None),
        Err(error) => {
            eprintln!("config: {error}");
            2
        }
    }
}

pub(crate) fn write_json(path: &Path, value: &Value) -> Result<(), String> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("create report staging: {e}"))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)
        .map_err(|e| format!("serialize report: {e}"))?;
    writer
        .write_all(b"\n")
        .and_then(|_| writer.flush())
        .map_err(|e| format!("flush report: {e}"))?;
    let file = writer
        .into_inner()
        .map_err(|e| format!("close report buffer: {e}"))?;
    file.sync_all()
        .map_err(|e| format!("finalize report: {e}"))?;
    close_report_file(file)
}

use crate::checked_io::close_file as close_report_file;
fn write_html(path:&Path,value:&Value,fault:Fault)->Result<(),String>{
    #[cfg(test)]
    if matches!(fault,Fault::HtmlWrite){fs::create_dir(path).map_err(|e|e.to_string())?;}
    let file=OpenOptions::new().create_new(true).write(true).open(path).map_err(|e|format!("create HTML staging: {e}"))?;
    let mut writer=BufWriter::with_capacity(8192,file);
    crate::native_html::write(&mut writer,value).and_then(|_|writer.flush()).map_err(|e|format!("write/flush HTML: {e}"))?;
    let file=writer.into_inner().map_err(|e|format!("close HTML buffer: {e}"))?;
    file.sync_all().map_err(|e|format!("sync HTML: {e}"))?;
    close_report_file(file)?;
    #[cfg(test)]
    if matches!(fault,Fault::HtmlClose){return Err("injected required HTML close failure".into());}
    let _=fault;Ok(())
}



fn binary_hash() -> Option<String> {
    let path = std::env::current_exe().ok()?;
    let result = std::process::Command::new("sha256sum")
        .arg(path)
        .output()
        .ok()?;
    if !result.status.success() {
        return None;
    }
    String::from_utf8(result.stdout)
        .ok()?
        .split_whitespace()
        .next()
        .map(str::to_string)
}

fn run(plan: &RunPlan, fault: Fault) -> i32 { run_impl(plan,fault,None,None) }
pub(crate) fn run_api(plan:&RunPlan, io:std::sync::Arc<crate::native_io::OwnedIo>)->(i32,Option<Value>){
    let mut capture=None;
    let code=run_impl(plan,Fault::None,Some(io),Some(&mut capture));
    (code,capture)
}
fn run_impl(plan: &RunPlan, fault: Fault, api_io:Option<std::sync::Arc<crate::native_io::OwnedIo>>, mut capture:Option<&mut Option<Value>>) -> i32 {
    let started = Instant::now();
    let cpu_start = if plan.metrics {
        crate::native_runtime::process_cpu_seconds()
    } else {
        None
    };
    let runtime = std::sync::Arc::new(crate::native_runtime::Runtime::new(
        plan.progress_seconds,
        plan.sample_id.clone(),
    ));
    runtime.qc_only.store(plan.process.qc_only,std::sync::atomic::Ordering::Relaxed);
    runtime.event("starting", true);
    let run_id = format!(
        "m2-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let stage_parent=plan.api_spec.as_ref().map(|s|s.json_report.as_ref().and_then(|p|p.parent()).map(Path::to_path_buf).unwrap_or_else(||std::env::var_os("TMPDIR").map(PathBuf::from).unwrap_or_else(||PathBuf::from(".scratch/tmp")))).unwrap_or_else(||plan.report.parent().unwrap().to_owned());
    let stage = stage_parent.join(format!(".{run_id}"));
    if let Err(e) = fs::create_dir(&stage) {
        eprintln!("write: create staging: {e}");
        return 4;
    }
    let mut gp = GlobalParams::new();
    gp.native_io=api_io.clone();
    gp.native_m1 = true;
    gp.native_interleaved = plan.interleaved;
    gp.native_out_interleaved = plan.out_interleaved;
    gp.native_limit_units = plan.limit_units;
    gp.native_lane_manifest = plan.lane_manifest.clone();
    gp.native_lane_parent = plan.lane_parent.to_string_lossy().into_owned();
    gp.native_stdout = plan.outputs.first().is_some_and(|p|p.as_os_str()=="-");
    gp.native_stdin_gzip = if plan.inputs.first().is_some_and(|p|p.as_os_str() == "-") && api_io.is_none() {
        Some(plan.input_codecs[0] == "gzip")
    } else {
        None
    };
    gp.native_runtime = Some(runtime.clone());
    gp.native_process = plan.process.clone();
    gp.native_split_templates = plan.split_templates.clone();
    if plan.match_report.is_some() {
        gp.native_match_audit = "matches.tsv".into();
        gp.native_match_limit = plan.match_limit;
    }

    gp.native_aux = std::array::from_fn(|i| {
        if plan.auxiliary_enabled(i) {
            format!(
                "{}.fq{}",
                [
                    "reject1",
                    "reject2",
                    "transformed1",
                    "transformed2",
                    "singleton1",
                    "singleton2"
                ][i],
                if api_io.as_ref().and_then(|io|io.spec.outputs.iter().find(|d|d.channel==["reject1","reject2","transformed1","transformed2","singleton1","singleton2"][i])).map_or(plan.auxiliary_codecs[i]=="gzip",|d|crate::job_api::output_codec(d)=="gzip") {
                    ".gz"
                } else {
                    ""
                }
            )
        } else {
            String::new()
        }
    });
    if plan.record_manifest.is_some() {
        gp.native_record_manifest = "records.tsv".into();
    }

    gp.native_cuts = plan.cuts;
    gp.native_quality_q = plan.quality_q;
    gp.native_quality_window = plan.quality_window;
    gp.native_tail_mask = plan.tail_mask;
    gp.native_tail_min = plan.tail_min;
    gp.native_stage_order = plan.stage_order;
    gp.native_quality_mode = match plan.quality_mode.as_str() {
        "window" => crate::native_processing::QualityMode::Window,
        "mott" => crate::native_processing::QualityMode::Mott,
        _ => crate::native_processing::QualityMode::Ends,
    };
    gp.native_memory_total = plan.memory_total_bytes;
    gp.native_gzip_level = plan.gzip_level as i32;
    gp.patch_size = 2500;
    gp.min_read_length = 1;
    gp.n_ratio = 2.0;
    gp.low_qual = -1;
    gp.low_quality_base_ratio = 2.0;
    gp.threads_num = plan.threads;
    gp.quality_phred = if api_io.is_some(){33}else{plan.input_phred};
    gp.output_quality_phred = if api_io.is_some(){33}else{plan.output_phred};
    gp.fq1_path = plan.inputs.first().map_or(String::new(),|p|p.to_string_lossy().into_owned());
    if plan.inputs.len() == 2 {
        gp.fq2_path = plan.inputs[1].to_string_lossy().into_owned();
    }
    gp.output_dir = stage.to_string_lossy().into_owned();
    gp.log = stage.join("legacy.log").to_string_lossy().into_owned();
    let main_declarations=api_io.as_ref().map(|io|io.spec.outputs.iter().filter(|d|crate::job_api::main_channel(&d.channel).is_some()).collect::<Vec<_>>());
    let names:Vec<String>=if let Some(declarations)=&main_declarations {
        declarations.iter().map(|d|{
            let channel=crate::job_api::main_channel(&d.channel).unwrap();
            let name=format!("{}.fq{}",["r1","r2","merged"][channel],if crate::job_api::output_codec(d)=="gzip"{".gz"}else{""});
            match channel{0=>gp.clean_fq1=name.clone(),1=>gp.clean_fq2=name.clone(),2=>gp.native_merged=name.clone(),_=>unreachable!()};name
        }).collect()
    }else{plan.outputs.iter().enumerate().map(|(i,_)|{let channel=plan.main_channel(i);let name=format!("{}.fq{}",["r1","r2","merged"][channel],if plan.output_codecs[i]=="gzip"{".gz"}else{""});match channel{0=>gp.clean_fq1=name.clone(),1=>gp.clean_fq2=name.clone(),2=>gp.native_merged=name.clone(),_=>unreachable!()};name}).collect()};
    if let Some(io)=&api_io {
        let mut channels=if plan.process.split_units==0 {main_declarations.as_ref().unwrap().iter().zip(&names).map(|(d,n)|(d.channel.clone(),n.clone())).collect::<Vec<_>>()}else{Vec::new()};
        for d in &io.spec.outputs {if let Some(i)=["reject1","reject2","transformed1","transformed2","singleton1","singleton2"].iter().position(|n|d.channel==*n){channels.push((d.channel.clone(),gp.native_aux[i].clone()));}}
        if let Err(e)=io.prepare(&stage,&channels){eprintln!("write: owned preparation: {e}");return 4;}
    }
    let decoder = if api_io.as_ref().is_some_and(|io|io.choice(plan.threads).backend==crate::source::InputBackend::Zlib) || gp.native_stdin_gzip.is_some() {
        "zlib"
    } else if plan.decoder == "auto" {
        if plan.threads >= 14 {
            "rapidgzip"
        } else {
            "zlib"
        }
    } else {
        &plan.decoder
    };
    if api_io.is_none(){std::env::set_var("SOAPNUKE_INPUT_DECODER", decoder);
    std::env::set_var("SOAPNUKE_RAPIDGZIP_THREADS", "1");}
    #[cfg(test)]
    if matches!(fault, Fault::Writer) {
        fs::create_dir(stage.join(&names[0])).unwrap();
    }
    #[cfg(test)]
    if matches!(fault, Fault::Worker) {
        std::env::set_var("SOAPNUKE_TEST_PANIC_WORKER", "1");
    }
    #[cfg(test)]
    if matches!(fault, Fault::Delay) {
        std::env::set_var("SOAPNUKE_TEST_DELAY_BATCH_ID", "1");
        std::env::set_var("SOAPNUKE_TEST_DELAY_MS", "30");
    }
    #[cfg(test)]
    {
        if matches!(fault, Fault::Lane1) {
            std::env::set_var("SOAPNUKE_TEST_FAIL_LANE_SPAWN", "1");
        }
        if matches!(fault, Fault::Lane2) {
            std::env::set_var("SOAPNUKE_TEST_FAIL_LANE_SPAWN", "2");
        }
    }
    #[cfg(test)]
    if matches!(fault, Fault::R2Append) {
        std::env::set_var("SOAPNUKE_TEST_NATIVE_FAIL_R2_APPEND", "1");
    }
    #[cfg(test)]
    if matches!(fault, Fault::Compression) {
        std::env::set_var("SOAPNUKE_TEST_FAIL_COMPRESSION", "1");
    }
    let statistics_plan = crate::native_execution::Execution::validate_budget_with_statistics(
        plan.threads,
        if plan.paired() { 2 } else { 1 },
        plan.memory_total_bytes,
        plan.process.additional_statistics(
            plan.process.capture.enabled()
                || plan.has_auxiliary()
                || plan.match_report.is_some(),
        ),
    );
    let mut stats = GlobalStats::new(&gp);
    let result = statistics_plan.map_err(Failure::from).and_then(|_| crate::pipeline::run_filter_typed(&gp, &mut stats));
    #[cfg(test)]
    if matches!(fault, Fault::Compression) {
        std::env::remove_var("SOAPNUKE_TEST_FAIL_COMPRESSION");
    }
    #[cfg(test)]
    if matches!(fault, Fault::R2Append) {
        std::env::remove_var("SOAPNUKE_TEST_NATIVE_FAIL_R2_APPEND");
    }
    #[cfg(test)]
    {
        if matches!(fault, Fault::Lane1 | Fault::Lane2) {
            std::env::remove_var("SOAPNUKE_TEST_FAIL_LANE_SPAWN");
        }
        if matches!(fault, Fault::Worker) {
            std::env::remove_var("SOAPNUKE_TEST_PANIC_WORKER");
        }
        if matches!(fault, Fault::Delay) {
            std::env::remove_var("SOAPNUKE_TEST_DELAY_BATCH_ID");
            std::env::remove_var("SOAPNUKE_TEST_DELAY_MS");
        }
    }
    let mut error = result.err().map(|failure| {
        (failure.category.name().to_string(), failure.category.exit(), failure.process_message())
    });
    #[cfg(test)]
    {
        if error.is_none() && matches!(fault, Fault::Close) {
            error = Some((
                "write".into(),
                4,
                "injected required close failure before commit".into(),
            ));
        }
        if error.is_none() && matches!(fault, Fault::Cancel) {
            error = Some((
                "worker".into(),
                5,
                "controlled cancellation before commit, workers joined".into(),
            ));
        }
    }
    let split = runtime
        .split
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let split_chunks = split.get("chunks").and_then(Value::as_array);
    #[cfg(test)]
    if matches!(fault, Fault::SplitPublish) {
        let destination = crate::output::native_chunk_path(&plan.outputs[1].to_string_lossy(), 2);
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .unwrap()
            .write_all(b"owned split publication sentinel")
            .unwrap();
    }

    let mut split_published = vec![[false; 3]; split_chunks.map_or(0, Vec::len)];
    let mut published = vec![false; names.len()];
    if error.is_none() && plan.process.split_units > 0 {
        if let Some(chunks) = split_chunks {
            'chunks: for (chunk_index, chunk) in chunks.iter().enumerate() {
                for channel in 0..plan.outputs.len() {
                    let destination = PathBuf::from(chunk["paths"][channel].as_str().unwrap());
                    let staged = PathBuf::from(chunk["staged"][channel].as_str().unwrap());
                    if let Err(e) = fs::hard_link(&staged, &destination) {
                        error = Some(("write".into(), 4, format!("publish split chunk: {e}")));
                        break 'chunks;
                    }
                    split_published[chunk_index][channel] = true;
                }
            }
        } else {
            error = Some(("worker".into(), 5, "split output index unavailable".into()));
        }
    }
    if error.is_none() && plan.process.split_units == 0 {
        let destinations:Vec<Option<&Path>>=if let Some(ds)=&main_declarations{ds.iter().map(|d|if api_io.as_ref().is_some_and(|io|io.owned_output_key(&d.channel).is_some()){None}else if let crate::job_api::Location::Path(p)=&d.location{Some(p.as_path())}else{None}).collect()}else{plan.outputs.iter().map(|p|Some(p.as_path())).collect()};
        for (i, destination) in destinations.into_iter().enumerate() {
            let Some(dest)=destination else{published[i]=true;continue;};
            if dest.as_os_str() == "-" {
                published[i] = true;
                continue;
            }
            if let Err(e) = fs::hard_link(stage.join(&names[i]), dest) {
                error = Some((
                    "write".into(),
                    4,
                    format!("publish output channel{}: {e}", i + 1),
                ));
                break;
            }
            published[i] = true;
        }
    }
    let mut aux_published = [false; 6];
    let mut manifest_published = false;
    if error.is_none() {
        for i in 0..6 {
            if let Some(dest) = &plan.auxiliary[i] {
                if api_io.as_ref().is_some_and(|io|io.owned_output_key(["reject1","reject2","transformed1","transformed2","singleton1","singleton2"][i]).is_some()){aux_published[i]=true;continue;}
                if let Err(e) = fs::hard_link(stage.join(&gp.native_aux[i]), dest) {
                    error = Some(("write".into(), 4, format!("publish optional output: {e}")));
                    break;
                }
                aux_published[i] = true;
            }
        }
    }
    if error.is_none() {
        if let Some(dest) = &plan.record_manifest {
            match fs::hard_link(stage.join(&gp.native_record_manifest), dest) {
                Ok(()) => manifest_published = true,
                Err(e) => {
                    error = Some(("write".into(), 4, format!("publish record manifest: {e}")))
                }
            }
        }
    }
    let mut match_published = false;
    if error.is_none() {
        if let Some(dest) = &plan.match_report {
            match fs::hard_link(stage.join(&gp.native_match_audit), dest) {
                Ok(()) => match_published = true,
                Err(e) => error = Some(("write".into(), 4, format!("publish match report: {e}"))),
            }
        }
    }
    let report_stage = stage.join("report.json");
    #[cfg(test)]
    if error.is_none() && matches!(fault, Fault::Report) {
        error = Some((
            "write".into(),
            4,
            "injected report publication failure".into(),
        ));
    }
    let written = runtime.written.lock().unwrap_or_else(|p| p.into_inner());
    let merged_guard = runtime.merged.lock().unwrap_or_else(|p| p.into_inner());
    let empty_merged = crate::native_qc::Qc::merged();
    let merged_qc = merged_guard.as_deref().unwrap_or(&empty_merged);
    let auxiliary = runtime.auxiliary.lock().unwrap_or_else(|p| p.into_inner());
    let raw1 = stats.raw1.native_qc.as_ref().unwrap();
    let raw2 = stats.raw2.native_qc.as_ref().unwrap();
    let clean1 = stats.clean1.native_qc.as_ref().unwrap();
    let clean2 = stats.clean2.native_qc.as_ref().unwrap();
    let paired = plan.paired();
    let input = if paired {
        raw1.reads.min(raw2.reads)
    } else {
        raw1.reads
    };
    let accepted = if paired {
        clean1.reads.min(clean2.reads)
    } else {
        clean1.reads
    };
    let rejected = stats.fs.native.rejected;
    let raw: Vec<_> = if paired {
        vec![raw1.as_ref(), raw2.as_ref()]
    } else {
        vec![raw1.as_ref()]
    };
    let clean: Vec<_> = if paired {
        vec![clean1.as_ref(), clean2.as_ref()]
    } else {
        vec![clean1.as_ref()]
    };
    let final_qc: Vec<_> = if plan.process.overlap.merge {
        vec![&written[0], &written[1], merged_qc]
    } else if paired {
        vec![&written[0], &written[1]]
    } else {
        vec![&written[0]]
    };
    let written_units = if plan.process.overlap.merge {
        written[0].reads.min(written[1].reads) + merged_qc.reads
    } else if paired {
        written[0].reads.min(written[1].reads)
    } else {
        written[0].reads
    };
    let mut scope = if error.is_none() {
        "complete"
    } else {
        "partial"
    };
    let make_populations = |scope: &str| {
        if plan.process.qc_only{return json!({"input":crate::native_qc::population(&raw,"input",input,scope,false),"accepted":null,"written":null});}
        let mut populations = json!({"input":crate::native_qc::population(&raw,"input",input,scope,false),"accepted":crate::native_qc::population(&clean,"accepted",accepted,scope,false),"written":crate::native_qc::written_population(&final_qc,written_units,scope,plan.out_interleaved && !plan.process.overlap.merge)});
        if plan.process.overlap.merge {
            if plan.process.overlap.discard_unmerged {
                populations["written"]["channels"] =
                    json!([merged_qc.metrics("written", "merged", merged_qc.reads, scope)]);
            } else if plan.out_interleaved {
                let mut combined = crate::native_qc::Qc::default();
                combined.merge(&written[0]);
                combined.merge(&written[1]);
                populations["written"]["channels"] = json!([
                    combined.metrics(
                        "written",
                        "r1",
                        written[0].reads.min(written[1].reads),
                        scope
                    ),
                    merged_qc.metrics("written", "merged", merged_qc.reads, scope)
                ]);
            }
        }
        if let Some(aux) = auxiliary.as_ref() {
            if plan.has_auxiliary() {
                let mut total = if plan.process.overlap.merge {
                    crate::native_qc::Qc::merged()
                } else {
                    crate::native_qc::Qc::default()
                };
                for q in &final_qc {
                    total.merge(q);
                }
                let mut children = populations["written"]["channels"]
                    .as_array()
                    .unwrap()
                    .clone();
                let mut units = written_units;
                for i in 0..6 {
                    if plan.auxiliary_enabled(i) {
                        total.merge(&aux[i]);
                        children.push(aux[i].metrics(
                            "written",
                            [
                                "reject1",
                                "reject2",
                                "transformed1",
                                "transformed2",
                                "singleton1",
                                "singleton2",
                            ][i],
                            aux[i].reads,
                            scope,
                        ));
                    }
                }
                for i in [0, 2] {
                    if plan.auxiliary_enabled(i) {
                        units += if paired {
                            aux[i].reads.min(aux[i + 1].reads)
                        } else {
                            aux[i].reads
                        };
                    }
                }
                units += aux[4].reads + aux[5].reads;
                let mut written = total.metrics("written", "all", units, scope);
                written["channels"] = Value::Array(children);
                populations["written"] = written;
            }
        }
        populations
    };
    let mut populations = make_populations(scope);
    let summary = crate::native_qc::summary(&plan.sample_id, &populations);
    #[cfg(test)]
    let summary_fail = matches!(fault, Fault::Summary);
    #[cfg(not(test))]
    let summary_fail = false;
    let summary_result = if summary_fail {
        Err(std::io::Error::other(
            "injected summary finalization failure",
        ))
    } else {
        std::io::stderr()
            .write_all(summary.as_bytes())
            .and_then(|_| std::io::stderr().flush())
    };
    if let Err(e) = summary_result {
        if error.is_none() {
            error = Some(("write".into(), 4, format!("required summary: {e}")));
        }
    }
    let mut complete = error.is_none();
    if !complete && scope == "complete" {
        scope = "partial";
        populations = make_populations(scope);
    }
    let mut reasons = serde_json::Map::new();
    for (i, name) in [
        "label-coordinate",
        "retain-coordinate",
        "label-quality",
        "label-N",
    ]
    .iter()
    .enumerate()
    {
        if stats.fs.native.capture_first[i] > 0 {
            reasons.insert((*name).into(), json!(stats.fs.native.capture_first[i]));
        }
    }
    for (i, name) in [
        "srna-missing-3prime",
        "srna-short-insert",
        "srna-5prime",
        "srna-polyA",
    ]
    .iter()
    .enumerate()
    {
        if stats.fs.native.srna_first[i] > 0 {
            reasons.insert((*name).into(), json!(stats.fs.native.srna_first[i]));
        }
    }
    for (i, reason) in crate::native_processing::REASONS.iter().enumerate() {
        if stats.fs.native.first[i] > 0 {
            reasons.insert((*reason).into(), json!(stats.fs.native.first[i]));
        }
    }
    if stats.fs.native.insert_first > 0 {
        reasons.insert(
            "inferred-short-insert".into(),
            json!(stats.fs.native.insert_first),
        );
    }
    let mut predicate_facts = stats.fs.native.clone();
    if plan.process.infer_mode != "off" {
        let unprocessed = input.saturating_sub(accepted + rejected);
        for mate in 0..if paired { 2 } else { 1 } {
            for i in 0..9 {
                if plan.process.enabled(mate)[i] {
                    predicate_facts.not_evaluated[mate][i] += unprocessed;
                }
            }
        }
    }
    let mut predicate_accounting =
        predicate_facts.report(&plan.process, paired, [raw1.reads, raw2.reads]);
    if plan.process.overlap.min_insert.is_some() {
        predicate_accounting["inferred-short-insert"] = json!({"enabled":true,"evaluated":stats.fs.native.overlap_evaluated,"scored":stats.fs.native.overlap_scored,"unscorable":stats.fs.native.overlap_evaluated-stats.fs.native.overlap_scored,"failed":stats.fs.native.insert_failed,"first_rejected_units":stats.fs.native.insert_first,"scope":"source-global pre-correction insert evidence; unavailable overlap retains unit under other tests"});
    }
    if plan.process.profile == "srna" {
        for (i, name) in [
            "srna-missing-3prime",
            "srna-short-insert",
            "srna-5prime",
            "srna-polyA",
        ]
        .iter()
        .enumerate()
        {
            let enabled = i < 2
                || (i == 2 && plan.process.srna_reject5)
                || (i == 3 && plan.process.srna_reject_poly_a);
            predicate_accounting[*name] = json!({"enabled":enabled,"evaluated":stats.fs.native.srna_evaluated[i],"failed":stats.fs.native.srna_failed[i],"first_rejected_units":stats.fs.native.srna_first[i],"not_evaluated":if enabled {input.saturating_sub(stats.fs.native.srna_evaluated[i])}else{0},"stage":"original","population":"original-input"});
        }
    }
    let mut observed_bytes = Some(0u64);
    let mut outputs: Vec<Value> = if plan.process.split_units > 0 {
        Vec::new()
    } else {
        plan.outputs.iter().enumerate().map(|(i,p)| {
        let qchannel=plan.main_channel(i);let q=&final_qc[qchannel];let staged=stage.join(&names[i]);let stdout=p.as_os_str()=="-";let bytes=if stdout {None}else{fs::metadata(if published[i] {p}else{&staged}).ok().map(|m|m.len())};
        observed_bytes=observed_bytes.zip(bytes).map(|(a,b)|a+b);
        json!({"channel":(["r1","r2","merged"][qchannel]),"path":p,"codec":plan.output_codecs[i],"encoding":plan.output_phred,"staged_path":if stdout {None}else{Some(&staged)},"publication_scope":if stdout {"streamed; no rollback or cross-file commit"}else{"staged file publication"},"read_records":if plan.out_interleaved && qchannel==0 {written[0].reads+written[1].reads}else{q.reads},"bases":if plan.out_interleaved && qchannel==0 {written[0].bases.iter().sum::<u64>()+written[1].bases.iter().sum::<u64>()}else{q.bases.iter().sum::<u64>()},"state":if complete {"committed"}else if published[i] {"published"}else{"failed"},"bytes":bytes,"bytes_unavailable_reason":if stdout {Some("stdout is nonseekable; process does not re-read captured stdout")}else if bytes.is_none(){Some("owned artifact unavailable after execution failure")}else{None},"hash":null,"hash_unavailable_reason":"not computed in process; independent harness checks whole decoded records and artifact digests","written_scope":"successfully appended complete channel chunks; not fsync durability; current failed partial append excluded"})
    }).collect()
    };
    if let Some(io)=api_io.as_ref().filter(|_|plan.process.split_units==0) {
        outputs.clear();
        for (i,d)in main_declarations.as_ref().unwrap().iter().enumerate(){
            let channel=crate::job_api::main_channel(&d.channel).unwrap();let qc=&final_qc[channel];
            let owned=io.owned_output_key(&d.channel);let path=if let Some(key)=owned{if matches!(d.location,crate::job_api::Location::Handle(_))&&plan.supplied.iter().any(|s|s=="out1"||s=="out-interleaved"){json!("-")}else{json!({"handle":key})}}else if let crate::job_api::Location::Path(p)=&d.location{json!(p)}else{Value::Null};
            outputs.push(json!({"channel":(["r1","r2","merged"][channel]),"path":path,"codec":crate::job_api::output_codec(d),"encoding":d.phred,"read_records":if channel==0&&plan.out_interleaved{written[0].reads+written[1].reads}else{qc.reads},"bases":if channel==0&&plan.out_interleaved{written[0].bases.iter().sum::<u64>()+written[1].bases.iter().sum::<u64>()}else{qc.bases.iter().sum::<u64>()},"bytes":if owned.is_some(){Some(io.bytes.lock().unwrap().get(&d.channel).copied().unwrap_or(0))}else if let crate::job_api::Location::Path(p)=&d.location{fs::metadata(if published[i]{p.clone()}else{stage.join(&names[i])}).ok().map(|m|m.len())}else{None},"state":if complete{"committed"}else if published[i]{"published"}else{"failed"},"staged_path":if owned.is_none()&&matches!(d.location,crate::job_api::Location::Path(_)){Some(stage.join(&names[i]))}else{None},"binding_kind":if owned.is_some(){if matches!(d.location,crate::job_api::Location::Path(_)){"owned_override"}else{"owned_handle"}}else{"staged_path"},"declared_path":if let crate::job_api::Location::Path(p)=&d.location{Some(p)}else{None},"publication_scope":if owned.is_some(){"owned stream; no rollback"}else{"staged file publication"}}));
        }
    }
    if let Some(chunks) = split_chunks {
        for (chunk_index, chunk) in chunks.iter().enumerate() {
            for channel in 0..plan.outputs.len() {
                let destination = chunk["paths"][channel].as_str().map(PathBuf::from);
                let staged = chunk["staged"][channel].as_str().map(PathBuf::from);
                let chosen = if split_published[chunk_index][channel] {
                    destination.as_ref()
                } else {
                    staged.as_ref()
                };
                let bytes = chosen.and_then(|p| fs::metadata(p).ok().map(|m| m.len()));
                observed_bytes = observed_bytes.zip(bytes).map(|(a, b)| a + b);
                outputs.push(json!({"channel":(["r1","r2","merged"][plan.main_channel(channel)]),"chunk":chunk["chunk"],"path":destination,"codec":plan.output_codecs[channel],"encoding":plan.output_phred,"staged_path":staged,"publication_scope":"staged split file publication; no cross-file atomicity","read_records":chunk["read_records"][channel],"bases":chunk["bases"][channel],"selected_units":chunk["selected_units"],"state":if complete{"committed"}else if split_published[chunk_index][channel]{"published"}else{"failed"},"bytes":bytes,"bytes_unavailable_reason":if bytes.is_none(){Some("owned artifact unavailable after execution failure")}else{None},"hash":null,"hash_unavailable_reason":"independent harness validates decoded records","written_scope":"successfully appended complete paired fragments; failed current fragment excluded"}));
            }
        }
    }
    {
        let empty = crate::native_qc::Qc::default();
        for i in 0..6 {
            if let Some(dest) = &plan.auxiliary[i] {
                if api_io.as_ref().is_some_and(|io|io.owned_output_key(["reject1","reject2","transformed1","transformed2","singleton1","singleton2"][i]).is_some()){continue;}
                let qc = auxiliary.as_ref().map(|a| &a[i]).unwrap_or(&empty);
                let staged = stage.join(&gp.native_aux[i]);
                let bytes = fs::metadata(if aux_published[i] { dest } else { &staged })
                    .ok()
                    .map(|m| m.len());
                observed_bytes = observed_bytes.zip(bytes).map(|(a, b)| a + b);
                let declaration=api_io.as_ref().and_then(|io|io.spec.outputs.iter().find(|d|d.channel==["reject1","reject2","transformed1","transformed2","singleton1","singleton2"][i]));
                let codec=declaration.map(crate::job_api::output_codec).unwrap_or_else(||plan.auxiliary_codecs[i].clone());let encoding=declaration.map_or(plan.output_phred,|d|d.phred as i32);
                outputs.push(json!({"channel":(["reject1","reject2","transformed1","transformed2","singleton1","singleton2"][i]),"path":dest,"codec":codec,"encoding":encoding,"staged_path":staged,"publication_scope":"staged file publication","read_records":qc.reads,"bases":qc.bases.iter().sum::<u64>(),"state":if complete{"committed"}else if aux_published[i]{"published"}else{"failed"},"bytes":bytes,"bytes_unavailable_reason":if bytes.is_none(){Some("owned artifact unavailable after execution failure")}else{None},"hash":null,"hash_unavailable_reason":"independent harness validates actual channel","written_scope":"successfully appended complete channel chunks"}));
            }
        }
    }
    if let Some(io)=&api_io {
        for (i,channel) in ["reject1","reject2","transformed1","transformed2","singleton1","singleton2"].iter().enumerate() {
            if let Some(d)=io.spec.outputs.iter().find(|d|d.channel==*channel) {
                if let Some(handle)=io.owned_output_key(&d.channel) {
                    let empty=crate::native_qc::Qc::default();
                    let qc=auxiliary.as_ref().map(|a|&a[i]).unwrap_or(&empty);
                    outputs.push(json!({"channel":channel,"path":{"handle":handle},"codec":crate::job_api::output_codec(d),"encoding":d.phred,"staged_path":null,"read_records":qc.reads,"bases":qc.bases.iter().sum::<u64>(),"bytes":io.bytes.lock().unwrap().get(&d.channel).copied().unwrap_or(0),"state":if complete{"committed"}else{"failed"},"publication_scope":"owned stream; no rollback","binding_kind":if matches!(d.location,crate::job_api::Location::Path(_)){"owned_override"}else{"owned_handle"},"declared_path":if let crate::job_api::Location::Path(p)=&d.location{Some(p)}else{None}}));
                }
            }
        }
    }
    let binary_sha = binary_hash();
    let mut operations = Vec::new();
    if plan.cuts.iter().flatten().any(|&v| v != 0) {
        operations.push("fixed_cut");
    }
    let quality_operation = match plan.quality_mode.as_str() {
        "window" => "quality_window",
        "mott" => "quality_mott",
        _ => "quality_ends",
    };
    let mut resolved_operations = vec!["input_validation", "raw_qc"];
    if plan.process.infer_mode != "off" {
        operations.push("adapter_inference");
        resolved_operations.push("inference_preparation_then_replay");
    }
    if plan.process.capture.enabled() {
        operations.push("original_capture");
        resolved_operations.push("original_capture");
    }
    resolved_operations.push("fixed_cut");
    for stage in plan.stage_order {
        use crate::native_processing::ProcessStage;
        match stage {
            ProcessStage::Tails => {
                if plan.tail_mask != 0 {
                    operations.push("tails");
                    resolved_operations.push("tails");
                }
            }
            ProcessStage::Quality => {
                if plan.quality_q.is_some() {
                    operations.push(quality_operation);
                }
                resolved_operations.push(quality_operation);
            }
            ProcessStage::Adapters => {
                if plan.process.primers.iter().any(|v| !v.is_empty()) {
                    operations.push("adapter5");
                    resolved_operations.push("adapter5");
                }
                if plan.process.matchers.iter().any(Option::is_some) {
                    operations.push("adapter3");
                }
                if plan.process.adapter_overlap {
                    operations.push("adapter_overlap");
                    resolved_operations.push("adapter_overlap");
                }
                resolved_operations.push("adapter3");
            }
        }
    }
    resolved_operations.push("transformed_filters");
    if plan.process.screen.enabled() {
        resolved_operations.extend(["reference_evidence", "reference_action"]);
        operations.push("reference_inspection");
    }
    resolved_operations.extend(["accepted_qc", "writer_encoding"]);
    if plan.process.min_length != 1
        || plan.process.max_length.is_some()
        || plan.process.max_n_count.is_some()
        || plan.process.max_n_fraction.is_some()
        || plan.process.low_q.is_some()
        || plan.process.min_mean_q.is_some()
        || plan.process.min_complexity.is_some()
    {
        operations.push("transformed_filters");
    }
    let all_output_codecs: Vec<&String> = plan
        .output_codecs
        .iter()
        .chain(plan.auxiliary_codecs.iter().filter(|v| !v.is_empty()))
        .collect();
    let mut report = json!({"schema":"soapnuke-v3.report/2","capabilities":["process.core-file"],"status":if complete {"complete"} else {"failed"},
        "run":{"run_id":run_id,"job":"process","sample_id":plan.sample_id,"layout":if plan.paired() {"PE"} else {"SE"},"input_encoding":plan.input_phred,"output_encoding":plan.output_phred,"input_codec":if plan.input_codecs.iter().all(|v|v==&plan.input_codecs[0]) {plan.input_codecs[0].clone()} else {"mixed".into()},"output_codec":if plan.output_codecs.is_empty(){None::<String>}else{Some(if all_output_codecs.iter().all(|v|*v==&plan.output_codecs[0]) {plan.output_codecs[0].clone()} else {"mixed".into()})}},
        "identity":{"dependency_manifest_sha256":option_env!("SOAPNUKE_DEPENDENCY_SHA256"),"dependency_manifest_sha256_unavailable_reason":if option_env!("SOAPNUKE_DEPENDENCY_SHA256").is_none(){Some("build omitted dependency manifest binding")}else{None},"binary_sha256":binary_sha,"binary_sha256_unavailable_reason":if binary_sha.is_none() {Some("sha256sum unavailable or failed")} else {None},
            "source_manifest_sha256":option_env!("SOAPNUKE_SOURCE_SHA256"),"source_manifest_sha256_unavailable_reason":if option_env!("SOAPNUKE_SOURCE_SHA256").is_none() {Some("build omitted source manifest binding")} else {None},
            "build_identity":option_env!("SOAPNUKE_BUILD_ID"),"build_identity_unavailable_reason":if option_env!("SOAPNUKE_BUILD_ID").is_none() {Some("build omitted provenance binding")} else {None}},
        "settings":{"effective":{"sample_id":plan.sample_id,"progress_seconds":plan.progress_seconds,"metrics":plan.metrics,"input_phred":plan.input_phred,"output_phred":plan.output_phred,"threads":plan.threads,"threads_auto":plan.threads_auto,"thread_budget":{"ceiling":plan.thread_budget.ceiling,"default_threads":plan.thread_budget.default_threads,"detected_parallelism":plan.thread_budget.detected_parallelism,"constraints":plan.thread_budget.constraints.iter().map(|(source,value)|json!({"source":source,"value":value})).collect::<Vec<_>>(),"ignored_constraints":plan.thread_budget.ignored_constraints},"decoder":plan.decoder,"selected_decoder":decoder,"input_codecs":plan.input_codecs,"output_codecs":plan.output_codecs,"min_length":plan.process.min_length,"processing":plan.process.settings(),"adapter_files":plan.adapter_files,"stage_order":plan.stage_order.map(|s|s.name()),"tail_bases":plan.tail_bases,"tail_min":if plan.tail_mask!=0 {Some(plan.tail_min)} else {None},"quality_mode":plan.quality_mode,"quality_q":plan.quality_q,"quality_window":if plan.quality_mode=="window" {Some(plan.quality_window)} else {None},"cut_r1":plan.cuts[0],"cut_r2":if plan.paired() {Some(plan.cuts[1])} else {None},"operations":operations,"gzip_level":plan.gzip_level,"gzip_level_active":plan.gzip_level_active,"max_read_length_supported":1000},"supplied_keys":plan.supplied,"provenance":plan.provenance(),"resolved_operations":resolved_operations},
        "disposition":{"input_units":input,"accepted_units":accepted,"rejected_units":rejected,"scope":scope,"first_reasons":reasons,"predicates":predicate_accounting,"pair_policy":plan.process.pair_policy,"predicate_units":"mate read records; overlapping/first counts are source units","not_evaluated_reason":"enabled fractions/mean are not evaluated on empty transformed records"},
        "populations":populations,
        "execution":runtime.execution_facts(),"resources":plan.resources,"unit_definitions":{"source_unit":"one SE record or associated PE pair; partial totals count observed validated complete units only","read_record":"one mate/read FASTQ record","cycle":"one-based sequence position, denominator records reaching that cycle","quality":"numerical Phred score independent of declared ASCII encoding","canonical":"A/C/G/T only; N and other excluded from GC denominator","observation_scope":"complete validated ACGTN records returned by workers; unseen/invalid records and lost worker observations are not invented zeros","written_scope":"per-channel successfully appended complete chunks; later failure preserves counts, without durability or completion claims"},
        "qualification":{"authority":"external","implemented_scope":"process","assessment":null,"assessment_unavailable_reason":"processing does not load release qualification; consult the external release assessment for this exact identity","assessment_key":{"binary_sha256":binary_sha,"source_manifest_sha256":option_env!("SOAPNUKE_SOURCE_SHA256"),"dependency_manifest_sha256":option_env!("SOAPNUKE_DEPENDENCY_SHA256")}},"summary":summary,
        "completion":{"marker":"native report published last after all required closes and summary finalization","cross_file_atomic":false,"crash_durable":false},
        "outputs":outputs,"error":error.as_ref().map(|(class,_,message)|json!({"class":class,"stage":"execution_or_publication","message":message})),
        "measurements":crate::native_runtime::metrics(plan.metrics,started,cpu_start,observed_bytes)});
    if let Some(aux) = auxiliary.as_ref() {
        let qcs = |start: usize| {
            if paired {
                vec![&aux[start], &aux[start + 1]]
            } else {
                vec![&aux[start]]
            }
        };
        let units = |start: usize| {
            if paired {
                aux[start].reads.min(aux[start + 1].reads)
            } else {
                aux[start].reads
            }
        };
        report["processing"] = json!({"transformation":{"completed_units":stats.fs.native.transformed,"original_fallback_units":stats.fs.native.fallback,"completed_population":crate::native_qc::population(&qcs(6),"transformed",units(6),scope,false),"fallback_population":crate::native_qc::population(&qcs(8),"original-fallback",units(8),scope,false),"conservation_scope":"I=A+R and I=T+F for complete jobs only; diagnostics overlap accepted/rejected channels"},"capture":{"failed_units_by_reason":stats.fs.native.capture_failed,"reason_order":["label-coordinate","retain-coordinate","label-quality","label-N"]},"record_manifest":{"path":plan.record_manifest,"staged_path":if gp.native_record_manifest.is_empty(){None}else{Some(stage.join(&gp.native_record_manifest))},"state":if plan.record_manifest.is_none(){"disabled"}else if complete{"committed"}else if manifest_published{"published"}else{"failed"},"schema":"soapnuke-v3.records/1","unit":"original zero-based source unit","columns":["unit","record_stage","reasons","evaluation","channels"]}});
        if plan.has_auxiliary() {
            report["unit_definitions"]["written_source_units"] = json!(
                "channel-family source-unit representations; may overlap, not unique input units"
            );
        }
    }
    if plan.process.profile == "srna" {
        report["processing"]["srna"] = json!({"profile":"srna","population":"original-input","stage":"original","evidence_units":stats.fs.native.srna_evaluated[0],"failed_raw_profile_units":stats.fs.native.srna_first.iter().sum::<u64>(),"minimum_insert":plan.process.srna_min_insert,"reject5":plan.process.srna_reject5,"reject_poly_a":plan.process.srna_reject_poly_a,"tail_min":plan.tail_min,"scope":"all available raw evidence evaluated atomically; failures keep originals and do not evaluate later ordinary filters; success keeps original prefix before selected3prime adapter only"});
        report["settings"]["resolved_operations"] = json!([
            "input_qc",
            "srna_raw_evidence",
            "srna_prefix_before_3prime",
            "ordinary_filters",
            "accepted_qc",
            "selection",
            "writer_encoding"
        ]);
    }
    if plan.process.overlap.enabled() {
        report["processing"]["overlap"] = json!({"minimum":plan.process.overlap.minimum,"max_error":plan.process.overlap.max_error,"minimum_insert_length":plan.process.overlap.min_insert,"evaluated_units":stats.fs.native.overlap_evaluated,"scored_units":stats.fs.native.overlap_scored,"unscorable_units":stats.fs.native.overlap_evaluated-stats.fs.native.overlap_scored,"too_short_units":stats.fs.native.insert_failed,"merged_units":stats.fs.native.merged,"unmerged_units":stats.fs.native.unmerged,"corrected_bases":stats.fs.native.corrected_bases,"correct_overlap":plan.process.overlap.correct,"merge":plan.process.overlap.merge,"unmerged_mode":if plan.process.overlap.discard_unmerged {"discard"} else {"paired"},"unwritten_unmerged_units":if plan.process.overlap.discard_unmerged {stats.fs.native.unmerged} else {0},"merged_maximum_supported_length":crate::native_overlap::MERGED_MAX_LEN,"evidence_scope":"ungapped pre-correction; canonical identical matches only; score then intersection length then smallest signed offset","insert_geometry":"signed offset + R2 length; merged span is union of both reads, retaining overhangs","correction_scope":"selected accepted both-pass units only; accepted QC before correction/merge"});
    }
    if plan.process.adapter_overlap {
        let f = &stats.fs.native;
        report["processing"]["adapter_overlap"] = json!({"enabled":true,"evaluated_pairs":f.adapter_overlap_evaluated,"clipped_pairs":f.adapter_overlap_clipped,"ambiguous_pairs":f.adapter_overlap_ambiguous,"abstained_pairs":f.adapter_overlap_evaluated-f.adapter_overlap_clipped,"rule":"both mates expose supplied 3prime adapter prefixes; unique complete canonical overlap >=30 bases with mismatches <=min(3,floor(0.02*overlap)); tails of 1-2 bases exact, longer tails allow floor(length/10) mismatches","stage":"adapters before ordinary 3prime matching; previous tail and quality operations follow stage-order","audit":"--match-report records overlap:<compatible supplied adapter name> rows, both mates, start=inferred insert; ordinary matching remains active"});
    }
    if plan.process.sample_fraction != 1.0 || plan.supplied.iter().any(|k| k == "sample-fraction") {
        report["processing"]["selection"] = json!({"fraction":plan.process.sample_fraction,"seed":plan.process.sample_seed,"selected_units":stats.fs.native.selected,"not_selected_units":stats.fs.native.not_selected,"population":"accepted source units before writers; QC accepted remains unsampled","ordinal_origin":0,"encoding":"u64_be(seed)||u64_be(original_global_source_ordinal)","hash":"SHA256 first8bytes unsigned big-endian","threshold":"H < ceil(binary64 fraction * 2^64); exact integer comparison","threshold_u65_decimal":crate::native_processing::sample_threshold(plan.process.sample_fraction).to_string(),"scope":scope});
    }
    if plan.process.split_units > 0 {
        report["processing"]["split"] = split.clone();
        report["processing"]["split"]["units_per_chunk"] = json!(plan.process.split_units);
        report["processing"]["split"]["templates"] = json!(plan.outputs);
        report["processing"]["split"]["empty_selection"] =
            json!("one empty000001 per main channel, zero selected units");
        report["populations"]["written"]["channel_layout"] = json!(
            "main logical channels aggregate their split files; auxiliary channels unsplit; outputs lists each physical file and its scalar counts"
        );
    }
    report["disposition"]["mate_pass_flags"] = json!({"mates":(0..if paired{2}else{1}).map(|mate|json!({"mate":mate+1,"passed_units":stats.fs.native.mate_passed[mate],"failed_units":stats.fs.native.mate_failed[mate],"not_evaluated_units":input.saturating_sub(stats.fs.native.mate_passed[mate]+stats.fs.native.mate_failed[mate])})).collect::<Vec<_>>(),"scope":"original mate predicate outcomes after normal transformation; alternate pair policy never changes failed to passed; capture fallback not evaluated; per-record flags in enabled singleton/alternate-policy manifest"});

    if plan.process.infer_mode != "off" {
        report["processing"]["inference"] = runtime.inference_report();
        report["disposition"]["unprocessed_units"] =
            json!(input.saturating_sub(accepted + rejected));
        report["disposition"]["conservation_applies"] = json!(complete);
    }
    if let Some(path) = &plan.match_report {
        let counts = runtime
            .match_counts
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let staged = stage.join(&gp.native_match_audit);
        report["processing"]["match_audit"] = json!({"path":path,"staged_path":staged,"recorded_units":counts[0],"omitted_units":counts[1],"limit_units":plan.match_limit,"maximum_rows_per_unit":if paired{4}else{2},"coordinates":"zero-based half-open within the selected adapters stage;5prime before3prime with recomputed coordinates","schema":"soapnuke-v3.matches/1","codec":"tsv","state":if complete{"committed"}else if match_published{"published"}else{"failed"},"scope":scope,"count_scope":"matched source units from successfully appended complete batches; failed partial append excluded","bytes":fs::metadata(if match_published{path}else{&staged}).ok().map(|m|m.len())});
    }
    if plan.process.capture.enabled() {
        let enabled = [
            !plan.process.capture.labels.is_empty(),
            plan.process.capture.retain.iter().any(Option::is_some),
            plan.process.capture.min_q.is_some(),
            !plan.process.capture.labels.is_empty(),
        ];
        let mut checks = serde_json::Map::new();
        for (i, name) in [
            "label-coordinate",
            "retain-coordinate",
            "label-quality",
            "label-N",
        ]
        .iter()
        .enumerate()
        {
            checks.insert((*name).into(),json!({"enabled":enabled[i],"evaluated":stats.fs.native.capture_evaluated[i],"failed":stats.fs.native.capture_failed[i],"not_evaluated":if enabled[i]{input.saturating_sub(stats.fs.native.capture_evaluated[i])}else{0},"disabled":if enabled[i]{0}else{input},"unit":"source units; enabled label-quality/N checks evaluate all captured segments together"}));
        }
        report["processing"]["capture"]["checks"] = Value::Object(checks);
    }
    if plan.process.screen.enabled() {
        if report.get("processing").is_none() {
            report["processing"] = json!({});
        }
        report["processing"]["reference"] = runtime
            .screen
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map_or(Value::Null, |s| s.report());
        if stats.fs.native.reference_first > 0 {
            report["disposition"]["first_reasons"]["reference-action"] =
                json!(stats.fs.native.reference_first);
        }
        if let Some(reference) = report["processing"]["reference"].as_object() {
            let failed = reference
                .get("action_failed_units")
                .cloned()
                .unwrap_or(json!(0));
            report["disposition"]["predicates"]["reference-action"] = json!({"enabled":plan.process.screen.action_enabled(),"overlapping_failed_units":failed,"first_rejected_units":stats.fs.native.reference_first,"unit":"source units"});
        }
    }
    if report.get("processing").is_some() {
        report["disposition"]["unprocessed_units"] =
            json!(input.saturating_sub(accepted + rejected));
        report["disposition"]["conservation_applies"] = json!(complete);
    }
    if plan.process.qc_only {
        report["processing"]["qc_only"]=json!(true);report["processing"]["acceptance_evaluated"]=json!(false);report["processing"]["writers_enabled"]=json!(false);
        for key in ["accepted_units","rejected_units","unprocessed_units"]{report["disposition"][key]=Value::Null;}
        report["disposition"]["first_reasons"]=json!({});report["disposition"]["predicates"]=Value::Null;report["disposition"]["conservation_applies"]=json!(false);report["disposition"]["unavailable_reason"]=json!("QC-only: acceptance and rejection not evaluated; FASTQ writers not created");
        report["populations_unavailable_reasons"]=json!({"accepted":"QC-only: acceptance not evaluated","written":"QC-only: no FASTQ writer"});report["settings"]["resolved_operations"]=json!(["input_qc","enabled_original_inspection","native_report"]);report["settings"]["effective"]["output_phred_scope"]=json!("shared declaration only; no conversion or FASTQ writer in QC-only");report["measurements"]["output_bytes"]=Value::Null;report["measurements"]["output_bytes_unavailable_reason"]=json!("QC-only: no FASTQ writer");
    }
    if plan.process.repetition {
        report["processing"]["repetition"]=runtime.repetition.lock().unwrap_or_else(|p|p.into_inner()).as_ref().map_or(Value::Null,|estimator|estimator.report(input,complete));
        if report["processing"]["repetition"].is_null(){report["processing"]["repetition_unavailable_reason"]=json!("preparation failed before estimator initialization");}
    }
    let limit_reached = plan.limit_units.is_some_and(|n| input >= n);
    report["input_scope"] = json!({"requested_limit_units":plan.limit_units,"observed_source_units":input,"scope":if limit_reached {"prefix; full input not established"}else{"requested input"},"termination":if !complete {"error"}else if limit_reached {"limit"}else{"eof"},"full_input_complete":complete&&!limit_reached,"ordinal_origin":0,"lane_count":if plan.lane_count==0 {1}else{plan.lane_count},"interleaved":plan.interleaved});
    report["settings"]["effective"]["output_codecs"] = json!(all_output_codecs);
    report["settings"]["effective"]["output_codecs_scope"] = json!(
        "main then enabled reject1/reject2/transformed1/transformed2 FASTQ channels; text diagnostics excluded"
    );
    report["measurements"]["output_bytes_scope"] =
        json!("FASTQ channels only; excludes JSON and TSV diagnostic metadata");
    report["settings"]["effective"]["input_codecs_scope"] = json!(
        if plan.lane_manifest.is_some() {
            "declared formats in TSV lane/mate order; unused lane content not verified; actual opened decoder facts in execution"
        } else {
            "configured input files/stream; actual opened decoder facts in execution"
        }
    );
    report["settings"]["effective"]["input_layout"] = json!(if plan.lane_manifest.is_some() {
        "lanes"
    } else if plan.interleaved {
        "interleaved"
    } else if paired {
        "separate-paired"
    } else {
        "single"
    });
    report["settings"]["effective"]["output_layout"] = json!(if plan.out_interleaved {
        "interleaved"
    } else if paired {
        "separate-paired"
    } else {
        "single"
    });
    if let Some(dest)=&plan.html_report {
        report["processing"]["html"]=json!({"path":dest,"authority":"non-authoritative metric snapshot; native JSON sole final completion marker","embedded_groups":["schema","unit_definitions","populations","disposition","processing"]});
        let html_stage=stage.join("metrics.html");
        #[cfg(test)]
        if matches!(fault,Fault::HtmlLink){fs::write(dest,b"owned HTML link sentinel").unwrap();}
        let buffer=runtime.html_buffer.lock().unwrap_or_else(|p|p.into_inner()).take();
        let result=if buffer.is_some(){write_html(&html_stage,&report,fault).and_then(|_|fs::hard_link(&html_stage,dest).map_err(|e|format!("publish HTML: {e}")))}else{Err("HTML buffer unavailable after failed preparation".into())};
        drop(buffer);
        if let Err(message)=result {
            complete=false;error=Some(("write".into(),4,message.clone()));report["status"]=json!("failed");report["disposition"]["scope"]=json!("partial");report["error"]=json!({"class":"write","stage":"HTML publication","message":message});report["disposition"]["conservation_applies"]=json!(false);report["processing"]["html"]["publication_failed"]=json!(true);
            if let Some(outputs)=report["outputs"].as_array_mut(){for output in outputs{if output["state"]=="committed"{output["state"]=json!("published");}}}
        }
    }
    #[cfg(test)]
    if matches!(fault, Fault::ReportLink) {
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&plan.report)
            .unwrap()
            .write_all(b"owned test publication-race sentinel")
            .unwrap();
    }
    if let Some(io)=&api_io {report["api"] = json!({"execution_admitted":true,"major":1,"internal_phred":33,"inputs":io.spec.lanes.iter().map(|l|json!({"lane":l.id,"channels":l.inputs.iter().map(|d|json!({"channel":d.channel,"phred":d.phred,"location":match &d.location{crate::job_api::Location::Path(p)=>json!({"kind":"path","path":p}),crate::job_api::Location::Handle(h)=>json!({"kind":"handle","name":h})}})).collect::<Vec<_>>()})).collect::<Vec<_>>(),"outputs":io.spec.outputs.iter().map(|d|json!({"channel":d.channel,"phred":d.phred,"location_scope":"declared; actual binding is recorded in outputs","actual_binding_kind":if io.owned_output_key(&d.channel).is_some(){"owned"}else{"path"},"location":match &d.location{crate::job_api::Location::Path(p)=>json!({"kind":"path","path":p}),crate::job_api::Location::Handle(h)=>json!({"kind":"handle","name":h})}})).collect::<Vec<_>>()});}
    if api_io.is_some()&&plan.api_spec.as_ref().unwrap().json_report.is_none(){report["completion"]["marker"]=json!("returned structured RunResult after required closes; no physical JSON requested");}
    if api_io.is_some(){report["processing"]["api"]=json!({"input_units":report["populations"]["input"]["source_units"],"accepted_units":report["populations"]["accepted"]["source_units"],"written_units":report["populations"]["written"]["source_units"]});}
    if let Some(io)=&api_io {
        report["api"]["decoder_capabilities"]=io.decoder_bindings(plan.threads);
        let offsets=io.spec.lanes.iter().flat_map(|l|&l.inputs).map(|d|d.phred).collect::<std::collections::BTreeSet<_>>();
        report["run"]["input_encoding"]=if offsets.len()==1{json!(offsets.first())}else{Value::Null};
        let offsets=io.spec.outputs.iter().map(|d|d.phred).collect::<std::collections::BTreeSet<_>>();
        report["run"]["output_encoding"]=if offsets.len()==1{json!(offsets.first())}else{Value::Null};
        report["api"]["encoding_scope"]=json!("per-declaration; a null job-wide encoding is unavailable for mixed/absent channels, not a guessed offset");
    }
    let destination=plan.api_spec.as_ref().map_or(Some(plan.report.as_path()),|s|s.json_report.as_deref());
    let publish_report = if let Some(destination)=destination {write_json(&report_stage,&report).and_then(|_|fs::hard_link(&report_stage,destination).map_err(|e|format!("publish report: {e}")))}else{Ok(())};
    if let Some(c)=capture.as_deref_mut(){*c=Some(report.clone());}
    if let Err(e) = publish_report {
        report["status"]=json!("failed");report["error"]=json!({"class":"write","stage":"report-publication","message":e});
        if let Some(c)=capture.as_deref_mut(){*c=Some(report.clone());}
        eprintln!(
            "write: {e}; owned staged artifacts={} published={published:?}",
            stage.display()
        );
        return 4;
    }
    if complete {
        for name in names.iter().take(plan.outputs.len()) {
            let _ = fs::remove_file(stage.join(name));
        }
        let _ = fs::remove_file(&report_stage);
        let _ = fs::remove_dir(&stage);
    }
    runtime.event(if complete { "complete" } else { "failed" }, true);
    if let Some((class, code, message)) = error {
        eprintln!(
            "{class}: {message}; failed report={}",
            plan.report.display()
        );
        code
    } else {
        if plan.process.qc_only {let _=writeln!(std::io::stderr(),"complete: QC-only input_units={input} accepted/written=N/A report={}",plan.report.display());}else{
        let _ = writeln!(
            std::io::stderr(),
            "complete: input_units={input} accepted_units={accepted} rejected_units={rejected} report={}",
            plan.report.display()
        );
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn plan(label: &str, content: &str) -> RunPlan {
        let dir = std::path::PathBuf::from(std::env::var("TMPDIR").unwrap()).join(format!(
            "native-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&dir).unwrap();
        fs::write(dir.join("input.fq"), content).unwrap();
        let args = [
            "--r1".into(),
            dir.join("input.fq").to_string_lossy().into_owned(),
            "--out1".into(),
            dir.join("output.fq").to_string_lossy().into_owned(),
            "--report".into(),
            dir.join("report.json").to_string_lossy().into_owned(),
        ];
        native_config::parse(&args).unwrap()
    }
    #[test]
    fn m8_html_required_publication_and_json_last_failures(){
        for (label,fault,exit) in [("ok",Fault::None,0),("html-write",Fault::HtmlWrite,4),("html-close",Fault::HtmlClose,4),("html-link",Fault::HtmlLink,4),("json-link",Fault::ReportLink,4)] {
            let mut p=plan(label,"@full comment\nACGT\n+\nIIII\n");let html=p.report.with_extension("html");p.html_report=Some(html.clone());std::sync::Arc::make_mut(&mut p.process).html_report=true;assert_eq!(run(&p,fault),exit);
            if matches!(fault,Fault::ReportLink){assert_eq!(fs::read(&p.report).unwrap(),b"owned test publication-race sentinel");let view=fs::read_to_string(&html).unwrap();assert!(view.contains("sole final job completion marker"));assert!(!view.contains("\"status\":\"complete\""));continue;}
            let r:Value=serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();assert_eq!(r["status"],if exit==0{"complete"}else{"failed"});assert_eq!(r["disposition"]["scope"],if exit==0{"complete"}else{"partial"});
            if exit==0{let view=fs::read_to_string(html).unwrap();let data=view.split("id=\"soapnuke-report\">").nth(1).unwrap().split("</script>").next().unwrap();let embedded:Value=serde_json::from_str(data).unwrap();for k in ["schema","unit_definitions","populations","disposition","processing"]{assert_eq!(embedded[k],r[k]);}}
            else if matches!(fault,Fault::HtmlLink){assert_eq!(fs::read(html).unwrap(),b"owned HTML link sentinel");}else{assert!(!html.exists());}
        }
    }
    #[test]
    fn m8_repetition_actual_partial_worker(){
        let mut p=plan("repetition-partial","@a\nACGT\n+\nIIII\n@bad\nACGT\n+\n!!!!\n");
        let settings=std::sync::Arc::make_mut(&mut p.process);settings.repetition=true;settings.repetition_units=3;p.input_phred=64;
        assert_eq!(run(&p,Fault::None),3);let r:Value=serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();assert_eq!(r["populations"]["input"]["source_units"],1);assert_eq!(r["processing"]["repetition"]["sample_units"],1);assert_eq!(r["processing"]["repetition"]["prefix_end"],"error-or-cancellation");
    }
    #[test]
    fn m8_qc_only_numeric_empty_and_partial_failure_without_writers(){
        for invalid in [false,true] {
            let dir=std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../.scratch/m8-qc-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));std::fs::create_dir_all(&dir).unwrap();let input=dir.join("raw.fastq");
            std::fs::write(&input,if invalid{"@first\nAC\n+\nII\n@bad\nAC\n+\n I\n"}else{"@first full comment\nAC\n+\n!~\n@empty\n\n+\n\n"}).unwrap();let report=dir.join("report.json");
            let args=vec!["--r1".into(),input.to_string_lossy().into_owned(),"--qc-only".into(),"--report".into(),report.to_string_lossy().into_owned(),"--threads".into(),"1".into(),"--output-phred".into(),"64".into()];let plan=crate::native_config::parse(&args).unwrap();assert!(plan.outputs.is_empty());let code=run(&plan,if invalid{Fault::None}else{Fault::Compression});assert_eq!(code,if invalid{3}else{0});let v:serde_json::Value=serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();assert!(v["populations"]["accepted"].is_null()&&v["populations"]["written"].is_null());assert!(v["disposition"]["accepted_units"].is_null()&&v["disposition"]["rejected_units"].is_null());assert_eq!(v["outputs"],serde_json::json!([]));assert_eq!(v["processing"]["writers_enabled"],false);assert_eq!(v["populations"]["input"]["read_records"],if invalid{1}else{2});if !invalid{assert_eq!(v["populations"]["input"]["quality_histogram"][93],1);assert_eq!(v["populations"]["input"]["length"]["histogram"][0],1);}assert!(v["summary"].as_str().unwrap().contains("accepted\tstatus=not-applicable"));assert_eq!(v["execution"]["active"],0);
        }
    }
    #[test]
    fn m6_merged_channel_required_failures_preserve_failed_status() {
        for (label, fault) in [
            ("merged-write", Fault::Writer),
            ("merged-report", Fault::ReportLink),
        ] {
            let mut p = plan(label, "@p/1\nAACCGG\n+\nIIIIII\n");
            let mate = p.inputs[0].with_file_name("mate.fq");
            fs::write(&mate, "@p/2\nCCGGTT\n+\nIIIIII\n").unwrap();
            p.inputs.push(mate);
            p.input_codecs.push("plain".into());
            p.outputs = vec![p.outputs[0].with_file_name("merged.fq")];
            p.output_codecs = vec!["plain".into()];
            let settings = std::sync::Arc::make_mut(&mut p.process);
            settings.overlap.merge = true;
            settings.overlap.minimum = 6;
            settings.overlap.discard_unmerged = true;
            assert_eq!(run(&p, fault), 4);
            if matches!(fault, Fault::Writer) {
                let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
                assert_eq!(report["status"], "failed");
                assert_eq!(report["populations"]["written"]["read_records"], 0);
                assert!(!p.outputs[0].exists());
            } else {
                assert_eq!(
                    fs::read(&p.report).unwrap(),
                    b"owned test publication-race sentinel"
                );
            }
        }
    }
    #[test]
    fn m6_actual_t32_1999_output_and_discard_split_empty_units() {
        let sequence1 = format!("{}C", "A".repeat(999));
        let sequence2 = format!("{}G", "A".repeat(999));
        let mut p = plan(
            "m6-max",
            &format!(
                "@max/1 full comment\n{sequence1}\n+\n{}\n",
                "I".repeat(1000)
            ),
        );
        let mate = p.inputs[0].with_file_name("mate.fq");
        fs::write(
            &mate,
            format!(
                "@max/2 full comment\n{sequence2}\n+\n{}\n",
                "I".repeat(1000)
            ),
        )
        .unwrap();
        p.inputs.push(mate);
        p.input_codecs.push("plain".into());
        p.outputs.push(p.outputs[0].with_file_name("r2.fq"));
        p.outputs.push(p.outputs[0].with_file_name("merged.fq"));
        p.output_codecs = vec!["plain".into(); 3];
        p.threads = 32;
        let settings = std::sync::Arc::make_mut(&mut p.process);
        settings.overlap.merge = true;
        settings.overlap.minimum = 1;
        settings.overlap.max_error = 0.;
        assert_eq!(run(&p, Fault::None), 0);
        let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
        assert_eq!(report["populations"]["written"]["length"]["maximum"], 1999);
        assert_eq!(
            report["populations"]["written"]["length"]["histogram"][1999],
            1
        );
        assert_eq!(report["populations"]["input"]["length"]["maximum"], 1000);
        assert!(
            report["execution"]["statistics_reserved_bytes"]
                .as_u64()
                .unwrap()
                <= 134217728
        );
        assert_eq!(
            report["execution"]["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        let output = fs::read_to_string(p.outputs.last().unwrap()).unwrap();
        assert_eq!(output.lines().nth(0).unwrap(), "@max/1 full comment");
        assert_eq!(output.lines().nth(1).unwrap().len(), 1999);
        let mut p = plan(
            "m6-discard-split",
            "@u0/1\nTTTT\n+\nIIII\n@u1/1\nAACCGG\n+\nIIIIII\n",
        );
        let mate = p.inputs[0].with_file_name("mate.fq");
        fs::write(&mate, "@u0/2\nTTTT\n+\nIIII\n@u1/2\nCCGGTT\n+\nIIIIII\n").unwrap();
        p.inputs.push(mate);
        p.input_codecs.push("plain".into());
        p.threads = 32;
        p.outputs = vec![p.outputs[0].with_file_name("merged-{chunk}.fq")];
        p.output_codecs = vec!["plain".into()];
        p.split_templates = Some(std::sync::Arc::new(p.outputs.clone()));
        let settings = std::sync::Arc::make_mut(&mut p.process);
        settings.overlap.merge = true;
        settings.overlap.minimum = 4;
        settings.overlap.max_error = 0.;
        settings.overlap.discard_unmerged = true;
        settings.split_units = 1;
        settings.split_metadata_unit_bound = 16384;
        assert_eq!(run(&p, Fault::None), 0);
        let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
        assert_eq!(report["processing"]["overlap"]["merged_units"], 1);
        assert_eq!(report["processing"]["overlap"]["unwritten_unmerged_units"], 1);
        assert_eq!(
            report["processing"]["split"]["chunks"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(report["outputs"][0]["selected_units"], 1);
        assert_eq!(report["outputs"][0]["read_records"], 0);
        assert_eq!(report["outputs"][1]["selected_units"], 1);
        assert_eq!(report["outputs"][1]["read_records"], 1);
        assert_eq!(
            fs::read(&report["outputs"][0]["path"].as_str().unwrap())
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            report["execution"]["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
    }
    #[test]
    fn t32_paired_native_job_preserves_records_and_qc() {
        let mut p = plan("t32-paired", "@x/1\nAC\n+\n!~\n");
        let mate = p.inputs[0].with_file_name("r2.fq");
        fs::write(&mate, "@x/2\nGT\n+\n~!\n").unwrap();
        p.inputs.push(mate);
        p.input_codecs.push("plain".into());
        p.outputs.push(p.outputs[0].with_file_name("r2-out.fq"));
        p.output_codecs.push("plain".into());
        p.threads = 32;
        assert_eq!(run(&p, Fault::None), 0);
        let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
        assert_eq!(report["status"], "complete");
        assert_eq!(report["populations"]["input"]["read_records"], 2);
        assert_eq!(report["populations"]["accepted"]["read_records"], 2);
        assert_eq!(report["populations"]["written"]["read_records"], 2);
        assert_eq!(
            fs::read_to_string(&p.outputs[0]).unwrap(),
            "@x/1\nAC\n+\n!~\n"
        );
        assert_eq!(
            fs::read_to_string(&p.outputs[1]).unwrap(),
            "@x/2\nGT\n+\n~!\n"
        );
    }

    #[test]
    fn compression_error_uses_real_worker_failure_path_without_complete_outputs() {
        let mut p = plan("compression-error", "@x\nAC\n+\nII\n");
        p.outputs[0].set_extension("fq.gz");
        p.output_codecs[0] = "gzip".into();
        p.gzip_level_active = true;
        assert_eq!(run(&p, Fault::Compression), 4);
        let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
        assert_eq!(report["status"], "failed");
        assert!(
            report["error"]["message"]
                .as_str()
                .unwrap()
                .contains("gzip compress failed")
        );
        assert!(!p.outputs[0].exists());
    }
    #[test]
    fn successful_channel_observations_survive_later_append_or_close_failure() {
        let mut p = plan("r2-append", "@x/1\nAC\n+\nII\n");
        let mate = p.inputs[0].with_file_name("r2.fq");
        fs::write(&mate, "@x/2\nGT\n+\nII\n").unwrap();
        p.inputs.push(mate);
        p.input_codecs.push("plain".into());
        p.outputs.push(p.outputs[0].with_file_name("r2-out.fq"));
        p.output_codecs.push("plain".into());
        assert_eq!(run(&p, Fault::R2Append), 4);
        let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
        assert_eq!(report["status"], "failed");
        assert_eq!(report["populations"]["input"]["read_records"], 2);
        assert_eq!(report["populations"]["accepted"]["read_records"], 2);
        assert_eq!(
            report["populations"]["written"]["channels"][0]["read_records"],
            1
        );
        assert_eq!(
            report["populations"]["written"]["channels"][1]["read_records"],
            0
        );
        assert_eq!(report["outputs"][0]["read_records"], 1);
        assert!(p.outputs.iter().all(|p| !p.exists()));
        for fault in [Fault::Close, Fault::Summary] {
            let p = plan("later-finalization", "@x\nAC\n+\nII\n");
            assert_eq!(run(&p, fault), 4);
            let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
            assert_eq!(report["status"], "failed");
            assert_eq!(report["populations"]["written"]["read_records"], 1);
            assert_eq!(report["outputs"][0]["read_records"], 1);
            assert_ne!(report["outputs"][0]["state"], "committed");
        }
    }
    #[test]
    fn metrics_disable_preserves_fields_and_sample_does_not_rename_outputs() {
        let mut p = plan("metrics-disabled", "@x\nAC\n+\nII\n");
        p.metrics = false;
        p.progress_seconds = 0;
        p.sample_id = "explicit sample".into();
        let output = p.outputs[0].clone();
        assert_eq!(run(&p, Fault::None), 0);
        let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
        assert_eq!(report["run"]["sample_id"], "explicit sample");
        assert_eq!(report["outputs"][0]["path"], output.to_str().unwrap());
        for key in ["wall_seconds", "cpu_seconds", "peak_rss_bytes"] {
            assert!(report["measurements"][key].is_null());
            assert_eq!(
                report["measurements"][format!("{key}_unavailable_reason")],
                "disabled by --metrics=false"
            );
        }
    }
    #[test]
    fn existing_complete_report_prevents_new_owned_outputs() {
        let p = plan("old-complete", "@x\nAC\n+\nII\n");
        assert_eq!(run(&p, Fault::None), 0);
        let marker = fs::read(&p.report).unwrap();
        let original_output = fs::read(&p.outputs[0]).unwrap();
        let new_output = p.outputs[0].with_file_name("new-output.fq");
        let args = vec![
            "--r1".into(),
            p.inputs[0].to_string_lossy().into_owned(),
            "--out1".into(),
            new_output.to_string_lossy().into_owned(),
            "--report".into(),
            p.report.to_string_lossy().into_owned(),
        ];
        assert_eq!(entry(&args), 2);
        assert_eq!(fs::read(&p.report).unwrap(), marker);
        assert_eq!(fs::read(&p.outputs[0]).unwrap(), original_output);
        assert!(!new_output.exists());
    }
    #[test]
    fn native_empty_record_is_completed_rejection() {
        let p = plan("empty", "@empty\n\n+\n\n");
        assert_eq!(run(&p, Fault::None), 0);
        let r: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
        assert_eq!(r["status"], "complete");
        assert_eq!(r["disposition"]["input_units"], 1);
        assert_eq!(r["disposition"]["accepted_units"], 0);
        assert_eq!(r["disposition"]["rejected_units"], 1);
        assert_eq!(r["populations"]["input"]["read_records"], 1);
        assert_eq!(r["populations"]["input"]["bases"], 0);
        assert!(fs::read(&p.outputs[0]).unwrap().is_empty());
    }
    #[test]
    fn native_split_required_append_close_cancel_and_publication_failures_are_not_complete() {
        for (label, fault, code) in [
            ("split-r2", Fault::R2Append, 4),
            ("split-close", Fault::Close, 4),
            ("split-cancel", Fault::Cancel, 5),
            ("split-publish", Fault::SplitPublish, 4),
            ("split-report", Fault::ReportLink, 4),
        ] {
            let mut p = plan(label, "@x/1\nAC\n+\nII\n@y/1\nTG\n+\nII\n");
            let mate = p.inputs[0].with_file_name("r2.fq");
            fs::write(&mate, "@x/2\nGT\n+\nII\n@y/2\nCA\n+\nII\n").unwrap();
            p.inputs.push(mate);
            p.input_codecs.push("plain".into());
            let parent = p.report.parent().unwrap();
            p.outputs = vec![
                parent.join("chunk-{chunk}.r1.fq"),
                parent.join("chunk-{chunk}.r2.fq"),
            ];
            p.output_codecs = vec!["plain".into(), "plain".into()];
            p.split_templates = Some(std::sync::Arc::new(p.outputs.clone()));
            let settings = std::sync::Arc::make_mut(&mut p.process);
            settings.split_units = 1;
            settings.split_metadata_unit_bound = 16384;
            assert_eq!(run(&p, fault), code);
            if matches!(fault, Fault::ReportLink) {
                assert_eq!(
                    fs::read(&p.report).unwrap(),
                    b"owned test publication-race sentinel"
                );
                assert!(parent.join("chunk-000002.r2.fq").exists());
                continue;
            }
            let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
            assert_eq!(report["status"], "failed");
            assert_eq!(report["execution"]["active"], 0);
            assert_eq!(
                report["execution"]["memory_budget"]["current_feature_reserved_bytes"],
                0
            );
            if matches!(fault, Fault::R2Append) {
                assert_eq!(report["populations"]["written"]["read_records"], 0);
                assert_eq!(report["processing"]["split"]["processing_complete"], false);
                assert!(!parent.join("chunk-000001.r1.fq").exists());
            } else {
                assert_eq!(report["populations"]["written"]["read_records"], 4);
                assert_eq!(report["processing"]["split"]["processing_complete"], true);
            }
            if matches!(fault, Fault::SplitPublish) {
                assert_eq!(
                    fs::read(parent.join("chunk-000002.r2.fq")).unwrap(),
                    b"owned split publication sentinel"
                );
                assert_eq!(report["outputs"][0]["state"], "published");
                assert_eq!(report["outputs"][3]["state"], "failed");
            }
        }
    }

    #[test]
    fn native_failure_paths_never_complete() {
        for (label, fault, code) in [
            ("writer", Fault::Writer, 4),
            ("close", Fault::Close, 4),
            ("worker", Fault::Worker, 5),
            ("cancel", Fault::Cancel, 5),
            ("report", Fault::Report, 4),
        ] {
            let p = plan(label, "@positive full comment\nAC\n+\nII\n");
            assert_eq!(run(&p, fault), code, "{label}");
            let r: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
            assert_eq!(r["status"], "failed", "{label}");
            assert!(
                r["outputs"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|o| o["state"] != "committed")
            );
        }
    }
    #[test]
    fn native_m4_window_delays_and_empty_completions_release_all_ownership() {
        let text = (0..24000)
            .map(|i| format!("@{i} full comment\nAC\n+\nII\n"))
            .collect::<String>();
        for threads in [1, 4, 8, 16] {
            let mut p = plan("m4-window", &text);
            p.threads = threads;
            assert_eq!(run(&p, Fault::Delay), 0);
            assert_eq!(fs::read_to_string(&p.outputs[0]).unwrap(), text);
            let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
            let e = &report["execution"];
            assert_eq!(e["active"], 0);
            assert_eq!(e["retired"], e["admitted"]);
            assert_eq!(e["active_chunk_bytes"], 0);
            assert!(e["peak_active"].as_u64().unwrap() <= e["window_limit"].as_u64().unwrap());
            assert!(e["statistics_reserved_bytes"].as_u64().unwrap() <= 128 * 1024 * 1024);
            assert!(e["compute"]["peak_active"].as_u64().unwrap() <= threads as u64);
            let mut p = plan("m4-empty", &text);
            p.threads = threads;
            p.process = std::sync::Arc::new(crate::native_processing::ProcessSettings {
                min_length: 3,
                ..Default::default()
            });
            assert_eq!(run(&p, Fault::Delay), 0);
            assert!(fs::read(&p.outputs[0]).unwrap().is_empty());
            let report: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
            assert_eq!(report["disposition"]["rejected_units"], 24000);
            assert_eq!(report["execution"]["active"], 0);
            assert_eq!(
                report["execution"]["retired"],
                report["execution"]["admitted"]
            );
        }
    }
    #[test]
    fn native_delayed_first_chunk_keeps_order() {
        let text = (0..6000)
            .map(|i| format!("@{i} full comment\nAC\n+\nII\n"))
            .collect::<String>();
        let mut p = plan("delay", &text);
        p.threads = 4;
        assert_eq!(run(&p, Fault::Delay), 0);
        assert_eq!(fs::read_to_string(&p.outputs[0]).unwrap(), text);
    }
    #[test]
    fn native_rapidgzip_final_quality_without_newline_is_not_lost() {
        let text = "@endpoint full comment\nACGT\n+\n!5I~";
        let mut p = plan("rapid-eof", text);
        let compressed = crate::ffi::Compressor::new(2)
            .unwrap()
            .gzip_compress_alloc(text.as_bytes());
        let gz = p.inputs[0].with_extension("fq.gz");
        fs::write(&gz, compressed).unwrap();
        p.inputs[0] = gz;
        p.input_codecs = vec!["gzip".into()];
        p.threads = 16;
        assert_eq!(run(&p, Fault::None), 0);
        assert_eq!(
            fs::read_to_string(&p.outputs[0]).unwrap(),
            format!("{text}\n")
        );
    }

    #[test]
    fn native_lane_spawn_errors_are_worker_failures_and_join_started_lanes() {
        for fault in [Fault::Lane1, Fault::Lane2] {
            let mut p = plan("lane-spawn", "@source/1\nAC\n+\nII\n");
            if matches!(fault, Fault::Lane2) {
                let r2 = p.inputs[0].with_file_name("input-r2.fq");
                fs::write(&r2, "@source/2\nGT\n+\nII\n").unwrap();
                p.inputs.push(r2);
                p.outputs.push(p.outputs[0].with_file_name("output-r2.fq"));
                p.input_codecs.push("plain".into());
                p.output_codecs.push("plain".into());
            }
            assert_eq!(run(&p, fault), 5);
            let r: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
            assert_eq!(r["status"], "failed");
            assert_eq!(r["error"]["class"], "worker");
            assert!(p.outputs.iter().all(|v| !v.exists()));
            for task in fs::read_dir("/proc/self/task").unwrap() {
                if let Ok(name) = fs::read_to_string(task.unwrap().path().join("comm")) {
                    assert!(
                        !matches!(name.trim(), "lane1" | "lane2" | "sink")
                            && !name.trim().starts_with("worker"),
                        "owned pipeline thread survived return: {name}"
                    );
                }
            }
        }
    }
    #[test]
    fn native_reader_construction_errors_have_input_class() {
        for rapid in [false, true] {
            let mut p = plan("open-error", "@positive\nAC\n+\nII\n");
            if rapid {
                let bad = p.inputs[0].with_extension("fq.gz");
                fs::write(&bad, b"\x1f\x8bbad-gzip").unwrap();
                p.inputs[0] = bad;
                p.input_codecs = vec!["gzip".into()];
                p.threads = 16;
            } else {
                fs::remove_file(&p.inputs[0]).unwrap();
            }
            assert_eq!(run(&p, Fault::None), 3);
            let r: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
            assert_eq!(r["error"]["class"], "input");
            assert_eq!(r["status"], "failed");
            assert!(p.outputs.iter().all(|v| !v.exists()));
        }
    }
    #[test]
    fn actual_report_hard_link_failure_is_not_a_complete_output_set() {
        let p = plan("actual-report-link", "@positive\nAC\n+\nII\n");
        assert_eq!(run(&p, Fault::ReportLink), 4);
        assert_eq!(
            fs::read(&p.report).unwrap(),
            b"owned test publication-race sentinel"
        );
        assert_eq!(
            fs::read_to_string(&p.outputs[0]).unwrap(),
            "@positive\nAC\n+\nII\n"
        );
        assert!(serde_json::from_slice::<Value>(&fs::read(&p.report).unwrap()).is_err());
    }

    #[test]
    fn tagged_errors_do_not_depend_on_path_or_payload_words() {
        for (failure, class, code) in [
            (Failure::input("[input] cannot open /some/worker/input.fq"), "input", 3),
            (Failure::write("[write] FASTQ output /some/worker/out.fq"), "write", 4),
            (Failure::worker("[worker] lane spawn: Resource temporarily unavailable"), "worker", 5),
        ] {
            assert_eq!((failure.category.name(), failure.category.exit()), (class, code));
        }
    }

    #[test]
    fn native_noop_retains_all_n_read_and_reports_nonzero_qc() {
        let text = "@all-n full comment\nNNn\n+\n!5I\n";
        let p = plan("nonzero-n-qc", text);
        assert_eq!(run(&p, Fault::None), 0);
        let r: Value = serde_json::from_slice(&fs::read(&p.report).unwrap()).unwrap();
        for stage in ["input", "accepted"] {
            assert_eq!(r["populations"][stage]["bases"], 3);
            assert_eq!(r["populations"][stage]["n_bases"], 3);
            assert_eq!(r["populations"][stage]["q20_bases"], 2);
            assert_eq!(r["populations"][stage]["q30_bases"], 1);
        }
        assert_eq!(r["disposition"]["accepted_units"], 1);
        assert_eq!(fs::read_to_string(&p.outputs[0]).unwrap(), text);
    }
}
