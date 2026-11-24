use crate::{dedup_app::{Plan, report_ticket}, dedup_runtime::Runtime, job_api::NativeReport};
use std::{fs, io::{self, Write}, path::{Path, PathBuf}, sync::Arc};
#[cfg(test)]
use crate::dedup_app::{REPORT_FAULT, R4_UNLINK};

pub(crate) struct Outcome {
    pub code:i32,
    pub category:Option<&'static str>,
    pub message:Option<String>,
    pub core:Option<crate::dedup_grouping::Facts>,
}
pub(crate) struct Completion {
    pub result:(i32,NativeReport,Option<&'static str>),
    pub report_ticket:Option<crate::native_execution::FeatureTicket>,
}
pub(crate) struct Finalization<'a,'p> {
    pub plan:&'p Plan,
    pub execution:&'a Arc<crate::native_execution::Execution>,
    pub io:&'a crate::native_io::OwnedIo,
    pub runtime:&'a Runtime,
    pub working:&'a mut crate::dedup_report::HeaderWorking,
    pub scratch:&'a Scratch,
    pub emergency:&'a mut crate::dedup_report::Emergency,
    pub publication:&'a mut Option<crate::dedup_publication::Publication<'p>>,
    pub api:bool,
    pub started:std::time::Instant,
    pub cpu_start:Option<f64>,
}
// Emergency reserves this fixed control allowance before processing. Dynamic
// header paths, scratch and normal/compact report buffers keep their own tickets.
const CONTROL_BYTES:usize=std::mem::size_of::<Finalization<'static,'static>>()
    +2*std::mem::size_of::<Outcome>()+std::mem::size_of::<Completion>()
    +std::mem::size_of::<crate::dedup_report::Header<'static>>()
    +std::mem::size_of::<crate::dedup_report::Views<'static>>()
    +std::mem::size_of::<Scratch>()
    +std::mem::size_of::<[Option<crate::dedup_report::Output>;4]>()
    +std::mem::size_of::<std::io::BufWriter<std::fs::File>>()
    +std::mem::size_of::<crate::dedup_report::Failure<'static>>();
const _:()=assert!(CONTROL_BYTES<=4096);

#[derive(Debug)]
pub(crate) struct CleanupFailure {
    pub stage: &'static str,
    pub path: PathBuf,
    pub error: Option<io::Error>,
}

impl std::fmt::Display for CleanupFailure {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(out, "{}: {}", self.stage, self.path.display())?;
        if let Some(error) = &self.error {
            write!(out, ": {error}")?;
        }
        Ok(())
    }
}

fn publish_fastq(
    staged: &Path,
    destination: &Path,
    execution: &Arc<crate::native_execution::Execution>,
    cleanup: &mut Option<CleanupFailure>,
) -> Result<(), String> {
    match fs::hard_link(staged, destination) {
        Ok(()) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::CrossesDevices => {}
        Err(error) => return Err(error.to_string()),
    }

    // The copy stays hidden until it is complete. The link in the destination
    // filesystem preserves the existing no-overwrite publication contract.
    let destination_parent = destination.parent().unwrap_or(Path::new("."));
    let scratch_name = staged.parent().and_then(Path::file_name).unwrap();
    let staged_name = staged.file_name().unwrap();
    let capacity = destination_parent.as_os_str().len()
        .checked_add(scratch_name.len())
        .and_then(|n| n.checked_add(staged_name.len() + 2))
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| n.checked_add(16384))
        .ok_or("cross-filesystem publication memory overflow")?;
    let _reservation = execution.reserve_finalization_feature(capacity).map_err(|e| e.to_string())?;
    let mut name = scratch_name.to_os_string();
    name.push("-");
    name.push(staged_name);
    let temporary = destination_parent.join(name);
    let mut source = fs::File::open(staged).map_err(|e| e.to_string())?;
    let permissions = source.metadata().map_err(|e| e.to_string())?.permissions();
    use std::os::unix::fs::OpenOptionsExt;
    let mut local = fs::OpenOptions::new().write(true).create_new(true).mode(0o600)
        .open(&temporary).map_err(|e| format!("create staging file {}: {e}", temporary.display()))?;
    let copied = io::copy(&mut source, &mut local)
        .and_then(|_| local.set_permissions(permissions))
        .and_then(|_| local.sync_all());
    let closed = crate::checked_io::close_file(local);
    let result = copied.map_err(|e| e.to_string()).and(closed)
        .and_then(|_| fs::hard_link(&temporary, destination).map_err(|e| e.to_string()));
    if let Err(error) = fs::remove_file(&temporary) {
        let cause = CleanupFailure { stage: "destination-stage-unlink", path: temporary, error: Some(error) };
        eprintln!("dedup cleanup: {cause}");
        if cleanup.is_none() {
            *cleanup = Some(cause);
        }
    }
    result
}

impl Finalization<'_,'_> {
 pub(crate) fn finish(self,outcome:Outcome)->Completion {
    let Self { plan,execution:e,io,runtime,working:header_working,scratch,emergency,publication,api,started,cpu_start }=self;
    let Outcome {mut code,category:mut failure,mut message,core}=outcome;
    let c=&plan.common;
    if let Err(err) = io.finish() {
        if code == 0 {
            code = 4;
            failure = Some("output");
            message = Some(err.to_string());
        }
    }
    let s = runtime.state.lock().unwrap();
    let _header=match report_ticket(e,std::mem::size_of::<crate::dedup_report::Header>()+4096,1){Ok(v)=>v,Err(_)=>return failed_finalization(plan,&s,e,emergency,scratch,None,publication,code,failure,"initial-header",message.as_deref())};
    let mut outputs=[None;4];
    let mut destination_cleanup = None;
    if s.representatives.is_some(){for(i,ch)in plan.channels.iter().enumerate(){
        let owned=io.owned_output_key(&ch.io).is_some();let staged=header_working.staged[i].as_ref().unwrap();let mut published=false;
        if code == 0 && !owned {
            match publish_fastq(staged, &ch.path, e, &mut destination_cleanup) {
                Ok(()) => published = true,
                Err(err) => {
                    code = 4;
                    failure = Some("output");
                    message = Some(format!("publication {}: {err}", ch.path.display()));
                }
            }
        }
        let actual=if published{&ch.path}else{staged};outputs[i]=Some(crate::dedup_report::Output{owned,failed:code!=0,bytes:if owned{io.bytes.lock().unwrap().get(&ch.io).copied()}else{fs::metadata(actual).ok().map(|m|m.len())}});
    }}
    header_working.observe(started,cpu_start);
    let mut header=crate::dedup_report::Header{plan,working:&header_working,outputs,message:message.as_deref(),stage:failure,late:None,code,core,cleanup_stage:None,cleanup:None,publication:None,execution:e};
    let views = crate::dedup_report::Views::new(
        &plan.spec,
        io,
        c.threads,
        api,
        plan.catalogue.as_ref(),
    );
    let (mut text, mut ticket) =
        match crate::dedup_report::typed_encode(&header, &s, &plan.channels, e, &views,2) {
            Ok(v) => (v.0, Some(v.1)),
            Err(_) => return failed_finalization(plan,&s,e,emergency,scratch,destination_cleanup.as_ref(),publication,code,failure,"initial-body",message.as_deref()),
        };
    #[cfg(test)] if REPORT_FAULT.with(|v|v.get()%10==3){if let Some(path)=&c.html_report{fs::create_dir(path).unwrap();}}
    if let Some(path) = &c.html_report {
        let result = (|| -> Result<(), String> {
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|e| e.to_string())?;
            let mut w = std::io::BufWriter::with_capacity(8192, file);
            crate::native_html::write_nullable_snapshot(
                &mut w,
                &c.sample_id,
                &crate::dedup_report::TypedDocument {
                    header: &header,
                    state: &s,
                    channels: &plan.channels,
                    snapshot: true,
                    views: &views,
                },
                [
                    Some((&s.input, s.input_units)),
                    s.representatives.map(|n| (&s.accepted, n)),
                    Some((&s.written, s.written_units)),
                ],
            )
            .map_err(|e| e.to_string())?;
            w.flush().map_err(|e| e.to_string())?;
            let file = w.into_inner().map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            crate::checked_io::close_file(file)
        })();
        if let Err(err) = result {
            if code==0{code=4;failure=Some("output");header.code=code;header.stage=failure;header.late=Some(("html-publication",err));}
            drop(text);
            drop(ticket.take());
            let v = match crate::dedup_report::typed_encode(&header, &s, &plan.channels, e, &views, 3) {
                Ok(v)=>v, Err(_)=>return failed_finalization(plan,&s,e,emergency,scratch,destination_cleanup.as_ref(),publication,code,failure,"html-reencode",header.message()),
            };
            text = v.0;
            ticket = Some(v.1);
        }
    }
    let scratch_cleanup = scratch.cleanup(code == 0).err();
    if let Some(cause) = scratch_cleanup {
        eprintln!("dedup cleanup: {cause}");
    }
    if let Some(cause) = scratch_cleanup.or(destination_cleanup.as_ref()) {
        if code==0{code=4;failure=Some("cleanup");header.code=code;header.stage=failure;header.late=Some(("cleanup",cause.stage.into()));}
        header.cleanup_stage=Some(cause.stage);
        header.cleanup=Some(cause);
        drop(text);drop(ticket.take());
        let v=match crate::dedup_report::typed_encode(&header,&s,&plan.channels,e,&views,6){Ok(v)=>v,Err(_)=>return failed_finalization(plan,&s,e,emergency,scratch,destination_cleanup.as_ref(),publication,code,failure,"cleanup-reencode",header.message())};text=v.0;ticket=Some(v.1);
    }
    #[cfg(test)] if REPORT_FAULT.with(|v|v.get()%10==4){if let Some(path)=&plan.spec.json_report{fs::create_dir(path).unwrap();}}
    if let Some(p)=publication.as_mut(){
        if let Err(err)=p.commit(&text){
            if code==0{code=4;failure=Some("report-publication");header.code=code;header.stage=failure;header.late=Some(("report-publication",err.stage.into()));}
            header.publication=Some(err);
            drop(text);drop(ticket.take());
            let v=match crate::dedup_report::typed_encode(&header,&s,&plan.channels,e,&views,4){Ok(v)=>v,Err(_)=>return failed_finalization(plan,&s,e,emergency,scratch,destination_cleanup.as_ref(),publication,code,failure,"json-reencode",Some(err.stage))};text=v.0;ticket=Some(v.1);
        }
    }

    if code != 0 {
        eprintln!("dedup error: {}", header.message().unwrap_or("execution failure"));
    }
    let result = NativeReport {
        status: if code == 0 { "complete" } else { "failed" },
        input_units: s.input_units,
        accepted_units: s.representatives,
        written_units: Some(s.written_units),
        json: text,
    };
    drop(header);
    drop(s);
    Completion { result: (code, result, failure), report_ticket: ticket.take() }
 }
}

pub(crate) struct Scratch {
    pub(crate) path: PathBuf,
    finished: std::cell::Cell<bool>,
    failure: std::cell::OnceCell<CleanupFailure>,
    _ticket: crate::native_execution::FeatureTicket,
}
impl Scratch {
    pub(crate) fn new(path: PathBuf, ticket: crate::native_execution::FeatureTicket) -> Self {
        Self { path, finished: std::cell::Cell::new(false), failure: std::cell::OnceCell::new(), _ticket: ticket }
    }

    fn cleanup(&self, success: bool) -> Result<(), &CleanupFailure> {
        if !self.finished.replace(true) {
            if let Err(cause) = self.remove_files(success) {
                let _ = self.failure.set(cause);
            }
        }
        self.failure.get().map_or(Ok(()), Err)
    }

    fn remove_files(&self, success: bool) -> Result<(), CleanupFailure> {
        #[cfg(test)]
        if R4_UNLINK.with(|v| v.replace(false)) {
            let path = self.path.join("payload.bin");
            if path.is_file() {
                fs::remove_file(&path).unwrap();
                fs::create_dir(path).unwrap();
            }
        }
        let mut partial = false;
        let directory_error = |stage, error| CleanupFailure {
            stage, path: self.path.clone(), error: Some(error),
        };
        for entry in fs::read_dir(&self.path).map_err(|e| directory_error("scratch-directory", e))? {
            let entry = entry.map_err(|e| directory_error("scratch-entry", e))?;
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| CleanupFailure {
                stage: "scratch-name", path: entry.path(), error: None,
            })?;
            let channel = name.starts_with("channel-");
            if name.starts_with("run-")
                || ["payload.bin", "classification.bin", "report.json"].contains(&name)
                || (success && channel)
            {
                let path = entry.path();
                fs::remove_file(&path).map_err(|error| CleanupFailure {
                    stage: "internal-file", path, error: Some(error),
                })?;
            } else if channel {
                partial = true;
            } else {
                return Err(CleanupFailure { stage: "unknown-owned-entry", path: entry.path(), error: None });
            }
        }
        if !partial {
            fs::remove_dir(&self.path).map_err(|e| directory_error("scratch-remove-directory", e))?;
        }
        Ok(())
    }
}
impl Drop for Scratch {fn drop(&mut self){let _=self.cleanup(false);}}
fn failed_finalization(plan:&Plan,s:&crate::dedup_runtime::State,e:&Arc<crate::native_execution::Execution>,emergency:&mut crate::dedup_report::Emergency,scratch:&Scratch,destination_cleanup:Option<&CleanupFailure>,publication:&mut Option<crate::dedup_publication::Publication<'_>>,code:i32,category:Option<&'static str>,secondary:&'static str,message:Option<&str>)->Completion{
 let cleanup=scratch.cleanup(false).err().or(destination_cleanup);
 if let Some(cause) = cleanup { eprintln!("dedup cleanup: {cause}"); }
 let cleanup_stage=cleanup.map(|cause|cause.stage);let cleanup_failed=cleanup_stage.is_some();
 let actual_code=if code==0{5}else{code};let category=category.unwrap_or("report-generation");
 let f=crate::dedup_report::Failure{code:actual_code,category,stage:s.error.as_ref().map_or(category,|v|v.stage),ordinal:s.error.as_ref().map(|v|v.ordinal),lane:s.error_lane,message:s.error.as_ref().map(|v|v.message.as_str()).or(message).unwrap_or("detailed report generation failed"),secondary,cleanup_failed,cleanup_stage,cleanup,admitted:true,publication_failed:publication.as_ref().is_some_and(|p|p.failed()),publication_cleanup_stage:publication.as_ref().and_then(|p|p.cleanup_stage())};
 let(mut text,_ticket)=emergency.failed(s,f,e);
 if let Some(p)=publication.as_mut().filter(|p|p.can_attempt()) {
  if let Err(err)=p.commit(&text){
   if let Some(stage)=err.cleanup_stage{let needle="\"publication_cleanup_stage\":null";if let Some(at)=text.find(needle){text.replace_range(at+needle.len()-4..at+needle.len(),&format!("\"{stage}\""));}}
   for (key,needed) in [("publication_failed",true),("cleanup_failed",err.cleanup_failed)]{
    if needed{let needle=format!("\"{key}\":false");if let Some(at)=text.find(&needle){text.replace_range(at+needle.len()-5..at+needle.len(),"true");}}
   }
  }
 }
 Completion { result: (actual_code,NativeReport{status:"failed",input_units:s.input_units,accepted_units:s.representatives,written_units:Some(s.written_units),json:text},Some(category)), report_ticket: None }
}

pub(crate) fn after_return(mut result:(i32,NativeReport,Option<&'static str>),admitted:bool,scratch:&Scratch,emergency:&mut crate::dedup_report::Emergency,e:&Arc<crate::native_execution::Execution>)->(i32,NativeReport,Option<&'static str>){
    if let Err(cause)=scratch.cleanup(result.0==0) {
        if !admitted {
            let state=crate::dedup_runtime::State::unstarted();let category=result.2.unwrap_or("admission");
            eprintln!("dedup cleanup: {cause}");
            let f=crate::dedup_report::Failure{code:result.0,category,stage:"admission",ordinal:None,lane:None,message:"execution setup failed; full original diagnostic unavailable",secondary:cause.stage,cleanup_failed:true,cleanup_stage:Some(cause.stage),cleanup:Some(cause),admitted:false,publication_failed:false,publication_cleanup_stage:None};
            let(text,_)=emergency.failed(&state,f,&e);result.1.json=text;
        }
    }
    result
}

#[cfg(test)]
mod tests {
 #[test] fn setup_cleanup_uses_admission_fact_not_report_text(){
    let e=crate::native_execution::Execution::new(1,1,std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false))).unwrap();
    let root=crate::test_support::workspace("admission-control");
    let directory=root.join("scratch");std::fs::create_dir(&directory).unwrap();
    std::fs::create_dir(directory.join("payload.bin")).unwrap();
    let scratch=super::Scratch::new(directory.clone(),e.reserve_preparation_feature(4096).unwrap());
    let mut emergency=crate::dedup_report::Emergency::new(&e).unwrap();
    let report=crate::job_api::NativeReport{status:"failed",input_units:0,accepted_units:None,written_units:Some(0),json:r#"{"execution_admitted":true}"#.into()};
    let result=super::after_return((5,report,Some("memory")),false,&scratch,&mut emergency,&e);
    assert_eq!((result.0,result.2),(5,Some("memory")));
    let value:serde_json::Value=serde_json::from_str(&result.1.json).unwrap();
    assert_eq!(value["api"]["execution_admitted"],false);
    assert_eq!(value["error"]["stage"],"admission");
    assert_eq!(value["report_generation"]["cleanup_stage"],"internal-file");
    if let Some(details) = value["report_generation"].get("cleanup") {
        assert_eq!(details["path"], directory.join("payload.bin").to_str().unwrap());
        assert_eq!(details["os_error_code"], 21);
    } else {
        assert_eq!(value["report_generation"]["cleanup_os_error_code"], 21);
    }
    assert!(directory.join("payload.bin").is_dir());
    drop(value);drop(result);drop(emergency);drop(scratch);
    assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
 }
 #[test] fn fixed_handoff_owners_fit_existing_emergency_control_allowance(){
    let bytes=super::CONTROL_BYTES;
    assert!(bytes<=4096);println!("fixed_control_bytes={bytes} allowance=4096");
 }
}
