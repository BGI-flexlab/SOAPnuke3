// Public Rust API v1; execution support is introduced incrementally.
use std::collections::BTreeMap;
#[cfg(test)]
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

pub const API_MAJOR: u16 = 1;
#[derive(Clone, Debug, PartialEq)]
pub enum Setting { Bool(bool), Int(i64), UInt(u64), Float(f64), Text(String), List(Vec<String>), Off }
pub use crate::io_types::{Layout, Format, Location, InputDecl, OutputDecl, LaneDecl, ReadSeek, InputHandle, ReportWriter, JobIo};
#[derive(Clone, Debug, PartialEq)]
pub struct JobSpec {
    pub configured: BTreeMap<String, Setting>,
    pub supplied: BTreeMap<String, Setting>,
    pub layout: Layout,
    pub lanes: Vec<LaneDecl>,
    pub outputs: Vec<OutputDecl>,
    pub json_report: Option<PathBuf>,
}
#[derive(Clone, Debug, PartialEq)]
pub enum JobConfig { Process(JobSpec), Demux(JobSpec), Dedup(JobSpec) }
#[derive(Clone, Debug, Default)]
pub struct ResourceResolver { pub catalogue: Option<PathBuf>, pub explicit_paths: Vec<(PathBuf, String)> }
#[derive(Clone, Debug)]
pub struct ResolvedResource { pub catalogue_id: Option<String>, pub kind: String, pub path: PathBuf, pub sha256: String, pub catalogue_sha256: Option<String> }
#[derive(Clone, Debug)]
pub struct ValidatedJob { authority: Arc<ValidatedAuthority>, pub config: JobConfig, pub effective: BTreeMap<String, Setting>, pub provenance: BTreeMap<String, &'static str>, resolved_resources: Vec<ResolvedResource>, plan: Option<Arc<crate::native_config::RunPlan>>, demux_plan:Option<Arc<crate::demux_app::Plan>>, dedup_plan:Option<Arc<crate::dedup_app::Plan>>, catalogue_path: Option<PathBuf>, catalogue_sha256: Option<String> }
#[derive(Clone, Debug)]
struct ValidatedAuthority { config: JobConfig, effective: BTreeMap<String,Setting>, provenance: BTreeMap<String,&'static str> }
impl ValidatedJob { fn matches_authority(&self)->bool { self.config==self.authority.config && self.effective==self.authority.effective && self.provenance==self.authority.provenance } }
impl ValidatedJob {
    pub fn configuration(&self)->&JobConfig { &self.authority.config }
    pub fn effective_settings(&self)->&BTreeMap<String,Setting> { &self.authority.effective }
    pub fn setting(&self,key:&str)->Option<&Setting> { self.authority.effective.get(key) }
    pub fn provenance(&self)->&BTreeMap<String,&'static str> { &self.authority.provenance }
    pub fn resolved_resources(&self)->&[ResolvedResource] { &self.resolved_resources }
}
#[derive(Clone, Debug)]
pub struct ConfigError { pub code: &'static str, pub field: String }
#[derive(Clone, Debug, PartialEq)]
pub struct ProgressEvent { pub stage: &'static str, pub input_units: u64, pub accepted_units: Option<u64>, pub written_units: Option<u64>, pub channel_records: BTreeMap<String,u64> }
pub trait ProgressObserver { fn observe(&mut self, event: ProgressEvent); }
#[derive(Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);
impl CancellationToken {
    pub fn cancel(&self) { self.0.store(true, Ordering::SeqCst); }
    pub fn is_cancelled(&self) -> bool { self.0.load(Ordering::SeqCst) }
}
#[derive(Clone, Debug)]
pub struct NativeReport { pub status: &'static str, pub input_units: u64, pub accepted_units: Option<u64>, pub written_units: Option<u64>, pub json: String }
#[derive(Clone, Debug)]
pub struct Artifact { pub channel: String, pub closed: bool, pub format: Option<&'static str>, pub consumer_contract_sha256: Option<String> }
#[derive(Debug)]
pub struct RunResult { pub report: NativeReport, pub artifacts: Vec<Artifact> }
#[derive(Debug)]
pub struct RunError { pub stage: &'static str, pub partial: NativeReport, pub artifacts: Vec<Artifact> }
#[derive(Debug)]
pub enum ReportFormat { Html, Tsv, Multiqc, Mgnify }
pub struct ConsumerContract { pub name: String, pub version: String, pub manifest_path: PathBuf, pub manifest_sha256: String, pub allow_unqualified: bool }
#[derive(Debug)]
pub struct ReportError { pub stage: &'static str }

fn config_error(code:&'static str,field:impl Into<String>)->ConfigError{ConfigError{code,field:field.into()}}
fn resource_error(e:crate::resources_app::Error)->ConfigError{config_error(e.code,e.field)}
pub(crate) fn setting_json(v:&Setting)->serde_json::Value {use serde_json::{json,Value};match v{Setting::Bool(b)=>json!(b),Setting::Int(n)=>json!(n),Setting::UInt(n)=>json!(n),Setting::Float(n)=>json!(n),Setting::Text(s)=>json!(s),Setting::List(s)=>json!(s),Setting::Off=>Value::String("off".into())}}
fn spec(config:&JobConfig)->&JobSpec{match config{JobConfig::Process(s)|JobConfig::Demux(s)|JobConfig::Dedup(s)=>s}}
pub fn validate(config:JobConfig,resolver:ResourceResolver)->Result<ValidatedJob,ConfigError>{
    let s=spec(&config);
    if s.lanes.is_empty() || s.lanes.iter().any(|l| l.id.is_empty() || l.inputs.is_empty()) {return Err(config_error("config","r1"));}
    for values in [&s.configured,&s.supplied]{for (key,value) in values{
        if matches!(value,Setting::Float(n) if !n.is_finite()){return Err(config_error("config",key));}
        let json=setting_json(value);
        let json=if ["adapter-file","screen-reference"].contains(&key.as_str()) && json.is_string(){serde_json::json!([json])}else{json};
        if matches!(config,JobConfig::Demux(_)){crate::demux_app::config_value(key,&json).map_err(|_|config_error("config",key))?;}else if matches!(config,JobConfig::Dedup(_)){crate::dedup_app::config_value(key,&json).map_err(|_|config_error("config",key))?;}else{crate::native_config::config_arguments(key,&json).map_err(|_|config_error("config",key))?;}
    }}
    for lane in &s.lanes {for input in &lane.inputs {if !matches!(input.phred,33|64){return Err(config_error("config","input-phred"));}}}
    for output in &s.outputs{if !matches!(output.phred,33|64){return Err(config_error("config","output-phred"));}}
    let mut effective=s.configured.clone();let mut provenance=BTreeMap::new();
    for key in effective.keys(){provenance.insert(key.clone(),"config");}
    for (key,value) in &s.supplied {effective.insert(key.clone(),value.clone());provenance.insert(key.clone(),"api");}
    if matches!(config,JobConfig::Process(_)){validate_process_declarations(s,&effective)?;}
    let canonical_catalogue=|p:&PathBuf|std::fs::canonicalize(p).map_err(|_|config_error("resource-catalogue-io","resources.catalogue"));
    let supplied_catalogue=match effective.get("resource-catalog"){Some(Setting::Text(p))=>Some(canonical_catalogue(&PathBuf::from(p))?),None=>None,_=>return Err(config_error("config","resource-catalog"))};
    let resolver_catalogue=resolver.catalogue.as_ref().map(canonical_catalogue).transpose()?;
    if supplied_catalogue.as_ref().zip(resolver_catalogue.as_ref()).is_some_and(|(a,b)|a!=b){return Err(config_error("resource-catalogue-conflict","resource-catalog"));}
    let catalogue_path=supplied_catalogue.or(resolver_catalogue);
    let catalogue=catalogue_path.as_deref().map(crate::resources_app::Catalogue::load).transpose().map_err(resource_error)?;
    let catalogue_sha256=catalogue.as_ref().map(|c|c.sha256.clone());
    let mut resolved_resources=Vec::new();
    for (key,kind) in [("adapter-file","adapter"),("screen-reference","reference")]{
        let names:Vec<&str>=match effective.get(key){Some(Setting::List(v))=>v.iter().map(String::as_str).collect(),Some(Setting::Text(v))=>vec![v.as_str()],None=>vec![],_=>return Err(config_error("config",key))};
        for name in names{
            let (path,sha256,id,catalogue_sha256)=if let Some(id)=name.strip_prefix("id:"){
                let catalog=catalogue.as_ref().ok_or_else(||config_error("resource-id",key))?;
                let (path,sha)=catalog.resolve(id,kind,key).map_err(resource_error)?;(path,sha,Some(id.into()),Some(catalog.sha256.clone()))
            }else{let (path,sha)=crate::resources_app::verify(std::path::Path::new(name),kind,key,None,512<<20).map_err(resource_error)?;(path,sha,None,None)};
            resolved_resources.push(ResolvedResource{catalogue_id:id,kind:kind.into(),path,sha256,catalogue_sha256});
        }
    }
    for (name,kind) in resolver.explicit_paths{
        let (path,sha256)=crate::resources_app::verify(&name,&kind,"resources.explicit_paths",None,512<<20).map_err(resource_error)?;
        resolved_resources.push(ResolvedResource{catalogue_id:None,kind,path,sha256,catalogue_sha256:None});
    }
    let authority=Arc::new(ValidatedAuthority{config:config.clone(),effective:effective.clone(),provenance:provenance.clone()});
    let mut job=ValidatedJob{authority,config,effective,provenance,resolved_resources,plan:None,demux_plan:None,dedup_plan:None,catalogue_path,catalogue_sha256};
    if matches!(job.config,JobConfig::Process(_)) {
        let args=option_arguments(&job)?;
        job.plan=Some(Arc::new(crate::native_config::parse_api(&args,spec(&job.config)).map_err(|e|config_error("config",e))?));
        let p=job.plan.as_ref().unwrap();
        let mut protected=job.resolved_resources.iter().map(|r|r.path.clone()).collect::<Vec<_>>();protected.extend(job.catalogue_path.iter().cloned());
        let extra=p.record_manifest.iter().chain(p.match_report.iter()).chain(p.html_report.iter()).cloned().collect::<Vec<_>>();
        check_path_aliases(spec(&job.config),&protected,&extra).map_err(|e|config_error("config",e))?;
    }
    if let JobConfig::Demux(s)=&job.config{
        let mut p=crate::demux_app::compile_api(s).map_err(|e|config_error("config",e))?;
        if let Some(c)=&catalogue{p.catalogue=Some(crate::demux_app::catalogue_metadata(c));}
        let mut protected=job.resolved_resources.iter().map(|r|r.path.clone()).collect::<Vec<_>>();protected.extend(job.catalogue_path.iter().cloned());crate::demux_app::protect_resources(&p,&protected).map_err(|e|config_error("config",e))?;
        job.resolved_resources.push(ResolvedResource{catalogue_id:None,kind:"sample-table".into(),path:p.samples_identity.0.clone(),sha256:p.samples_identity.1.clone(),catalogue_sha256:None});
        job.plan=Some(Arc::new(p.common.clone()));job.demux_plan=Some(Arc::new(p));
    }
    if let JobConfig::Dedup(s)=&job.config {
        let mut p=crate::dedup_app::compile_api(s).map_err(|e|config_error("config",e))?;
        if let Some(c)=&job.catalogue_path {p.values.insert("resource-catalog".into(),c.to_string_lossy().into_owned());}if let Some(c)=&catalogue{p.catalogue=Some(crate::dedup_app::CatalogueMetadata::from_catalogue(c));}
        let mut protected=job.resolved_resources.iter().map(|r|r.path.clone()).collect::<Vec<_>>();protected.extend(job.catalogue_path.iter().cloned());
        let extra=p.common.html_report.iter().cloned().collect::<Vec<_>>();
        check_path_aliases(&p.spec,&protected,&extra).map_err(|e|config_error("config",e))?;
        job.plan=Some(Arc::new(p.common.clone()));job.dedup_plan=Some(Arc::new(p));
    }
    Ok(job)
}
fn validate_process_declarations(s:&JobSpec,effective:&BTreeMap<String,Setting>)->Result<(),ConfigError>{
    let mut lanes=std::collections::BTreeSet::new();let mut inputs=std::collections::BTreeSet::new();
    for lane in &s.lanes {
        if !lanes.insert(&lane.id){return Err(config_error("config","duplicate lane ID"));}
        let roles:&[&str]=if s.layout==Layout::SplitPe{&["R1","R2"]}else{&["R1"]};
        if lane.inputs.len()!=roles.len() || lane.inputs.iter().zip(roles).any(|(d,r)|!d.channel.eq_ignore_ascii_case(r)){return Err(config_error("config","Process lane layout requires ordered R1/R2 roles with exact arity"));}
        for d in &lane.inputs{if let Location::Handle(h)=&d.location{if h.is_empty()||!inputs.insert(h){return Err(config_error("config","duplicate or empty input binding"));}}}
    }
    let qc_only=matches!(effective.get("qc-only"),Some(Setting::Bool(true)));let mut channels=std::collections::BTreeSet::new();let mut handles=std::collections::BTreeSet::new();
    for d in &s.outputs {
        let key=if let Some(n)=main_channel(&d.channel){format!("main{n}")}else if ["reject1","reject2","transformed1","transformed2","singleton1","singleton2"].contains(&d.channel.as_str()){d.channel.clone()}else{return Err(config_error("config","unknown Process output channel"));};
        if !channels.insert(key){return Err(config_error("config","duplicate output channel"));}
        if let Location::Handle(h)=&d.location{if h.is_empty()||!handles.insert(h){return Err(config_error("config","duplicate or empty output binding"));}}
        if s.layout==Layout::Se&&(main_channel(&d.channel)==Some(1)||["reject2","transformed2","singleton1","singleton2"].contains(&d.channel.as_str())){return Err(config_error("config","R2/singleton output requires paired input"));}
    }
    if qc_only&&!s.outputs.is_empty(){return Err(config_error("config","qc-only forbids writer declarations"));}
    if !qc_only&&!s.outputs.iter().any(|d|main_channel(&d.channel).is_some()){return Err(config_error("config","Process requires a main output declaration"));}
    Ok(())
}
fn admission_error(report:&mut NativeReport,category:&str,message:&str){
    let mut value:serde_json::Value=serde_json::from_str(&report.json).unwrap();
    value["api"]["execution_admitted"]=serde_json::json!(false);
    value["error"]=serde_json::json!({"stage":"admission","category":category,"message":message});report.json=value.to_string();
}
fn empty_report(s:&JobSpec,status:&'static str)->NativeReport{
    let qc_only=matches!(s.supplied.get("qc-only").or_else(||s.configured.get("qc-only")),Some(Setting::Bool(true)));
    let q=crate::native_qc::Qc::default();let input=crate::native_qc::population(&[&q],"input",0,"partial",false);
    let accepted=if qc_only{serde_json::Value::Null}else{crate::native_qc::population(&[&q],"accepted",0,"partial",false)};
    let written=if qc_only{serde_json::Value::Null}else{crate::native_qc::written_population(&[&q],0,"partial",false)};
    let sample=s.supplied.get("sample-id").or_else(||s.configured.get("sample-id")).and_then(|v|if let Setting::Text(s)=v{Some(s.as_str())}else{None}).unwrap_or_else(||s.lanes.first().map(|l|l.id.as_str()).unwrap_or("input"));
    let json=serde_json::json!({"schema":"soapnuke-v3.report/2","status":status,"run":{"job":"process","sample_id":sample,"layout":if s.layout==Layout::Se{"SE"}else{"PE"}},"populations":{"input":input,"accepted":accepted,"written":written},"api":{"execution_admitted":false},"processing":{"api":{"input_units":0,"accepted_units":if qc_only{None}else{Some(0u64)},"written_units":if qc_only{None}else{Some(0u64)}}}});
    NativeReport{status,input_units:0,accepted_units:if qc_only{None}else{Some(0)},written_units:if qc_only{None}else{Some(0)},json:json.to_string()}
}
fn reject_unstarted(stage:&'static str,mut io:JobIo,report:NativeReport,mut observer:Option<&mut dyn ProgressObserver>)->Result<RunResult,RunError>{
    for writer in io.outputs.values_mut(){let _=writer.flush();let _=writer.close();}
    drop(io);
    if let Some(o)=observer.as_deref_mut(){o.observe(ProgressEvent{stage:"failed",input_units:report.input_units,accepted_units:report.accepted_units,written_units:report.written_units,channel_records:BTreeMap::new()});}
    Err(RunError{stage,partial:report,artifacts:vec![]})
}
pub fn run(job:ValidatedJob,io:JobIo,mut observer:Option<&mut dyn ProgressObserver>,cancel:&CancellationToken)->Result<RunResult,RunError>{
    let settings=if let Some(p)=&job.dedup_plan{&p.spec}else{job.demux_plan.as_ref().map_or_else(||spec(&job.authority.config),|p|&p.spec)};
    let qc_only=job.plan.as_ref().is_some_and(|p|p.process.qc_only);
    let mut report=empty_report(settings,"failed");
    if job.demux_plan.is_some(){let mut v:serde_json::Value=serde_json::from_str(&report.json).unwrap();v["run"]["job"]=serde_json::json!("demux");v["demux"]=v.as_object_mut().unwrap().remove("processing").unwrap();report.json=v.to_string();}
    if job.dedup_plan.is_some(){let mut v:serde_json::Value=serde_json::from_str(&report.json).unwrap();v["run"]["job"]=serde_json::json!("dedup");v["dedup"]=v.as_object_mut().unwrap().remove("processing").unwrap();v["populations"]["accepted"]=serde_json::Value::Null;v["dedup"]["api"]["accepted_units"]=serde_json::Value::Null;v["dedup"]["dedup"]=serde_json::json!({"representative_units":null,"duplicate_units":null,"grouping_complete":false});report.accepted_units=None;report.json=v.to_string();}
    if let Some(o)=observer.as_deref_mut(){o.observe(ProgressEvent{stage:"starting",input_units:0,accepted_units:report.accepted_units,written_units:report.written_units,channel_records:BTreeMap::new()});}
    let missing=settings.lanes.iter().flat_map(|l|&l.inputs).any(|d|matches!(&d.location,Location::Handle(h)if !io.inputs.contains_key(h))) || settings.outputs.iter().any(|d|matches!(&d.location,Location::Handle(h)if !io.outputs.contains_key(h)));
    let extra=io.inputs.keys().any(|k|!settings.lanes.iter().flat_map(|l|&l.inputs).any(|d|match &d.location{Location::Handle(h)=>h==k,_=>&d.channel==k}))||io.outputs.keys().any(|k|!settings.outputs.iter().any(|d|match &d.location{Location::Handle(h)=>h==k,_=>&d.channel==k}));
    if missing||extra {admission_error(&mut report,"binding","missing or undeclared owned IO binding");return reject_unstarted("binding",io,report,observer);}
    if cancel.is_cancelled(){admission_error(&mut report,"cancelled","API cancelled before execution admission");return reject_unstarted("cancelled",io,report,observer);}
    if !job.matches_authority(){admission_error(&mut report,"stale-validation","execution-relevant inspection snapshots changed; revalidate before run");return reject_unstarted("stale-validation",io,report,observer);}
    if let Some(p)=&job.dedup_plan{if let Err(message)=crate::dedup_app::check_spill_bindings(p,&io){admission_error(&mut report,"config",&message);return reject_unstarted("binding",io,report,observer);}}
    if job.plan.as_ref().is_some_and(|p|p.process.split_units>0) && settings.outputs.iter().filter(|d|main_channel(&d.channel).is_some()).any(|d|io.outputs.contains_key(match &d.location{Location::Handle(h)=>h,_=>&d.channel})) {
        admission_error(&mut report,"unsupported-split-output","split file families cannot use a fixed owned writer override");return reject_unstarted("unsupported-split-output",io,report,observer);
    }
    let threads=job.plan.as_ref().map_or(1,|p|p.threads);
    let additional=if let Some(p)=&job.dedup_plan{match crate::dedup_runtime::Runtime::statistics_bound(p.channels.len()){Ok(n)=>n,Err(e)=>{admission_error(&mut report,"memory",&e.to_string());return reject_unstarted("memory",io,report,observer);}}}else if let Some(p)=&job.demux_plan{match crate::demux_runtime::Runtime::statistics_bound(p.channels.len()){Ok(n)=>n,Err(e)=>{admission_error(&mut report,"memory",&e.to_string());return reject_unstarted("memory",io,report,observer);}}}else{job.plan.as_ref().map_or(0,|p|p.process.additional_statistics(p.process.capture.enabled()||p.has_auxiliary()||p.match_report.is_some()))};
    let stop=Arc::new(AtomicBool::new(false));
    let execution=match crate::native_execution::Execution::with_statistics_budget(threads,if settings.layout==Layout::Se{1}else{2},stop.clone(),job.plan.as_ref().and_then(|p|p.memory_total_bytes),additional){Ok(e)=>e,Err(e)=>{admission_error(&mut report,"memory",&e.to_string());return reject_unstarted("memory",io,report,observer)}};
    let bound=match metadata_bound(settings).and_then(|n|n.checked_mul(if job.demux_plan.is_some()||job.dedup_plan.is_some(){8}else{4})).and_then(|n|authority_bound(&job.authority).and_then(|a|n.checked_add(a))).and_then(|n|n.checked_add(job.dedup_plan.as_ref().and_then(|p|p.catalogue.as_ref()).map_or(Some(0),crate::dedup_app::CatalogueMetadata::capacity)?)).and_then(|n|n.checked_add((io.inputs.len()+io.outputs.len()+settings.outputs.len()+job.resolved_resources.len())*2048)).and_then(|n|n.checked_add(job.catalogue_path.as_ref().map_or(0,PathBuf::capacity))).and_then(|n|n.checked_add(job.catalogue_sha256.as_ref().map_or(0,String::capacity))){Some(n)=>n,None=>{admission_error(&mut report,"memory","framework metadata reservation size overflow");return reject_unstarted("memory",io,report,observer)}};
    let reservation=match execution.reserve_persistent_feature(bound){Ok(t)=>t,Err(e)=>{admission_error(&mut report,"memory",&e.to_string());return reject_unstarted("memory",io,report,observer)}};
    let owned=Arc::new(crate::native_io::OwnedIo::new(settings.clone(),io,cancel.clone(),execution,stop,reservation));
    let mut stage=None;
    if stage.is_none() {
        if matches!(job.effective.get("decoder"),Some(Setting::Text(d))if d=="rapidgzip") {
            if let Some(capability)=owned.unsupported_rapid(){
                stage=Some(if capability==crate::native_io::InputCapability::OwnedSeekable{"unsupported-owned-decoder"}else{"unsupported-stream-decoder"});
                admission_error(&mut report,stage.unwrap(),"rapidgzip requires a regular path binding; opaque owned interface unsupported");
                let mut value:serde_json::Value=serde_json::from_str(&report.json).unwrap();value["error"]["message"]=serde_json::json!("rapidgzip requires a regular path binding; this opaque interface is unsupported; no fallback from explicit request");value["api"]["decoder_capabilities"]=owned.decoder_bindings(threads);report.json=value.to_string();
            }
        }
    }
    if stage.is_none(){
        for resource in &job.resolved_resources {if crate::resources_app::hash(&resource.path,"resources").ok().as_ref()!=Some(&resource.sha256){stage=Some("resource-changed");break;}}
        if let Some((path,expected))=job.catalogue_path.as_ref().zip(job.catalogue_sha256.as_ref()){if crate::resources_app::hash(path,"resources.catalogue").ok().as_ref()!=Some(expected){stage=Some("resource-changed");}}
    }
    if stage==Some("resource-changed"){admission_error(&mut report,"resource-changed","validated resource identity changed before use");}
    if stage.is_none(){
        if let Some(plan)=&job.plan{
            let mut initial:serde_json::Value=serde_json::from_str(&report.json).unwrap();initial["api"]["execution_admitted"]=serde_json::json!(true);report.json=initial.to_string();
            if let Some(p)=&job.dedup_plan{let(code,value,hint)=crate::dedup_app::run(p,Some(owned.clone()));report=value;if code!=0{stage=Some(if cancel.is_cancelled(){"cancelled"}else{hint.unwrap_or(if code==3{"input"}else if code==4{"output"}else{"runtime"})});}}
            else if let Some(p)=&job.demux_plan{let(code,value,hint)=crate::demux_app::run_api(p,owned.clone());report=value;if code!=0{stage=Some(if cancel.is_cancelled(){"cancelled"}else{hint.unwrap_or(if code==3{"input"}else if code==4{"output"}else{"runtime"})});}}
            else{let(code,value)=crate::native_app::run_api(plan,owned.clone());
                if let Some(mut value)=value{let status=if code==0{"complete"}else{"failed"};value["status"]=serde_json::json!(status);report=NativeReport{status,input_units:value["populations"]["input"]["source_units"].as_u64().unwrap_or(0),accepted_units:if qc_only{None}else{value["populations"]["accepted"]["source_units"].as_u64()},written_units:if qc_only{None}else{value["populations"]["written"]["source_units"].as_u64()},json:value.to_string()};if code!=0{stage=Some(if cancel.is_cancelled(){"cancelled"}else if value["error"]["stage"]=="report-publication"{"report-publication"}else if code==3{"input"}else if code==4{"output"}else{"runtime"});}}else{stage=Some(if code==4{"output"}else if code==3{"input"}else{"runtime"});}}
        }else{stage=Some("not-implemented");}
    }
    if let Err(_)=owned.finish(){if stage.is_none(){stage=Some("output");report.status="failed";}}
    let artifacts=settings.outputs.iter().map(|d|Artifact{channel:d.channel.clone(),closed:true,format:None,consumer_contract_sha256:None}).collect::<Vec<_>>();
    drop(job);
    drop(owned);
    if let Some(o)=observer.as_deref_mut(){o.observe(ProgressEvent{stage:if stage.is_some(){"failed"}else{"complete"},input_units:report.input_units,accepted_units:report.accepted_units,written_units:report.written_units,channel_records:BTreeMap::new()});}
    if let Some(stage)=stage{Err(RunError{stage,partial:report,artifacts})}else{Ok(RunResult{report,artifacts})}
}
pub fn render(report:&NativeReport,format:ReportFormat,mut sink:Box<dyn ReportWriter>,contract:Option<ConsumerContract>)->Result<Artifact,ReportError>{
    let working=crate::report_app::Working::new();let value=working.as_ref().map_err(|e|e.clone()).and_then(|w|crate::report_app::parse(report.json.as_bytes(),w));let mut stage=None;let mut contract_sha=None;
    match value{Err(_)=>stage=Some("source-report"),Ok(value)=>{if value.text(&["status"]).ok().flatten().as_deref()!=Some(report.status){stage=Some("source-report");}else if matches!(format,ReportFormat::Html|ReportFormat::Tsv){if crate::report_app::write(&mut sink,&value,&format).is_err(){stage=Some("write");}}else{match contract.as_ref().ok_or("consumer-contract required").and_then(|c|crate::consumer_export::Named::new(&value,&format,c,None,&[],"rust-api/1").map_err(|_|"invalid consumer-contract")){Err(_)=>stage=Some("consumer-contract"),Ok(named)=>{contract_sha=Some(named.contract_sha.clone());if match format{ReportFormat::Multiqc=>named.write_multiqc(&mut sink,&value),ReportFormat::Mgnify=>named.write_csv(&mut sink),_=>unreachable!()}.is_err(){stage=Some("write");}}}}}}
    if stage.is_none()&&sink.flush().is_err(){stage=Some("flush");}if sink.close().is_err()&&stage.is_none(){stage=Some("close");}
    if let Some(stage)=stage{Err(ReportError{stage})}else{Ok(Artifact{channel:"report".into(),closed:true,format:Some(match format{ReportFormat::Html=>"html",ReportFormat::Tsv=>"tsv",ReportFormat::Multiqc=>"multiqc-json",ReportFormat::Mgnify=>"rawr-csv"}),consumer_contract_sha256:contract_sha})}
}

pub(crate) fn input_codec(d:&InputDecl)->String{match d.format{Format::Gzip=>"gzip",Format::Fastq=>"plain",Format::Auto=>match &d.location{Location::Path(p)if p.extension().is_some_and(|e|e=="gz")=>"gzip",_=>"plain"}}.into()}
pub(crate) fn output_codec(d:&OutputDecl)->String{match d.format{Format::Gzip=>"gzip",Format::Fastq=>"plain",Format::Auto=>match &d.location{Location::Path(p)if p.extension().is_some_and(|e|e=="gz")=>"gzip",_=>"plain"}}.into()}
pub(crate) fn main_channel(name:&str)->Option<usize>{match name.to_ascii_lowercase().as_str(){"r1"|"out-interleaved"=>Some(0),"r2"=>Some(1),"merged"|"merged-out"=>Some(2),_=>None}}
pub(crate) fn path_identity(p:&std::path::Path)->Result<PathBuf,String>{if let Ok(p)=std::fs::canonicalize(p){return Ok(p);}let parent=p.parent().filter(|p|!p.as_os_str().is_empty()).unwrap_or(std::path::Path::new("."));Ok(std::fs::canonicalize(parent).map_err(|e|format!("path parent: {e}"))?.join(p.file_name().ok_or("path needs filename")?))}
pub(crate) fn check_path_aliases(s:&JobSpec,protected:&[PathBuf],additional_outputs:&[PathBuf])->Result<(),String>{
    let mut occupied=std::collections::BTreeSet::new();
    for input in s.lanes.iter().flat_map(|l|&l.inputs){if let Location::Path(p)=&input.location{occupied.insert(path_identity(p)?);}}
    for p in protected{occupied.insert(path_identity(p)?);}
    let mut outputs=Vec::new();for output in &s.outputs{if let Location::Path(p)=&output.location{outputs.push(p);}}
    if let Some(p)=&s.json_report{outputs.push(p);}
    outputs.extend(additional_outputs);
    for p in outputs{let id=path_identity(p)?;if !occupied.insert(id)||p.symlink_metadata().is_ok(){return Err("input/output/report alias or existing output".into());}}
    Ok(())
}
fn option_arguments(job:&ValidatedJob)->Result<Vec<String>,ConfigError>{
    let mut args=Vec::new();
    if let Some(path)=job.catalogue_path.as_ref() {args.extend(["--resource-catalog".into(),path.to_string_lossy().into_owned()]);}
    for (key,value)in &job.effective{
        if key=="config"||key=="resource-catalog"{continue;}
        let json=setting_json(value);
        let json=if ["adapter-file","screen-reference"].contains(&key.as_str())&&json.is_string(){serde_json::json!([json])}else{json};
        for v in crate::native_config::config_arguments(key,&json).map_err(|_|config_error("config",key))?{args.push(format!("--{key}"));args.push(v);}
    }
    Ok(args)
}

fn setting_capacity(v:&Setting)->usize{match v{Setting::Text(v)=>v.capacity(),Setting::List(v)=>v.capacity()*std::mem::size_of::<String>()+v.iter().map(String::capacity).sum::<usize>(),_=>0}}
fn authority_bound(a:&ValidatedAuthority)->Option<usize>{
    // A full private JobSpec snapshot plus two BTree maps; static provenance values allocate no strings.
    let mut n=metadata_bound(spec(&a.config))?.checked_add(std::mem::size_of::<ValidatedAuthority>())?;
    for(k,v)in &a.effective {n=n.checked_add(1024)?.checked_add(k.capacity())?.checked_add(setting_capacity(v))?;}
    for k in a.provenance.keys(){n=n.checked_add(1024)?.checked_add(k.capacity())?;}
    Some(n)
}
pub(crate) fn metadata_bound(s:&JobSpec)->Option<usize>{
    let mut n=std::mem::size_of::<JobSpec>()+65536;
    for map in [&s.configured,&s.supplied]{for(k,v)in map{n=n.checked_add(1024)?.checked_add(k.capacity())?.checked_add(setting_capacity(v))?;}}
    n=n.checked_add(s.lanes.capacity().checked_mul(std::mem::size_of::<LaneDecl>())?)?;
    for lane in &s.lanes{n=n.checked_add(lane.id.capacity())?.checked_add(lane.inputs.capacity().checked_mul(std::mem::size_of::<InputDecl>())?)?;for d in &lane.inputs{n=n.checked_add(d.channel.capacity())?.checked_add(match &d.location{Location::Path(p)=>p.capacity(),Location::Handle(h)=>h.capacity()})?;}}
    n=n.checked_add(s.outputs.capacity().checked_mul(std::mem::size_of::<OutputDecl>())?)?;
    for d in &s.outputs{n=n.checked_add(d.channel.capacity())?.checked_add(match &d.location{Location::Path(p)=>p.capacity(),Location::Handle(h)=>h.capacity()})?;}
    n.checked_add(s.json_report.as_ref().map_or(0,PathBuf::capacity))
}

#[cfg(test)]
mod numeric_boundary_tests {
    use super::*;
    fn record(phred:i32,scores:&[u8])->crate::record::FastqRecord{
        let mut r=crate::record::FastqRecord::new(2);r.seq_id="@literal full comment".into();r.source_index=7;r.sequence="A".repeat(scores.len());r.qual_seq=String::from_utf8(scores.iter().map(|q|q+phred as u8).collect()).unwrap();r.input_phred=Some(phred);r
    }
    #[test]
    fn declared_phred_endpoints_normalize_in_place_and_replay_once(){
        for (offset,values)in [(33,vec![0,31,62,63,93]),(64,vec![0,31,62])]{
            let mut r=record(offset,&values);let cap=r.qual_seq.capacity();
            crate::native_kernel::validate_normalize(&mut r,33).unwrap();
            assert_eq!(r.qual_seq.bytes().map(|b|b-33).collect::<Vec<_>>(),values);assert_eq!(r.qual_seq.capacity(),cap);
            let first=r.qual_seq.clone();crate::native_kernel::validate_normalize(&mut r,33).unwrap();assert_eq!(r.qual_seq,first);assert_eq!(r.seq_id,"@literal full comment");
        }
    }
    #[test]
    fn invalid_source_retains_original_offset_mate_and_ordinal(){
        for encoded in [63,127]{let mut r=record(64,&[0]);r.qual_seq=String::from_utf8(vec![encoded]).unwrap();let original=r.qual_seq.clone();let e=crate::native_kernel::validate_normalize(&mut r,33).unwrap_err();assert!(e.contains("record 8 mate 2")&&e.contains("Phred64"));assert_eq!(r.qual_seq,original);}
    }
    fn spec(outputs:Vec<OutputDecl>)->JobSpec{JobSpec{configured:BTreeMap::new(),supplied:BTreeMap::new(),layout:Layout::Se,lanes:vec![LaneDecl{id:"literal".into(),inputs:vec![InputDecl{channel:"R1".into(),location:Location::Handle("source".into()),format:Format::Fastq,phred:33,seekable:false}]}],outputs,json_report:None}}
    #[test]
    fn shared_epoch_metadata_reservation_releases_and_target_encoding_is_exact(){
        let s=spec(vec![OutputDecl{channel:"R1".into(),location:Location::Handle("sink".into()),format:Format::Fastq,phred:64}]);
        let bound=metadata_bound(&s).unwrap()*4;let stop=Arc::new(AtomicBool::new(false));let e=crate::native_execution::Execution::new(1,1,stop.clone()).unwrap();let ticket=e.reserve_persistent_feature(bound).unwrap();
        let io=Arc::new(crate::native_io::OwnedIo::new(s,JobIo{inputs:BTreeMap::new(),outputs:BTreeMap::new()},CancellationToken::default(),e.clone(),stop,ticket));
        let mut gp=crate::params::GlobalParams::new();gp.native_io=Some(io.clone());gp.quality_phred=33;
        let r=record(33,&[0,62]);let mut bytes=Vec::new();crate::output::serialize_native_record(&r,false,&gp,"R1",&mut bytes).unwrap();assert_eq!(bytes,b"@literal full comment\nAA\n+\n@~\n");
        let r=record(33,&[63]);let mut bytes=Vec::new();assert!(crate::output::serialize_native_record(&r,false,&gp,"R1",&mut bytes).is_err());assert!(bytes.is_empty());
        assert!(e.facts()["memory_budget"]["current_feature_reserved_bytes"].as_u64().unwrap_or(0)>0);
        drop(gp);drop(io);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
    }
}

#[cfg(test)]
mod output_prefix_tests {
    use super::*;
    #[test]
    fn fallible_api_output_does_not_precount_failed_unit_as_accepted(){
        let spec=JobSpec{configured:BTreeMap::new(),supplied:BTreeMap::new(),layout:Layout::Se,lanes:vec![LaneDecl{id:"literal".into(),inputs:vec![InputDecl{channel:"R1".into(),location:Location::Handle("in".into()),format:Format::Fastq,phred:33,seekable:false}]}],outputs:vec![OutputDecl{channel:"R1".into(),location:Location::Handle("out".into()),format:Format::Fastq,phred:64}],json_report:None};
        let stop=Arc::new(AtomicBool::new(false));let execution=crate::native_execution::Execution::new(1,1,stop.clone()).unwrap();let ticket=execution.reserve_persistent_feature(metadata_bound(&spec).unwrap()*4).unwrap();
        let mut gp=crate::params::GlobalParams::new();gp.native_m1=true;gp.quality_phred=33;gp.output_quality_phred=33;gp.native_io=Some(Arc::new(crate::native_io::OwnedIo::new(spec,JobIo{inputs:BTreeMap::new(),outputs:BTreeMap::new()},CancellationToken::default(),execution,stop,ticket)));
        let mut r=crate::record::FastqRecord::new(1);r.source_index=0;r.seq_id="@q63".into();r.sequence="A".into();r.qual_seq="`".into();r.input_phred=Some(33);
        let mut stats=crate::stats::PeLocalStats::new(&gp);
        let error=match crate::native_kernel::apply_recorded(vec![r],None,&mut stats,&gp,None){Err(e)=>e,Ok(_)=>panic!("unrepresentable emission must fail")};assert!(error.contains("quality.unrepresentable"));
        assert_eq!(stats.raw1.native_qc.as_ref().unwrap().reads,1);assert_eq!(stats.clean1.native_qc.as_ref().unwrap().reads,0,"failing emitted unit must not enter accepted QC");
    }
}

#[cfg(test)]
mod process_declaration_tests {
    use super::*;
    fn base()->JobSpec{JobSpec{configured:BTreeMap::new(),supplied:BTreeMap::new(),layout:Layout::Se,lanes:vec![LaneDecl{id:"L1".into(),inputs:vec![InputDecl{channel:"R1".into(),location:Location::Handle("in".into()),format:Format::Fastq,phred:33,seekable:false}]}],outputs:vec![OutputDecl{channel:"R1".into(),location:Location::Handle("out".into()),format:Format::Fastq,phred:33}],json_report:None}}
    #[test]
    fn process_roles_are_strict_but_do_not_prohibit_demux_index_declarations(){
        let s=base();assert!(validate(JobConfig::Process(s.clone()),ResourceResolver::default()).is_ok());
        let mut indexed=s.clone();let mut i=indexed.lanes[0].inputs[0].clone();i.channel="I1".into();i.location=Location::Handle("index".into());indexed.lanes[0].inputs.push(i);
        assert!(validate(JobConfig::Process(indexed.clone()),ResourceResolver::default()).is_err());
        let root=crate::test_support::workspace("indexed-demux");
        let samples=root.join("samples.tsv");std::fs::write(&samples,b"sample\tbarcode1\tbarcode2\nS1\tACGT\t\n").unwrap();
        indexed.supplied.insert("barcode-source".into(),Setting::Text("index".into()));
        indexed.supplied.insert("samples".into(),Setting::Text(samples.to_string_lossy().into_owned()));
        indexed.outputs[0].channel="sample/S1.R1".into();
        let demux=validate(JobConfig::Demux(indexed),ResourceResolver::default());assert!(demux.is_ok(),"complete indexed Demux declarations must validate: {demux:?}");
        let mut duplicate=s.clone();duplicate.lanes.push(duplicate.lanes[0].clone());assert!(validate(JobConfig::Process(duplicate),ResourceResolver::default()).unwrap_err().field.contains("duplicate lane"));
        let mut wrong=s.clone();wrong.lanes[0].inputs[0].channel="R2".into();assert!(validate(JobConfig::Process(wrong),ResourceResolver::default()).is_err());
        let mut missing=s.clone();missing.layout=Layout::SplitPe;assert!(validate(JobConfig::Process(missing),ResourceResolver::default()).is_err());
        let mut output=s.clone();output.outputs.push(output.outputs[0].clone());assert!(validate(JobConfig::Process(output),ResourceResolver::default()).is_err());
        let mut qc=s;qc.supplied.insert("qc-only".into(),Setting::Bool(true));assert!(validate(JobConfig::Process(qc),ResourceResolver::default()).is_err());
    }
    #[test]
    fn split_capability_and_cross_chunk_aliases_are_checked_without_input_io(){
        let mut handle=base();handle.supplied.insert("split-units".into(),Setting::Int(1));assert!(validate(JobConfig::Process(handle),ResourceResolver::default()).unwrap_err().field.contains("Path templates"));
        let root=std::env::temp_dir().join(format!("api-split-{}",std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));std::fs::create_dir(&root).unwrap();
        let mut s=base();s.supplied.insert("split-units".into(),Setting::Int(1));s.outputs[0].location=Location::Path(root.join("out-{chunk}.fq"));
        assert!(validate(JobConfig::Process(s.clone()),ResourceResolver::default()).is_ok());assert_eq!(std::fs::read_dir(&root).unwrap().count(),0,"validate creates no output and consumes no handle");
        s.layout=Layout::SplitPe;let mut second=s.lanes[0].inputs[0].clone();second.channel="R2".into();second.location=Location::Handle("mate2".into());s.lanes[0].inputs.push(second);
        s.outputs[0].location=Location::Path(root.join("{chunk}0"));let mut other=s.outputs[0].clone();other.channel="R2".into();other.location=Location::Path(root.join("0{chunk}"));s.outputs.push(other);
        assert!(validate(JobConfig::Process(s),ResourceResolver::default()).unwrap_err().field.contains("alias across chunk"));assert_eq!(std::fs::read_dir(&root).unwrap().count(),0);
    }
    #[test]
    fn resource_and_required_side_artifact_aliases_are_preflight_errors(){
        let root=std::env::temp_dir().join(format!("api-alias-{}",std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));std::fs::create_dir(&root).unwrap();let path=root.join("reference.fa");std::fs::write(&path,b">literal\nACGT\n").unwrap();
        let mut s=base();s.outputs[0].location=Location::Path(path.clone());assert!(validate(JobConfig::Process(s),ResourceResolver{catalogue:None,explicit_paths:vec![(path.clone(),"reference".into())]}).unwrap_err().field.contains("alias"));assert_eq!(std::fs::read(&path).unwrap(),b">literal\nACGT\n");
        let mut s=base();s.outputs[0].location=Location::Path(root.join("same.html"));s.supplied.insert("html-report".into(),Setting::Text(root.join("same.html").to_string_lossy().into_owned()));assert!(validate(JobConfig::Process(s),ResourceResolver::default()).unwrap_err().field.contains("alias"));assert_eq!(std::fs::read_dir(&root).unwrap().count(),1);
    }
}

#[cfg(test)]
mod validated_snapshot_tests {
    use super::*;
    fn job()->ValidatedJob{let s=JobSpec{configured:BTreeMap::new(),supplied:BTreeMap::new(),layout:Layout::Se,lanes:vec![LaneDecl{id:"L".into(),inputs:vec![InputDecl{channel:"R1".into(),location:Location::Handle("in".into()),format:Format::Fastq,phred:33,seekable:false}]}],outputs:vec![OutputDecl{channel:"R1".into(),location:Location::Handle("out".into()),format:Format::Fastq,phred:33}],json_report:None};validate(JobConfig::Process(s),ResourceResolver::default()).unwrap()}
    #[test]
    fn readonly_accessors_borrow_private_authority_after_snapshot_mutation(){
        let mut job=job();
        assert!(std::ptr::eq(job.configuration(),&job.authority.config));
        assert!(std::ptr::eq(job.effective_settings(),&job.authority.effective));
        assert!(std::ptr::eq(job.provenance(),&job.authority.provenance));
        assert!(job.setting("decoder").is_none());
        job.effective.insert("decoder".into(),Setting::Text("rapidgzip".into()));
        job.provenance.insert("decoder".into(),"fabricated");
        assert!(job.setting("decoder").is_none());assert!(!job.provenance().contains_key("decoder"));
        assert!(!job.matches_authority());
    }
    #[test]
    fn changed_snapshots_reject_before_read_and_cancel_keeps_precedence(){
        use std::io::{Cursor,Read,Write};
        use std::sync::Mutex;
        struct Reader(Cursor<Vec<u8>>,Arc<Mutex<[usize;5]>>);
        impl Read for Reader{fn read(&mut self,b:&mut[u8])->std::io::Result<usize>{self.1.lock().unwrap()[0]+=1;self.0.read(b)}}
        impl Drop for Reader{fn drop(&mut self){self.1.lock().unwrap()[1]+=1;}}
        struct Writer(Arc<Mutex<[usize;5]>>);
        impl Write for Writer{fn write(&mut self,b:&[u8])->std::io::Result<usize>{self.0.lock().unwrap()[2]+=1;Ok(b.len())}fn flush(&mut self)->std::io::Result<()>{Ok(())}}
        impl ReportWriter for Writer{fn close(&mut self)->std::io::Result<()>{self.0.lock().unwrap()[3]+=1;Ok(())}}
        impl Drop for Writer{fn drop(&mut self){self.0.lock().unwrap()[4]+=1;}}
        let original=job();
        for mode in 0..5 {
            let mut changed=original.clone();
            match mode {
                0|4=>{changed.effective.insert("threads".into(),Setting::Int(2));},
                1=>changed.config=JobConfig::Demux(spec(&changed.config).clone()),
                2=>if let JobConfig::Process(s)=&mut changed.config{s.lanes[0].inputs[0].phred=64;},
                _=>{changed.provenance.insert("threads".into(),"fabricated");},
            }
            let state=Arc::new(Mutex::new([0;5]));let cancel=CancellationToken::default();if mode==4{cancel.cancel();}
            let io=JobIo{inputs:BTreeMap::from([("in".into(),InputHandle::Stream(Box::new(Reader(Cursor::new(b"@a\nAC\n+\nII\n".to_vec()),state.clone()))))]),outputs:BTreeMap::from([("out".into(),Box::new(Writer(state.clone()))as Box<dyn ReportWriter>)])};
            let error=run(changed,io,None,&cancel).unwrap_err();
            assert_eq!(error.stage,if mode==4{"cancelled"}else{"stale-validation"});
            assert_eq!((error.partial.status,error.partial.input_units,error.partial.accepted_units,error.partial.written_units),("failed",0,Some(0),Some(0)));
            let report:serde_json::Value=serde_json::from_str(&error.partial.json).unwrap();
            assert_eq!(report["api"]["execution_admitted"],false);
            for population in ["input","accepted","written"]{assert_eq!(report["populations"][population]["read_records"],0);assert_eq!(report["populations"][population]["bases"],0);assert!(report["populations"][population]["quality_histogram"].as_array().unwrap().iter().all(|v|v==0));}
            assert!(error.artifacts.is_empty());assert_eq!(*state.lock().unwrap(),[0,1,0,1,1]);
        }
    }
    #[test]
    fn public_inspection_mutations_never_match_private_authority(){
        let original=job();assert!(original.matches_authority());
        let mut kind=original.clone();kind.config=JobConfig::Demux(spec(&kind.config).clone());assert!(!kind.matches_authority());
        let mut declarations=original.clone();if let JobConfig::Process(s)=&mut declarations.config{s.lanes[0].inputs[0].phred=64;}assert!(!declarations.matches_authority());
        let mut settings=original.clone();settings.effective.insert("decoder".into(),Setting::Text("rapidgzip".into()));assert!(!settings.matches_authority());
        let mut provenance=original.clone();provenance.provenance.insert("threads".into(),"fabricated");assert!(!provenance.matches_authority());
        let mut configured=original.clone();if let JobConfig::Process(s)=&mut configured.config{s.configured.insert("min-length".into(),Setting::Int(100));}assert!(!configured.matches_authority());
        assert!(authority_bound(&original.authority).unwrap()>=metadata_bound(spec(&original.config)).unwrap());
    }
}

#[cfg(test)]
mod review_repair_tests {
    use super::*;
    fn fixture()->PathBuf {
        let root=std::env::temp_dir().join(format!("catalog-authority-{}",std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));std::fs::create_dir(&root).unwrap();
        for(name,seq,sha)in [("A","ACG","70c559f09377ae756afdba8cfbc828d6a3dea56607e9b86566eae37cb58bb856"),("B","TTT","53f0d86dd2229017ca21253090e4c5b65fb5a19a18724cb6aac68a8aaf843c58")] {
            std::fs::write(root.join(format!("{name}.fa")),format!(">{name}\n{seq}\n")).unwrap();
            std::fs::write(root.join(format!("{name}.json")),serde_json::json!({"schema":"soapnuke-v3.resources/1","resources":[{"id":"same","kind":"reference","path":format!("{name}.fa"),"sha256":sha}]}).to_string()).unwrap();
        }root
    }
    fn spec(root:&std::path::Path)->JobSpec {
        JobSpec{configured:BTreeMap::new(),supplied:BTreeMap::from([("screen-reference".into(),Setting::List(vec!["id:same".into()]))]),layout:Layout::Se,lanes:vec![LaneDecl{id:"L".into(),inputs:vec![InputDecl{channel:"R1".into(),location:Location::Handle("input".into()),format:Format::Fastq,phred:33,seekable:false}]}],outputs:vec![OutputDecl{channel:"R1".into(),location:Location::Path(root.join("output.fastq")),format:Format::Fastq,phred:33}],json_report:None}
    }
    #[test]
    fn one_canonical_catalogue_controls_effective_resolver_and_compile() {
        let root=fixture();let a=root.join("A.json");let b=root.join("B.json");
        let resolver=validate(JobConfig::Process(spec(&root)),ResourceResolver{catalogue:Some(a.clone()),explicit_paths:vec![]}).unwrap();assert_eq!(resolver.resolved_resources()[0].sha256,"70c559f09377ae756afdba8cfbc828d6a3dea56607e9b86566eae37cb58bb856");
        let mut effective=spec(&root);effective.supplied.insert("resource-catalog".into(),Setting::Text(b.to_string_lossy().into_owned()));let selected=validate(JobConfig::Process(effective.clone()),ResourceResolver::default()).unwrap();assert_eq!(selected.resolved_resources()[0].sha256,"53f0d86dd2229017ca21253090e4c5b65fb5a19a18724cb6aac68a8aaf843c58");assert_eq!(selected.catalogue_path.as_ref(),Some(&b));assert_eq!(selected.plan.as_ref().unwrap().resources[0]["path"],serde_json::json!(root.join("B.fa")));
        assert_eq!(validate(JobConfig::Process(effective),ResourceResolver{catalogue:Some(a.clone()),explicit_paths:vec![]}).unwrap_err().code,"resource-catalogue-conflict");
        let mut same=spec(&root);same.supplied.insert("resource-catalog".into(),Setting::Text(root.join("./A.json").to_string_lossy().into_owned()));let selected=validate(JobConfig::Process(same),ResourceResolver{catalogue:Some(a.clone()),explicit_paths:vec![]}).unwrap();assert_eq!(selected.catalogue_path.as_ref(),Some(&a));assert_eq!(selected.plan.as_ref().unwrap().resources[0]["catalogue_sha256"].as_str(),selected.catalogue_sha256.as_deref());
        assert!(!root.join("output.fastq").exists());
    }
    #[test]
    fn selected_catalogue_and_resource_are_protected_from_split_outputs() {
        let root=fixture();let a=root.join("A.json");let mut resource=spec(&root);resource.outputs[0].location=Location::Path(root.join("A.fa"));assert!(validate(JobConfig::Process(resource),ResourceResolver{catalogue:Some(a.clone()),explicit_paths:vec![]}).is_err());
        let renamed=root.join("C000001.json");std::fs::copy(&a,&renamed).unwrap();let mut split=spec(&root);split.supplied.insert("split-units".into(),Setting::Int(1));split.outputs[0].location=Location::Path(root.join("C{chunk}.json"));let e=validate(JobConfig::Process(split),ResourceResolver{catalogue:Some(renamed),explicit_paths:vec![]}).unwrap_err();assert!(e.field.contains("aliases declared"));assert!(!root.join("C000002.json").exists());
    }
    struct Writer;impl std::io::Write for Writer{fn write(&mut self,b:&[u8])->std::io::Result<usize>{Ok(b.len())}fn flush(&mut self)->std::io::Result<()>{Ok(())}}impl ReportWriter for Writer{fn close(&mut self)->std::io::Result<()>{Ok(())}}
    #[test]
    fn output_owner_cache_does_not_confuse_prepared_file_with_override() {
        let root=fixture();let mut spec=spec(&root);spec.supplied.clear();let stop=Arc::new(AtomicBool::new(false));let e=crate::native_execution::Execution::new(1,1,stop.clone()).unwrap();
        let plain=crate::native_io::OwnedIo::new(spec.clone(),JobIo{inputs:BTreeMap::new(),outputs:BTreeMap::new()},CancellationToken::default(),e.clone(),stop.clone(),e.reserve_persistent_feature(4096).unwrap());assert_eq!(plain.owned_output_key("R1"),None);plain.prepare(&root,&[("R1".into(),"stage.fq".into())]).unwrap();assert_eq!(plain.owned_output_key("R1"),None);drop(plain);
        let owned=crate::native_io::OwnedIo::new(spec,JobIo{inputs:BTreeMap::new(),outputs:BTreeMap::from([("R1".into(),Box::new(Writer)as Box<dyn ReportWriter>)])},CancellationToken::default(),e.clone(),stop,e.reserve_persistent_feature(4096).unwrap());assert_eq!(owned.owned_output_key("R1"),Some("R1"));owned.prepare(&root,&[("R1".into(),"not-created.fq".into())]).unwrap();assert!(!root.join("not-created.fq").exists());drop(owned);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
    }
    #[test]
    fn split_finalization_skips_gzip_main_family_and_closes_auxiliary() {
        let root=fixture();let mut s=spec(&root);s.supplied.clear();s.outputs[0].location=Location::Path(root.join("out-{chunk}.gz"));s.outputs[0].format=Format::Gzip;s.outputs.push(OutputDecl{channel:"reject1".into(),location:Location::Handle("aux".into()),format:Format::Fastq,phred:33});
        let stop=Arc::new(AtomicBool::new(false));let e=crate::native_execution::Execution::new(1,1,stop.clone()).unwrap();let io=crate::native_io::OwnedIo::new(s,JobIo{inputs:BTreeMap::new(),outputs:BTreeMap::from([("aux".into(),Box::new(Writer)as Box<dyn ReportWriter>)])},CancellationToken::default(),e.clone(),stop,e.reserve_persistent_feature(4096).unwrap());io.finish_completed(2,true).unwrap();assert!(io.bytes.lock().unwrap().is_empty());assert!(!root.join("out-000001.gz").exists());drop(io);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
    }

}
#[cfg(test)]mod dedup_header_tests{
 use super::*;
 #[test]fn borrowed_all_lanes_catalogue_authority_and_release(){
  let root=crate::test_support::workspace("header");let e=crate::native_execution::Execution::with_statistics_budget(1,1,Arc::new(AtomicBool::new(false)),None,crate::dedup_runtime::Runtime::statistics_bound(1).unwrap()).unwrap();let caller=e.reserve_preparation_feature(32<<20).unwrap();let expected:serde_json::Value=serde_json::from_slice(&std::fs::read(root.join("literal.json")).unwrap()).unwrap();let mut s=JobSpec{configured:BTreeMap::new(),supplied:BTreeMap::from([("spill-dir".into(),Setting::Text(root.join("spill").to_string_lossy().into_owned())),("sample-id".into(),Setting::Text("HEADER_NATIVE".into())),("threads".into(),Setting::Int(1))]),layout:Layout::Se,lanes:(0..1024).map(|i|LaneDecl{id:format!("L{i:04} \"<&"),inputs:vec![InputDecl{channel:"R1".into(),location:Location::Handle(format!("reader-{i}")),format:Format::Fastq,phred:if i%2==0{33}else{64},seekable:false}]}).collect(),outputs:vec![OutputDecl{channel:"R1".into(),location:Location::Handle("writer".into()),format:Format::Fastq,phred:33}],json_report:None};let files=std::fs::read_dir(&root).unwrap().count();let a=root.join("catalogue-a.json");let b=root.join("catalogue-b.json");
  s.supplied.insert("resource-catalog".into(),Setting::Text(a.to_string_lossy().into_owned()));println!("API_VALIDATE_ENTER");let job=validate(JobConfig::Dedup(s.clone()),ResourceResolver::default()).unwrap();let plan=job.dedup_plan.as_ref().unwrap();assert_eq!(serde_json::to_value(plan.catalogue.as_ref().unwrap()).unwrap(),expected["a"]);
  let mut resolver_spec=s.clone();resolver_spec.supplied.remove("resource-catalog");println!("API_VALIDATE_ENTER");let resolver=validate(JobConfig::Dedup(resolver_spec),ResourceResolver{catalogue:Some(b.clone()),explicit_paths:vec![]}).unwrap();assert_eq!(serde_json::to_value(resolver.dedup_plan.as_ref().unwrap().catalogue.as_ref().unwrap()).unwrap(),expected["b"]);drop(resolver);
  println!("API_VALIDATE_ENTER");let same=validate(JobConfig::Dedup(s.clone()),ResourceResolver{catalogue:Some(root.join("./catalogue-a.json")),explicit_paths:vec![]}).unwrap();assert_eq!(serde_json::to_value(same.dedup_plan.as_ref().unwrap().catalogue.as_ref().unwrap()).unwrap(),expected["a"]);drop(same);
  println!("API_VALIDATE_ENTER");let conflict=validate(JobConfig::Dedup(s.clone()),ResourceResolver{catalogue:Some(b),explicit_paths:vec![]}).err().unwrap();assert_eq!(conflict.code,"resource-catalogue-conflict");assert_eq!(std::fs::read_dir(&root).unwrap().count(),files);assert!(!root.join("spill").exists());
  let snapshot=plan.catalogue.as_ref().unwrap();let metadata=metadata_bound(&s).unwrap()*8+snapshot.capacity().unwrap();let ticket=e.reserve_preparation_feature(metadata).unwrap();let input=s.lanes.iter().flat_map(|l|l.inputs.iter()).map(|d|{let Location::Handle(h)=&d.location else{unreachable!()};(h.clone(),InputHandle::Stream(Box::new(std::io::empty())as Box<dyn Read+Send>))}).collect();let io=Arc::new(crate::native_io::OwnedIo::new(s.clone(),JobIo{inputs:input,outputs:BTreeMap::new()},CancellationToken::default(),e.clone(),Arc::new(AtomicBool::new(false)),ticket));let runtime=crate::dedup_runtime::Runtime::new(&root,&plan.channels,&e).unwrap();let state=runtime.state.lock().unwrap();let header_ticket=e.reserve_preparation_feature(262144).unwrap();let mut header=serde_json::json!({"schema":"soapnuke-v3.report/2","status":"failed","run":{"job":"dedup"},"settings":{"effective":{"threads":1},"provenance":{}},"api":{"execution_admitted":false},"dedup":{"dedup":{"grouping_complete":false}},"execution":e.facts()});let views=crate::dedup_report::Views::new(&s,&io,1,true,Some(snapshot));let(text,report_ticket)=crate::dedup_report::encode(&mut header,&state,&plan.channels,&e,&views).unwrap();assert!(text.capacity()<=report_ticket.reserved_bytes());let parsed:serde_json::Value=serde_json::from_str(&text).unwrap();assert_eq!(parsed["settings"]["effective"]["input_declarations"].as_array().unwrap().len(),1024);assert_eq!(parsed["settings"]["effective"]["input_declarations"][0],expected["first"]);assert_eq!(parsed["settings"]["effective"]["input_declarations"][1023],expected["last"]);assert_eq!(parsed["settings"]["effective"]["input_codecs"],serde_json::json!(vec!["plain";1024]));assert_eq!(parsed["api"]["decoder_capabilities"],io.decoder_bindings(1));assert_eq!(parsed["api"]["decoder_capabilities"]["bindings"][0]["inputs"][0]["actual_binding_capability"],"OwnedStream");assert_eq!(parsed["api"]["decoder_capabilities"]["bindings"][0]["inputs"][0]["actual_seekable"],false);assert_eq!(parsed["dedup"]["catalogue"],expected["a"]);assert_eq!(parsed["populations"]["input"]["source_units"],0);assert!(parsed["populations"]["accepted"].is_null());std::fs::write(root.join("produced.json"),&text).unwrap();println!("R84_HEADER_PROBE lanes=1024 report_capacity={} reserved={} peak_total={}",text.capacity(),report_ticket.reserved_bytes(),e.facts()["memory_budget"]["peak_total_reserved_bytes"]);drop(parsed);drop(text);drop(report_ticket);drop(header);drop(header_ticket);drop(views);drop(state);drop(runtime);drop(io);drop(job);drop(s);drop(expected);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],32<<20);drop(caller);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);assert_eq!(e.facts()["memory_budget"]["current_persistent_feature_reserved_bytes"],0);
 }
}

#[cfg(test)]
mod typed_failure_tests {
    use super::*;
    use std::io::{self, Cursor};
    use std::sync::Mutex;

    struct Writer {
        message: Option<&'static str>,
        lifetime: Arc<Mutex<[usize; 2]>>,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            match self.message {
                Some(message) => Err(io::Error::other(message)),
                None => Ok(bytes.len()),
            }
        }
        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }
    impl ReportWriter for Writer {
        fn close(&mut self) -> io::Result<()> { self.lifetime.lock().unwrap()[0] += 1; Ok(()) }
    }
    impl Drop for Writer {
        fn drop(&mut self) { self.lifetime.lock().unwrap()[1] += 1; }
    }
    struct Reader(&'static str);
    impl Read for Reader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> { Err(io::Error::other(self.0)) }
    }
    fn fail(source: bool, message: &'static str) {
        let config=JobConfig::Process(JobSpec {
            configured:BTreeMap::new(), supplied:BTreeMap::from([("threads".into(),Setting::Int(1))]),
            layout:Layout::Se,
            lanes:vec![LaneDecl{id:"L1".into(),inputs:vec![InputDecl{channel:"R1".into(),location:Location::Handle("input".into()),format:Format::Fastq,phred:33,seekable:false}]}],
            outputs:vec![OutputDecl{channel:"R1".into(),location:Location::Handle("output".into()),format:Format::Fastq,phred:33}],json_report:None,
        });
        let job=validate(config,ResourceResolver::default()).unwrap();
        let reader:Box<dyn Read+Send>=if source {Box::new(Reader(message))}else{Box::new(Cursor::new(b"@u full\nAC\n+\nII\n".to_vec()))};
        let lifetime=Arc::new(Mutex::new([0,0]));
        let writer=Writer{message:if source{None}else{Some(message)},lifetime:lifetime.clone()};
        let error=run(job,JobIo{inputs:BTreeMap::from([("input".into(),InputHandle::Stream(reader))]),outputs:BTreeMap::from([("output".into(),Box::new(writer)as Box<dyn ReportWriter>)])},None,&CancellationToken::default()).unwrap_err();
        assert_eq!(error.partial.status,"failed");
        let report:serde_json::Value=serde_json::from_str(&error.partial.json).unwrap();
        assert_eq!(report["error"]["class"],if source{"input"}else{"write"});
        assert!(!report["error"]["message"].as_str().unwrap().starts_with("pair."));
        assert_eq!(error.partial.input_units,if source{0}else{1});
        assert_eq!(error.partial.written_units,Some(0));
        assert_eq!(report["populations"]["written"]["read_records"],0);
        assert_eq!(report["populations"]["written"]["bases"],0);
        assert_eq!(*lifetime.lock().unwrap(),[1,1]);
    }
    #[test]
    fn source_failure_category_does_not_depend_on_diagnostic_words() {
        for message in ["opaque", "worker quality FASTQ [write] panicked", "index-count read counts differ"] {fail(true,message);}
    }
    #[test]
    fn owned_write_failure_category_does_not_depend_on_diagnostic_words() {
        for message in ["opaque", "worker quality FASTQ [input] panicked", "index-count identifiers differ"] {fail(false,message);}
    }
}
