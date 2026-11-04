use crate::{
    job_api::*,
    demux::{Sample, Settings, Table},
    demux_runtime::{Channel, Runtime, State},
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
#[derive(Clone,Debug)]
pub(crate) struct Plan {
    pub common: crate::native_config::RunPlan,
    pub spec: JobSpec,
    pub table: Arc<Table>,
    pub samples_identity:(PathBuf,String),
    pub catalogue:Option<Value>,
    pub settings: Settings,
    pub channels: Vec<Channel>,
    pub mates: usize,
    pub index_source: bool,
    pub unassigned: Option<usize>,
    pub ambiguous: Option<usize>,
    pub out_dir: PathBuf,
    pub values: BTreeMap<String, String>,
    pub configured: Vec<String>,
    pub supplied: Vec<String>,
}
fn required(v: &BTreeMap<String, String>, key: &str) -> Result<String, String> {
    v.get(key)
        .filter(|s| !s.is_empty())
        .cloned()
        .ok_or_else(|| format!("{key} required"))
}
fn boolean(v: &BTreeMap<String, String>, key: &str, default: bool) -> Result<bool, String> {
    match v.get(key).map(String::as_str) {
        None => Ok(default),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        _ => Err(format!("{key} requires boolean")),
    }
}
fn unsigned(v: &BTreeMap<String, String>, key: &str, default: usize) -> Result<usize, String> {
    match v.get(key) {
        None => Ok(default),
        Some(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) => {
            s.parse().map_err(|_| format!("{key} integer overflow"))
        }
        _ => Err(format!("{key} requires nonnegative integer")),
    }
}
fn range(s: &str) -> Result<[usize; 2], String> {
    let (a, b) = s
        .split_once(':')
        .ok_or("barcode range requires start:end")?;
    let a = a
        .parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or("barcode range start must be positive")?;
    let b = b
        .parse::<usize>()
        .ok()
        .filter(|n| *n >= a)
        .ok_or("barcode range reversed")?;
    Ok([a - 1, b])
}
pub(crate) fn parse(args: &[String]) -> Result<Plan, String> {
    let args = crate::native_cli::normalize_demux(args)?;
    let mut values = BTreeMap::new();
    let mut i = 0;
    while i < args.len() {
        let key = args[i]
            .strip_prefix("--")
            .ok_or("expected named demux option")?;
        let (k, inline) = key
            .split_once('=')
            .map_or((key, None), |(a, b)| (a, Some(b)));
        if !crate::option_metadata::lookup(crate::option_metadata::Job::Demux,k).is_some() {
            return Err(format!("unknown demux option {k}"));
        }
        let val = if let Some(v) = inline {
            v.to_string()
        } else if [
            "interleaved",
            "metrics",
            "barcode-remove",
            "help",
            "version",
        ]
        .contains(&k)
            && args.get(i + 1).is_none_or(|s| s.starts_with("--"))
        {
            "true".into()
        } else {
            i += 1;
            args.get(i).ok_or_else(|| format!("missing {k}"))?.clone()
        };
        if values.insert(k.into(), val).is_some() {
            return Err(format!("duplicate demux option {k}"));
        }
        i += 1;
    }
    let supplied = values.keys().cloned().collect::<Vec<_>>();
    let mut configured=Vec::new();
    if let Some(path)=values.get("config"){
        let text=fs::read_to_string(path).map_err(|e|format!("config: {e}"))?;
        let config=crate::native_config::decode_job_config(&text,"demux")?;
        for (key,value) in config{let text=config_value(&key,&value)?;configured.push(key.clone());values.entry(key).or_insert(text);}
    }
    compile(values,supplied,configured,None)
}
pub(crate) fn config_value(key:&str,value:&Value)->Result<String,String>{
    if !crate::option_metadata::lookup(crate::option_metadata::Job::Demux,key).is_some()||key=="config"{return Err(format!("unknown config option {key}"));}
    if ["barcode-errors","barcode-max-n","barcode-min-q"].contains(&key){
        if key=="barcode-min-q" && value.as_str()==Some("off"){return Ok("off".into());}
        let n=value.as_u64().and_then(|n|usize::try_from(n).ok()).ok_or_else(||format!("config {key} requires integer"))?;
        if key=="barcode-min-q"&&n>93{return Err(format!("config {key} must be0..93"));}return Ok(n.to_string());
    }
    if key=="barcode-remove"{return value.as_bool().map(|b|b.to_string()).ok_or_else(||format!("config {key} requires boolean"));}
    if ["samples","barcode-source","barcode-r1","barcode-r2","i1","i2","output-template","unassigned1","unassigned2","ambiguous1","ambiguous2"].contains(&key){return value.as_str().map(str::to_owned).ok_or_else(||format!("config {key} requires string"));}
    crate::native_config::config_arguments(key,value)?;
    Ok(match value{Value::String(v)=>v.clone(),_=>value.to_string()})
}
fn compile(values:BTreeMap<String,String>, supplied:Vec<String>,configured:Vec<String>,declared:Option<&JobSpec>)->Result<Plan,String>{
    boolean(&values,"help",false)?;boolean(&values,"version",false)?;
    let report = if let Some(s)=declared{s.json_report.clone().unwrap_or_default()}else{PathBuf::from(required(&values,"report")?)};
    if report.as_os_str() == "-" {
        return Err("report cannot be stdout".into());
    }
    let samples = PathBuf::from(required(&values, "samples")?);
    let size = fs::metadata(&samples)
        .map_err(|e| format!("samples: {e}"))?
        .len();
    if size > 512 * 1024 * 1024 {
        return Err("samples do not fit managed capacity".into());
    }
    let samples_hash=crate::native_screen::sha256(&samples.to_string_lossy())?;
    let validation_execution=crate::native_execution::Execution::new(1,1,Arc::new(std::sync::atomic::AtomicBool::new(false))).map_err(|e|e.to_string())?;
    let table_bound=usize::try_from(size).ok().and_then(|n|n.checked_mul(32)).and_then(|n|n.checked_add(65536)).ok_or("samples capacity overflow")?;
    let _table_working=validation_execution.reserve_preparation_feature(table_bound).map_err(|e| e.to_string())?;
    let mut file=fs::File::open(&samples).map_err(|e|e.to_string())?;
    let mut bytes=Vec::new();bytes.try_reserve_exact(size as usize).map_err(|e|e.to_string())?;
    use std::io::Read;file.take(size+1).read_to_end(&mut bytes).map_err(|e|e.to_string())?;
    if bytes.len() as u64 != size {
        return Err("samples changed during read".into());
    }
    if crate::native_screen::sha256(&samples.to_string_lossy())?!=samples_hash{return Err("samples changed during validation".into());}
    let samples_identity=(fs::canonicalize(&samples).map_err(|e|e.to_string())?,samples_hash);
    let text = std::str::from_utf8(&bytes).map_err(|e| e.to_string())?;
    let mut lines = text.lines();
    if lines.next() != Some("sample\tbarcode1\tbarcode2") {
        return Err("samples header requires sample,barcode1,barcode2".into());
    }
    let row_count=text.lines().count().saturating_sub(1);
    if row_count.checked_mul(std::mem::size_of::<Sample>()+2*std::mem::size_of::<String>()).and_then(|n|n.checked_add(bytes.capacity())).and_then(|n|n.checked_add(bytes.len())).is_none_or(|n|n>table_bound){return Err("samples representation exceeds admitted capacity".into());}
    let mut rows = Vec::with_capacity(row_count);
    for line in lines {
        let f = line.split('\t').collect::<Vec<_>>();
        if f.len() != 3 {
            return Err("samples requires three columns".into());
        }
        let mut barcodes = vec![f[1].to_string()];
        if !f[2].is_empty() {
            barcodes.push(f[2].into());
        }
        rows.push(Sample {
            name: f[0].into(),
            barcodes,
        });
    }
    let table = Arc::new(Table::validate(rows)?);
    let index_source=match values.get("barcode-source").map_or("inline",String::as_str){"inline"=>false,"index"=>true,_=>return Err("barcode-source must be inline or index".into())};
    let ranges=if index_source{
        if values.contains_key("barcode-r1")||values.contains_key("barcode-r2"){return Err("index source excludes inline barcode ranges".into());}
        if values.contains_key("barcode-remove"){return Err("barcode-remove is inline-only, including explicitly supplied false".into());}
        if declared.is_none(){required(&values,"i1")?;if values.contains_key("i2")!=(table.samples[0].barcodes.len()==2){return Err("index columns require exactly i1/i2 matching sample table".into());}}
        Vec::new()
    }else{
        if values.contains_key("i1")||values.contains_key("i2"){return Err("inline source excludes index inputs".into());}
        let mut ranges=vec![range(&required(&values,"barcode-r1")?).map_err(|e|format!("barcode-r1: {e}"))?];if let Some(s)=values.get("barcode-r2"){ranges.push(range(s).map_err(|e|format!("barcode-r2: {e}"))?);}
        if ranges.len()!=table.samples[0].barcodes.len()||ranges.iter().zip(&table.samples[0].barcodes).any(|([a,b],s)|b-a!=s.len()){return Err("barcode columns/range lengths mismatch".into());}ranges
    };
    let min_q = values
        .get("barcode-min-q")
        .filter(|s| s.as_str() != "off")
        .map(|_| unsigned(&values, "barcode-min-q", 0))
        .transpose()?;
    if min_q.is_some_and(|q| q > 93) {
        return Err("barcode-min-q must be0..93".into());
    }
    let settings = Settings {
        ranges,
        errors: unsigned(&values, "barcode-errors", 0)?,
        min_q: min_q.map(|q| q as u8),
        max_n: unsigned(&values, "barcode-max-n", 0)?,
        remove: boolean(&values, "barcode-remove", false)?,
    };
    let interleaved = boolean(&values, "interleaved", false)?;
    if interleaved && values.contains_key("r2") {return Err("interleaved/r2 layout conflict".into());}
    if values.contains_key("lanes") && ["r1","r2","interleaved"].iter().any(|k|values.contains_key(*k)){return Err("lanes excludes r1/r2/interleaved".into());}
    let lane_manifest=if let Some(path)=values.get("lanes"){
        if !values.contains_key("sample-id"){return Err("lanes requires sample-id".into());}
        Some(crate::native_config::inspect_lanes(path)?)
    }else{None};
    let r1=if declared.is_some()||lane_manifest.is_some(){String::new()}else{required(&values,"r1")?};
    let r2=values.get("r2").cloned();
    if r2.as_deref()==Some("-"){return Err("r2 cannot be stdin".into());}
    if r1=="-"&&!values.contains_key("sample-id"){return Err("stdin requires sample-id".into());}
    let mates=declared.map(|s|if s.layout==Layout::Se{1}else{2}).unwrap_or_else(||if let Some((text,..))=&lane_manifest{if text.lines().skip(1).next().unwrap().split('\t').nth(2).is_some_and(|s|!s.is_empty()){2}else{1}}else if r2.is_some()||interleaved{2}else{1});
    if settings.ranges.len()>mates{return Err("R2 barcode requires paired input".into());}
    let explicit_outputs=declared.is_some_and(|s|!s.outputs.is_empty());
    let out_dir=if explicit_outputs&&!values.contains_key("out-dir"){PathBuf::new()}else{PathBuf::from(required(&values,"out-dir")?)};
    if out_dir.as_os_str() == "-" || out_dir.symlink_metadata().is_ok() {
        return Err("out-dir requires a fresh directory distinct from inputs".into());
    }
    let parent = out_dir
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let out_dir=if out_dir.as_os_str().is_empty(){out_dir}else{fs::canonicalize(parent).map_err(|e|e.to_string())?.join(out_dir.file_name().ok_or("out-dir filename")?)};
    let template = values
        .get("output-template")
        .map_or("{sample}_R{mate}.fastq.gz", String::as_str);
    let output_score=unsigned(&values,"output-phred",33)?;if ![33,64].contains(&output_score){return Err("output-phred must be33 or64".into());}
    let input_score=unsigned(&values,"input-phred",33)?;if ![33,64].contains(&input_score){return Err("input-phred must be33 or64".into());}
    let phred=output_score as u8;let input_phred=input_score as u8;
    let format = |key: &str| -> Result<Format, String> {
        match values.get(key).map_or("auto", String::as_str) {
            "auto" => Ok(Format::Auto),
            "fastq" => Ok(Format::Fastq),
            "gzip" => Ok(Format::Gzip),
            _ => Err(format!("invalid {key}")),
        }
    };
    let output_format = format("output-format")?;
    let input_format = format("input-format")?;
    let maximum_channels=table.samples.len().checked_mul(mates).and_then(|n|n.checked_add(4)).ok_or("sample destination count overflow")?;
    if Runtime::statistics_bound(maximum_channels)?>crate::native_execution::STATS_BYTES{return Err("sample destination statistics exceed128MiB".into());}
    let max_name=table.samples.iter().map(|s|s.name.len()).max().unwrap_or(0);
    let expanded=template.matches("{sample}").count().checked_mul(max_name).and_then(|n|n.checked_add(template.len())).and_then(|n|n.checked_add(out_dir.as_os_str().len())).and_then(|n|n.checked_add(16)).ok_or("destination size overflow")?;
    let destination_bound=expanded.checked_mul(16).and_then(|n|n.checked_add(8192)).and_then(|n|n.checked_mul(maximum_channels)).and_then(|n|n.checked_add(values.iter().map(|(k,v)|k.len()+v.len()).sum::<usize>().checked_mul(16)?)).and_then(|n|n.checked_add(262144)).ok_or("destination reservation overflow")?;
    let _destination_working=validation_execution.reserve_preparation_feature(destination_bound).map_err(|e| e.to_string())?;
    let mut channels = Vec::with_capacity(maximum_channels);
    let mut declarations = Vec::with_capacity(maximum_channels);
    let mut paths = Vec::with_capacity(maximum_channels);
    let mut add = |name: String, path: PathBuf| -> Result<(), String> {
        if path.as_os_str() == "-" {
            return Err("demux outputs cannot use stdout".into());
        }
        if paths.contains(&path){return Err("output-template requires distinct channel destinations".into());}paths.push(path.clone());
        let decl = OutputDecl {
            channel: name.clone(),
            location: Location::Path(path.clone()),
            format: output_format.clone(),
            phred,
        };
        let codec = crate::job_api::output_codec(&decl);
        channels.push(Channel {
            name,
            path,
            codec,
            phred: phred as i32,
        });
        declarations.push(decl);
        Ok(())
    };
    for s in &table.samples {
        for mate in 1..=mates {
            let expanded = template
                .replace("{sample}", &s.name)
                .replace("{mate}", &mate.to_string());
            let p = Path::new(&expanded);
            if expanded.contains(['{', '}'])
                || p.is_absolute()
                || p.components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err("invalid output-template placeholder/path".into());
            }
            add(format!("sample/{}.R{mate}", s.name), out_dir.join(p))?;
        }
    }
    let mut aux = [None, None];
    for (group, stem) in ["unassigned", "ambiguous"].iter().enumerate() {
        if mates == 1 && values.contains_key(&format!("{stem}2")) {
            return Err(format!("{stem}2 requires a paired R2 mate"));
        }
        let typed_aux=declared.and_then(|s|s.outputs.iter().find(|d|d.channel==format!("{stem}.R1")));
        if values.contains_key(&format!("{stem}1"))||typed_aux.is_some(){
            let path=values.get(&format!("{stem}1")).cloned().unwrap_or_default();
            aux[group] = Some(
                table.samples.len() * mates
                    + if group == 1 && aux[0].is_some() {
                        mates
                    } else {
                        0
                    },
            );
            for mate in 1..=mates {
                let p = if mate == 1 {
                    path.clone()
                } else {
                    if typed_aux.is_some(){values.get(&format!("{stem}2")).cloned().unwrap_or_default()}else{required(&values,&format!("{stem}2"))?}
                };
                if p=="-"{return Err(format!("{stem}{mate} cannot use stdout"));}
                add(
                    format!("{stem}.R{mate}"),
                    if p.is_empty(){PathBuf::from(format!("{stem}.R{mate}"))}else{crate::job_api::path_identity(Path::new(&p))?},
                )?;
            }
        }
    }
    let input = |channel: &str, path: String| InputDecl {
        channel: channel.into(),
        location: if path == "-" {
            Location::Handle(channel.into())
        } else {
            Location::Path(PathBuf::from(path))
        },
        format: input_format.clone(),
        phred: input_phred,
        seekable: true,
    };
    let (layout,mut lanes)=if let Some(s)=declared{(s.layout.clone(),s.lanes.clone())}else if let Some((text,parent,..))=lane_manifest{
        let lanes=text.lines().skip(1).map(|line|{let f=line.split('\t').collect::<Vec<_>>();let mut inputs=vec![input("R1",parent.join(f[1]).to_string_lossy().into_owned())];if !f[2].is_empty(){inputs.push(input("R2",parent.join(f[2]).to_string_lossy().into_owned()));}LaneDecl{id:f[0].into(),inputs}}).collect();(if mates==1{Layout::Se}else{Layout::SplitPe},lanes)
    }else{let mut inputs=vec![input("R1",r1)];if let Some(r2)=r2{inputs.push(input("R2",r2));}(if interleaved{Layout::InterleavedPe}else if mates==2{Layout::SplitPe}else{Layout::Se},vec![LaneDecl{id:"input".into(),inputs}])};
    if index_source && declared.is_none(){for role in ["i1","i2"]{if let Some(path)=values.get(role){if path=="-"{return Err("index input cannot be stdin".into());}lanes[0].inputs.push(input(&role.to_ascii_uppercase(),path.clone()));}}}
    if let Some(s)=declared{if !s.outputs.is_empty(){
        let mut seen=std::collections::BTreeSet::new();let mut handles=std::collections::BTreeSet::new();
        for d in &s.outputs{if !seen.insert(&d.channel)||!channels.iter().any(|c|c.name==d.channel){return Err("unknown or duplicate demux output channel".into());}if !matches!(d.phred,33|64){return Err("invalid output-phred".into());}if let Location::Handle(h)=&d.location{if h.is_empty()||!handles.insert(h){return Err("duplicate or empty output handle".into());}}}
        if channels.len()!=s.outputs.len(){return Err("demux requires complete sample/mate output declarations".into());}
        declarations=s.outputs.clone();for c in &mut channels{let d=declarations.iter().find(|d|d.channel==c.name).unwrap();c.path=match &d.location{Location::Path(p)=>p.clone(),Location::Handle(_)=>PathBuf::new()};c.codec=crate::job_api::output_codec(d);c.phred=d.phred as i32;}
    }}
    let mut spec=JobSpec{
        configured:BTreeMap::new(),
        supplied:values.iter().map(|(k,v)|(k.clone(),Setting::Text(v.clone()))).collect(),
        layout,lanes,
        outputs: declarations,
        json_report: declared.map_or_else(||Some(report.clone()),|s|s.json_report.clone()),
    };
    if let Some(s)=declared{spec.configured=s.configured.clone();spec.supplied=s.supplied.clone();}
    let mut common_args = Vec::new();
    for key in [
        "r1",
        "r2",
        "lanes",
        "limit-units",
        "interleaved",
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
        "resource-catalog",
    ] {
        if let Some(value) = values.get(key) {
            common_args.extend([format!("--{key}"), value.clone()]);
        }
    }
    let mut common_spec = spec.clone();
    common_spec.outputs.clear();
    for lane in &mut common_spec.lanes{lane.inputs.retain(|d|["R1","R2"].contains(&d.channel.as_str()));}
    if values.get("sample-id").is_some_and(String::is_empty){return Err("sample-id must not be empty".into());}
    if declared.is_none()&&values.get("decoder").is_some_and(|v|v=="rapidgzip")&&spec.lanes.iter().flat_map(|l|&l.inputs).any(|d|!matches!(&d.location,Location::Path(p)if p.is_file())){return Err("rapidgzip requires seekable regular paths, not stdin/streams".into());}
    let mut common = crate::native_config::parse_api(&common_args, &common_spec)?;
    let level = unsigned(&values, "gzip-level", 4)?;
    if !(1..=9).contains(&level) {
        return Err("gzip-level must be1..9".into());
    }
    if values.contains_key("gzip-level") && !channels.iter().any(|c| c.codec == "gzip") {
        return Err("explicit gzip-level without gzip output".into());
    }
    common.gzip_level = level as u32;
    common.input_phred = input_phred as i32;
    common.output_phred = phred as i32;
    let mut protected = spec
        .lanes
        .iter()
        .flat_map(|l| &l.inputs)
        .filter_map(|d| {
            if let Location::Path(p) = &d.location {
                Some(crate::job_api::path_identity(p))
            } else {
                None
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    protected.push(crate::job_api::path_identity(&samples)?);
    for key in ["config","lanes","resource-catalog"]{if let Some(path)=values.get(key){protected.push(crate::job_api::path_identity(Path::new(path))?);}}
    let report_id=if report.as_os_str().is_empty(){PathBuf::new()}else{crate::job_api::path_identity(&report)?};
    for p in &protected {
        if p == &report_id || (!out_dir.as_os_str().is_empty()&&(p == &out_dir || p.starts_with(&out_dir))) {
            return Err("input/report/out-dir alias".into());
        }
    }
    if common.html_report.as_ref().is_some_and(|p|p.is_dir()){return Err("html-report destination is a directory".into());}
    if let Some(path)=&common.html_report{let id=crate::job_api::path_identity(path)?;if protected.contains(&id)||id==report_id||channels.iter().any(|c|c.path==id){return Err("html/input/output/report alias".into());}}
    let mut destination_ids=Vec::with_capacity(channels.len());
    for c in &channels {
        if !c.path.as_os_str().is_empty(){let id=destination_identity(&c.path)?;if destination_ids.contains(&id){return Err("output channels require distinct destinations".into());}destination_ids.push(id);}
        if !c.path.as_os_str().is_empty() && (protected.contains(&destination_identity(&c.path)?) || destination_identity(&c.path)?==report_id || c.path.symlink_metadata().is_ok()) {return Err("output/input/report alias or existing output".into());}
    }
    let catalogue=values.get("resource-catalog").map(|p|crate::resources_app::Catalogue::load(Path::new(p)).map(|c|catalogue_metadata(&c)).map_err(|e|format!("{}: {}",e.code,e.message))).transpose()?;
    crate::native_execution::Execution::validate_budget_with_statistics(common.threads,mates,common.memory_total_bytes,Runtime::statistics_bound(channels.len())?).map_err(|e| e.to_string())?;
    Ok(Plan {
        common,
        spec,
        table,
        samples_identity,
        catalogue,
        settings,
        channels,
        mates,
        index_source,
        unassigned: aux[0],
        ambiguous: aux[1],
        out_dir,
        values,
        configured,
        supplied,
    })
}
pub(crate) fn catalogue_metadata(c:&crate::resources_app::Catalogue)->Value{json!({"schema":c.value["schema"],"catalogue_version":c.value.get("catalogue_version").unwrap_or(&Value::Null),"sha256":c.sha256,"path":c.path})}
fn destination_identity(p:&Path)->Result<PathBuf,String>{
    if let Ok(path)=fs::canonicalize(p){return Ok(path);}
    let name=p.file_name().ok_or("output path requires filename")?;let parent=p.parent().filter(|p|!p.as_os_str().is_empty()).unwrap_or(Path::new("."));Ok(destination_identity(parent)?.join(name))
}
pub(crate) fn protect_resources(p:&Plan,resources:&[PathBuf])->Result<(),String>{
    let protected=resources.iter().map(|p|destination_identity(p)).collect::<Result<Vec<_>,_>>()?;
    for path in p.channels.iter().filter(|c|!c.path.as_os_str().is_empty()).map(|c|&c.path).chain(p.spec.json_report.iter()).chain(p.common.html_report.iter()){
        if protected.contains(&destination_identity(path)?){return Err("selected resource/output/report alias".into());}
    }Ok(())
}
fn report_metadata_bound(p:&Plan)->Option<usize>{
    let mut n=256usize.checked_mul(1024)?;
    for c in &p.channels{n=n.checked_add(4096)?.checked_add(c.name.capacity().checked_mul(8)?)?.checked_add(c.path.capacity().checked_mul(8)?)?.checked_add(p.common.report.capacity().checked_mul(8)?)?;}
    n=n.checked_add(crate::job_api::metadata_bound(&p.spec)?.checked_mul(8)?)?;
    if let Some(c)=&p.catalogue{n=n.checked_add(c["catalogue_version"].as_str().map_or(0,str::len).checked_mul(8)?)?.checked_add(4096)?;}
    for(k,v)in &p.values{n=n.checked_add(2048)?.checked_add(k.capacity().checked_mul(8)?)?.checked_add(v.capacity().checked_mul(8)?)?;}Some(n)
}
pub(crate) fn compile_api(s:&JobSpec)->Result<Plan,String>{
    let mut values=BTreeMap::new();for map in [&s.configured,&s.supplied]{for(k,v)in map{values.insert(k.clone(),config_value(k,&setting_json(v))?);}}
    if s.lanes.is_empty(){return Err("demux requires sequencing lanes".into());}
    let index=values.get("barcode-source").is_some_and(|s|s=="index");let mut lane_ids=std::collections::BTreeSet::new();let mut handles=std::collections::BTreeSet::new();
    for(i,lane)in s.lanes.iter().enumerate(){if lane.id.is_empty()||!lane_ids.insert(&lane.id){return Err("duplicate or empty lane ID".into());}
        let roles=if s.layout==Layout::SplitPe{vec!["R1","R2"]}else{vec!["R1"]};
        for role in &roles{if lane.inputs.iter().filter(|d|&d.channel==role).count()!=1{return Err("demux lane requires exact biological R1/R2 roles".into());}}
        for d in &lane.inputs{if !roles.contains(&d.channel.as_str())&&!(i==0&&index&&["I1","I2"].contains(&d.channel.as_str())){return Err(format!("demux {:?} layout does not admit input role {} here; index roles belong to first sequencing lane",s.layout,d.channel));}if !matches!(d.phred,33|64){return Err("invalid input-phred".into());}if let Location::Handle(h)=&d.location{if h.is_empty()||!handles.insert(h){return Err("duplicate or empty input binding".into());}}}
    }
    let plan=compile(values,s.supplied.keys().cloned().collect(),s.configured.keys().cloned().collect(),Some(s))?;
    let actual=s.lanes[0].inputs.iter().filter(|d|d.channel=="I1"||d.channel=="I2").map(|d|d.channel.as_str()).collect::<Vec<_>>();
    if index&&(actual!=if plan.table.samples[0].barcodes.len()==2{vec!["I1","I2"]}else{vec!["I1"]}){return Err("demux requires ordered global I1/I2 matching table columns".into());}
    Ok(plan)
}
fn binary_hash() -> Option<String> {
    std::env::current_exe()
        .ok()
        .and_then(|p| crate::native_screen::sha256(p.to_str()?).ok())
}
pub(crate) fn run(plan:&Plan)->(i32,NativeReport,Option<&'static str>){run_owned(plan,None)}
pub(crate) fn run_api(plan:&Plan,owned:Arc<crate::native_io::OwnedIo>)->(i32,NativeReport,Option<&'static str>){run_owned(plan,Some(owned))}
fn run_owned(plan: &Plan, provided:Option<Arc<crate::native_io::OwnedIo>>) -> (i32, NativeReport,Option<&'static str>) {
    let api_mode=provided.is_some();
    let started = std::time::Instant::now();
    let cpu_start = crate::native_runtime::process_cpu_seconds();
    let c = &plan.common;
    let stage_parent=plan.spec.json_report.as_ref().and_then(|p|p.parent()).map(Path::to_owned).unwrap_or_else(||PathBuf::from(std::env::var_os("TMPDIR").expect("workspace TMPDIR")));
    let stage = stage_parent.join(format!(
        ".demux-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let event = Arc::new(crate::native_runtime::Runtime::new(
        c.progress_seconds,
        c.sample_id.clone(),
    ));
    event.event("starting", true);
    let additional = Runtime::statistics_bound(plan.channels.len()).unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let execution=if let Some(io)=&provided{Ok(io.execution.clone())}else{crate::native_execution::Execution::with_statistics_budget(
        c.threads,
        plan.mates,
        stop.clone(),
        c.memory_total_bytes,
        additional,
    )};
    let demux_bound=Runtime::metadata_bound(&plan.table,&plan.channels,&stage).expect("compiled metadata size");
    let demux_reservation=execution.as_ref().map_err(|e|e.clone()).and_then(|e|e.reserve_preparation_feature(demux_bound));
    let demux_ticket=match demux_reservation{Ok(t)=>t,Err(e)=>{eprintln!("memory: {e}");return(5,NativeReport{status:"failed",input_units:0,accepted_units:Some(0),written_units:Some(0),json:"{\"schema\":\"soapnuke-v3.report/2\",\"status\":\"failed\",\"error\":{\"stage\":\"memory\"}}".into()},Some("memory"));}};
    let d = Arc::new(Runtime {
        table: plan.table.clone(),
        settings: plan.settings.clone(),
        channels: plan.channels.clone(),
        mates: plan.mates,
        index_source: plan.index_source,
        unassigned: plan.unassigned,
        ambiguous: plan.ambiguous,
        stage: stage.clone(),
        level: c.gzip_level as i32,
        state: Mutex::new(None),
        metadata: Mutex::new(Some(demux_ticket)),
    });
    let mut error = None;
    let mut code = 0;
    let mut owned = None;
    let report_execution=execution.as_ref().ok().cloned();
    match execution {
        Err(e) => {
            code = 5;
            error = Some(e.to_string());
        }
        Ok(e) => {
            let bound=crate::job_api::metadata_bound(&plan.spec).and_then(|n|n.checked_mul(8)).unwrap_or(usize::MAX);
            let prepared=if let Some(io)=provided{Ok(io)}else{e.reserve_preparation_feature(bound).map(|ticket|{
                let mut io=JobIo{inputs:BTreeMap::new(),outputs:BTreeMap::new()};
                for input in &plan.spec.lanes[0].inputs{if let Location::Handle(h)=&input.location{io.inputs.insert(h.clone(),InputHandle::Stream(Box::new(std::io::stdin())));}}
                Arc::new(crate::native_io::OwnedIo::new(plan.spec.clone(),io,CancellationToken::default(),e,stop,ticket))
            })};
            match prepared{Err(e)=>{code=5;error=Some(e.to_string())},Ok(io)=>{
                    owned = Some(io.clone());
                    if let Err(e) = fs::create_dir(&stage) {
                        code = 4;
                        error = Some(e.to_string());
                    } else {
                        let mut gp = crate::params::GlobalParams::new();
                        gp.native_m1 = true;
                        gp.native_demux = Some(d.clone());
                        gp.native_io = Some(io);
                        gp.native_runtime = Some(event.clone());
                        gp.native_interleaved = plan.spec.layout == Layout::InterleavedPe;
                        gp.threads_num = c.threads;
                        gp.native_memory_total = c.memory_total_bytes;
                        gp.native_limit_units=c.limit_units;
                        gp.native_gzip_level = c.gzip_level as i32;
                        gp.quality_phred = c.input_phred;
                        gp.output_quality_phred = c.output_phred;
                        gp.patch_size = 2500;
                        gp.output_dir = stage.to_string_lossy().into_owned();
                        gp.fq1_path = c
                            .inputs
                            .first()
                            .map_or(String::new(), |p| p.to_string_lossy().into_owned());
                        gp.fq2_path = c
                            .inputs
                            .get(1)
                            .map_or(String::new(), |p| p.to_string_lossy().into_owned());
                        let mut stats = crate::stats::GlobalStats::new(&gp);
                        if let Err(e) = crate::pipeline::run_filter_typed(&gp, &mut stats) {
                            code = e.category.exit();
                            error = Some(e.to_string());
                        }
                    }
                }
            }
        }
    }
    let mut state = d.state.lock().unwrap();
    let mut empty = State {
        input: Default::default(),
        accepted: Default::default(),
        written: Default::default(),
        channels: (0..plan.channels.len())
            .map(|_| Default::default())
            .collect(),
        counts: [0; 5],
        indices:Default::default(),error_lane:None,
        error: None,
    };
    let s = state.as_mut().unwrap_or(&mut empty);
    if let Some(e)=&s.error{code=e.code;}
    let header_bound=report_metadata_bound(plan).unwrap_or(usize::MAX);
    let header_ticket=report_execution.as_ref().and_then(|e|e.reserve_finalization_feature(header_bound).ok());
    if header_ticket.is_none(){return(5,NativeReport{status:"failed",input_units:s.counts[0],accepted_units:Some(s.counts[1]),written_units:Some(s.counts[4]),json:format!("{{\"schema\":\"soapnuke-v3.report/2\",\"status\":\"failed\",\"error\":{{\"stage\":\"memory\",\"message\":\"demux report metadata cannot fit managed budget\"}}}}")},Some("memory"));}
    let mut outputs = Vec::with_capacity(plan.channels.len());
    if code == 0 && !plan.out_dir.as_os_str().is_empty() && plan.channels.iter().any(|c|owned.as_ref().is_none_or(|io|io.owned_output_key(&c.name).is_none())) {
        if let Err(e) = fs::create_dir(&plan.out_dir) {
            code = 4;
            error = Some(e.to_string());
        }
    }
    for (i, ch) in plan.channels.iter().enumerate() {
        if code==0 && i >= plan.table.samples.len() * plan.mates && s.channels[i].reads == 0 { continue; }
        let staged = stage.join(format!(
            "demux-{i:06}.fq{}",
            if ch.codec == "gzip" { ".gz" } else { "" }
        ));
        let declaration=plan.spec.outputs.iter().find(|d|d.channel==ch.name).unwrap();
        let owned_key=owned.as_ref().and_then(|io|io.owned_output_key(&ch.name));
        let mut published = false;
        if code == 0 && owned_key.is_none() {
            if let Some(parent) = ch.path.parent() {
                if let Err(e) = fs::create_dir_all(parent) {
                    code = 4;
                    error = Some(e.to_string());
                }
            }
            if code == 0 {
                match fs::hard_link(&staged, &ch.path) {
                    Ok(()) => published = true,
                    Err(e) => {
                        code = 4;
                        error = Some(format!("publish demux: {e}"));
                    }
                }
            }
        }
        let path = if published { &ch.path } else { &staged };
        if let Some(_)=owned_key{let io=owned.as_ref().unwrap();outputs.push(json!({"channel":ch.name,"path":match &declaration.location{Location::Handle(h)=>json!({"handle":h}),Location::Path(_)=>Value::Null},"declared_path":match &declaration.location{Location::Path(p)=>Some(p),_=>None},"binding_kind":if matches!(declaration.location,Location::Path(_)){"owned_override"}else{"owned_handle"},"codec":ch.codec,"encoding":ch.phred,"state":if code==0{"committed"}else{"failed"},"bytes":io.bytes.lock().unwrap().get(&ch.name).copied().unwrap_or(0),"read_records":s.channels[i].reads,"bases":s.channels[i].bases.iter().sum::<u64>()}));}
        else{outputs.push(json!({"channel":ch.name,"path":ch.path,"staged_path":staged,"binding_kind":"staged_path","codec":ch.codec,"encoding":ch.phred,"state":if published{"committed"}else{"failed"},"bytes":fs::metadata(path).ok().map(|m|m.len()),"read_records":s.channels[i].reads,"bases":s.channels[i].bases.iter().sum::<u64>()}));}
    }
    let facts = event.execution_facts();
    let mut report = json!({"schema":"soapnuke-v3.report/2","status":if code==0{"complete"}else{"failed"},"run":{"job":"demux","sample_id":c.sample_id,"layout":if plan.mates==1{"SE"}else{"PE"},"input_encoding":c.input_phred,"output_encoding":c.output_phred},"identity":{"binary_sha256":binary_hash(),"source_manifest_sha256":option_env!("SOAPNUKE_SOURCE_SHA256"),"dependency_manifest_sha256":option_env!("SOAPNUKE_DEPENDENCY_SHA256")},"settings":{"effective":{"threads":c.threads,"input_phred":c.input_phred,"output_phred":c.output_phred,"gzip_level":c.gzip_level,"progress_seconds":c.progress_seconds,"metrics":c.metrics,"sample_id":c.sample_id,"barcode_errors":plan.settings.errors,"barcode_min_q":plan.settings.min_q,"barcode_max_n":plan.settings.max_n,"barcode_source":if plan.index_source{"index"}else{"inline"},"barcode_remove":plan.settings.remove},"provenance":{"explicit_keys":{"cli":plan.supplied,"config":plan.configured}}},"demux":{"routing":{"input_units":s.counts[0],"routed_units":s.counts[1],"ambiguous_units":s.counts[2],"unassigned_units":s.counts[3]}},"outputs":outputs,"execution":facts,"error":s.error.as_ref().map(|e|json!({"source_unit_ordinal":e.ordinal,"stage":e.stage,"lane_id":s.error_lane.and_then(|i|plan.spec.lanes.get(i)).map(|l|l.id.as_str()),"message":e.message})),"measurements":crate::native_runtime::metrics(c.metrics,started,cpu_start,None),"disposition":{"input_units":s.counts[0],"accepted_units":s.counts[1],"written_units":s.counts[4]},"unit_definitions":{"accepted":"routed source units; no ordinary process filters","written":"original source units with at least one emitted destination record"},"qualification":{"authority":"external","assessment":null}});
    if api_mode{report["settings"]["provenance"]["explicit_keys"]=json!({"api":plan.supplied,"config":plan.configured});}
    report["input_scope"]=json!({"kind":if c.limit_units.is_some(){"original-source-prefix"}else{"input-until-EOF-or-error"},"requested_unit_limit":c.limit_units,"observed_units":s.counts[0],"full_input_scanned":if c.limit_units.is_none()&&code==0{Some(true)}else{None},"scope":"requested prefix is a complete job; transport read-ahead may occur; unexamined later records/lanes and index trailer are not validated"});
    report["settings"]["effective"]["limit_units"]=json!(c.limit_units);
    report["settings"]["effective"]["input_codecs"]=json!(plan.spec.lanes.iter().flat_map(|l|l.inputs.iter().map(crate::job_api::input_codec)).collect::<Vec<_>>());
    report["inputs"]=json!(plan.spec.lanes.iter().map(|l|json!({"lane_id":l.id,"inputs":l.inputs.iter().map(|d|json!({"role":d.channel,"location":match &d.location{Location::Path(p)=>json!({"path":p}),Location::Handle(h)=>json!({"handle":h})},"declared_phred":d.phred,"declared_seekable":d.seekable,"codec":crate::job_api::input_codec(d)})).collect::<Vec<_>>()})).collect::<Vec<_>>());
    report["resources"]=json!([{"kind":"sample-table","path":plan.samples_identity.0,"sha256":plan.samples_identity.1,"hash_scope":"exact validated TSV bytes; not sequencing-read content"}]);
    if let Some(catalogue)=&plan.catalogue{report["demux"]["catalogue"]=catalogue.clone();}
    report["demux"]["barcode_evidence"]=json!({"population":"original biological intervals or forward index records before removal","source_ordinal_origin":0,"index_association":"global I1/I2 streams on first sequencing lane; normalized identifier and ordinal","intervals_original_inclusive":plan.settings.ranges.iter().map(|[a,b]|format!("{}:{b}",a+1)).collect::<Vec<_>>()});
    if api_mode{report["api"]=json!({"execution_admitted":true,"decoder_capabilities":owned.as_ref().map(|io|io.decoder_bindings(c.threads))});report["demux"]["api"]=json!({"input_units":s.counts[0],"accepted_units":s.counts[1],"written_units":s.counts[4]});}
    if report["error"].is_null() {
        if let Some(e) = error {
            report["error"] = json!({"stage":"execution_or_publication","message":e});
        }
    }
    if let Some(io) = &owned {
        if let Err(e) = io.finish() {
            code = 4;
            report["status"] = json!("failed");
            report["error"] = json!({"stage":"write","message":e.to_string()});
        }
    }
    let staged = stage.join("report.json");
    let execution=report_execution.as_ref().expect("compiled demux budget admitted");
    let encoded=crate::demux_report::encode(&mut report,s,&plan.channels,execution);
    let(mut json,mut json_ticket)=match encoded{Ok((text,ticket))=>(text,Some(ticket)),Err(e)=>{eprintln!("demux report memory: {e}");return(5,NativeReport{status:"failed",input_units:s.counts[0],accepted_units:Some(s.counts[1]),written_units:Some(s.counts[4]),json:format!("{{\"schema\":\"soapnuke-v3.report/2\",\"status\":\"failed\",\"error\":{{\"stage\":\"memory\"}}}}")},Some("memory"));}};
    let mut failure_stage=None;
    if let Some(path)=&c.html_report{
        let result=(||->Result<(),String>{let file=fs::OpenOptions::new().write(true).create_new(true).open(path).map_err(|e|e.to_string())?;let mut writer=std::io::BufWriter::with_capacity(8192,file);crate::demux_report::html(&mut writer,&crate::demux_report::Document{header:&report,state:s,channels:&plan.channels},&c.sample_id).map_err(|e|e.to_string())?;writer.flush().map_err(|e|e.to_string())?;let f=writer.into_inner().map_err(|e|e.to_string())?;f.sync_all().map_err(|e|e.to_string())?;crate::checked_io::close_file(f)})();
        if let Err(e)=result{code=4;report["status"]=json!("failed");report["error"]=json!({"stage":"html-publication","message":e});failure_stage=Some("output");}
    }
    if failure_stage.is_some(){drop(json);drop(json_ticket.take());let(text,ticket)=crate::demux_report::encode(&mut report,s,&plan.channels,execution).expect("failed HTML report remains within admitted metadata");json=text;json_ticket=Some(ticket);}
    if let Some(destination)=&plan.spec.json_report{
        let result=(||->Result<(),String>{let mut file=fs::OpenOptions::new().write(true).create_new(true).open(&staged).map_err(|e|e.to_string())?;file.write_all(json.as_bytes()).map_err(|e|e.to_string())?;file.sync_all().map_err(|e|e.to_string())?;crate::checked_io::close_file(file)?;fs::hard_link(&staged,destination).map_err(|e|e.to_string())})();
        if let Err(e)=result{code=4;report["status"]=json!("failed");report["error"]=json!({"stage":"report-publication","message":e});failure_stage=Some("report-publication");eprintln!("report-publication: {e}");drop(json);drop(json_ticket.take());let(text,ticket)=crate::demux_report::encode(&mut report,s,&plan.channels,execution).expect("failed publication report remains within admitted metadata");json=text;json_ticket=Some(ticket);}
    }
    if code != 0 {
        eprintln!("demux error: {}", report["error"]);
    }
    if plan.spec.json_report.as_ref().is_some_and(|p|p.is_file()){let _=fs::remove_file(&staged);}
    if code == 0 {
        for (i, ch) in plan.channels.iter().enumerate() {
            let _ = fs::remove_file(stage.join(format!("demux-{i:06}.fq{}", if ch.codec == "gzip" { ".gz" } else { "" })));
        }
        let _ = fs::remove_file(&staged);
        let _ = fs::remove_dir(&stage);
    }
    event.event(if code == 0 { "complete" } else { "failed" }, true);
    let result=NativeReport{status:if code==0{"complete"}else{"failed"},input_units:s.counts[0],accepted_units:Some(s.counts[1]),written_units:Some(s.counts[4]),json};
    drop(json_ticket);drop(report);drop(header_ticket);drop(state);
    (code,result,failure_stage)
}
pub(crate) fn entry(args: &[String]) -> i32 {
    let normalized=match crate::native_cli::normalize_demux(args){Ok(v)=>v,Err(e)=>{eprintln!("config: {e}");return 2;}};
    let metadata=|key:&str|normalized.iter().enumerate().any(|(i,s)|s==&format!("--{key}=true")||(s==&format!("--{key}")&&normalized.get(i+1).is_none_or(|v|v!="false")));
    if metadata("help"){print!("{}",include_str!("demux_help.txt"));return 0;}
    if metadata("version"){crate::native_config::help(true);return 0;}
    for key in ["report","html-report"]{if normalized.iter().find_map(|s|s.split_once('=').filter(|(k,_)|*k==format!("--{key}")).map(|(_,v)|v)).is_some_and(|p|Path::new(p).is_dir()){eprintln!("{key} publication destination is a directory");return 3;}}
    match parse(args) {
        Ok(plan) => {let code=run(&plan).0;if code==4{3}else{code}},
        Err(e) => {
            eprintln!("config: {e}");
            2
        }
    }
}
