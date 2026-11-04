use crate::{
    dedup_runtime::{Channel, State},
    demux_report::QcView,
};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::Value;
use std::io::{self, Write};
#[cfg(test)]
pub(crate) struct Document<'a> {
    pub header: &'a Value,
    pub state: &'a State,
    pub channels: &'a [Channel],
    pub snapshot: bool,
    pub views: &'a Views<'a>,
}
pub(crate) struct Views<'a> {
    spec: &'a crate::job_api::JobSpec,
    io: &'a crate::native_io::OwnedIo,
    requested: &'a str,
    rapid: bool,
    api: bool,
    catalogue: Option<&'a crate::dedup_app::CatalogueMetadata>,
}
impl<'a> Views<'a> {
    pub(crate) fn new(
        spec: &'a crate::job_api::JobSpec,
        io: &'a crate::native_io::OwnedIo,
        threads: usize,
        api: bool,
        catalogue: Option<&'a crate::dedup_app::CatalogueMetadata>,
    ) -> Self {
        let requested = spec
            .supplied
            .get("decoder")
            .or_else(|| spec.configured.get("decoder"))
            .and_then(|v| {
                if let crate::job_api::Setting::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
            .unwrap_or("auto");
        let rapid = io.choice(threads).backend == crate::source::InputBackend::Rapidgzip;
        Self {
            spec,
            io,
            requested,
            rapid,
            api,
            catalogue,
        }
    }
}
fn codec(d: &crate::job_api::InputDecl) -> &'static str {
    use crate::job_api::{Format, Location};
    match d.format {
        Format::Gzip => "gzip",
        Format::Fastq => "plain",
        Format::Auto => match &d.location {
            Location::Path(p) if p.extension().is_some_and(|v| v == "gz") => "gzip",
            _ => "plain",
        },
    }
}
struct Codecs<'a>(&'a crate::job_api::JobSpec);
impl Serialize for Codecs<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(None)?;
        for lane in &self.0.lanes {
            for d in &lane.inputs {
                seq.serialize_element(codec(d))?;
            }
        }
        seq.end()
    }
}
struct Declarations<'a>(&'a crate::job_api::JobSpec);
struct Declaration<'a> {
    lane_id: &'a str,
    inputs: DeclaredInputs<'a>,
}
struct DeclaredInputs<'a>(&'a [crate::job_api::InputDecl]);
struct DeclaredInput<'a> {
    role: &'a str,
    phred: u8,
    codec: &'static str,
}
impl Serialize for DeclaredInputs<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for d in self.0 {
            seq.serialize_element(&DeclaredInput {
                role: &d.channel,
                phred: d.phred,
                codec: codec(d),
            })?;
        }
        seq.end()
    }
}
impl Serialize for Declarations<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.lanes.len()))?;
        for l in &self.0.lanes {
            seq.serialize_element(&Declaration {
                lane_id: &l.id,
                inputs: DeclaredInputs(&l.inputs),
            })?;
        }
        seq.end()
    }
}
#[cfg(test)]
struct Settings<'a>(&'a Document<'a>);
#[cfg(test)]
struct Effective<'a>(&'a Document<'a>);
#[cfg(test)]
impl Serialize for Settings<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        for (k, v) in self.0.header["settings"].as_object().unwrap() {
            if k == "effective" {
                m.serialize_entry(k, &Effective(self.0))?;
            } else {
                m.serialize_entry(k, v)?;
            }
        }
        m.end()
    }
}
#[cfg(test)]
impl Serialize for Effective<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        for (k, v) in self.0.header["settings"]["effective"].as_object().unwrap() {
            if k != "input_declarations" && k != "input_codecs" {
                m.serialize_entry(k, v)?;
            }
        }
        m.serialize_entry("input_declarations", &Declarations(self.0.views.spec))?;
        m.serialize_entry("input_codecs", &Codecs(self.0.views.spec))?;
        m.end()
    }
}
#[cfg(test)]
struct Api<'a>(&'a Document<'a>);
#[cfg(test)]
impl Serialize for Api<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        for (k, v) in self.0.header["api"].as_object().unwrap() {
            if k != "decoder_capabilities" {
                m.serialize_entry(k, v)?;
            }
        }
        m.serialize_entry("decoder_capabilities", &Capabilities(self.0.views))?;
        m.end()
    }
}
struct Capabilities<'a>(&'a Views<'a>);
struct Bindings<'a>(&'a Views<'a>);
struct Binding<'a> {
    lane: &'a str,
    inputs: BindingInputs<'a>,
}
struct BindingInputs<'a> {
    view: &'a Views<'a>,
    lane: usize,
}
struct BindingInput<'a> {
    channel: &'a str,
    declared_seekable: bool,
    actual_binding_capability: &'static str,
    actual_seekable: Option<bool>,
    requested_decoder: &'a str,
    selected_backend: Option<&'static str>,
    selection_reason: &'static str,
}
impl Serialize for Capabilities<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(3))?;
        m.serialize_entry(
            "rapidgzip_interface",
            "seekable regular path with no owned override; opaque ReadSeek is not a FileReader ABI",
        )?;
        m.serialize_entry("bindings", &Bindings(self.0))?;
        m.serialize_entry("scope","binding and selection at run start; actual opened backends appear in execution.decoders")?;
        m.end()
    }
}
impl Serialize for Bindings<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.spec.lanes.len()))?;
        for (i, l) in self.0.spec.lanes.iter().enumerate() {
            seq.serialize_element(&Binding {
                lane: &l.id,
                inputs: BindingInputs {
                    view: self.0,
                    lane: i,
                },
            })?;
        }
        seq.end()
    }
}
impl Serialize for BindingInputs<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use crate::native_io::InputCapability as C;
        let v = self.view;
        let inputs = &v.spec.lanes[self.lane].inputs;
        let mut seq = s.serialize_seq(Some(inputs.len()))?;
        for (j, d) in inputs.iter().enumerate() {
            let cap = v.io.input_capability(self.lane, j);
            let gzip = codec(d) == "gzip";
            let actual_binding_capability = match cap {
                C::PathFile => "PathFile",
                C::OwnedStream => "OwnedStream",
                C::OwnedSeekable => "OwnedSeekable",
                C::UnavailablePath => "UnavailablePath",
            };
            seq.serialize_element(&BindingInput{channel:&d.channel,declared_seekable:d.seekable,actual_binding_capability,actual_seekable:match cap{C::PathFile|C::OwnedSeekable=>Some(true),C::OwnedStream=>Some(false),C::UnavailablePath=>None},requested_decoder:v.requested,selected_backend:if v.requested=="rapidgzip"&&cap!=C::PathFile{None}else{Some(if !gzip{"plain"}else if cap==C::PathFile&&v.rapid{"rapidgzip"}else{"zlib"})},selection_reason:if matches!(cap,C::OwnedStream|C::OwnedSeekable){"opaque owned IO uses existing plain/zlib interface; explicit rapidgzip is rejected before Read/Seek"}else{"native regular-path decoder policy"}})?;
        }
        seq.end()
    }
}

struct Populations<'a>(&'a State);
impl Serialize for Populations<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(3))?;
        m.serialize_entry(
            "input",
            &QcView {
                q: &self.0.input,
                stage: "input",
                channel: "all",
                units: self.0.input_units,
                scope: "original complete validated biological units",
            },
        )?;
        if let Some(n) = self.0.representatives {
            m.serialize_entry(
                "accepted",
                &QcView {
                    q: &self.0.accepted,
                    stage: "accepted",
                    channel: "all",
                    units: n,
                    scope: "complete full-input representative population before writing",
                },
            )?;
        } else {
            m.serialize_entry("accepted", &Option::<u64>::None)?;
        }
        m.serialize_entry(
            "written",
            &QcView {
                q: &self.0.written,
                stage: "written",
                channel: "all",
                units: self.0.written_units,
                scope: "original source units with at least one complete emitted record",
            },
        )?;
        m.end()
    }
}
struct Channels<'a> {
    state: &'a State,
    channels: &'a [Channel],
}
impl Serialize for Channels<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(self.channels.len()))?;
        for (i, c) in self.channels.iter().enumerate() {
            m.serialize_entry(
                &c.logical,
                &QcView {
                    q: &self.state.channels[i],
                    stage: "written",
                    channel: &c.logical,
                    units: self.state.channel_units[i],
                    scope: "original units with a complete emitted record in this channel",
                },
            )?;
        }
        m.end()
    }
}
#[cfg(test)]
struct Dedup<'a>(&'a Document<'a>);
#[cfg(test)]
impl Serialize for Dedup<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        for (k, v) in self.0.header["dedup"].as_object().unwrap() {
            m.serialize_entry(k, v)?;
        }
        if let Some(c) = self.0.views.catalogue {
            m.serialize_entry("catalogue", c)?;
        }
        m.serialize_entry(
            "channel_qc",
            &Channels {
                state: self.0.state,
                channels: self.0.channels,
            },
        )?;
        m.end()
    }
}
#[cfg(test)]
impl Serialize for Document<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        for (k, v) in self.header.as_object().unwrap() {
            if k != "populations"
                && k != "dedup"
                && k != "settings"
                && !(self.views.api && k == "api")
                && !(self.snapshot && k == "status")
            {
                m.serialize_entry(k, v)?;
            }
        }
        if self.snapshot {
            m.serialize_entry("status", "snapshot")?;
            m.serialize_entry("authoritative", &false)?;
        }
        m.serialize_entry("settings", &Settings(self))?;
        if self.views.api {
            m.serialize_entry("api", &Api(self))?;
        }
        m.serialize_entry("populations", &Populations(self.state))?;
        m.serialize_entry("dedup", &Dedup(self))?;
        m.end()
    }
}
struct Count(usize);
impl Write for Count {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(b.len())
            .ok_or_else(|| io::Error::other("report size overflow"))?;
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Fixed<'a> {
    data: &'a mut Vec<u8>,
    limit: usize,
}
impl Write for Fixed<'_> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if b.len() > self.limit - self.data.len() {
            return Err(io::Error::other("counted report capacity exceeded"));
        }
        self.data.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[cfg(test)]
pub(crate) fn encode(
    header: &mut Value,
    state: &State,
    channels: &[Channel],
    e: &std::sync::Arc<crate::native_execution::Execution>,
    views: &Views<'_>,
) -> Result<(String, crate::native_execution::FeatureTicket), String> {
    encode_at(header,state,channels,e,views,2)
}
#[cfg(test)]
pub(crate) fn encode_at(header:&mut Value,state:&State,channels:&[Channel],e:&std::sync::Arc<crate::native_execution::Execution>,views:&Views<'_>,site:u8)->Result<(String,crate::native_execution::FeatureTicket),String>{
    let mut count = Count(0);
    serde_json::to_writer(
        &mut count,
        &Document {
            header,
            state,
            channels,
            snapshot: false,
            views,
        },
    )
    .map_err(|e| e.to_string())?;
    let cap = count.0.checked_add(4096).ok_or("report size overflow")?;
    let ticket = crate::dedup_app::report_ticket(e,cap,site)?;
    header["execution"] = e.facts();
    let mut data = Vec::new();
    data.try_reserve_exact(cap).map_err(|e| e.to_string())?;
    if data.capacity() != cap {
        return Err("report capacity mismatch".into());
    }
    serde_json::to_writer(
        Fixed {
            data: &mut data,
            limit: cap,
        },
        &Document {
            header,
            state,
            channels,
            snapshot: false,
            views,
        },
    )
    .map_err(|e| e.to_string())?;
    Ok((String::from_utf8(data).map_err(|e| e.to_string())?, ticket))
}

impl Serialize for Declaration<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(2))?;
        m.serialize_entry("lane_id", self.lane_id)?;
        m.serialize_entry("inputs", &self.inputs)?;
        m.end()
    }
}
impl Serialize for DeclaredInput<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(3))?;
        m.serialize_entry("role", self.role)?;
        m.serialize_entry("phred", &self.phred)?;
        m.serialize_entry("codec", self.codec)?;
        m.end()
    }
}
impl Serialize for Binding<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(2))?;
        m.serialize_entry("lane", self.lane)?;
        m.serialize_entry("inputs", &self.inputs)?;
        m.end()
    }
}
impl Serialize for BindingInput<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(7))?;
        m.serialize_entry("channel", self.channel)?;
        m.serialize_entry("declared_seekable", &self.declared_seekable)?;
        m.serialize_entry("actual_binding_capability", self.actual_binding_capability)?;
        m.serialize_entry("actual_seekable", &self.actual_seekable)?;
        m.serialize_entry("requested_decoder", self.requested_decoder)?;
        m.serialize_entry("selected_backend", &self.selected_backend)?;
        m.serialize_entry("selection_reason", self.selection_reason)?;
        m.end()
    }
}
impl Serialize for crate::dedup_app::CatalogueMetadata {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(4))?;
        m.serialize_entry("schema", &self.schema)?;
        m.serialize_entry("catalogue_version", &self.catalogue_version)?;
        m.serialize_entry("sha256", &self.sha256)?;
        m.serialize_entry("path", &self.path)?;
        m.end()
    }
}

pub(crate) struct Emergency {
    text: String,
    compact_tried: bool,
    compact_owner: Option<crate::native_execution::FeatureTicket>,
    pub(crate) _ticket: crate::native_execution::FeatureTicket,
}
#[derive(Clone, Copy)]
pub(crate) struct Failure<'a> {
    pub code: i32,
    pub category: &'static str,
    pub stage: &'static str,
    pub ordinal: Option<u64>,
    pub lane: Option<usize>,
    pub message: &'a str,
    pub secondary: &'static str,
    pub cleanup_failed: bool,
    pub cleanup_stage: Option<&'static str>,
    pub admitted: bool,
    pub publication_failed: bool,
    pub publication_cleanup_stage: Option<&'static str>,
}
impl Emergency {
    const CAPACITY: usize = 16384;
    pub(crate) fn new(e: &std::sync::Arc<crate::native_execution::Execution>) -> Result<Self,String> {
        let ticket=e.reserve_preparation_feature(Self::CAPACITY+std::mem::size_of::<Self>()+std::mem::size_of::<State>()+4096).map_err(|e| e.to_string())?;
        let mut text=String::new();text.try_reserve_exact(Self::CAPACITY).map_err(|e|e.to_string())?;
        if text.capacity()!=Self::CAPACITY{return Err("emergency report capacity mismatch".into());}
        Ok(Self{text,compact_tried:false,compact_owner:None,_ticket:ticket})
    }
    pub(crate) fn failed(&mut self,state:&State,f:Failure<'_>,e:&std::sync::Arc<crate::native_execution::Execution>)->(String,Option<crate::native_execution::FeatureTicket>){
        if !self.compact_tried {
            self.compact_tried=true;
            let compact=Compact{state,f};
            let mut count=Count(0);
            if serde_json::to_writer(&mut count,&compact).is_ok(){
                if let Some(cap)=count.0.checked_add(256){if let Ok(ticket)=crate::dedup_app::report_ticket(e,cap,5){
                    let mut bytes=Vec::new();
                    if bytes.try_reserve_exact(cap).is_ok()&&bytes.capacity()==cap&&serde_json::to_writer(Fixed{data:&mut bytes,limit:cap},&compact).is_ok(){
                        if let Ok(text)=String::from_utf8(bytes){self.compact_owner=Some(ticket);return(text,None);}
                    }
                }}
            }
        }
        let out=&mut self.text;out.clear();
        out.push_str("{\"schema\":\"soapnuke-v3.report/2\",\"status\":\"failed\",\"run\":{\"job\":\"dedup\"},\"api\":{\"execution_admitted\":");
        out.push_str(if f.admitted{"true"}else{"false"});out.push_str("},\"disposition\":");counts(out,state);out.push_str(",\"dedup\":{\"dedup\":");counts(out,state);
        out.push_str(",\"api\":{\"input_units\":");number(out,state.input_units);out.push_str(",\"accepted_units\":");optional(out,state.representatives);out.push_str(",\"written_units\":");number(out,state.written_units);out.push_str("}},\"populations\":{\"input\":null,\"accepted\":null,\"written\":null},\"population_summaries\":{");
        for (i,(name,q,units)) in [("input",&state.input,Some(state.input_units)),("accepted",&state.accepted,state.representatives),("written",&state.written,Some(state.written_units))].into_iter().enumerate(){
            if i!=0{out.push(',');}quoted(out,name);out.push(':');
            if let Some(units)=units{out.push_str("{\"source_units\":");number(out,units);out.push_str(",\"read_records\":");number(out,q.reads);out.push_str(",\"bases\":");number(out,q.bases.iter().sum());out.push_str(",\"q20_bases\":");number(out,q.qualities[20..].iter().sum());out.push_str(",\"q30_bases\":");number(out,q.qualities[30..].iter().sum());out.push('}');}else{out.push_str("null");}
        }
        out.push_str("},\"error\":{\"category\":");quoted(out,f.category);out.push_str(",\"stage\":");quoted(out,f.stage);out.push_str(",\"code\":");number(out,f.code as u64);out.push_str(",\"source_unit_ordinal\":");optional(out,f.ordinal);out.push_str(",\"lane_index\":");optional(out,f.lane.map(|n|n as u64));
        out.push_str(",\"message_excerpt\":");let mut end=f.message.len().min(512);while !f.message.is_char_boundary(end){end-=1;}quoted(out,&f.message[..end]);out.push_str(",\"message_truncated\":");out.push_str(if end<f.message.len(){"true"}else{"false"});
        out.push_str("},\"report_generation\":{\"status\":\"failed\",\"detailed_qc\":\"unavailable_report_generation\",\"metadata\":\"omitted\",\"identities_and_paths\":\"omitted_not_truncated\",\"secondary_stage\":");quoted(out,f.secondary);out.push_str(",\"cleanup_failed\":");out.push_str(if f.cleanup_failed{"true"}else{"false"});out.push_str(",\"cleanup_stage\":");if let Some(stage)=f.cleanup_stage{quoted(out,stage)}else{out.push_str("null")};out.push_str(",\"publication_cleanup_stage\":");if let Some(stage)=f.publication_cleanup_stage{quoted(out,stage)}else{out.push_str("null")};out.push_str(",\"publication_failed\":");out.push_str(if f.publication_failed{"true"}else{"false"});out.push_str("}}");
        (std::mem::take(&mut self.text),None)
    }
}
fn number(out:&mut String,mut n:u64){let mut digits=[0u8;20];let mut at=20;loop{at-=1;digits[at]=b'0'+(n%10)as u8;n/=10;if n==0{break;}}for b in &digits[at..]{out.push(*b as char);}}
fn optional(out:&mut String,n:Option<u64>){if let Some(n)=n{number(out,n)}else{out.push_str("null")}}
fn quoted(out:&mut String,s:&str){out.push('"');for c in s.chars(){match c{'"'=>out.push_str("\\\""),'\\'=>out.push_str("\\\\"),'\n'=>out.push_str("\\n"),'\r'=>out.push_str("\\r"),'\t'=>out.push_str("\\t"),c if c<' '=>{out.push_str("\\u00");let n=c as u8;out.push(b"0123456789abcdef"[(n>>4)as usize]as char);out.push(b"0123456789abcdef"[(n&15)as usize]as char);},_=>out.push(c)}}out.push('"');}
fn counts(out:&mut String,s:&State){out.push_str("{\"input_units\":");number(out,s.input_units);out.push_str(",\"accepted_units\":");optional(out,s.representatives);out.push_str(",\"representative_units\":");optional(out,s.representatives);out.push_str(",\"duplicate_units\":");optional(out,s.duplicates);out.push_str(",\"written_units\":");number(out,s.written_units);out.push_str(",\"grouping_complete\":");out.push_str(if s.representatives.is_some(){"true"}else{"false"});out.push_str(",\"unavailable_reason\":");if s.representatives.is_some(){out.push_str("null")}else{quoted(out,"full-input classification and representative QC preparation not complete")};out.push('}');}
struct Compact<'a>{state:&'a State,f:Failure<'a>}
impl Serialize for Compact<'_>{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{
    struct Fields<'a>(&'a State);impl Serialize for Fields<'_>{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let mut m=s.serialize_map(None)?;m.serialize_entry("input_units",&self.0.input_units)?;m.serialize_entry("accepted_units",&self.0.representatives)?;m.serialize_entry("representative_units",&self.0.representatives)?;m.serialize_entry("duplicate_units",&self.0.duplicates)?;m.serialize_entry("written_units",&self.0.written_units)?;m.serialize_entry("grouping_complete",&self.0.representatives.is_some())?;m.serialize_entry("unavailable_reason",&if self.0.representatives.is_some(){None}else{Some("full-input classification and representative QC preparation not complete")})?;m.end()}}
    struct Phase<'a>(&'a State);impl Serialize for Phase<'_>{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let mut m=s.serialize_map(None)?;m.serialize_entry("dedup",&Fields(self.0))?;m.serialize_entry("api",&Fields(self.0))?;m.end()}}
    struct Error<'a>(Failure<'a>);impl Serialize for Error<'_>{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let f=self.0;let mut m=s.serialize_map(None)?;m.serialize_entry("code",&f.code)?;m.serialize_entry("category",f.category)?;m.serialize_entry("stage",f.stage)?;m.serialize_entry("source_unit_ordinal",&f.ordinal)?;m.serialize_entry("lane_index",&f.lane)?;let mut end=f.message.len().min(512);while !f.message.is_char_boundary(end){end-=1;}m.serialize_entry("message_excerpt",&f.message[..end])?;m.serialize_entry("message_truncated",&(end<f.message.len()))?;m.end()}}
    struct Marker<'a>(Failure<'a>);impl Serialize for Marker<'_>{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let mut m=s.serialize_map(None)?;m.serialize_entry("status","failed")?;m.serialize_entry("detailed_qc","retained_compact")?;m.serialize_entry("metadata","omitted")?;m.serialize_entry("identities_and_paths","omitted_not_truncated")?;m.serialize_entry("secondary_stage",self.0.secondary)?;m.serialize_entry("cleanup_failed",&self.0.cleanup_failed)?;m.serialize_entry("cleanup_stage",&self.0.cleanup_stage)?;m.serialize_entry("publication_failed",&self.0.publication_failed)?;m.serialize_entry("publication_cleanup_stage",&self.0.publication_cleanup_stage)?;m.end()}}
    struct Job;impl Serialize for Job{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let mut m=s.serialize_map(Some(1))?;m.serialize_entry("job","dedup")?;m.end()}}struct Api(bool);impl Serialize for Api{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let mut m=s.serialize_map(Some(1))?;m.serialize_entry("execution_admitted",&self.0)?;m.end()}}
    let mut m=s.serialize_map(None)?;m.serialize_entry("schema","soapnuke-v3.report/2")?;m.serialize_entry("status","failed")?;m.serialize_entry("run",&Job)?;
    m.serialize_entry("api",&Api(self.f.admitted))?;
    m.serialize_entry("disposition",&Fields(self.state))?;m.serialize_entry("dedup",&Phase(self.state))?;m.serialize_entry("populations",&Populations(self.state))?;m.serialize_entry("error",&Error(self.f))?;m.serialize_entry("report_generation",&Marker(self.f))?;m.end()
}}

#[cfg(test)]
mod r2_capacity {
 use super::*;
 #[test]
 fn emergency_upper_endpoint_and_admission(){
  use std::sync::{Arc,atomic::AtomicBool};let e=crate::native_execution::Execution::new(1,1,Arc::new(AtomicBool::new(false))).unwrap();let caller=e.reserve_preparation_feature(1<<20).unwrap();let capacity=e.facts()["chunk_capacity_bytes"].as_u64().unwrap()as usize;
  let held=e.reserve_preparation_feature(capacity-(1<<20)-1024).unwrap();assert!(Emergency::new(&e).is_err());assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],capacity-1024);drop(held);
  let mut emergency=Emergency::new(&e).unwrap();emergency.compact_tried=true;let ptr=emergency.text.as_ptr();let cap=emergency.text.capacity();
  let mut state=State::unstarted();state.input_units=u64::MAX;state.representatives=Some(u64::MAX);state.duplicates=Some(0);state.written_units=u64::MAX;for q in [&mut state.input,&mut state.accepted,&mut state.written]{q.reads=u64::MAX;q.bases[0]=u64::MAX;q.qualities[40]=u64::MAX;}
  let message="\0".repeat(1024);let f=Failure{code:5,category:"report-generation",stage:"report-generation",ordinal:Some(u64::MAX),lane:Some(usize::MAX),message:&message,secondary:"json-reencode",cleanup_failed:true,cleanup_stage:Some("internal-file"),admitted:true,publication_failed:true,publication_cleanup_stage:Some("stage-inspection")};let(text,ticket)=emergency.failed(&state,f,&e);assert!(ticket.is_none());assert_eq!(text.as_ptr(),ptr);assert_eq!(text.capacity(),cap);assert_eq!(cap,16384);assert!(text.len()<8192);let v:Value=serde_json::from_str(&text).unwrap();assert_eq!(v["disposition"]["input_units"],u64::MAX);assert_eq!(v["population_summaries"]["input"]["bases"],u64::MAX);assert_eq!(v["error"]["message_excerpt"].as_str().unwrap().len(),512);assert_eq!(v["error"]["message_truncated"],true);assert!(v["populations"]["input"].is_null());println!("R84_EMERGENCY_BOUND bytes={} capacity={} fixed_owner={} state={} reservation={}",text.len(),cap,std::mem::size_of::<Emergency>(),std::mem::size_of::<State>(),emergency._ticket.reserved_bytes());drop(v);drop(text);drop(emergency);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],1<<20);drop(caller);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
 }
}

#[path="dedup_header.rs"]mod typed;
pub(crate) use typed::{Working as HeaderWorking,Header,Output,TypedDocument,typed_encode};
