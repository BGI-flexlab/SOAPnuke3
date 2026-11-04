use serde::ser::{Serialize,SerializeMap,Serializer};
use serde_json::Value;
use std::io::{self,Write};

struct Snapshot<'a>(&'a Value);
impl Serialize for Snapshot<'_> {
    fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{
        let mut map=s.serialize_map(Some(5))?;
        for key in ["schema","unit_definitions","populations","disposition","processing"]{map.serialize_entry(key,&self.0[key])?;}map.end()
    }
}
struct ScriptFormatter;
impl serde_json::ser::Formatter for ScriptFormatter {
    fn write_string_fragment<W:Write+?Sized>(&mut self,w:&mut W,s:&str)->io::Result<()>{
        for c in s.chars(){match c{'<'=>w.write_all(b"\\u003c")?,'>'=>w.write_all(b"\\u003e")?,'&'=>w.write_all(b"\\u0026")?,'\u{2028}'=>w.write_all(b"\\u2028")?,'\u{2029}'=>w.write_all(b"\\u2029")?,_=>{let mut bytes=[0;4];w.write_all(c.encode_utf8(&mut bytes).as_bytes())?;}}}Ok(())
    }
}
fn text<W:Write>(w:&mut W,s:&str)->io::Result<()>{for c in s.chars(){match c{'&'=>w.write_all(b"&amp;")?,'<'=>w.write_all(b"&lt;")?,'>'=>w.write_all(b"&gt;")?,'"'=>w.write_all(b"&quot;")?,'\''=>w.write_all(b"&#39;")?,_=>{let mut bytes=[0;4];w.write_all(c.encode_utf8(&mut bytes).as_bytes())?;}}}Ok(())}
pub fn write<W:Write>(w:&mut W,report:&Value)->io::Result<()>{
    w.write_all(b"<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"><title>SOAPnuke process metrics</title><style>body{font:16px system-ui;max-width:72rem;margin:2rem auto;padding:0 1rem;color:#182a36}table{border-collapse:collapse;width:100%;margin:1rem 0}th,td{padding:.5rem;border-bottom:1px solid #ccd;text-align:left}.bar{height:.7rem;background:#246a96;max-width:100%}small{color:#456}code{overflow-wrap:anywhere}</style><h1>SOAPnuke process metrics</h1><p><strong>Non-authoritative metric snapshot.</strong> Native JSON is the sole final job completion marker. This HTML may remain if required JSON publication fails; its existence does not establish job success.</p><p>Sample: ")?;
    text(w,report["run"]["sample_id"].as_str().unwrap_or(""))?;
    w.write_all(b"</p><h2>QC populations</h2><table><tr><th>Stage</th><th>Source units</th><th>Read records</th><th>Bases</th><th>Relative observed units</th></tr>")?;
    let input=report["disposition"]["input_units"].as_u64().unwrap_or(0);
    for stage in ["input","accepted","written"]{
        write!(w,"<tr><th>{stage}</th>")?;let p=&report["populations"][stage];
        if p.is_null(){w.write_all(b"<td colspan=\"4\">N/A: not evaluated or no FASTQ writer</td></tr>")?;continue;}
        let units=p["units"].as_u64().or_else(||p["source_units"].as_u64()).unwrap_or(0);
        for key in ["source_units","read_records","bases"]{w.write_all(b"<td>")?;if let Some(v)=p.get(key){serde_json::to_writer(&mut *w,v)?;}else{w.write_all(b"N/A")?;}w.write_all(b"</td>")?;}
        let percent=if input==0{0.0}else{100.0*units as f64/input as f64};write!(w,"<td><div class=\"bar\" style=\"width:{:.3}%\"></div><small>{:.3}% of observed input units; stages are not additive</small></td></tr>",percent.min(100.0),percent)?;
    }
    w.write_all(b"</table><h2>Definitions and enabled process metrics</h2><p>Qualities are numerical Phred scores. Inspection, transformed, accepted and written populations have distinct denominators. Repetition concerns only its observed original-input prefix; diagnostic channels overlap other populations.</p><details><summary>Metric data and exact definitions</summary><pre id=\"metric-view\"></pre></details><script type=\"application/json\" id=\"soapnuke-report\">")?;
    Snapshot(report).serialize(&mut serde_json::Serializer::with_formatter(&mut *w,ScriptFormatter)).map_err(io::Error::other)?;
    w.write_all(b"</script><script>document.getElementById('metric-view').textContent=JSON.stringify(JSON.parse(document.getElementById('soapnuke-report').textContent),null,2);</script></html>\n")?;Ok(())
}
#[cfg(test)]
mod tests {
 use super::*;
 #[test]
 fn m8_html_embedded_groups_escape_and_write_failure(){
  let r=serde_json::json!({"run":{"sample_id":"</script><img src=x onerror=evil>&\u{2028}\u{2029}"},"schema":"soapnuke-v3.report/2","unit_definitions":{"hostile":"</script>&\u{2028}\u{2029}"},"populations":{"input":{"units":2,"read_records":2,"bases":8},"accepted":null,"written":null},"disposition":{"input_units":2},"processing":{"qc_only":true},"status":"complete"});
  let mut out=Vec::new();write(&mut out,&r).unwrap();let s=String::from_utf8(out).unwrap();assert!(!s.contains("<img"));assert!(s.contains("&lt;/script&gt;"));assert!(!s.contains("\"status\":\"complete\""));let data=s.split("id=\"soapnuke-report\">").nth(1).unwrap().split("</script>").next().unwrap();assert!(!data.contains('<'));assert!(!data.contains('\u{2028}'));let got:Value=serde_json::from_str(data).unwrap();for k in ["schema","unit_definitions","populations","disposition","processing"]{assert_eq!(got[k],r[k]);}
  struct Fail;impl Write for Fail{fn write(&mut self,_:&[u8])->io::Result<usize>{Err(io::Error::other("owned write failure"))}fn flush(&mut self)->io::Result<()>{Ok(())}}assert!(write(&mut Fail,&r).is_err());
 }
}

pub(crate) fn write_saved<W:Write>(w:&mut W,report:&crate::report_app::Document<'_>)->io::Result<()>{
    let value=|p:&[&str]|report.text(p).map_err(io::Error::other);
    w.write_all(b"<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><title>SOAPnuke saved report</title><style>body{font:16px system-ui;max-width:72rem;margin:2rem auto;padding:1rem;color:#182a36}table{border-collapse:collapse;width:100%}td,th{padding:.5rem;border-bottom:1px solid #ccd}pre{overflow:auto}</style><h1>SOAPnuke saved report</h1><p>This view preserves saved source status and metrics. It is not a new processing or release completion marker.</p><p>Sample: ")?;text(w,&value(&["run","sample_id"])?.unwrap_or_default())?;w.write_all(b"; source status: <strong>")?;text(w,&value(&["status"])?.unwrap_or_default())?;w.write_all(b"</strong></p><table><tr><th>Population</th><th>Source units</th><th>Reads</th><th>Bases</th></tr>")?;
    for stage in ["input","accepted","written"]{write!(w,"<tr><th>{stage}</th>")?;let p=report.get(&["populations",stage]).map_err(io::Error::other)?;if p.is_none_or(|v|v.get()=="null"){if report.text(&["report_generation","detailed_qc"]).map_err(io::Error::other)?.as_deref()==Some("unavailable_report_generation"){w.write_all(b"<td colspan=\"3\">Unavailable: detailed report generation failed; known counts remain in the JSON summaries</td>")?;}else{w.write_all(b"<td colspan=\"3\">N/A: not evaluated</td>")?;}}else{for key in ["source_units","read_records","bases"]{w.write_all(b"<td>")?;if let Some(v)=report.number(&["populations",stage,key]).map_err(io::Error::other)?{write!(w,"{v}")?;}else{w.write_all(b"null")?;}w.write_all(b"</td>")?;}}w.write_all(b"</tr>")?;}
    w.write_all(b"</table><details><summary>Exact saved metrics and definitions</summary><pre id=\"metric-view\"></pre></details><script type=\"application/json\" id=\"soapnuke-report\">")?;
    for c in report.raw().chars(){match c{'<'=>w.write_all(b"\\u003c")?,'>'=>w.write_all(b"\\u003e")?,'&'=>w.write_all(b"\\u0026")?,'\u{2028}'=>w.write_all(b"\\u2028")?,'\u{2029}'=>w.write_all(b"\\u2029")?,_=>{let mut b=[0;4];w.write_all(c.encode_utf8(&mut b).as_bytes())?;}}}
    w.write_all(b"</script><script>document.getElementById('metric-view').textContent=JSON.stringify(JSON.parse(document.getElementById('soapnuke-report').textContent),null,2);</script></html>\n")?;Ok(())
}

pub(crate) fn write_snapshot<W:Write,S:Serialize>(w:&mut W,sample:&str,data:&S,rows:[(&crate::native_qc::Qc,u64);3])->io::Result<()>{
    w.write_all(b"<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><title>SOAPnuke demux metrics</title><h1>SOAPnuke demux metrics</h1><p>Non-authoritative metric snapshot. Native JSON is the final completion marker.</p><p>Sample: ")?;text(w,sample)?;w.write_all(b"</p><table><tr><th>Population</th><th>Source units</th><th>Reads</th><th>Bases</th></tr>")?;
    for(stage,(q,units))in ["input","accepted","written"].iter().zip(rows){write!(w,"<tr><th>{stage}</th><td>{units}</td><td>{}</td><td>{}</td></tr>",q.reads,q.bases.iter().sum::<u64>())?;}w.write_all(b"</table><pre id=\"metric-view\"></pre><script type=\"application/json\" id=\"soapnuke-report\">")?;
    data.serialize(&mut serde_json::Serializer::with_formatter(&mut *w,ScriptFormatter)).map_err(io::Error::other)?;w.write_all(b"</script><script>document.getElementById('metric-view').textContent=JSON.stringify(JSON.parse(document.getElementById('soapnuke-report').textContent),null,2);</script></html>\n")
}

pub(crate) fn write_nullable_snapshot<W:Write,S:Serialize>(w:&mut W,sample:&str,data:&S,rows:[Option<(&crate::native_qc::Qc,u64)>;3])->io::Result<()>{
 w.write_all(b"<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><title>SOAPnuke dedup metrics</title><h1>SOAPnuke dedup metrics</h1><p>Non-authoritative metric snapshot. Native JSON is the final completion marker.</p><p>Sample: ")?;text(w,sample)?;w.write_all(b"</p><table><tr><th>Population</th><th>Source units</th><th>Reads</th><th>Bases</th></tr>")?;for(stage,row)in ["input","accepted","written"].iter().zip(rows){write!(w,"<tr><th>{stage}</th>")?;if let Some((q,n))=row{write!(w,"<td>{n}</td><td>{}</td><td>{}</td>",q.reads,q.bases.iter().sum::<u64>())?;}else{w.write_all(b"<td colspan=\"3\">N/A: grouping not completed</td>")?;}w.write_all(b"</tr>")?;}w.write_all(b"</table><pre id=\"metric-view\"></pre><script type=\"application/json\" id=\"soapnuke-report\">")?;data.serialize(&mut serde_json::Serializer::with_formatter(&mut *w,ScriptFormatter)).map_err(io::Error::other)?;w.write_all(b"</script><script>document.getElementById('metric-view').textContent=JSON.stringify(JSON.parse(document.getElementById('soapnuke-report').textContent),null,2);</script></html>\n")
}
