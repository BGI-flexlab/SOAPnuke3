use crate::job_api::JobSpec;
use serde::ser::{Serialize,Serializer,SerializeMap,SerializeSeq};
pub(crate) use crate::decoder::{Decoder, Snapshot};
pub(crate) struct View<'a>{pub snapshot:Snapshot,pub spec:&'a JobSpec}
struct Lanes<'a>(&'a [Option<Decoder>;2]);impl Serialize for Lanes<'_>{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let mut a=s.serialize_seq(Some(self.0.iter().flatten().count()))?;for v in self.0.iter().flatten(){a.serialize_element(v)?;}a.end()}}
impl Serialize for View<'_>{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let mut m=s.serialize_map(None)?;m.serialize_entry("lanes",&Lanes(&self.snapshot.lanes))?;if let Some(i)=self.snapshot.current{let lane=self.spec.lanes.get(i).ok_or_else(||serde::ser::Error::custom("decoder declaration association mismatch"))?;m.serialize_entry("current_lane_id",&lane.id)?;m.serialize_entry("opened_lane_groups",&(i+1))?;m.serialize_entry("layout_source","bounded existing readers on producer; decode/framing under source CPU credit")?;}else{m.serialize_entry("scope",self.snapshot.scope)?;}m.end()}}
