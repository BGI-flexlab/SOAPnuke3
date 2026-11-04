// Filter algorithm: faithful port of v2's read_filter.cpp / sequence.cpp /
// peprocess.cpp (stat_read, fastq_trim, adapter_pos, hasContam, pe_discard,
// filter_pe_fqs, stat_pe_fqs, quality encoding check, cal_quar_from_array).
use crate::params::{GlobalParams, READ_MAX_LEN};
use crate::record::FastqRecord;
use crate::stats::FileStat;

#[cfg(test)]
#[path = "filter_regression_tests.rs"]
mod filter_regression_tests;

pub const MIN_QUAL: i32 = 0;

#[derive(Clone, Copy)]
struct HardTrimSpec {
    head: i32,
    tail: i32,
}

fn parse_hard_trim(gp: &GlobalParams, paired: bool) -> Result<Option<[HardTrimSpec; 2]>, String> {
    if gp.trim.is_empty() {
        return Ok(None);
    }
    let values: Vec<i32> = gp
        .trim
        .split(',')
        .map(|value| {
            let parsed = value
                .parse::<i32>()
                .map_err(|_| format!("invalid trim value: {}", value))?;
            if parsed < 0 {
                return Err(format!("trim values must be non-negative: {}", value));
            }
            Ok(parsed)
        })
        .collect::<Result<_, String>>()?;
    let expected = if paired { 4 } else { 2 };
    if values.len() != expected {
        return Err(format!("trim requires {} integer values", expected));
    }
    let mut specs = [HardTrimSpec { head: 0, tail: 0 }; 2];
    specs[0] = HardTrimSpec {
        head: values[0],
        tail: values[1],
    };
    if paired {
        specs[1] = HardTrimSpec {
            head: values[2],
            tail: values[3],
        };
    }
    Ok(Some(specs))
}

fn apply_hard_trim(read: &mut FastqRecord, spec: HardTrimSpec) {
    // Keep the numeric fields as the per-record trim metadata.  The command
    // line string is parsed once per batch, not once per read.
    read.head_hdcut = spec.head;
    read.tail_hdcut = spec.tail;
}

#[derive(Clone, Default)]
pub struct ReadResult {
    pub a_num: i32,
    pub c_num: i32,
    pub g_num: i32,
    pub t_num: i32,
    pub n_num: i32,
    pub include_adapter_seq: i32,
    pub include_contam: i32,
    pub include_global_contam: i32,
    pub low_qual_base_ratio: f32,
    pub mean_quality: f32,
    pub contig_base: i32,
    pub over_lapped: bool,
    pub read_tile: String,
    pub read_fov: String,
}

pub fn validated_trim_raw_length(read: &FastqRecord, label: &str) -> Result<i32, String> {
    let context = if label.is_empty() { "read" } else { label };
    if read.sequence.len() > READ_MAX_LEN || read.qual_seq.len() > READ_MAX_LEN {
        return Err(format!(
            "{} sequence/quality length exceeds supported maximum of {}",
            context, READ_MAX_LEN
        ));
    }
    let mut raw_length = read.raw_length;
    if raw_length == 0 {
        raw_length = read.sequence.len() as i32;
    }
    if raw_length < 0 || raw_length as usize > READ_MAX_LEN {
        return Err(format!(
            "{} raw_length={} is outside supported range [0,{}]",
            context, read.raw_length, READ_MAX_LEN
        ));
    }
    let names = ["head_hdcut", "head_lqcut", "tail_hdcut", "tail_lqcut", "adacut_pos"];
    let values = [
        read.head_hdcut,
        read.head_lqcut,
        read.tail_hdcut,
        read.tail_lqcut,
        read.adacut_pos,
    ];
    for i in 0..5 {
        if values[i] < -1 || values[i] > READ_MAX_LEN as i32 {
            return Err(format!(
                "{} trim metadata {}={} is outside supported range [-1,{}]",
                context, names[i], values[i], READ_MAX_LEN
            ));
        }
    }
    let tail_cut = 0i32.max(read.tail_hdcut.max(read.tail_lqcut.max(read.adacut_pos)));
    if tail_cut > 0 {
        let tail_position = raw_length - tail_cut + 1;
        if tail_position < 1 || tail_position as usize > READ_MAX_LEN {
            return Err(format!(
                "{} derived tail trim position {} is outside [1,{}] (raw_length={}, tail_cut={})",
                context, tail_position, READ_MAX_LEN, raw_length, tail_cut
            ));
        }
    }
    Ok(raw_length)
}

fn check_tile_or_fov(tile: &str, tile_parameter: &str) -> bool {
    // Inclusive numeric ranges are an intentional repair, not a literal copy
    // of v2's range loop, which compared the wrong string.
    tile_parameter.split(',').any(|raw| {
        let item = raw.trim();
        if item.is_empty() {
            return false;
        }
        if let Some((lo_text, hi_text)) = item.split_once('-') {
            let Ok(lo) = lo_text.trim().parse::<i32>() else {
                return false;
            };
            let Ok(hi) = hi_text.trim().parse::<i32>() else {
                return false;
            };
            let Ok(value) = tile.trim().parse::<i32>() else {
                return false;
            };
            return lo <= value && value <= hi;
        }
        tile == item
    })
}

fn extract_tile(seq_id: &str, seq_type: &str) -> Option<String> {
    let required_colons = if seq_type == "0" { 2 } else { 4 };
    let bytes = seq_id.as_bytes();
    let mut colons = 0usize;
    let mut offset = None;
    for (i, byte) in bytes.iter().enumerate() {
        if *byte == b':' {
            colons += 1;
            if colons == required_colons {
                offset = Some(i);
                break;
            }
        }
    }
    let offset = offset?;
    let end = offset.checked_add(5)?;
    if end > bytes.len() {
        return None;
    }
    let tile = &bytes[offset + 1..end];
    if tile.iter().all(u8::is_ascii_digit) {
        Some(String::from_utf8_lossy(tile).into_owned())
    } else {
        None
    }
}

fn extract_fov(seq_id: &str) -> Option<String> {
    let bytes = seq_id.as_bytes();
    for i in 0..bytes.len() {
        let Some(end) = i.checked_add(8) else {
            continue;
        };
        if bytes[i] == b'C' && end <= bytes.len() && bytes[i + 4] == b'R' {
            return seq_id.get(i..end).map(str::to_owned);
        }
    }
    None
}

pub fn stat_read(
    read: &mut FastqRecord,
    gp: &GlobalParams,
    mut stat: Option<&mut FileStat>,
) -> Result<ReadResult, String> {
    let mut r = ReadResult {
        include_adapter_seq: -1,
        include_contam: -1,
        include_global_contam: -1,
        ..Default::default()
    };
    r.include_adapter_seq = -1;
    r.include_contam = -1;
    r.include_global_contam = -1;
    read.raw_length = read.sequence.len() as i32;
    if !gp.tile.is_empty() {
        r.read_tile = extract_tile(&read.seq_id, &gp.seq_type).unwrap_or_else(|| {
            eprintln!("Warning:input fastq maybe not include tile information");
            String::new()
        });
    }
    if !gp.fov.is_empty() {
        if gp.seq_type == "0" {
            r.read_fov = extract_fov(&read.seq_id).unwrap_or_else(|| {
                eprintln!("Warning:input fastq maybe not include fov information");
                String::new()
            });
        } else {
            return Err("Warning:Zebra-500 data(--fov), --seqType is 0".into());
        }
    }
    if gp.module_name == "filtersRNA" {
        let ada_pos = s_rna_find_adapter(&read.sequence, &gp.adapter2_seq, gp);
        r.include_adapter_seq = ada_pos;
    } else {
        let mut ada_pos = -1i32;
        let adapters = if read.pair_pos == 1 {
            &gp.ada1s
        } else {
            &gp.ada2s
        };
        for ada_seq in adapters {
            ada_pos = adapter_pos(&read.sequence, ada_seq, gp);
            if ada_pos >= 0 {
                break;
            }
        }
        if ada_pos >= 0 {
            r.include_adapter_seq = 1;
            read.adacut_pos = read.sequence.len() as i32 - ada_pos;
        }
        let contam_seq = if read.pair_pos == 1 {
            &gp.contam1_seq
        } else {
            &gp.contam2_seq
        };
        if !contam_seq.contains(',') {
            read.contam_pos = has_contam(&read.sequence, contam_seq, gp)?;
            if read.contam_pos >= 0 {
                r.include_contam = 1;
            }
        } else {
            let contam_poses = has_contams(&read.sequence, contam_seq, gp)?;
            for p in contam_poses {
                if p >= 0 {
                    r.include_contam = 1;
                    if read.contam_pos != -1 {
                        if p <= read.contam_pos {
                            read.contam_pos = p;
                        }
                    } else {
                        read.contam_pos = p;
                    }
                }
            }
        }
        if !gp.global_contams.is_empty() {
            let poses = has_global_contams(&read.sequence, gp)?;
            for p in poses {
                if p >= 0 {
                    r.include_global_contam = 1;
                    if read.global_contam_5pos != -1 {
                        if p <= read.global_contam_5pos {
                            read.global_contam_5pos = p;
                        }
                    } else {
                        read.global_contam_5pos = p;
                    }
                }
            }
            if read.global_contam_5pos != -1
                && (read.global_contam_5pos as usize) <= read.sequence.len() / 2
            {
                let rev = reverse_complementary(&read.sequence);
                let poses3 = has_global_contams(&rev, gp)?;
                for p in poses3 {
                    if p >= 0 {
                        r.include_global_contam = 1;
                        if read.global_contam_3pos != -1 {
                            if p <= read.global_contam_3pos {
                                read.global_contam_3pos = p;
                            }
                        } else {
                            read.global_contam_3pos = p;
                        }
                    }
                }
            }
            if read.global_contam_5pos >= 0 && read.global_contam_3pos >= 0 {
                if read.global_contam_5pos < read.global_contam_3pos {
                    read.global_contam_5pos = -1;
                } else {
                    read.global_contam_3pos = -1;
                }
            }
        }
    }
    if read.sequence.is_empty() {
        return Err("empty sequence".into());
    }
    if stat.is_some() {
        // stat_pe_fqs validates the read before accumulating; replicate the
        // check here so the fused pass fails identically.
        validated_trim_raw_length(read, "")?;
    }
    let seq_len = read.sequence.len();
    let bytes = read.sequence.as_bytes();
    let mut last_char = b'Q';
    let mut contig_base = 0i32;
    let mut max_contig = 1i32;
    for ix in 0..seq_len {
        if gp.poly_x_num != -1 {
            if bytes[ix] == last_char {
                contig_base += 1;
                if max_contig < contig_base {
                    max_contig = contig_base;
                }
            } else {
                contig_base = 1;
            }
        }
        last_char = bytes[ix];
        match bytes[ix] {
            b'a' | b'A' => {
                r.a_num += 1;
                if let Some(s) = stat.as_deref_mut() {
                    s.gs.a_number += 1;
                    s.bs.position_acgt_content[ix][0] += 1;
                }
            }
            b'c' | b'C' => {
                r.c_num += 1;
                if let Some(s) = stat.as_deref_mut() {
                    s.gs.c_number += 1;
                    s.bs.position_acgt_content[ix][1] += 1;
                }
            }
            b'g' | b'G' => {
                r.g_num += 1;
                if let Some(s) = stat.as_deref_mut() {
                    s.gs.g_number += 1;
                    s.bs.position_acgt_content[ix][2] += 1;
                }
            }
            b't' | b'T' => {
                r.t_num += 1;
                if let Some(s) = stat.as_deref_mut() {
                    s.gs.t_number += 1;
                    s.bs.position_acgt_content[ix][3] += 1;
                }
            }
            b'n' | b'N' => {
                r.n_num += 1;
                if let Some(s) = stat.as_deref_mut() {
                    s.gs.n_number += 1;
                    s.bs.position_acgt_content[ix][4] += 1;
                }
            }
            _ => {
                return Err(format!(
                    "unrecognized sequence,{}",
                    String::from_utf8_lossy(read.sequence.as_bytes())
                ));
            }
        }
    }
    r.contig_base = max_contig;
    let len_f = seq_len as f32;
    let qual_basis = gp.quality_phred;
    let i_low_qual_threshold = gp.low_qual;
    let mut total_base_quality = 0i64;
    let mut low_qual_base_num = 0i32;
    let mut q20_num = 0i32;
    let mut q30_num = 0i32;
    let qbytes = read.qual_seq.as_bytes();
    for ix in 0..seq_len {
        let base_quality = qbytes[ix] as i32 - qual_basis;
        total_base_quality += base_quality as i64;
        if base_quality <= i_low_qual_threshold {
            low_qual_base_num += 1;
        }
        if base_quality >= 20 {
            q20_num += 1;
        }
        if base_quality >= 30 {
            q30_num += 1;
        }
        if let Some(s) = stat.as_deref_mut() {
            if base_quality < MIN_QUAL {
                return Err("base quality index is below zero".into());
            }
            if ix >= READ_MAX_LEN || base_quality as usize >= gp.quality_bins() {
                return Err("quality matrix index is out of range".into());
            }
            let bins = gp.quality_bins();
            s.qs.position_qual[ix * bins + base_quality as usize] += 1;
            if base_quality >= 20 {
                s.gs.q20_num += 1;
            }
            if base_quality >= 30 {
                s.gs.q30_num += 1;
            }
        }
    }
    r.low_qual_base_ratio = low_qual_base_num as f32 / len_f;
    r.mean_quality = total_base_quality as f32 / len_f;
    Ok(r)
}

pub fn reverse_complementary(seq: &str) -> String {
    let mut out = String::with_capacity(seq.len());
    for b in seq.bytes().rev() {
        out.push(match b {
            b'a' | b'A' => 'T',
            b'c' | b'C' => 'G',
            b'g' | b'G' => 'C',
            b't' | b'T' => 'A',
            b'n' | b'N' => 'N',
            _ => b as char,
        });
    }
    out
}

fn line_split(s: &str, sep: char, out: &mut Vec<String>) {
    for part in s.split(sep) {
        out.push(part.to_string());
    }
}

pub fn has_contam(ref_sequence: &str, contam: &str, gp: &GlobalParams) -> Result<i32, String> {
    let read_len = ref_sequence.len();
    let contam_len = contam.len();
    if contam_len == 0 {
        return Ok(-1);
    }
    let ct_match_r = gp
        .ct_match_r
        .parse::<f32>()
        .map_err(|_| format!("invalid ctMatchR value: {}", gp.ct_match_r))?;
    let mis_grad = (contam_len as f32 - gp.ada_edge as f32) / (gp.ada_mis as f32 + 1.0);
    let seg_match_thr = (contam_len as f32 * ct_match_r).ceil() as i32;
    let seg_grad = if seg_match_thr - 7 + 1 == 0 {
        0.0
    } else {
        (contam_len as f32 - gp.ada_edge as f32) / (seg_match_thr - 7 + 1) as f32
    };
    let rb = ref_sequence.as_bytes();
    let cb = contam.as_bytes();
    for r1 in 0..(contam_len as i32 - gp.ada_edge) {
        let mut mis = 0i32;
        let mut max_seg_match = 0i32;
        let mis_match_temp = (r1 as f32 / mis_grad) as i32;
        let seg_match_temp = if seg_grad != 0.0 {
            7 + (r1 as f32 / seg_grad) as i32
        } else {
            7
        };
        let r1 = r1 as usize;
        for c in 0..(r1 + gp.ada_edge as usize) {
            if cb[contam_len - r1 - gp.ada_edge as usize + c] == rb[c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_temp {
                    return Ok(0);
                }
            } else if rb[c] != b'N' {
                mis += 1;
                max_seg_match = 0;
                if mis > mis_match_temp {
                    break;
                }
            }
        }
        if mis <= mis_match_temp {
            return Ok(0);
        }
    }
    for r1 in 0..=(read_len as i32 - contam_len as i32) {
        let r1 = r1 as usize;
        let mut max_seg_match = 0i32;
        let mut mis = 0i32;
        for c in 0..contam_len {
            if cb[c] == rb[r1 + c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_thr {
                    return Ok(r1 as i32);
                }
            } else if rb[r1 + c] != b'N' {
                mis += 1;
                max_seg_match = 0;
                if mis > gp.ada_mis {
                    break;
                }
            }
        }
        if mis <= gp.ada_mis {
            return Ok(r1 as i32);
        }
    }
    for r1 in 0..(contam_len as i32 - gp.ada_edge) {
        let mut mis = 0i32;
        let mut max_seg_match = 0i32;
        let mis_match_temp = (r1 as f32 / mis_grad) as i32;
        let seg_match_temp = 7 + (r1 as f32 / seg_grad) as i32;
        let r1 = r1 as usize;
        for c in 0..(r1 + gp.ada_edge as usize) {
            if cb[c] == rb[read_len - r1 - gp.ada_edge as usize + c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_temp {
                    return Ok((read_len - r1 - gp.ada_edge as usize) as i32);
                }
            } else if rb[read_len - r1 - gp.ada_edge as usize + c] != b'N' {
                mis += 1;
                max_seg_match = 0;
                if mis > mis_match_temp {
                    break;
                }
            }
        }
        if mis <= mis_match_temp {
            return Ok((read_len - r1 - gp.ada_edge as usize) as i32);
        }
    }
    Ok(-1)
}

pub fn has_contams(ref_sequence: &str, contam: &str, gp: &GlobalParams) -> Result<Vec<i32>, String> {
    let mut mrs: Vec<String> = Vec::new();
    let mut contams: Vec<String> = Vec::new();
    line_split(&gp.ct_match_r, ',', &mut mrs);
    line_split(contam, ',', &mut contams);
    if contams.len() != mrs.len() {
        return Err(format!(
            "the number of ctMatchR value should equal to that of contam sequences,{}.vs.{}",
            contams.len(),
            mrs.len()
        ));
    }
    let mut poses = Vec::new();
    for i in 0..contams.len() {
        let tmp_mr = mrs[i]
            .parse::<f32>()
            .map_err(|_| format!("invalid ctMatchR value: {}", mrs[i]))?;
        let tmp_pos = has_contam_mr(ref_sequence, &contams[i], gp, tmp_mr);
        poses.push(tmp_pos);
        if tmp_pos >= 0 && tmp_pos < gp.min_read_length {
            break;
        }
    }
    Ok(poses)
}

pub fn has_contam_mr(ref_sequence: &str, contam: &str, gp: &GlobalParams, mr: f32) -> i32 {
    let read_len = ref_sequence.len();
    let contam_len = contam.len();
    if contam_len == 0 {
        return -1;
    }
    let mis_grad = (contam_len as f32 - gp.ada_edge as f32) / (gp.ada_mis as f32 + 1.0);
    let seg_match_thr = (contam_len as f32 * mr).ceil() as i32;
    let seg_grad = if seg_match_thr - 7 + 1 == 0 {
        0.0
    } else {
        (contam_len as f32 - gp.ada_edge as f32) / (seg_match_thr - 7 + 1) as f32
    };
    let rb = ref_sequence.as_bytes();
    let cb = contam.as_bytes();
    for r1 in 0..(contam_len as i32 - gp.ada_edge) {
        let mut mis = 0i32;
        let mut max_seg_match = 0i32;
        let mis_match_temp = (r1 as f32 / mis_grad) as i32;
        let seg_match_temp = if seg_grad != 0.0 {
            7 + (r1 as f32 / seg_grad) as i32
        } else {
            7
        };
        let r1 = r1 as usize;
        for c in 0..(r1 + gp.ada_edge as usize) {
            if cb[contam_len - r1 - gp.ada_edge as usize + c] == rb[c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_temp {
                    return 0;
                }
            } else if rb[c] != b'N' {
                mis += 1;
                max_seg_match = 0;
                if mis > mis_match_temp {
                    break;
                }
            }
        }
        if mis <= mis_match_temp {
            return 0;
        }
    }
    for r1 in 0..=(read_len as i32 - contam_len as i32) {
        let r1 = r1 as usize;
        let mut max_seg_match = 0i32;
        let mut mis = 0i32;
        for c in 0..contam_len {
            if cb[c] == rb[r1 + c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_thr {
                    return r1 as i32;
                }
            } else if rb[r1 + c] != b'N' {
                mis += 1;
                max_seg_match = 0;
                if mis > gp.ada_mis {
                    break;
                }
            }
        }
        if mis <= gp.ada_mis {
            return r1 as i32;
        }
    }
    for r1 in 0..(contam_len as i32 - gp.ada_edge) {
        let mut mis = 0i32;
        let mut max_seg_match = 0i32;
        let mis_match_temp = (r1 as f32 / mis_grad) as i32;
        let seg_match_temp = 7 + (r1 as f32 / seg_grad) as i32;
        let r1 = r1 as usize;
        for c in 0..(r1 + gp.ada_edge as usize) {
            if cb[c] == rb[read_len - r1 - gp.ada_edge as usize + c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_temp {
                    return (read_len - r1 - gp.ada_edge as usize) as i32;
                }
            } else if rb[read_len - r1 - gp.ada_edge as usize + c] != b'N' {
                mis += 1;
                max_seg_match = 0;
                if mis > mis_match_temp {
                    break;
                }
            }
        }
        if mis <= mis_match_temp {
            return (read_len - r1 - gp.ada_edge as usize) as i32;
        }
    }
    -1
}

pub fn adapter_pos(ref_sequence: &str, adapter: &str, gp: &GlobalParams) -> i32 {
    let adpt_len = adapter.len();
    if adpt_len == 0 {
        return -1;
    }
    let read_len = ref_sequence.len();
    let min_edge5 = 5i32;
    let mis_grad5 = (adpt_len as f32 - min_edge5 as f32) / (gp.ada_mis as f32 + 1.0);
    let mis_grad = (adpt_len as f32 - gp.ada_edge as f32) / (gp.ada_mis as f32 + 1.0);
    let seg_match_thr = (adpt_len as f32 * gp.ada_mr).ceil() as i32;
    let rb = ref_sequence.as_bytes();
    let ab = adapter.as_bytes();
    for r1 in 1..=min_edge5 {
        let mut mis = 0i32;
        let mut max_seg_match = 0i32;
        let mis_match_temp = ((adpt_len as i32 - r1) as f32 / mis_grad5) as i32;
        let r1 = r1 as usize;
        for c in 0..(adpt_len - r1) {
            if ab[r1 + c] == rb[c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_thr {
                    return 0;
                }
            } else {
                mis += 1;
                max_seg_match = 0;
                if mis > mis_match_temp {
                    break;
                }
            }
        }
        if mis <= mis_match_temp {
            return 0;
        }
    }
    for r1 in 0..=(read_len as i32 - adpt_len as i32) {
        let r1 = r1 as usize;
        let mut max_seg_match = 0i32;
        let mut mis = 0i32;
        for c in 0..adpt_len {
            if ab[c] == rb[r1 + c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_thr {
                    return r1 as i32;
                }
            } else {
                mis += 1;
                max_seg_match = 0;
                if mis > gp.ada_mis {
                    break;
                }
            }
        }
        if mis <= gp.ada_mis {
            return r1 as i32;
        }
    }
    for r1 in 0..(adpt_len as i32 - gp.ada_edge) {
        let mut mis = 0i32;
        let mut max_seg_match = 0i32;
        let mis_match_temp = (r1 as f32 / mis_grad) as i32;
        let r1 = r1 as usize;
        for c in 0..(r1 + gp.ada_edge as usize) {
            if ab[c] == rb[read_len - r1 - gp.ada_edge as usize + c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_thr {
                    return (read_len - r1 - gp.ada_edge as usize) as i32;
                }
            } else {
                mis += 1;
                max_seg_match = 0;
                if mis > mis_match_temp {
                    break;
                }
            }
        }
        if mis <= mis_match_temp {
            return (read_len - r1 - gp.ada_edge as usize) as i32;
        }
    }
    -1
}

pub fn has_global_contams(ref_sequence: &str, gp: &GlobalParams) -> Result<Vec<i32>, String> {
    let mut contams: Vec<String> = Vec::new();
    let mut mrs: Vec<String> = Vec::new();
    let mut mms: Vec<String> = Vec::new();
    line_split(&gp.global_contams, ',', &mut contams);
    line_split(&gp.g_mrs, ',', &mut mrs);
    line_split(&gp.g_mms, ',', &mut mms);
    if contams.len() != mrs.len() || contams.len() != mms.len() {
        return Err("the number of gMR/gMM value should equal to that of global contam sequences".into());
    }
    let mut poses = Vec::new();
    for i in 0..contams.len() {
        let tmp_mr = mrs[i]
            .parse::<f32>()
            .map_err(|_| format!("invalid glob_cotm_mR value: {}", mrs[i]))?;
        let tmp_mm = mms[i]
            .parse::<i32>()
            .map_err(|_| format!("invalid glob_cotm_mM value: {}", mms[i]))?;
        let tmp_pos = has_global_contam(ref_sequence, &contams[i], tmp_mr, tmp_mm);
        poses.push(tmp_pos);
    }
    Ok(poses)
}

pub fn has_global_contam(ref_sequence: &str, contam: &str, mr: f32, mm: i32) -> i32 {
    let read_len = ref_sequence.len();
    let contam_len = contam.len();
    if contam_len == 0 {
        return -1;
    }
    let seg_match_thr = (contam_len as f32 * mr).ceil() as i32;
    let rb = ref_sequence.as_bytes();
    let cb = contam.as_bytes();
    for r1 in 0..=(read_len as i32 - contam_len as i32) {
        let r1 = r1 as usize;
        let mut max_seg_match = 0i32;
        let mut mis = 0i32;
        for c in 0..contam_len {
            if cb[c] == rb[r1 + c] {
                max_seg_match += 1;
                if max_seg_match >= seg_match_thr {
                    return r1 as i32;
                }
            } else if rb[r1 + c] != b'N' {
                mis += 1;
                max_seg_match = 0;
                if mis > mm {
                    break;
                }
            }
        }
        if mis <= mm {
            return r1 as i32;
        }
    }
    -1
}

fn s_rna_find_adapter(_seq: &str, _adapter: &str, _gp: &GlobalParams) -> i32 {
    -1
}

pub fn fastq_trim(read: &mut FastqRecord, gp: &GlobalParams) -> Result<(), String> {
    let hard_trim = &gp.trim;
    let low_qual_head_trim = &gp.trim_bad_head;
    let low_qual_tail_trim = &gp.trim_bad_tail;
    let ht_flag = !hard_trim.is_empty();
    let lqt_flag = !low_qual_head_trim.is_empty() || !low_qual_tail_trim.is_empty();
    let index_flag = gp.index_remove;
    let ada_trim_flag = gp.adapter_discard_or_trim == "trim";
    let contam_trim_flag = gp.contam_discard_or_trim == "trim";
    if !ht_flag && !lqt_flag && !index_flag && !ada_trim_flag && !contam_trim_flag && gp.poly_g_tail == -1.0
    {
        return Ok(());
    }
    if index_flag {
        if gp.seq_type == "0" {
            let mut tmp_id = String::new();
            let mut cp_flag = true;
            for ch in read.seq_id.chars() {
                if ch == '#' {
                    cp_flag = false;
                }
                if cp_flag {
                    tmp_id.push(ch);
                } else if ch == '/' {
                    cp_flag = true;
                    tmp_id.push(ch);
                }
            }
            read.seq_id = tmp_id;
        } else {
            read.seq_id = read.seq_id[..read.seq_id.rfind(':').unwrap_or(read.seq_id.len())].to_string();
        }
    }
    let mut head_cut = 0i32;
    let mut tail_cut = 0i32;
    if ht_flag {
        if read.head_hdcut < 0 || read.tail_hdcut < 0 {
            return Err("hard trim metadata was not initialized".into());
        }
        head_cut = read.head_hdcut;
        tail_cut = read.tail_hdcut;
    }
    if lqt_flag {
        let mut head_eles = Vec::new();
        let mut tail_eles = Vec::new();
        line_split(low_qual_head_trim, ',', &mut head_eles);
        line_split(low_qual_tail_trim, ',', &mut tail_eles);
        if head_eles.len() != 2 && tail_eles.len() != 2 {
            return Err(format!(
                "low quality base at end format error,{} {}",
                low_qual_head_trim, low_qual_head_trim
            ));
        }
        let mut head_low_qual_threshold = 0i32;
        let mut head_low_qual_trim_length_limit = 0i32;
        let mut tail_low_qual_threshold = 0i32;
        let mut tail_low_qual_trim_length_limit = 0i32;
        if head_eles.len() == 2 {
            head_low_qual_threshold = head_eles[0]
                .parse()
                .map_err(|_| format!("invalid trimBadHead value: {}", low_qual_head_trim))?;
            head_low_qual_trim_length_limit = head_eles[1]
                .parse()
                .map_err(|_| format!("invalid trimBadHead value: {}", low_qual_head_trim))?;
        }
        if tail_eles.len() == 2 {
            tail_low_qual_threshold = tail_eles[0]
                .parse()
                .map_err(|_| format!("invalid trimBadTail value: {}", low_qual_tail_trim))?;
            tail_low_qual_trim_length_limit = tail_eles[1]
                .parse()
                .map_err(|_| format!("invalid trimBadTail value: {}", low_qual_tail_trim))?;
        }
        let qual_basis = gp.quality_phred;
        let mut head_limit = head_low_qual_trim_length_limit;
        if head_limit < 0 {
            head_limit = 0;
        }
        if head_limit > read.qual_seq.len() as i32 {
            head_limit = read.qual_seq.len() as i32;
        }
        let mut head_ix = 0i32;
        let qbytes = read.qual_seq.as_bytes();
        for ix in 0..head_limit as usize {
            let base_quality = qbytes[ix] as i32 - qual_basis;
            if base_quality < head_low_qual_threshold {
                head_ix += 1;
            } else {
                break;
            }
        }
        let mut tail_limit = tail_low_qual_trim_length_limit;
        if tail_limit < 0 {
            tail_limit = 0;
        }
        if tail_limit > read.qual_seq.len() as i32 {
            tail_limit = read.qual_seq.len() as i32;
        }
        let mut tail_ix = 0i32;
        for ix in 0..tail_limit as usize {
            let base_quality = qbytes[read.qual_seq.len() - ix - 1] as i32 - qual_basis;
            if base_quality < tail_low_qual_threshold {
                tail_ix += 1;
            } else {
                break;
            }
        }
        read.head_lqcut = head_ix;
        read.tail_lqcut = tail_ix;
        head_cut = head_cut.max(head_ix);
        tail_cut = tail_cut.max(tail_ix);
    }
    if ada_trim_flag {
        if read.adacut_pos > 0 {
            tail_cut = tail_cut.max(read.adacut_pos);
        }
    }
    if gp.poly_g_tail != -1.0 {
        let poly_g_n = poly_g_number(&read.sequence);
        if poly_g_n as f32 >= gp.poly_g_tail {
            if poly_g_n > tail_cut {
                tail_cut = poly_g_n;
            }
        }
    }
    let slen = read.sequence.len() as i32;
    if head_cut + tail_cut > slen {
        read.sequence.clear();
        read.qual_seq.clear();
    } else {
        let h = head_cut as usize;
        let t = tail_cut as usize;
        let seq: Vec<u8> = read.sequence.as_bytes()[h..slen as usize - t].to_vec();
        let qual: Vec<u8> = read.qual_seq.as_bytes()[h..slen as usize - t].to_vec();
        read.sequence = String::from_utf8(seq).unwrap();
        read.qual_seq = String::from_utf8(qual).unwrap();
    }
    Ok(())
}

fn poly_g_number(seq: &str) -> i32 {
    let bytes = seq.as_bytes();
    let mut n = 0i32;
    for i in (0..bytes.len()).rev() {
        if bytes[i] == b'G' || bytes[i] == b'g' {
            n += 1;
        } else {
            break;
        }
    }
    n
}

pub fn pre_output(record: &mut FastqRecord, typ: i32, gp: &GlobalParams) -> Result<(), String> {
    if gp.whether_add_pe_info {
        if typ == 1 {
            record.seq_id = format!("{}/1", record.seq_id);
        } else {
            record.seq_id = format!("{}/2", record.seq_id);
        }
    }
    if !gp.base_convert.is_empty() {
        let mut base_convert = gp.base_convert.clone();
        if let Some(pos) = base_convert.find("TO") {
            base_convert.replace_range(pos..pos + 2, "");
        }
        if let Some(pos) = base_convert.find('2') {
            base_convert.remove(pos);
        }
        if base_convert.len() != 2 {
            return Err("base_conver value format error".into());
        }
        let from = base_convert.as_bytes()[0].to_ascii_uppercase();
        let to = base_convert.as_bytes()[1];
        let seq = unsafe { record.sequence.as_mut_vec() };
        for b in seq.iter_mut() {
            if b.to_ascii_uppercase() == from {
                *b = to;
            }
        }
    }
    Ok(())
}

pub fn pe_dis(a: bool, b: bool) -> i32 {
    let mut v = 0;
    if a {
        v += 1;
    }
    if b {
        v += 2;
    }
    v
}

/// Return whether paired reads contain a small-insert overlap. R1's suffix is
/// compared with the prefix of reverse-complemented R2; N is a mismatch.
/// The detector was present but unreachable in v2 and the Rust port; v1's
/// `isSmallSize` behavior is the semantic reference for this repair.
pub fn whether_over_overlapped(sequence1: &str, sequence2: &str, gp: &GlobalParams) -> bool {
    let reverse_sequence2 = reverse_complementary(sequence2);
    let max_match_length = sequence1.len().min(sequence2.len());
    let start = gp.overlap_length.max(0) as usize;
    if start > max_match_length {
        return false;
    }
    for overlap in start..=max_match_length {
        let max_mismatch = (gp.pe_mismatch_ratio * overlap as f32) as usize;
        let suffix_start = sequence1.len() - overlap;
        let mut mismatch = 0usize;
        for (left, right) in sequence1[suffix_start..]
            .bytes()
            .zip(reverse_sequence2[..overlap].bytes())
        {
            if left == b'N' || right == b'N' || left != right {
                mismatch += 1;
                if mismatch > max_mismatch {
                    break;
                }
            }
        }
        if mismatch <= max_mismatch {
            return true;
        }
    }
    false
}

// Returns Some(discard_reason_num) or None (keep).
pub fn pe_discard(
    fs: &mut crate::stats::FilterStat,
    gp: &GlobalParams,
    r1: &ReadResult,
    r2: &ReadResult,
    seq1_len: usize,
    seq2_len: usize,
) -> Option<i32> {
    let mut min_value = -1;
    if gp.module_name == "filterStLFR" {
        min_value = 1;
    }
    if !gp.tile.is_empty() && check_tile_or_fov(&r1.read_tile, &gp.tile) {
        fs.tile_num += 1;
        min_value = 1;
        return Some(min_value);
    }
    if !gp.fov.is_empty() && check_tile_or_fov(&r1.read_fov, &gp.fov) {
        fs.fov_num += 1;
        min_value = 1;
        return Some(min_value);
    }
    if gp.min_read_length != -1 {
        let v = pe_dis(seq1_len < gp.min_read_length as usize, seq2_len < gp.min_read_length as usize);
        if v > 0 {
            match v {
                1 => fs.short_len_num1 += 1,
                2 => fs.short_len_num2 += 1,
                3 => {
                    fs.short_len_num1 += 1;
                    fs.short_len_num2 += 1;
                    fs.short_len_num_overlap += 1;
                }
                _ => {}
            }
            fs.short_len_num += 1;
            return Some(1);
        }
    } else if seq1_len == 0 || seq2_len == 0 {
        return Some(1);
    }
    if gp.max_read_length != -1 {
        let v = pe_dis(seq1_len > gp.max_read_length as usize, seq2_len > gp.max_read_length as usize);
        if v > 0 {
            match v {
                1 => fs.long_len_num1 += 1,
                2 => fs.long_len_num2 += 1,
                3 => {
                    fs.long_len_num1 += 1;
                    fs.long_len_num2 += 1;
                    fs.long_len_num_overlap += 1;
                }
                _ => {}
            }
            fs.long_len_num += 1;
            return Some(1);
        }
    }
    if gp.contam_discard_or_trim == "discard" {
        let v = pe_dis(r1.include_global_contam == 1, r2.include_global_contam == 1);
        if v > 0 {
            match v {
                1 => fs.include_global_contam_seq_num1 += 1,
                2 => fs.include_global_contam_seq_num2 += 1,
                3 => {
                    fs.include_global_contam_seq_num1 += 1;
                    fs.include_global_contam_seq_num2 += 1;
                    fs.include_global_contam_seq_num_overlap += 1;
                }
                _ => {}
            }
            fs.include_global_contam_seq_num += 1;
            return Some(1);
        }
    }
    if gp.contam_discard_or_trim == "discard" {
        let v = pe_dis(r1.include_contam == 1, r2.include_contam == 1);
        if v > 0 {
            match v {
                1 => fs.include_contam_seq_num1 += 1,
                2 => fs.include_contam_seq_num2 += 1,
                3 => {
                    fs.include_contam_seq_num1 += 1;
                    fs.include_contam_seq_num2 += 1;
                    fs.include_contam_seq_num_overlap += 1;
                }
                _ => {}
            }
            fs.include_contam_seq_num += 1;
            return Some(1);
        }
    }
    if gp.n_ratio != -1.0 {
        let r1_n = r1.n_num as f32 / seq1_len as f32;
        let r2_n = r2.n_num as f32 / seq2_len as f32;
        let v = pe_dis(r1_n >= gp.n_ratio, r2_n >= gp.n_ratio);
        if v > 0 {
            match v {
                1 => fs.n_ratio_num1 += 1,
                2 => fs.n_ratio_num2 += 1,
                3 => {
                    fs.n_ratio_num1 += 1;
                    fs.n_ratio_num2 += 1;
                    fs.n_ratio_num_overlap += 1;
                }
                _ => {}
            }
            fs.n_ratio_num += 1;
            return Some(1);
        }
    }
    if gp.high_a_ratio != -1.0 {
        let r1_a = r1.a_num as f32 / seq1_len as f32;
        let r2_a = r2.a_num as f32 / seq2_len as f32;
        let v = pe_dis(r1_a >= gp.high_a_ratio, r2_a >= gp.high_a_ratio);
        if v > 0 {
            match v {
                1 => fs.high_a_num1 += 1,
                2 => fs.high_a_num2 += 1,
                3 => {
                    fs.high_a_num1 += 1;
                    fs.high_a_num2 += 1;
                    fs.high_a_num_overlap += 1;
                }
                _ => {}
            }
            fs.high_a_num += 1;
            return Some(1);
        }
    }
    if gp.poly_x_num != -1 {
        let v = pe_dis(r1.contig_base >= gp.poly_x_num, r2.contig_base >= gp.poly_x_num);
        if v > 0 {
            match v {
                1 => fs.poly_x_num1 += 1,
                2 => fs.poly_x_num2 += 1,
                3 => {
                    fs.poly_x_num1 += 1;
                    fs.poly_x_num2 += 1;
                    fs.poly_x_num_overlap += 1;
                }
                _ => {}
            }
            fs.poly_x_num += 1;
            return Some(1);
        }
    }
    if gp.low_quality_base_ratio != -1.0 {
        let v = pe_dis(
            r1.low_qual_base_ratio >= gp.low_quality_base_ratio,
            r2.low_qual_base_ratio >= gp.low_quality_base_ratio,
        );
        if v > 0 {
            if r1.low_qual_base_ratio > 1.0 || r2.low_qual_base_ratio > 1.0 {
                eprintln!("Error:low quality base ratio stat error");
                // v2 exits here with read id; keep simple error
                return Some(1);
            }
            match v {
                1 => fs.low_qual_base_ratio_num1 += 1,
                2 => fs.low_qual_base_ratio_num2 += 1,
                3 => {
                    fs.low_qual_base_ratio_num1 += 1;
                    fs.low_qual_base_ratio_num2 += 1;
                    fs.low_qual_base_ratio_num_overlap += 1;
                }
                _ => {}
            }
            fs.low_qual_base_ratio_num += 1;
            return Some(1);
        }
    }
    if gp.mean_quality != -1 {
        let v = pe_dis(
            (r1.mean_quality as i32) < gp.mean_quality,
            (r2.mean_quality as i32) < gp.mean_quality,
        );
        if v > 0 {
            match v {
                1 => fs.mean_quality_num1 += 1,
                2 => fs.mean_quality_num2 += 1,
                3 => {
                    fs.mean_quality_num1 += 1;
                    fs.mean_quality_num2 += 1;
                    fs.mean_quality_num_overlap += 1;
                }
                _ => {}
            }
            fs.mean_quality_num += 1;
            return Some(1);
        }
    }
    if gp.overlap_length != -1 {
        if r1.over_lapped {
            fs.over_lapped_num += 1;
            min_value = 1;
            return Some(min_value);
        }
    }
    if gp.adapter_discard_or_trim == "discard" {
        let v = pe_dis(r1.include_adapter_seq == 1, r2.include_adapter_seq == 1);
        if v > 0 {
            match v {
                1 => fs.include_adapter_seq_num1 += 1,
                2 => fs.include_adapter_seq_num2 += 1,
                3 => {
                    fs.include_adapter_seq_num1 += 1;
                    fs.include_adapter_seq_num2 += 1;
                    fs.include_adapter_seq_num_overlap += 1;
                }
                _ => {}
            }
            fs.include_adapter_seq_num += 1;
            return Some(1);
        }
    }
    None
}

// Single-end filtering has no mate-side or overlap counters. Keep this path
// separate from the paired predicate so a synthetic empty mate can never
// affect the report.
fn se_discard(
    fs: &mut crate::stats::FilterStat,
    gp: &GlobalParams,
    r: &ReadResult,
    seq_len: usize,
) -> Option<i32> {
    if !gp.tile.is_empty() && check_tile_or_fov(&r.read_tile, &gp.tile) {
        fs.tile_num += 1;
        return Some(1);
    }
    if !gp.fov.is_empty() && check_tile_or_fov(&r.read_fov, &gp.fov) {
        fs.fov_num += 1;
        return Some(1);
    }
    if gp.min_read_length != -1 {
        if seq_len < gp.min_read_length as usize {
            fs.short_len_num1 += 1;
            fs.short_len_num += 1;
            return Some(1);
        }
    } else if seq_len == 0 {
        return Some(1);
    }
    if gp.max_read_length != -1 && seq_len > gp.max_read_length as usize {
        fs.long_len_num1 += 1;
        fs.long_len_num += 1;
        return Some(1);
    }
    if gp.contam_discard_or_trim == "discard" && r.include_global_contam == 1 {
        fs.include_global_contam_seq_num1 += 1;
        fs.include_global_contam_seq_num += 1;
        return Some(1);
    }
    if gp.contam_discard_or_trim == "discard" && r.include_contam == 1 {
        fs.include_contam_seq_num1 += 1;
        fs.include_contam_seq_num += 1;
        return Some(1);
    }
    if gp.n_ratio != -1.0
        && seq_len > 0
        && r.n_num as f32 / seq_len as f32 >= gp.n_ratio
    {
        fs.n_ratio_num1 += 1;
        fs.n_ratio_num += 1;
        return Some(1);
    }
    if gp.high_a_ratio != -1.0
        && seq_len > 0
        && r.a_num as f32 / seq_len as f32 >= gp.high_a_ratio
    {
        fs.high_a_num1 += 1;
        fs.high_a_num += 1;
        return Some(1);
    }
    if gp.poly_x_num != -1 && r.contig_base >= gp.poly_x_num {
        fs.poly_x_num1 += 1;
        fs.poly_x_num += 1;
        return Some(1);
    }
    if gp.low_quality_base_ratio != -1.0 {
        if r.low_qual_base_ratio >= gp.low_quality_base_ratio {
            if r.low_qual_base_ratio > 1.0 {
                eprintln!("Error:low quality base ratio stat error");
                return Some(1);
            }
            fs.low_qual_base_ratio_num1 += 1;
            fs.low_qual_base_ratio_num += 1;
            return Some(1);
        }
    }
    if gp.mean_quality != -1 && (r.mean_quality as i32) < gp.mean_quality {
        fs.mean_quality_num1 += 1;
        fs.mean_quality_num += 1;
        return Some(1);
    }
    // Overlap is a paired-read concept and is intentionally not evaluated.
    if gp.adapter_discard_or_trim == "discard" && r.include_adapter_seq == 1 {
        fs.include_adapter_seq_num1 += 1;
        fs.include_adapter_seq_num += 1;
        return Some(1);
    }
    None
}

// Per-read raw-stat bookkeeping that stat_pe_fqs performs beyond the per-base
// matrices (now fused into the filter pass): trim-position tables plus
// read_length / read_max_length / base_number. Mirrors the raw (is_trim=false)
// branch of stat_pe_fqs exactly.
fn accum_raw_stat(
    read: &FastqRecord,
    stat: &mut FileStat,
    label: &str,
) -> Result<(), String> {
    let raw_length = validated_trim_raw_length(read, label)?;
    if read.head_hdcut > 0 || read.head_lqcut > 0 {
        if read.head_hdcut >= read.head_lqcut {
            stat.ts.ht[read.head_hdcut as usize] += 1;
        } else {
            stat.ts.hlq[read.head_lqcut as usize] += 1;
        }
    }
    if read.tail_hdcut > 0 || read.tail_lqcut > 0 || read.adacut_pos > 0 {
        if read.tail_hdcut >= read.tail_lqcut {
            if read.tail_hdcut >= read.adacut_pos {
                stat.ts.tt[(raw_length - read.tail_hdcut + 1) as usize] += 1;
            } else {
                stat.ts.ta[(raw_length - read.adacut_pos + 1) as usize] += 1;
            }
        } else if read.tail_lqcut >= read.adacut_pos {
            stat.ts.tlq[(raw_length - read.tail_lqcut + 1) as usize] += 1;
        } else {
            stat.ts.ta[(raw_length - read.adacut_pos + 1) as usize] += 1;
        }
    }
    stat.gs.reads_number += 1;
    let seq_len = read.sequence.len();
    stat.gs.read_length = stat.gs.read_length.max(seq_len as u64);
    stat.gs.read_max_length = stat.gs.read_max_length.max(seq_len as u64);
    stat.gs.base_number += seq_len as u64;
    Ok(())
}

pub fn filter_pe_fqs(
    reads1: &mut Vec<FastqRecord>,
    reads2: &mut Vec<FastqRecord>,
    fs: &mut crate::stats::FilterStat,
    gp: &GlobalParams,
    trim_enabled: bool,
    trim_output_enabled: bool,
    mut raw1: Option<&mut FileStat>,
    mut raw2: Option<&mut FileStat>,
) -> Result<(Vec<FastqRecord>, Vec<FastqRecord>, Vec<FastqRecord>, Vec<FastqRecord>), String> {
    let mut clean1: Vec<FastqRecord> = Vec::new();
    let mut clean2: Vec<FastqRecord> = Vec::new();
    let mut trim1: Vec<FastqRecord> = Vec::new();
    let mut trim2: Vec<FastqRecord> = Vec::new();
    let n = reads1.len();
    let gp2 = gp.clone_with_ada2();
    let hard_trim = parse_hard_trim(gp, true)?;
    for i in 0..n {
        // C_pe_fastq_filter copies + stat_read both. Take the record (a move)
        // instead of cloning: passing reads are moved into the clean output and
        // discarded reads are dropped, so no per-read deep copy is needed.
        let mut fq1 = std::mem::take(&mut reads1[i]);
        let mut fq2 = std::mem::take(&mut reads2[i]);
        let mut r1 = stat_read(&mut fq1, gp, raw1.as_deref_mut())?;
        let r2 = stat_read(&mut fq2, &gp2, raw2.as_deref_mut())?;
        if let Some(s1) = raw1.as_deref_mut() {
            accum_raw_stat(&fq1, s1, "read1")?;
        }
        if let Some(s2) = raw2.as_deref_mut() {
            accum_raw_stat(&fq2, s2, "read2")?;
        }
        if let Some(specs) = hard_trim {
            apply_hard_trim(&mut fq1, specs[0]);
            apply_hard_trim(&mut fq2, specs[1]);
        }
        if gp.overlap_length != -1 {
            r1.over_lapped = whether_over_overlapped(&fq1.sequence, &fq2.sequence, gp);
        }
        fastq_trim(&mut fq1, gp)?;
        fastq_trim(&mut fq2, &gp2)?;
        if trim_output_enabled {
            pre_output(&mut fq1, 1, gp)?;
            pre_output(&mut fq2, 2, gp)?;
            trim1.push(fq1.clone());
            trim2.push(fq2.clone());
        }
        let discard = pe_discard(fs, gp, &r1, &r2, fq1.sequence.len(), fq2.sequence.len());
        if discard.is_none() {
            if !gp.clean_fq1.is_empty() {
                pre_output(&mut fq1, 1, gp)?;
                pre_output(&mut fq2, 2, gp)?;
                clean1.push(fq1);
                clean2.push(fq2);
            }
        } else {
            // reads1[i] was moved out; fq1.raw_length (set by stat_read) is the
            // pre-trim sequence length, matching the original bookkeeping.
            fs.filtered_base_num1 += fq1.raw_length as u64;
            fs.filtered_base_num2 += fq2.raw_length as u64;
        }
    }
    Ok((trim1, trim2, clean1, clean2))
}

/// Single-end filtering uses the same per-record policy and statistics
/// machinery as paired-end filtering.  The second discard result is empty,
/// while its length is kept equal to the first read so length checks remain
/// single-end checks; this records a filter reason only for fastq1.
pub fn filter_se_fqs(
    reads: &mut Vec<FastqRecord>,
    fs: &mut crate::stats::FilterStat,
    gp: &GlobalParams,
    _trim_enabled: bool,
    trim_output_enabled: bool,
    mut raw: Option<&mut FileStat>,
) -> Result<(Vec<FastqRecord>, Vec<FastqRecord>), String> {
    let mut trim = Vec::new();
    let mut clean = Vec::new();
    let hard_trim = parse_hard_trim(gp, false)?;
    for i in 0..reads.len() {
        let mut read = std::mem::take(&mut reads[i]);
        let result = stat_read(&mut read, gp, raw.as_deref_mut())?;
        if let Some(stat) = raw.as_deref_mut() {
            accum_raw_stat(&read, stat, "read1")?;
        }
        if let Some(specs) = hard_trim {
            apply_hard_trim(&mut read, specs[0]);
        }
        fastq_trim(&mut read, gp)?;
        if trim_output_enabled {
            pre_output(&mut read, 1, gp)?;
            trim.push(read.clone());
        }
        if se_discard(fs, gp, &result, read.sequence.len()).is_none()
        {
            if !gp.clean_fq1.is_empty() {
                pre_output(&mut read, 1, gp)?;
                clean.push(read);
            }
        } else {
            fs.filtered_base_num1 += read.raw_length as u64;
        }
    }
    Ok((trim, clean))
}

#[derive(Clone, Copy, Default)]
pub struct Quartile {
    pub mean: f32,
    pub median: f32,
    pub lower_quar: f32,
    pub upper_quar: f32,
    pub first10_quar: f32,
    pub last10_quar: f32,
}

pub fn cal_quar_from_array(data: &[u64], count: usize) -> Quartile {
    let mut q = Quartile::default();
    let mut total_num: u64 = 0;
    let mut data_num: u64 = 0;
    for i in 0..count {
        total_num += (i as u64) * data[i];
        data_num += data[i];
    }
    if data_num == 0 {
        q.mean = 0.0;
    } else {
        q.mean = (total_num / data_num) as f32;
    }
    let lower_pos = data_num / 4;
    let upper_pos = (data_num / 4) * 3 + ((data_num % 4) * 3) / 4;
    let first10_pos = data_num / 10;
    let last10_pos = (data_num / 10) * 9 + ((data_num % 10) * 9) / 10;
    let median_pos = data_num / 2;
    let mut last_cur_pos: u64 = 0;
    let mut cur_pos: u64 = 0;
    for i in 0..count {
        cur_pos += data[i];
        if lower_pos >= last_cur_pos && lower_pos <= cur_pos {
            q.lower_quar = i as f32;
        }
        if upper_pos >= last_cur_pos && upper_pos <= cur_pos {
            q.upper_quar = i as f32;
        }
        if first10_pos >= last_cur_pos && first10_pos <= cur_pos {
            q.first10_quar = i as f32;
        }
        if last10_pos >= last_cur_pos && last10_pos <= cur_pos {
            q.last10_quar = i as f32;
        }
        if median_pos >= last_cur_pos && median_pos <= cur_pos {
            q.median = i as f32;
        }
        last_cur_pos = cur_pos;
    }
    q
}

// stat_pe_fqs: statistics for a pair of record vectors.
// quality_base >= 0 overrides; check_bq runs the one-time quality encoding check
// (only for the first call, guarded by the caller).
pub fn stat_pe_fqs(
    reads1: &[FastqRecord],
    reads2: &[FastqRecord],
    stat1: &mut FileStat,
    stat2: &mut FileStat,
    quality_base: i32,
    gp: &GlobalParams,
    check_bq: bool,
    is_trim: bool,
) -> Result<(), String> {
    stat1.gs.reads_number += reads1.len() as u64;
    stat2.gs.reads_number += reads2.len() as u64;
    if reads1.len() != reads2.len() {
        return Err("reads number in fq1 and fq2 are different".into());
    }
    let qb = if quality_base >= 0 { quality_base } else { gp.output_quality_phred };
    // fq1 loop
    for ix in reads1 {
        let raw_length = validated_trim_raw_length(ix, "read1")?;
        if ix.head_hdcut > 0 || ix.head_lqcut > 0 {
            if ix.head_hdcut >= ix.head_lqcut {
                stat1.ts.ht[ix.head_hdcut as usize] += 1;
            } else {
                stat1.ts.hlq[ix.head_lqcut as usize] += 1;
            }
        }
        if ix.tail_hdcut > 0 || ix.tail_lqcut > 0 || ix.adacut_pos > 0 {
            if ix.tail_hdcut >= ix.tail_lqcut {
                if ix.tail_hdcut >= ix.adacut_pos {
                    stat1.ts.tt[(raw_length - ix.tail_hdcut + 1) as usize] += 1;
                } else {
                    stat1.ts.ta[(raw_length - ix.adacut_pos + 1) as usize] += 1;
                }
            } else if ix.tail_lqcut >= ix.adacut_pos {
                stat1.ts.tlq[(raw_length - ix.tail_lqcut + 1) as usize] += 1;
            } else {
                stat1.ts.ta[(raw_length - ix.adacut_pos + 1) as usize] += 1;
            }
        }
        let seq_len = ix.sequence.len();
        let sbytes = ix.sequence.as_bytes();
        for i in 0..seq_len {
            let pos = &mut stat1.bs.position_acgt_content[i];
            match sbytes[i] {
                b'a' | b'A' => {
                    pos[0] += 1;
                    stat1.gs.a_number += 1;
                }
                b'c' | b'C' => {
                    pos[1] += 1;
                    stat1.gs.c_number += 1;
                }
                b'g' | b'G' => {
                    pos[2] += 1;
                    stat1.gs.g_number += 1;
                }
                b't' | b'T' => {
                    pos[3] += 1;
                    stat1.gs.t_number += 1;
                }
                b'n' | b'N' => {
                    pos[4] += 1;
                    stat1.gs.n_number += 1;
                }
                _ => {
                    return Err(format!(
                        "unrecognized sequence,{}",
                        String::from_utf8_lossy(ix.sequence.as_bytes())
                    ));
                }
            }
        }
        let qual_len = ix.qual_seq.len();
        let qbytes = ix.qual_seq.as_bytes();
        for i in 0..qual_len {
            let base_quality = qbytes[i] as i32 - qb;
            if base_quality < MIN_QUAL {
                return Err("base quality index is below zero".into());
            }
            if i >= READ_MAX_LEN || base_quality as usize >= gp.quality_bins() {
                return Err("quality matrix index is out of range".into());
            }
            let bins = gp.quality_bins();
            stat1.qs.position_qual[i * bins + base_quality as usize] += 1;
            if base_quality >= 20 {
                stat1.gs.q20_num += 1;
            }
            if base_quality >= 30 {
                stat1.gs.q30_num += 1;
            }
        }
        stat1.gs.read_length = stat1.gs.read_length.max(ix.sequence.len() as u64);
        let mut stat_max_length = ix.sequence.len() as u64;
        if is_trim {
            stat_max_length = stat_max_length.max(raw_length as u64);
        }
        stat1.gs.read_max_length = stat1.gs.read_max_length.max(stat_max_length);
        stat1.gs.base_number += ix.sequence.len() as u64;
    }
    if check_bq {
        bq_check(reads1, stat1, gp)?;
    }
    // fq2 loop
    for ix in reads2 {
        let raw_length = validated_trim_raw_length(ix, "read2")?;
        if ix.head_hdcut > 0 || ix.head_lqcut > 0 {
            if ix.head_hdcut >= ix.head_lqcut {
                stat2.ts.ht[ix.head_hdcut as usize] += 1;
            } else {
                stat2.ts.hlq[ix.head_lqcut as usize] += 1;
            }
        }
        if ix.tail_hdcut > 0 || ix.tail_lqcut > 0 || ix.adacut_pos > 0 {
            if ix.tail_hdcut >= ix.tail_lqcut {
                if ix.tail_hdcut >= ix.adacut_pos {
                    stat2.ts.tt[(raw_length - ix.tail_hdcut + 1) as usize] += 1;
                } else {
                    stat2.ts.ta[(raw_length - ix.adacut_pos + 1) as usize] += 1;
                }
            } else if ix.tail_lqcut >= ix.adacut_pos {
                stat2.ts.tlq[(raw_length - ix.tail_lqcut + 1) as usize] += 1;
            } else {
                stat2.ts.ta[(raw_length - ix.adacut_pos + 1) as usize] += 1;
            }
        }
        let seq_len = ix.sequence.len();
        let sbytes = ix.sequence.as_bytes();
        for i in 0..seq_len {
            let pos = &mut stat2.bs.position_acgt_content[i];
            match sbytes[i] {
                b'a' | b'A' => {
                    pos[0] += 1;
                    stat2.gs.a_number += 1;
                }
                b'c' | b'C' => {
                    pos[1] += 1;
                    stat2.gs.c_number += 1;
                }
                b'g' | b'G' => {
                    pos[2] += 1;
                    stat2.gs.g_number += 1;
                }
                b't' | b'T' => {
                    pos[3] += 1;
                    stat2.gs.t_number += 1;
                }
                b'n' | b'N' => {
                    pos[4] += 1;
                    stat2.gs.n_number += 1;
                }
                _ => {
                    return Err(format!(
                        "unrecognized sequence,{}",
                        String::from_utf8_lossy(ix.sequence.as_bytes())
                    ));
                }
            }
        }
        let qual_len = ix.qual_seq.len();
        let qbytes = ix.qual_seq.as_bytes();
        for i in 0..qual_len {
            let base_quality = qbytes[i] as i32 - qb;
            if base_quality < MIN_QUAL {
                return Err("base quality index is below zero".into());
            }
            if i >= READ_MAX_LEN || base_quality as usize >= gp.quality_bins() {
                return Err("quality matrix index is out of range".into());
            }
            let bins = gp.quality_bins();
            stat2.qs.position_qual[i * bins + base_quality as usize] += 1;
            if base_quality >= 20 {
                stat2.gs.q20_num += 1;
            }
            if base_quality >= 30 {
                stat2.gs.q30_num += 1;
            }
        }
        stat2.gs.read_length = stat2.gs.read_length.max(ix.sequence.len() as u64);
        let mut stat_max_length = ix.sequence.len() as u64;
        if is_trim {
            stat_max_length = stat_max_length.max(raw_length as u64);
        }
        stat2.gs.read_max_length = stat2.gs.read_max_length.max(stat_max_length);
        stat2.gs.base_number += ix.sequence.len() as u64;
    }
    Ok(())
}

/// Reuse the validated paired statistics implementation for one lane.  The
/// scratch destination is intentionally discarded by the caller; the first
/// destination is the only reportable SE statistic.
pub fn stat_se_fqs(
    reads: &[FastqRecord],
    stat: &mut FileStat,
    scratch: &mut FileStat,
    quality_base: i32,
    gp: &GlobalParams,
    is_trim: bool,
) -> Result<(), String> {
    stat_pe_fqs(
        reads,
        reads,
        stat,
        scratch,
        quality_base,
        gp,
        false,
        is_trim,
    )
}

// One-time quality encoding check (peprocess.cpp bq_check block).
pub(crate) fn bq_check(reads1: &[FastqRecord], _stat1: &FileStat, gp: &GlobalParams) -> Result<(), String> {
    let mut q1_exceed = 0i64;
    let mut q1_normal_bq = 0i64;
    let mut q1_mean_bq = 0i64;
    let mut q2_exceed = 0i64;
    let mut q2_normal_bq = 0i64;
    let mut q2_mean_bq = 0i64;
    for ix in reads1 {
        let qual_len = ix.qual_seq.len();
        let qbytes = ix.qual_seq.as_bytes();
        for i in 0..qual_len {
            let base_quality = qbytes[i] as i32 - gp.quality_phred;
            q1_mean_bq += base_quality as i64;
            if base_quality >= MIN_QUAL && base_quality <= gp.max_base_quality {
                q1_normal_bq += 1;
            } else if base_quality < MIN_QUAL - 10 || base_quality > gp.max_base_quality + 10 {
                q1_exceed += 1;
            }
            let another_q = if gp.quality_phred == 64 { 33 } else { 64 };
            let base_quality2 = qbytes[i] as i32 - another_q;
            q2_mean_bq += base_quality2 as i64;
            if base_quality2 >= MIN_QUAL && base_quality2 <= gp.max_base_quality {
                q2_normal_bq += 1;
            } else if base_quality2 < MIN_QUAL - 10 || base_quality2 > gp.max_base_quality + 10 {
                q2_exceed += 1;
            }
        }
    }
    let base_count: u64 = reads1.iter().map(|read| read.qual_seq.len() as u64).sum();
    if base_count == 0 {
        return Err("no data".into());
    }
    let base_num = base_count as f32;
    let q1_normal_ratio = q1_normal_bq as f32 / base_num;
    let q2_normal_ratio = q2_normal_bq as f32 / base_num;
    let q1_mean = q1_mean_bq as f32 / base_num;
    let q2_mean = q2_mean_bq as f32 / base_num;
    let mut q1_score = 0i32;
    let mut q2_score = 0i32;
    if q1_exceed == 0 {
        q1_score += 1;
    }
    if q2_exceed == 0 {
        q2_score += 1;
    }
    if q1_normal_ratio > q2_normal_ratio {
        q1_score += 3;
    } else if q1_normal_ratio < q2_normal_ratio {
        q2_score += 3;
    } else {
        q1_score += 3;
        q2_score += 3;
    }
    if q1_mean >= 10.0 && q1_mean <= gp.max_base_quality as f32 {
        q1_score += 2;
    }
    if q2_mean >= 10.0 && q2_mean <= gp.max_base_quality as f32 {
        q2_score += 2;
    }
    if q1_score - q2_score < -3 {
        return Err("base quality seems abnormal,please check the quality system parameter or fastq file".into());
    } else if q1_score - q2_score < 0 {
        eprintln!("Warning:base quality seems abnormal,please check the quality system parameter or fastq file");
    }
    Ok(())
}
