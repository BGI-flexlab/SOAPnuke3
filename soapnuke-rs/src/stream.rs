use crate::params::GlobalParams;
use crate::record::FastqRecord;
use crate::stats::{FileStat, FilterStat, PeLocalStats};
use std::io::{self, Write};

fn append_number_line(out: &mut Vec<u8>, values: &[u64]) {
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            out.push(b' ');
        }
        out.extend_from_slice(value.to_string().as_bytes());
    }
    out.push(b'\n');
}

fn append_file_summary(out: &mut Vec<u8>, raw: &FileStat, clean: &FileStat) {
    append_number_line(
        out,
        &[
            raw.gs.read_length,
            clean.gs.read_length,
            raw.gs.reads_number,
            clean.gs.reads_number,
            raw.gs.base_number,
            clean.gs.base_number,
            raw.gs.a_number,
            clean.gs.a_number,
            raw.gs.c_number,
            clean.gs.c_number,
            raw.gs.g_number,
            clean.gs.g_number,
            raw.gs.t_number,
            clean.gs.t_number,
            raw.gs.n_number,
            clean.gs.n_number,
            raw.gs.q20_num,
            clean.gs.q20_num,
            raw.gs.q30_num,
            clean.gs.q30_num,
        ],
    );
}

fn append_base_rows(out: &mut Vec<u8>, stat: &FileStat) {
    for position in 0..stat.gs.read_length as usize {
        append_number_line(out, &stat.bs.position_acgt_content[position]);
    }
}

fn append_quality_rows(out: &mut Vec<u8>, stat: &FileStat, quality_bins: usize) {
    for position in 0..stat.gs.read_length as usize {
        let start = position * stat.qs.quality_bins;
        append_number_line(out, &stat.qs.position_qual[start..start + quality_bins]);
    }
}

fn append_stat_section(out: &mut Vec<u8>, raw: &FileStat, clean: &FileStat, quality_bins: usize) {
    out.extend_from_slice(b"#Fq1_statistical_information\n");
    append_file_summary(out, raw, clean);
    out.extend_from_slice(b"#Base_distributions_by_read_position\n");
    append_base_rows(out, raw);
    append_base_rows(out, clean);
    out.extend_from_slice(b"#Raw_Base_quality_value_distribution_by_read_position\n");
    append_quality_rows(out, raw, quality_bins);
    append_quality_rows(out, clean, quality_bins);
}

fn append_filter_section(out: &mut Vec<u8>, fs: &FilterStat) {
    let total = fs.include_adapter_seq_num
        + fs.include_contam_seq_num
        + fs.low_qual_base_ratio_num
        + fs.mean_quality_num
        + fs.n_ratio_num
        + fs.over_lapped_num
        + fs.high_a_num
        + fs.poly_x_num;
    out.extend_from_slice(b"#Total_statistical_information\n");
    append_number_line(
        out,
        &[
            total,
            fs.include_adapter_seq_num,
            fs.include_contam_seq_num,
            fs.low_qual_base_ratio_num,
            fs.mean_quality_num,
            fs.n_ratio_num,
            fs.over_lapped_num,
            fs.high_a_num,
            fs.poly_x_num,
        ],
    );
}

fn append_stream_record(out: &mut Vec<u8>, record: &FastqRecord, mate: &str) {
    let id = record.seq_id.strip_prefix('@').unwrap_or(&record.seq_id);
    out.extend_from_slice(b">+\t");
    out.extend_from_slice(id.as_bytes());
    out.push(b'\t');
    out.extend_from_slice(mate.as_bytes());
    out.push(b'\t');
    out.extend_from_slice(record.sequence.as_bytes());
    out.push(b'\t');
    out.extend_from_slice(record.qual_seq.as_bytes());
    out.push(b'\n');
}

/// Build one complete legacy streaming message. The caller writes the returned
/// bytes only after this function returns, so a blocking stdout write never
/// holds a CpuBudget permit. PE batches intentionally emit all R1 records
/// before all R2 records, matching SOAPnuke's historical protocol.
pub(crate) fn format_batch(
    records1: &[FastqRecord],
    records2: &[FastqRecord],
    paired: bool,
    stats: &PeLocalStats,
    gp: &GlobalParams,
) -> Vec<u8> {
    let mut out = Vec::new();
    for record in records1 {
        append_stream_record(&mut out, record, "1");
    }
    if paired {
        for record in records2 {
            append_stream_record(&mut out, record, "2");
        }
    }
    append_filter_section(&mut out, &stats.fs);
    append_stat_section(&mut out, &stats.raw1, &stats.clean1, gp.quality_bins());
    if paired {
        out.extend_from_slice(b"#Fq2_statistical_information\n");
        append_file_summary(&mut out, &stats.raw2, &stats.clean2);
        out.extend_from_slice(b"#Base_distributions_by_read_position\n");
        append_base_rows(&mut out, &stats.raw2);
        append_base_rows(&mut out, &stats.clean2);
        out.extend_from_slice(b"#Raw_Base_quality_value_distribution_by_read_position\n");
        append_quality_rows(&mut out, &stats.raw2, gp.quality_bins());
        append_quality_rows(&mut out, &stats.clean2, gp.quality_bins());
    }
    out
}

pub(crate) fn write_stdout(bytes: &[u8]) -> Result<(), String> {
    #[cfg(test)]
    if std::env::var_os("SOAPNUKE_TEST_STREAM_WRITE_FAIL").is_some() {
        return Err("injected streaming write failure".into());
    }
    #[cfg(test)]
    if let Ok(path) = std::env::var("SOAPNUKE_TEST_STREAM_CAPTURE") {
        let mut capture = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| format!("streaming capture open failed: {error}"))?;
        return capture
            .write_all(bytes)
            .map_err(|error| format!("streaming capture write failed: {error}"));
    }
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(bytes)
        .map_err(|error| format!("streaming stdout write failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::format_batch;
    use crate::params::GlobalParams;
    use crate::record::FastqRecord;
    use crate::stats::PeLocalStats;

    fn record(id: &str, sequence: &str, quality: &str) -> FastqRecord {
        let mut value = FastqRecord::new(1);
        value.seq_id = id.to_string();
        value.sequence = sequence.to_string();
        value.qual_seq = quality.to_string();
        value
    }

    #[test]
    fn pe_streaming_protocol_orders_mates_before_batch_statistics() {
        let mut gp = GlobalParams::new();
        gp.output_file_type = "fastq".to_string();
        let stats = PeLocalStats::new(&gp);
        let bytes = format_batch(
            &[record("@r0/1", "ACGT", "IIII")],
            &[record("@r0/2", "TGCA", "JJJJ")],
            true,
            &stats,
            &gp,
        );
        let text = String::from_utf8(bytes).unwrap();
        let r1 = text.find(">+\tr0/1\t1\tACGT\tIIII\n").unwrap();
        let r2 = text.find(">+\tr0/2\t2\tTGCA\tJJJJ\n").unwrap();
        let stat = text.find("#Total_statistical_information\n").unwrap();
        assert!(r1 < r2 && r2 < stat);
    }

    #[test]
    fn se_streaming_protocol_has_no_mate_two_section() {
        let gp = GlobalParams::new();
        let stats = PeLocalStats::new(&gp);
        let bytes = format_batch(
            &[record("@r0", "ACGT", "IIII")],
            &[],
            false,
            &stats,
            &gp,
        );
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains(">+\tr0\t1\tACGT\tIIII\n"));
        assert!(!text.contains("#Fq2_statistical_information"));
    }

    #[test]
    fn streaming_write_failure_is_reported() {
        std::env::set_var("SOAPNUKE_TEST_STREAM_WRITE_FAIL", "1");
        let result = super::write_stdout(b"not emitted");
        std::env::remove_var("SOAPNUKE_TEST_STREAM_WRITE_FAIL");
        assert!(result.is_err());
    }
}
