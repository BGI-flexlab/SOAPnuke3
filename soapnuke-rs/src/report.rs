// print_stat: byte-exact port of peProcess::print_stat (10 txt reports).
use crate::filter::cal_quar_from_array;
use crate::params::GlobalParams;
use crate::stats::GlobalStats;
use std::fs::File;
use std::io::Write;

fn fmt_f32_2(v: f32) -> String {
    format!("{:.2}", v as f64)
}
fn fmt_f32_1(v: f32) -> String {
    format!("{:.1}", v as f64)
}
fn fmt_f32_0(v: f32) -> String {
    format!("{:.0}", v as f64)
}

pub fn print_stat(gp: &GlobalParams, gv: &GlobalStats) -> Result<(), String> {
    if gp.fq2_path.is_empty() {
        return print_stat_se(gp, gv);
    }
    print_stat_pe(gp, gv)
}

fn print_stat_pe(gp: &GlobalParams, gv: &GlobalStats) -> Result<(), String> {
    let filter_out = format!("{}/Statistics_of_Filtered_Reads.txt", gp.output_dir);
    let general_out = format!("{}/Basic_Statistics_of_Sequencing_Quality.txt", gp.output_dir);
    let bs1_out = format!("{}/Base_distributions_by_read_position_1.txt", gp.output_dir);
    let bs2_out = format!("{}/Base_distributions_by_read_position_2.txt", gp.output_dir);
    let qs1_out = format!(
        "{}/Base_quality_value_distribution_by_read_position_1.txt",
        gp.output_dir
    );
    let qs2_out = format!(
        "{}/Base_quality_value_distribution_by_read_position_2.txt",
        gp.output_dir
    );
    let q20_out1 = format!("{}/Distribution_of_Q20_Q30_bases_by_read_position_1.txt", gp.output_dir);
    let q20_out2 = format!("{}/Distribution_of_Q20_Q30_bases_by_read_position_2.txt", gp.output_dir);
    let trim_stat1 = format!("{}/Statistics_of_Trimming_Position_of_Reads_1.txt", gp.output_dir);
    let trim_stat2 = format!("{}/Statistics_of_Trimming_Position_of_Reads_2.txt", gp.output_dir);

    let mut of_filter = open_or_err(&filter_out)?;
    let mut of_general = open_or_err(&general_out)?;
    let mut of_bs1 = open_or_err(&bs1_out)?;
    let mut of_bs2 = open_or_err(&bs2_out)?;
    let mut of_qs1 = open_or_err(&qs1_out)?;
    let mut of_qs2 = open_or_err(&qs2_out)?;
    let mut of_q20_1 = open_or_err(&q20_out1)?;
    let mut of_q20_2 = open_or_err(&q20_out2)?;
    let mut of_trim1 = open_or_err(&trim_stat1)?;
    let mut of_trim2 = open_or_err(&trim_stat2)?;

    // ---------- Filtered reads ----------
    writeln!(of_filter, "Item\t\t\t\tTotal\tPercentage\tfastq1\tfastq2\toverlap").unwrap();
    let fs = &gv.fs;
    let total_filter = fs.include_global_contam_seq_num
        + fs.include_contam_seq_num
        + fs.short_len_num
        + fs.include_adapter_seq_num
        + fs.low_qual_base_ratio_num
        + fs.mean_quality_num
        + fs.n_ratio_num
        + fs.over_lapped_num
        + fs.high_a_num
        + fs.poly_x_num
        + fs.tile_num
        + fs.fov_num
        + fs.long_len_num;
    writeln!(
        of_filter,
        "Total filtered read pair number\t{}\t100.00%\t\t{}\t{}\t{}",
        total_filter, total_filter, total_filter, total_filter
    )
    .unwrap();
    // v2 order: duplicate(0), limited(0), tile, fov, short, long, global contam,
    // contam, n rate, highA, polyX, low quality, low mean quality, small insert, adapter.
    let items: [(&str, u64, u64, u64, u64); 13] = [
        ("Reads with filtered tile", fs.tile_num, fs.tile_num, fs.tile_num, fs.tile_num),
        ("Reads with filtered fov", fs.fov_num, fs.fov_num, fs.fov_num, fs.fov_num),
        ("Reads too short", fs.short_len_num, fs.short_len_num1, fs.short_len_num2, fs.short_len_num_overlap),
        ("Reads too long", fs.long_len_num, fs.long_len_num1, fs.long_len_num2, fs.long_len_num_overlap),
        ("Reads with global contam sequence", fs.include_global_contam_seq_num, fs.include_global_contam_seq_num1, fs.include_global_contam_seq_num2, fs.include_global_contam_seq_num_overlap),
        ("Reads with contam sequence", fs.include_contam_seq_num, fs.include_contam_seq_num1, fs.include_contam_seq_num2, fs.include_contam_seq_num_overlap),
        ("Reads with n rate exceed", fs.n_ratio_num, fs.n_ratio_num1, fs.n_ratio_num2, fs.n_ratio_num_overlap),
        ("Reads with highA", fs.high_a_num, fs.high_a_num1, fs.high_a_num2, fs.high_a_num_overlap),
        ("Reads with polyX", fs.poly_x_num, fs.poly_x_num1, fs.poly_x_num2, fs.poly_x_num_overlap),
        ("Reads with low quality", fs.low_qual_base_ratio_num, fs.low_qual_base_ratio_num1, fs.low_qual_base_ratio_num2, fs.low_qual_base_ratio_num_overlap),
        ("Reads with low mean quality", fs.mean_quality_num, fs.mean_quality_num1, fs.mean_quality_num2, fs.mean_quality_num_overlap),
        ("Reads with small insert size", fs.over_lapped_num, fs.over_lapped_num, fs.over_lapped_num, fs.over_lapped_num),
        ("Reads with adapter", fs.include_adapter_seq_num, fs.include_adapter_seq_num1, fs.include_adapter_seq_num2, fs.include_adapter_seq_num_overlap),
    ];
    for (name, n, pe1, pe2, ov) in items.iter() {
        if *n > 0 {
            let pct = fmt_f32_2(100.0f32 * (*n as f32) / total_filter as f32);
            writeln!(of_filter, "{}\t{}\t{}%\t{}\t{}\t{}", name, n, pct, pe1, pe2, ov).unwrap();
        }
    }
    drop(of_filter);

    // ---------- Basic statistics ----------
    let mut raw1_rl = 0.0f32;
    let mut raw2_rl = 0.0f32;
    let mut clean1_rl = 0.0f32;
    let mut clean2_rl = 0.0f32;
    let mut filter_r1_ratio = "0.00".to_string();
    let mut filter_r2_ratio = "0.00".to_string();
    let mut raw_r1: [String; 7] = std::array::from_fn(|_| "0.00".to_string());
    let mut clean_r1: [String; 7] = std::array::from_fn(|_| "0.00".to_string());
    let mut raw_r2: [String; 7] = std::array::from_fn(|_| "0.00".to_string());
    let mut clean_r2: [String; 7] = std::array::from_fn(|_| "0.00".to_string());
    if gv.raw1.gs.reads_number != 0 {
        raw1_rl = (1.0f64 * gv.raw1.gs.base_number as f64 / gv.raw1.gs.reads_number as f64) as f32;
        filter_r1_ratio = fmt_f32_2(100.0f32 * total_filter as f32 / gv.raw1.gs.reads_number as f32);
        if gv.raw1.gs.base_number != 0 {
            let b = gv.raw1.gs.base_number as f32;
            raw_r1[0] = fmt_f32_2(100.0f32 * gv.raw1.gs.a_number as f32 / b);
            raw_r1[1] = fmt_f32_2(100.0f32 * gv.raw1.gs.c_number as f32 / b);
            raw_r1[2] = fmt_f32_2(100.0f32 * gv.raw1.gs.g_number as f32 / b);
            raw_r1[3] = fmt_f32_2(100.0f32 * gv.raw1.gs.t_number as f32 / b);
            raw_r1[4] = fmt_f32_2(100.0f32 * gv.raw1.gs.n_number as f32 / b);
            raw_r1[5] = fmt_f32_2(100.0f32 * gv.raw1.gs.q20_num as f32 / b);
            raw_r1[6] = fmt_f32_2(100.0f32 * gv.raw1.gs.q30_num as f32 / b);
        }
    }
    if gv.clean1.gs.reads_number != 0 {
        clean1_rl = (1.0f64 * gv.clean1.gs.base_number as f64 / gv.clean1.gs.reads_number as f64) as f32;
        if gv.clean1.gs.base_number != 0 {
            let b = gv.clean1.gs.base_number as f32;
            clean_r1[0] = fmt_f32_2(100.0f32 * gv.clean1.gs.a_number as f32 / b);
            clean_r1[1] = fmt_f32_2(100.0f32 * gv.clean1.gs.c_number as f32 / b);
            clean_r1[2] = fmt_f32_2(100.0f32 * gv.clean1.gs.g_number as f32 / b);
            clean_r1[3] = fmt_f32_2(100.0f32 * gv.clean1.gs.t_number as f32 / b);
            clean_r1[4] = fmt_f32_2(100.0f32 * gv.clean1.gs.n_number as f32 / b);
            clean_r1[5] = fmt_f32_2(100.0f32 * gv.clean1.gs.q20_num as f32 / b);
            clean_r1[6] = fmt_f32_2(100.0f32 * gv.clean1.gs.q30_num as f32 / b);
        }
    }
    if gv.raw2.gs.reads_number != 0 {
        raw2_rl = (1.0f64 * gv.raw2.gs.base_number as f64 / gv.raw2.gs.reads_number as f64) as f32;
        filter_r2_ratio = fmt_f32_2(100.0f32 * total_filter as f32 / gv.raw2.gs.reads_number as f32);
        if gv.raw2.gs.base_number != 0 {
            let b = gv.raw2.gs.base_number as f32;
            raw_r2[0] = fmt_f32_2(100.0f32 * gv.raw2.gs.a_number as f32 / b);
            raw_r2[1] = fmt_f32_2(100.0f32 * gv.raw2.gs.c_number as f32 / b);
            raw_r2[2] = fmt_f32_2(100.0f32 * gv.raw2.gs.g_number as f32 / b);
            raw_r2[3] = fmt_f32_2(100.0f32 * gv.raw2.gs.t_number as f32 / b);
            raw_r2[4] = fmt_f32_2(100.0f32 * gv.raw2.gs.n_number as f32 / b);
            raw_r2[5] = fmt_f32_2(100.0f32 * gv.raw2.gs.q20_num as f32 / b);
            raw_r2[6] = fmt_f32_2(100.0f32 * gv.raw2.gs.q30_num as f32 / b);
        }
    }
    if gv.clean2.gs.reads_number != 0 {
        clean2_rl = (1.0f64 * gv.clean2.gs.base_number as f64 / gv.clean2.gs.reads_number as f64) as f32;
        if gv.clean2.gs.base_number != 0 {
            let b = gv.clean2.gs.base_number as f32;
            clean_r2[0] = fmt_f32_2(100.0f32 * gv.clean2.gs.a_number as f32 / b);
            clean_r2[1] = fmt_f32_2(100.0f32 * gv.clean2.gs.c_number as f32 / b);
            clean_r2[2] = fmt_f32_2(100.0f32 * gv.clean2.gs.g_number as f32 / b);
            clean_r2[3] = fmt_f32_2(100.0f32 * gv.clean2.gs.t_number as f32 / b);
            clean_r2[4] = fmt_f32_2(100.0f32 * gv.clean2.gs.n_number as f32 / b);
            clean_r2[5] = fmt_f32_2(100.0f32 * gv.clean2.gs.q20_num as f32 / b);
            clean_r2[6] = fmt_f32_2(100.0f32 * gv.clean2.gs.q30_num as f32 / b);
        }
    }
    writeln!(
        of_general,
        "Item\traw reads(fq1)\tclean reads(fq1)\traw reads(fq2)\tclean reads(fq2)"
    )
    .unwrap();
    writeln!(
        of_general,
        "Read length\t{}\t{}\t{}\t{}",
        fmt_f32_1(raw1_rl),
        fmt_f32_1(clean1_rl),
        fmt_f32_1(raw2_rl),
        fmt_f32_1(clean2_rl)
    )
    .unwrap();
    writeln!(
        of_general,
        "Total number of reads\t{} (100.00%)\t{} (100.00%)\t{} (100.00%)\t{} (100.00%)",
        gv.raw1.gs.reads_number,
        gv.clean1.gs.reads_number,
        gv.raw2.gs.reads_number,
        gv.clean2.gs.reads_number
    )
    .unwrap();
    writeln!(
        of_general,
        "Number of filtered reads\t{} ({}%)\t-\t{} ({}%)\t-",
        total_filter, filter_r1_ratio, total_filter, filter_r2_ratio
    )
    .unwrap();
    writeln!(
        of_general,
        "Total number of bases\t{} (100.00%)\t{} (100.00%)\t{} (100.00%)\t{} (100.00%)",
        gv.raw1.gs.base_number,
        gv.clean1.gs.base_number,
        gv.raw2.gs.base_number,
        gv.clean2.gs.base_number
    )
    .unwrap();
    writeln!(
        of_general,
        "Number of filtered bases\t{} ({}%)\t-\t{} ({}%)\t-",
        fs.filtered_base_num1, filter_r1_ratio, fs.filtered_base_num2, filter_r2_ratio
    )
    .unwrap();
    writeln!(
        of_general,
        "Number of base A\t{} ({}%)\t{} ({}%)\t{} ({}%)\t{} ({}%)",
        gv.raw1.gs.a_number, raw_r1[0], gv.clean1.gs.a_number, clean_r1[0],
        gv.raw2.gs.a_number, raw_r2[0], gv.clean2.gs.a_number, clean_r2[0]
    )
    .unwrap();
    writeln!(
        of_general,
        "Number of base C\t{} ({}%)\t{} ({}%)\t{} ({}%)\t{} ({}%)",
        gv.raw1.gs.c_number, raw_r1[1], gv.clean1.gs.c_number, clean_r1[1],
        gv.raw2.gs.c_number, raw_r2[1], gv.clean2.gs.c_number, clean_r2[1]
    )
    .unwrap();
    writeln!(
        of_general,
        "Number of base G\t{} ({}%)\t{} ({}%)\t{} ({}%)\t{} ({}%)",
        gv.raw1.gs.g_number, raw_r1[2], gv.clean1.gs.g_number, clean_r1[2],
        gv.raw2.gs.g_number, raw_r2[2], gv.clean2.gs.g_number, clean_r2[2]
    )
    .unwrap();
    writeln!(
        of_general,
        "Number of base T\t{} ({}%)\t{} ({}%)\t{} ({}%)\t{} ({}%)",
        gv.raw1.gs.t_number, raw_r1[3], gv.clean1.gs.t_number, clean_r1[3],
        gv.raw2.gs.t_number, raw_r2[3], gv.clean2.gs.t_number, clean_r2[3]
    )
    .unwrap();
    writeln!(
        of_general,
        "Number of base N\t{} ({}%)\t{} ({}%)\t{} ({}%)\t{} ({}%)",
        gv.raw1.gs.n_number, raw_r1[4], gv.clean1.gs.n_number, clean_r1[4],
        gv.raw2.gs.n_number, raw_r2[4], gv.clean2.gs.n_number, clean_r2[4]
    )
    .unwrap();
    writeln!(
        of_general,
        "Q20 number\t{} ({}%)\t{} ({}%)\t{} ({}%)\t{} ({}%)",
        gv.raw1.gs.q20_num, raw_r1[5], gv.clean1.gs.q20_num, clean_r1[5],
        gv.raw2.gs.q20_num, raw_r2[5], gv.clean2.gs.q20_num, clean_r2[5]
    )
    .unwrap();
    writeln!(
        of_general,
        "Q30 number\t{} ({}%)\t{} ({}%)\t{} ({}%)\t{} ({}%)",
        gv.raw1.gs.q30_num, raw_r1[6], gv.clean1.gs.q30_num, clean_r1[6],
        gv.raw2.gs.q30_num, raw_r2[6], gv.clean2.gs.q30_num, clean_r2[6]
    )
    .unwrap();
    drop(of_general);

    // ---------- Base distributions ----------
    writeln!(of_bs1, "Pos\tA\tC\tG\tT\tN\tclean A\tclean C\tclean G\tclean T\tclean N").unwrap();
    writeln!(of_bs2, "Pos\tA\tC\tG\tT\tN\tclean A\tclean C\tclean G\tclean T\tclean N").unwrap();
    let base_read_max_length = gv.raw1.gs.read_max_length.max(gv.raw2.gs.read_max_length);
    for i in 0..base_read_max_length {
        let iu = i as usize;
        if iu < gv.raw1.gs.read_max_length as usize {
            let mut line = format!("{}\t", i + 1);
            let mut raw_total = 0u64;
            let mut clean_total = 0u64;
            for j in 0..5 {
                raw_total += gv.raw1.bs.position_acgt_content[iu][j];
                clean_total += gv.clean1.bs.position_acgt_content[iu][j];
            }
            for j in 0..5 {
                if raw_total == 0 {
                    line.push_str("0%\t");
                } else {
                    line.push_str(&format!(
                        "{}%\t",
                        fmt_f32_2(100.0f32 * gv.raw1.bs.position_acgt_content[iu][j] as f32 / raw_total as f32)
                    ));
                }
            }
            for j in 0..5 {
                if clean_total == 0 {
                    line.push_str("0%");
                } else {
                    line.push_str(&fmt_f32_2(100.0f32 * gv.clean1.bs.position_acgt_content[iu][j] as f32 / clean_total as f32));
                    line.push('%');
                }
                if j != 4 {
                    line.push('\t');
                }
            }
            writeln!(of_bs1, "{}", line).unwrap();
        }
        if iu < gv.raw2.gs.read_max_length as usize {
            let mut line = format!("{}\t", i + 1);
            let mut raw_total = 0u64;
            let mut clean_total = 0u64;
            for j in 0..5 {
                raw_total += gv.raw2.bs.position_acgt_content[iu][j];
                clean_total += gv.clean2.bs.position_acgt_content[iu][j];
            }
            for j in 0..5 {
                if raw_total == 0 {
                    line.push_str("0%\t");
                } else {
                    line.push_str(&format!(
                        "{}%\t",
                        fmt_f32_2(100.0f32 * gv.raw2.bs.position_acgt_content[iu][j] as f32 / raw_total as f32)
                    ));
                }
            }
            for j in 0..5 {
                if clean_total == 0 {
                    line.push_str("0%");
                } else {
                    line.push_str(&fmt_f32_2(100.0f32 * gv.clean2.bs.position_acgt_content[iu][j] as f32 / clean_total as f32));
                    line.push('%');
                }
                if j != 4 {
                    line.push('\t');
                }
            }
            writeln!(of_bs2, "{}", line).unwrap();
        }
    }
    drop(of_bs1);
    drop(of_bs2);

    // ---------- Quality distributions ----------
    let bins = gp.quality_bins();
    let mut max_qual1 = 0usize;
    let mut max_qual2 = 0usize;
    for i in 0..gv.raw1.gs.read_max_length as usize {
        for j in 1..=gp.max_base_quality as usize {
            if gv.raw1.qs.position_qual[i * bins + j] > 0 && max_qual1 < j {
                max_qual1 = j;
            }
        }
    }
    for i in 0..gv.raw2.gs.read_max_length as usize {
        for j in 1..=gp.max_base_quality as usize {
            if gv.raw2.qs.position_qual[i * bins + j] > 0 && max_qual2 < j {
                max_qual2 = j;
            }
        }
    }
    let mut report_max_qual1 = max_qual1;
    let mut report_max_qual2 = max_qual2;
    if gv.raw1.gs.read_max_length == gv.raw2.gs.read_max_length
        && gv.clean1.gs.read_max_length == gv.clean2.gs.read_max_length
    {
        report_max_qual1 = max_qual1.max(max_qual2);
        report_max_qual2 = report_max_qual1;
    }
    let read_max_length = gv
        .raw1
        .gs
        .read_max_length
        .max(gv.raw2.gs.read_max_length)
        .max(gv.clean1.gs.read_max_length.max(gv.clean2.gs.read_max_length));
    let rml = read_max_length as usize;
    let mut raw1_q20 = vec![0.0f32; rml];
    let mut raw1_q30 = vec![0.0f32; rml];
    let mut raw2_q20 = vec![0.0f32; rml];
    let mut raw2_q30 = vec![0.0f32; rml];
    let mut clean1_q20 = vec![0.0f32; rml];
    let mut clean1_q30 = vec![0.0f32; rml];
    let mut clean2_q20 = vec![0.0f32; rml];
    let mut clean2_q30 = vec![0.0f32; rml];

    let qs_head = |r: usize| -> String {
        let mut h = "Pos\t".to_string();
        for i in 0..=r {
            h.push_str(&format!("Q{}\t", i));
        }
        h.push_str("Mean\tMedian\tLower quartile\tUpper quartile\t10th percentile\t90th percentile\n");
        h
    };

    writeln!(of_qs1, "#raw fastq1 quality distribution").unwrap();
    writeln!(of_qs2, "#raw fastq2 quality distribution").unwrap();
    of_qs1.write_all(qs_head(report_max_qual1).as_bytes()).unwrap();
    of_qs2.write_all(qs_head(report_max_qual2).as_bytes()).unwrap();

    for i in 0..read_max_length {
        let iu = i as usize;
        if iu < gv.raw1.gs.read_max_length as usize {
            let mut line = format!("{}\t", i + 1);
            let mut q20_num = 0u64;
            let mut q30_num = 0u64;
            let mut total = 0u64;
            let row = &gv.raw1.qs.position_qual[iu * bins..(iu + 1) * bins];
            for j in 0..=report_max_qual1 {
                if j >= 20 {
                    q20_num += row[j];
                }
                if j >= 30 {
                    q30_num += row[j];
                }
                total += row[j];
                line.push_str(&format!("{}\t", row[j]));
            }
            raw1_q20[iu] = if total == 0 { 0.0 } else { q20_num as f32 / total as f32 };
            raw1_q30[iu] = if total == 0 { 0.0 } else { q30_num as f32 / total as f32 };
            let quar = cal_quar_from_array(row, report_max_qual1 + 1);
            line.push_str(&fmt_f32_2(quar.mean));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.median));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.lower_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.upper_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.first10_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.last10_quar));
            line.push('\n');
            of_qs1.write_all(line.as_bytes()).unwrap();
        }
        if iu < gv.raw2.gs.read_max_length as usize {
            let mut line = format!("{}\t", i + 1);
            let mut q20_num = 0u64;
            let mut q30_num = 0u64;
            let mut total = 0u64;
            let row = &gv.raw2.qs.position_qual[iu * bins..(iu + 1) * bins];
            for j in 0..=report_max_qual2 {
                if j >= 20 {
                    q20_num += row[j];
                }
                if j >= 30 {
                    q30_num += row[j];
                }
                total += row[j];
                line.push_str(&format!("{}\t", row[j]));
            }
            raw2_q20[iu] = if total == 0 { 0.0 } else { q20_num as f32 / total as f32 };
            raw2_q30[iu] = if total == 0 { 0.0 } else { q30_num as f32 / total as f32 };
            let quar = cal_quar_from_array(row, report_max_qual2 + 1);
            line.push_str(&fmt_f32_2(quar.mean));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.median));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.lower_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.upper_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.first10_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.last10_quar));
            line.push('\n');
            of_qs2.write_all(line.as_bytes()).unwrap();
        }
    }
    writeln!(of_qs1, "#clean fastq1 quality distribution").unwrap();
    writeln!(of_qs2, "#clean fastq2 quality distribution").unwrap();
    of_qs1.write_all(qs_head(report_max_qual1).as_bytes()).unwrap();
    of_qs2.write_all(qs_head(report_max_qual2).as_bytes()).unwrap();

    // ---------- Q20/Q30 (interleaved with clean quality rows) ----------
    writeln!(
        of_q20_1,
        "Position in reads\tPercentage of Q20+ bases\tPercentage of Q30+ bases\tPercentage of Clean Q20+\tPercentage of Clean Q30+"
    )
    .unwrap();
    writeln!(
        of_q20_2,
        "Position in reads\tPercentage of Q20+ bases\tPercentage of Q30+ bases\tPercentage of Clean Q20+\tPercentage of Clean Q30+"
    )
    .unwrap();
    let clean_read_max_length = gv.clean1.gs.read_max_length.max(gv.clean2.gs.read_max_length);
    for i in 0..clean_read_max_length {
        let iu = i as usize;
        if iu < gv.clean1.gs.read_max_length as usize {
            let mut line = format!("{}\t", i + 1);
            let mut q20_num = 0u64;
            let mut q30_num = 0u64;
            let mut total = 0u64;
            let row = &gv.clean1.qs.position_qual[iu * bins..(iu + 1) * bins];
            for j in 0..=report_max_qual1 {
                if j >= 20 {
                    q20_num += row[j];
                }
                if j >= 30 {
                    q30_num += row[j];
                }
                total += row[j];
                line.push_str(&format!("{}\t", row[j]));
            }
            clean1_q20[iu] = if total == 0 { 0.0 } else { q20_num as f32 / total as f32 };
            clean1_q30[iu] = if total == 0 { 0.0 } else { q30_num as f32 / total as f32 };
            let quar = cal_quar_from_array(row, report_max_qual1 + 1);
            line.push_str(&fmt_f32_2(quar.mean));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.median));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.lower_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.upper_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.first10_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.last10_quar));
            line.push('\n');
            of_qs1.write_all(line.as_bytes()).unwrap();
            writeln!(
                of_q20_1,
                "{}\t{}%\t{}%\t{}%\t{}%",
                i + 1,
                fmt_f32_2(100.0f32 * raw1_q20[iu]),
                fmt_f32_2(100.0f32 * raw1_q30[iu]),
                fmt_f32_2(100.0f32 * clean1_q20[iu]),
                fmt_f32_2(100.0f32 * clean1_q30[iu])
            )
            .unwrap();
        }
        if iu < gv.clean2.gs.read_max_length as usize {
            let mut line = format!("{}\t", i + 1);
            let mut q20_num = 0u64;
            let mut q30_num = 0u64;
            let mut total = 0u64;
            let row = &gv.clean2.qs.position_qual[iu * bins..(iu + 1) * bins];
            for j in 0..=report_max_qual2 {
                if j >= 20 {
                    q20_num += row[j];
                }
                if j >= 30 {
                    q30_num += row[j];
                }
                total += row[j];
                line.push_str(&format!("{}\t", row[j]));
            }
            clean2_q20[iu] = if total == 0 { 0.0 } else { q20_num as f32 / total as f32 };
            clean2_q30[iu] = if total == 0 { 0.0 } else { q30_num as f32 / total as f32 };
            let quar = cal_quar_from_array(row, report_max_qual2 + 1);
            line.push_str(&fmt_f32_2(quar.mean));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.median));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.lower_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.upper_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.first10_quar));
            line.push('\t');
            line.push_str(&fmt_f32_0(quar.last10_quar));
            line.push('\n');
            of_qs2.write_all(line.as_bytes()).unwrap();
            writeln!(
                of_q20_2,
                "{}\t{}%\t{}%\t{}%\t{}%",
                i + 1,
                fmt_f32_2(100.0f32 * raw2_q20[iu]),
                fmt_f32_2(100.0f32 * raw2_q30[iu]),
                fmt_f32_2(100.0f32 * clean2_q20[iu]),
                fmt_f32_2(100.0f32 * clean2_q30[iu])
            )
            .unwrap();
        }
    }
    drop(of_qs1);
    drop(of_qs2);
    drop(of_q20_1);
    drop(of_q20_2);

    // ---------- Trim positions ----------
    writeln!(
        of_trim1,
        "Pos\tHeadLowQual\tHeadFixLen\tTailAdapter\tTailLowQual\tTailFixLen\tCleanHeadLowQual\tCleanHeadFixLen\tCleanTailAdapter\tCleanTailLowQual\tCleanTailFixLen"
    )
    .unwrap();
    writeln!(
        of_trim2,
        "Pos\tHeadLowQual\tHeadFixLen\tTailAdapter\tTailLowQual\tTailFixLen\tCleanHeadLowQual\tCleanHeadFixLen\tCleanTailAdapter\tCleanTailLowQual\tCleanTailFixLen"
    )
    .unwrap();
    let mut head_total1 = 0u64;
    let mut tail_total1 = 0u64;
    let mut head_total2 = 0u64;
    let mut tail_total2 = 0u64;
    let mut head_total_clean1 = 0u64;
    let mut tail_total_clean1 = 0u64;
    let mut head_total_clean2 = 0u64;
    let mut tail_total_clean2 = 0u64;
    let trim_stat_max_length1 = gv
        .raw1
        .gs
        .read_max_length
        .max(gv.trim1.gs.read_max_length)
        .max(gv.clean1.gs.read_max_length);
    let trim_stat_max_length2 = gv
        .raw2
        .gs
        .read_max_length
        .max(gv.trim2.gs.read_max_length)
        .max(gv.clean2.gs.read_max_length);
    let trim_stat_max_length = trim_stat_max_length1.max(trim_stat_max_length2);
    let tml = trim_stat_max_length as usize;
    let ts1 = &gv.raw1.ts;
    let ts2 = &gv.raw2.ts;
    let tsc1 = &gv.clean1.ts;
    let tsc2 = &gv.clean2.ts;
    for i in 0..=trim_stat_max_length {
        let iu = i as usize;
        if iu <= gv.raw1.gs.read_max_length as usize {
            head_total1 += ts1.ht[iu] + ts1.hlq[iu];
            tail_total1 += ts1.ta[iu] + ts1.tlq[iu] + ts1.tt[iu];
        }
        if iu <= gv.raw2.gs.read_max_length as usize {
            head_total2 += ts2.ht[iu] + ts2.hlq[iu];
            tail_total2 += ts2.ta[iu] + ts2.tlq[iu] + ts2.tt[iu];
        }
        if iu <= gv.clean1.gs.read_max_length as usize {
            head_total_clean1 += tsc1.ht[iu] + tsc1.hlq[iu];
            tail_total_clean1 += tsc1.ta[iu] + tsc1.tlq[iu] + tsc1.tt[iu];
        }
        if iu <= gv.clean2.gs.read_max_length as usize {
            head_total_clean2 += tsc2.ht[iu] + tsc2.hlq[iu];
            tail_total_clean2 += tsc2.ta[iu] + tsc2.tlq[iu] + tsc2.tt[iu];
        }
    }
    for i in 1..=tml {
        if i <= trim_stat_max_length1 as usize {
            let mut line = format!("{}\t", i);
            if head_total1 > 0 {
                line.push_str(&format!("{}\t{}%\t", ts1.hlq[i], fmt_f32_2(100.0f32 * ts1.hlq[i] as f32 / head_total1 as f32)));
                line.push_str(&format!("{}\t{}%\t", ts1.ht[i], fmt_f32_2(100.0f32 * ts1.ht[i] as f32 / head_total1 as f32)));
            } else {
                line.push_str(&format!("{}\t0.00%\t", ts1.hlq[i]));
                line.push_str(&format!("{}\t0.00%\t", ts1.ht[i]));
            }
            if tail_total1 > 0 {
                line.push_str(&format!("{}\t{}%\t", ts1.ta[i], fmt_f32_2(100.0f32 * ts1.ta[i] as f32 / tail_total1 as f32)));
                line.push_str(&format!("{}\t{}%\t", ts1.tlq[i], fmt_f32_2(100.0f32 * ts1.tlq[i] as f32 / tail_total1 as f32)));
                line.push_str(&format!("{}\t{}%\t", ts1.tt[i], fmt_f32_2(100.0f32 * ts1.tt[i] as f32 / tail_total1 as f32)));
            } else {
                line.push_str(&format!("{}\t0.00%\t", ts1.ta[i]));
                line.push_str(&format!("{}\t0.00%\t", ts1.tlq[i]));
                line.push_str(&format!("{}\t0.00%\t", ts1.tt[i]));
            }
            if head_total_clean1 > 0 {
                line.push_str(&format!("{}\t{}%\t", tsc1.hlq[i], fmt_f32_2(100.0f32 * tsc1.hlq[i] as f32 / head_total_clean1 as f32)));
                line.push_str(&format!("{}\t{}%\t", tsc1.ht[i], fmt_f32_2(100.0f32 * tsc1.ht[i] as f32 / head_total_clean1 as f32)));
            } else {
                line.push_str(&format!("{}\t0.00%\t", tsc1.hlq[i]));
                line.push_str(&format!("{}\t0.00%\t", tsc1.ht[i]));
            }
            if tail_total_clean1 > 0 {
                line.push_str(&format!("{}\t{}%\t", tsc1.ta[i], fmt_f32_2(100.0f32 * tsc1.ta[i] as f32 / tail_total_clean1 as f32)));
                line.push_str(&format!("{}\t{}%\t", tsc1.tlq[i], fmt_f32_2(100.0f32 * tsc1.tlq[i] as f32 / tail_total_clean1 as f32)));
                line.push_str(&format!("{}\t{}%", tsc1.tt[i], fmt_f32_2(100.0f32 * tsc1.tt[i] as f32 / tail_total_clean1 as f32)));
            } else {
                line.push_str(&format!("{}\t0.00%\t", tsc1.ta[i]));
                line.push_str(&format!("{}\t0.00%\t", tsc1.tlq[i]));
                line.push_str(&format!("{}\t0.00%", tsc1.tt[i]));
            }
            writeln!(of_trim1, "{}", line).unwrap();
        }
        if i <= trim_stat_max_length2 as usize {
            let mut line = format!("{}\t", i);
            if head_total2 > 0 {
                line.push_str(&format!("{}\t{}%\t", ts2.hlq[i], fmt_f32_2(100.0f32 * ts2.hlq[i] as f32 / head_total2 as f32)));
                line.push_str(&format!("{}\t{}%\t", ts2.ht[i], fmt_f32_2(100.0f32 * ts2.ht[i] as f32 / head_total2 as f32)));
            } else {
                line.push_str(&format!("{}\t0.00%\t", ts2.hlq[i]));
                line.push_str(&format!("{}\t0.00%\t", ts2.ht[i]));
            }
            if tail_total2 > 0 {
                line.push_str(&format!("{}\t{}%\t", ts2.ta[i], fmt_f32_2(100.0f32 * ts2.ta[i] as f32 / tail_total2 as f32)));
                line.push_str(&format!("{}\t{}%\t", ts2.tlq[i], fmt_f32_2(100.0f32 * ts2.tlq[i] as f32 / tail_total2 as f32)));
                line.push_str(&format!("{}\t{}%\t", ts2.tt[i], fmt_f32_2(100.0f32 * ts2.tt[i] as f32 / tail_total2 as f32)));
            } else {
                line.push_str(&format!("{}\t0.00%\t", ts2.ta[i]));
                line.push_str(&format!("{}\t0.00%\t", ts2.tlq[i]));
                line.push_str(&format!("{}\t0.00%\t", ts2.tt[i]));
            }
            if head_total_clean2 > 0 {
                line.push_str(&format!("{}\t{}%\t", tsc2.hlq[i], fmt_f32_2(100.0f32 * tsc2.hlq[i] as f32 / head_total_clean2 as f32)));
                line.push_str(&format!("{}\t{}%\t", tsc2.ht[i], fmt_f32_2(100.0f32 * tsc2.ht[i] as f32 / head_total_clean2 as f32)));
            } else {
                line.push_str(&format!("{}\t0.00%\t", tsc2.hlq[i]));
                line.push_str(&format!("{}\t0.00%\t", tsc2.ht[i]));
            }
            if tail_total_clean2 > 0 {
                line.push_str(&format!("{}\t{}%\t", tsc2.ta[i], fmt_f32_2(100.0f32 * tsc2.ta[i] as f32 / tail_total_clean2 as f32)));
                line.push_str(&format!("{}\t{}%\t", tsc2.tlq[i], fmt_f32_2(100.0f32 * tsc2.tlq[i] as f32 / tail_total_clean2 as f32)));
                line.push_str(&format!("{}\t{}%", tsc2.tt[i], fmt_f32_2(100.0f32 * tsc2.tt[i] as f32 / tail_total_clean2 as f32)));
            } else {
                line.push_str(&format!("{}\t0.00%\t", tsc2.ta[i]));
                line.push_str(&format!("{}\t0.00%\t", tsc2.tlq[i]));
                line.push_str(&format!("{}\t0.00%", tsc2.tt[i]));
            }
            writeln!(of_trim2, "{}", line).unwrap();
        }
    }
    drop(of_trim1);
    drop(of_trim2);
    Ok(())
}

fn print_stat_se(gp: &GlobalParams, gv: &GlobalStats) -> Result<(), String> {
    let fs = &gv.fs;
    let total_filter = fs.include_global_contam_seq_num
        + fs.include_contam_seq_num
        + fs.short_len_num
        + fs.include_adapter_seq_num
        + fs.low_qual_base_ratio_num
        + fs.mean_quality_num
        + fs.n_ratio_num
        + fs.over_lapped_num
        + fs.high_a_num
        + fs.poly_x_num
        + fs.tile_num
        + fs.fov_num
        + fs.long_len_num;
    let filter_path = format!("{}/Statistics_of_Filtered_Reads.txt", gp.output_dir);
    let general_path = format!("{}/Basic_Statistics_of_Sequencing_Quality.txt", gp.output_dir);
    let base_path = format!("{}/Base_distributions_by_read_position_1.txt", gp.output_dir);
    let qual_path = format!(
        "{}/Base_quality_value_distribution_by_read_position_1.txt",
        gp.output_dir
    );
    let q20_path = format!("{}/Distribution_of_Q20_Q30_bases_by_read_position_1.txt", gp.output_dir);
    let trim_path = format!("{}/Statistics_of_Trimming_Position_of_Reads_1.txt", gp.output_dir);

    let mut filter = open_or_err(&filter_path)?;
    writeln!(filter, "Item\tTotal\tPercentage\tfastq1\toverlap").unwrap();
    writeln!(filter, "Total filtered read number\t{}\t100.00%\t{}\t0", total_filter, total_filter).unwrap();
    let items = [
        ("Reads with filtered tile", fs.tile_num, fs.tile_num),
        ("Reads with filtered fov", fs.fov_num, fs.fov_num),
        ("Reads too short", fs.short_len_num, fs.short_len_num1),
        ("Reads too long", fs.long_len_num, fs.long_len_num1),
        ("Reads with global contam sequence", fs.include_global_contam_seq_num, fs.include_global_contam_seq_num1),
        ("Reads with contam sequence", fs.include_contam_seq_num, fs.include_contam_seq_num1),
        ("Reads with n rate exceed", fs.n_ratio_num, fs.n_ratio_num1),
        ("Reads with highA", fs.high_a_num, fs.high_a_num1),
        ("Reads with polyX", fs.poly_x_num, fs.poly_x_num1),
        ("Reads with low quality", fs.low_qual_base_ratio_num, fs.low_qual_base_ratio_num1),
        ("Reads with low mean quality", fs.mean_quality_num, fs.mean_quality_num1),
        ("Reads with small insert size", fs.over_lapped_num, 0),
        ("Reads with adapter", fs.include_adapter_seq_num, fs.include_adapter_seq_num1),
    ];
    for (name, total, first) in items {
        if total > 0 {
            let percentage = if total_filter == 0 {
                0.0
            } else {
                100.0 * total as f32 / total_filter as f32
            };
            writeln!(filter, "{}\t{}\t{}%\t{}\t0", name, total, fmt_f32_2(percentage), first).unwrap();
        }
    }
    drop(filter);

    let raw = &gv.raw1.gs;
    let clean = &gv.clean1.gs;
    let read_filter_percentage = if raw.reads_number == 0 {
        0.0
    } else {
        100.0 * total_filter as f32 / raw.reads_number as f32
    };
    let mut general = open_or_err(&general_path)?;
    writeln!(general, "Item\traw reads(fq1)\tclean reads(fq1)").unwrap();
    let raw_len = if raw.reads_number == 0 { 0.0 } else { raw.base_number as f32 / raw.reads_number as f32 };
    let clean_len = if clean.reads_number == 0 { 0.0 } else { clean.base_number as f32 / clean.reads_number as f32 };
    writeln!(general, "Read length\t{}\t{}", fmt_f32_1(raw_len), fmt_f32_1(clean_len)).unwrap();
    writeln!(general, "Total number of reads\t{} (100.00%)\t{} (100.00%)", raw.reads_number, clean.reads_number).unwrap();
    writeln!(general, "Number of filtered reads\t{} ({}%)\t-", total_filter, fmt_f32_2(read_filter_percentage)).unwrap();
    writeln!(general, "Total number of bases\t{} (100.00%)\t{} (100.00%)", raw.base_number, clean.base_number).unwrap();
    writeln!(general, "Number of filtered bases\t{} ({}%)\t-", fs.filtered_base_num1, fmt_f32_2(read_filter_percentage)).unwrap();
    for (label, raw_value, clean_value) in [
        ("Number of base A", raw.a_number, clean.a_number),
        ("Number of base C", raw.c_number, clean.c_number),
        ("Number of base G", raw.g_number, clean.g_number),
        ("Number of base T", raw.t_number, clean.t_number),
        ("Number of base N", raw.n_number, clean.n_number),
        ("Q20 number", raw.q20_num, clean.q20_num),
        ("Q30 number", raw.q30_num, clean.q30_num),
    ] {
        let raw_pct = if raw.base_number == 0 { 0.0 } else { 100.0 * raw_value as f32 / raw.base_number as f32 };
        let clean_pct = if clean.base_number == 0 { 0.0 } else { 100.0 * clean_value as f32 / clean.base_number as f32 };
        writeln!(general, "{}\t{} ({}%)\t{} ({}%)", label, raw_value, fmt_f32_2(raw_pct), clean_value, fmt_f32_2(clean_pct)).unwrap();
    }
    drop(general);

    let mut base = open_or_err(&base_path)?;
    writeln!(base, "Pos\tA\tC\tG\tT\tN\tclean A\tclean C\tclean G\tclean T\tclean N").unwrap();
    for i in 0..raw.read_max_length {
        let index = i as usize;
        let raw_total: u64 = gv.raw1.bs.position_acgt_content[index].iter().sum();
        let clean_total: u64 = gv.clean1.bs.position_acgt_content[index].iter().sum();
        let mut line = format!("{}\t", i + 1);
        for value in gv.raw1.bs.position_acgt_content[index] {
            line.push_str(&format!("{}%\t", if raw_total == 0 { "0.00".to_string() } else { fmt_f32_2(100.0 * value as f32 / raw_total as f32) }));
        }
        for (offset, value) in gv.clean1.bs.position_acgt_content[index].iter().enumerate() {
            let percentage = if clean_total == 0 { "0.00".to_string() } else { fmt_f32_2(100.0 * *value as f32 / clean_total as f32) };
            line.push_str(&percentage);
            line.push('%');
            if offset != 4 { line.push('\t'); }
        }
        writeln!(base, "{}", line).unwrap();
    }
    drop(base);

    let bins = gp.quality_bins();
    let mut max_qual = 0usize;
    for i in 0..raw.read_max_length as usize {
        for j in 1..=gp.max_base_quality as usize {
            if gv.raw1.qs.position_qual[i * bins + j] > 0 {
                max_qual = max_qual.max(j);
            }
        }
    }
    let mut quality = open_or_err(&qual_path)?;
    writeln!(quality, "#raw fastq1 quality distribution").unwrap();
    let mut header = "Pos\t".to_string();
    for i in 0..=max_qual { header.push_str(&format!("Q{}\t", i)); }
    header.push_str("Mean\tMedian\tLower quartile\tUpper quartile\t10th percentile\t90th percentile");
    writeln!(quality, "{}", header).unwrap();
    let mut raw_q20 = vec![0.0f32; raw.read_max_length as usize];
    let mut raw_q30 = vec![0.0f32; raw.read_max_length as usize];
    let mut clean_q20 = vec![0.0f32; raw.read_max_length as usize];
    let mut clean_q30 = vec![0.0f32; raw.read_max_length as usize];
    for i in 0..raw.read_max_length as usize {
        let row = &gv.raw1.qs.position_qual[i * bins..(i + 1) * bins];
        let mut line = format!("{}\t", i + 1);
        let mut q20 = 0u64;
        let mut q30 = 0u64;
        let mut total = 0u64;
        for j in 0..=max_qual {
            if j >= 20 { q20 += row[j]; }
            if j >= 30 { q30 += row[j]; }
            total += row[j];
            line.push_str(&format!("{}\t", row[j]));
        }
        raw_q20[i] = if total == 0 { 0.0 } else { q20 as f32 / total as f32 };
        raw_q30[i] = if total == 0 { 0.0 } else { q30 as f32 / total as f32 };
        let quartile = cal_quar_from_array(row, max_qual + 1);
        line.push_str(&format!("{}\t{}\t{}\t{}\t{}\t{}", fmt_f32_2(quartile.mean), fmt_f32_0(quartile.median), fmt_f32_0(quartile.lower_quar), fmt_f32_0(quartile.upper_quar), fmt_f32_0(quartile.first10_quar), fmt_f32_0(quartile.last10_quar)));
        writeln!(quality, "{}", line).unwrap();
        let clean_row = &gv.clean1.qs.position_qual[i * bins..(i + 1) * bins];
        let clean_total: u64 = clean_row.iter().take(max_qual + 1).sum();
        clean_q20[i] = if clean_total == 0 { 0.0 } else { clean_row.iter().enumerate().filter(|(j, _)| *j >= 20).map(|(_, n)| *n).sum::<u64>() as f32 / clean_total as f32 };
        clean_q30[i] = if clean_total == 0 { 0.0 } else { clean_row.iter().enumerate().filter(|(j, _)| *j >= 30).map(|(_, n)| *n).sum::<u64>() as f32 / clean_total as f32 };
    }
    drop(quality);
    let mut q20 = open_or_err(&q20_path)?;
    writeln!(q20, "Pos\traw Q20\traw Q30\tclean Q20\tclean Q30").unwrap();
    for i in 0..raw.read_max_length as usize {
        writeln!(q20, "{}\t{}%\t{}%\t{}%\t{}%", i + 1, fmt_f32_2(100.0 * raw_q20[i]), fmt_f32_2(100.0 * raw_q30[i]), fmt_f32_2(100.0 * clean_q20[i]), fmt_f32_2(100.0 * clean_q30[i])).unwrap();
    }
    drop(q20);

    let mut trim = open_or_err(&trim_path)?;
    writeln!(trim, "Pos\tHeadLowQual\tHeadFixLen\tTailAdapter\tTailLowQual\tTailFixLen\tCleanHeadLowQual\tCleanHeadFixLen\tCleanTailAdapter\tCleanTailLowQual\tCleanTailFixLen").unwrap();
    let max_trim = raw.read_max_length.max(gv.trim1.gs.read_max_length).max(clean.read_max_length) as usize;
    let head_total = gv.raw1.ts.ht.iter().zip(gv.raw1.ts.hlq.iter()).map(|(a, b)| a + b).sum::<u64>();
    let tail_total = gv.raw1.ts.ta.iter().zip(gv.raw1.ts.tlq.iter()).zip(gv.raw1.ts.tt.iter()).map(|((a, b), c)| a + b + c).sum::<u64>();
    let clean_head_total = gv.clean1.ts.ht.iter().zip(gv.clean1.ts.hlq.iter()).map(|(a, b)| a + b).sum::<u64>();
    let clean_tail_total = gv.clean1.ts.ta.iter().zip(gv.clean1.ts.tlq.iter()).zip(gv.clean1.ts.tt.iter()).map(|((a, b), c)| a + b + c).sum::<u64>();
    for i in 1..=max_trim {
        let pct = |n: u64, d: u64| if d == 0 { "0.00".to_string() } else { fmt_f32_2(100.0 * n as f32 / d as f32) };
        writeln!(trim, "{}\t{}\t{}%\t{}\t{}%\t{}\t{}\t{}%\t{}\t{}%\t{}", i, gv.raw1.ts.hlq[i], pct(gv.raw1.ts.hlq[i], head_total), gv.raw1.ts.ht[i], pct(gv.raw1.ts.ht[i], head_total), gv.raw1.ts.ta[i], gv.raw1.ts.tlq[i], pct(gv.raw1.ts.tlq[i], tail_total), gv.raw1.ts.tt[i], pct(gv.raw1.ts.tt[i], tail_total), gv.clean1.ts.ht[i]).unwrap();
        let _ = (clean_head_total, clean_tail_total);
    }
    Ok(())
}

fn open_or_err(path: &str) -> Result<File, String> {
    File::create(path).map_err(|e| format!("cannot open such file,{} ({})", path, e))
}

#[cfg(test)]
mod tests {
    use super::print_stat;
    use crate::params::GlobalParams;
    use crate::stats::GlobalStats;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture_dir() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.scratch/tests");
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!("soapnuke-report-se-{nonce}"))
    }

    #[test]
    fn single_end_report_omits_mate2_columns_and_files() {
        let dir = fixture_dir();
        fs::create_dir_all(&dir).unwrap();
        let mut gp = GlobalParams::new();
        gp.output_dir = dir.to_string_lossy().into_owned();
        let mut gv = GlobalStats::new(&gp);
        gv.raw1.gs.reads_number = 2;
        gv.raw1.gs.base_number = 8;
        gv.raw1.gs.read_max_length = 4;
        gv.clean1.gs.reads_number = 1;
        gv.clean1.gs.base_number = 4;
        gv.clean1.gs.read_max_length = 4;
        gv.fs.n_ratio_num = 1;
        gv.fs.n_ratio_num1 = 1;
        gv.fs.filtered_base_num1 = 4;
        print_stat(&gp, &gv).unwrap();

        let filtered = fs::read_to_string(dir.join("Statistics_of_Filtered_Reads.txt")).unwrap();
        let general = fs::read_to_string(dir.join("Basic_Statistics_of_Sequencing_Quality.txt")).unwrap();
        assert!(filtered.contains("fastq1"));
        assert!(!filtered.contains("fastq2"));
        assert!(!general.contains("fastq2"));
        assert_eq!(filtered.matches("Total filtered read number").count(), 1);
        assert!(!dir.join("Base_distributions_by_read_position_2.txt").exists());
        assert!(!dir.join("Base_quality_value_distribution_by_read_position_2.txt").exists());
        assert!(!dir.join("Distribution_of_Q20_Q30_bases_by_read_position_2.txt").exists());
        assert!(!dir.join("Statistics_of_Trimming_Position_of_Reads_2.txt").exists());
        fs::remove_dir_all(dir).unwrap();
    }
}
