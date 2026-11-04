mod checked_io;
mod dedup_finalize;
mod io_types;
mod decoder;
mod option_metadata;
mod cppfmt;
mod cpu_budget;
mod ffi;
mod filter;
mod output;
mod params;
mod pipeline;
mod rapidgzip_reader;
mod record;
mod report;
mod source;
mod stats;
mod stream;
mod zlib_reader;

mod native_cli;
mod native_adapter;
mod native_adapter_overlap;
mod native_app;
mod native_config;
mod native_execution;
mod native_kernel;
mod native_overlap;
mod native_processing;
mod native_qc;
mod native_runtime;
mod native_screen;
mod native_repetition;
mod native_html;


mod resources_app;
mod report_app;
mod demux;
mod demux_runtime;
mod demux_report;
mod demux_app;
mod consumer_export;

pub fn entry(argv: &[String]) -> i32 {
    if argv.get(1).is_some_and(|a|a=="dedup"){return dedup_app::entry(&argv[2..]);}
    if argv.get(1).is_some_and(|a| a == "demux") { return demux_app::entry(&argv[2..]); }
    if argv.get(1).is_some_and(|a| a == "report") { return report_app::entry(&argv[2..]); }
    if argv.get(1).is_some_and(|a| a == "resources") { return resources_app::entry(&argv[2..]); }
    if argv.get(1).is_some_and(|a| a == "process") { return native_app::entry(&argv[2..]); }
    if argv.len() == 1 || (argv.len() == 2 && matches!(argv[1].as_str(), "--help" | "-h")) { native_config::command_help(); return 0; }
    if argv.len() == 2 && matches!(argv[1].as_str(), "--version" | "-v") { native_config::help(true); return 0; }
    if argv.get(1).is_some_and(|a|a=="filter"){eprintln!("config: filter is no longer a separate command. Use SOAPnuke process with familiar flags (-1/-2/-C/-D/-o/-T). V3 rules and defaults apply; review SOAPnuke process --help before migrating.");}else{eprintln!("config: unknown command {}; use SOAPnuke process --help", argv.get(1).map(String::as_str).unwrap_or(""));}
    2
}

mod job_api;
pub use job_api::*;

mod native_io;

mod dedup_grouping;

mod dedup_runtime;

mod dedup_app;
mod dedup_report;
mod dedup_backend;
mod dedup_publication;

#[cfg(test)]
mod test_support;

mod failure;
