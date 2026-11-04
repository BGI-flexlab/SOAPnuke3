pub fn normalize(args:&[String])->Result<Vec<String>,String>{normalize_inner(args,false)}
pub(crate) fn normalize_demux(args:&[String])->Result<Vec<String>,String>{normalize_inner(args,true)}
fn normalize_inner(args: &[String], demux:bool) -> Result<Vec<String>, String> {
    let mut normalized = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        let token = &args[i];
        let (flag, attached) = if token.starts_with("--") {
            token
                .split_once('=')
                .map_or((token.as_str(), None), |(k, v)| (k, Some(v)))
        } else if token.starts_with('-') && token.get(..2).is_some() {
            let (flag, tail) = token.split_at(2);
            (
                flag,
                if tail.is_empty() {
                    None
                } else {
                    Some(tail.strip_prefix('=').unwrap_or(tail))
                },
            )
        } else {
            return Err(format!("expected a named process option, got {token}"));
        };
        let key = match flag {
            "--correction-delta" => return Err("--correction-delta was removed: overlap correction now uses fastp's fixed Q>=30 / Q<=14 rules".into()),
            "-h" if demux => "help",
            "-v" if demux => "version",
            "-J" | "--ada_trim" | "-E" | "--cutAdaptor" | "-G" => {
                if attached.is_some() {
                    return Err(format!("{flag} takes no value in process; v1/v2 options are not interchangeable"));
                }
                normalized.push(if flag == "-G" {
                    "--output-phred=33".into()
                } else {
                    "--adapter-action=trim".into()
                });
                i += 1;
                continue;
            }
            "-c" | "--configFile" => {
                return Err("-c has different meanings in v1/v2; use --config FILE for v3 JSON configuration. Legacy key=value files are not supported".into());
            }
            "-L" => {
                return Err("-L has different meanings in v1/v2; use explicit v3 options such as --max-length, --sample-fraction or --limit-units; these are not exact-count legacy sampling".into());
            }
            _ if !["-h","-v"].contains(&flag) && crate::option_metadata::alias(flag).is_some() => crate::option_metadata::alias(flag).unwrap(),
            _ if flag.starts_with("--") => &flag[2..],
            _ => return Err(format!("unsupported process option {flag}; see SOAPnuke process --help for supported v1/v2 spellings and v3 behavior")),
        };
        let boolean = crate::option_metadata::implicit_boolean(key, demux);
        let value = if let Some(value) = attached {
            value.to_string()
        } else if boolean && args.get(i + 1).is_none_or(|v| v.starts_with('-')) {
            "true".into()
        } else {
            i += 1;
            args.get(i)
                .ok_or_else(|| format!("missing value for {flag}"))?
                .clone()
        };
        if key == "trim" {
            let counts: Vec<_> = value.split(',').collect();
            if !matches!(counts.len(), 2 | 4)
                || counts
                    .iter()
                    .any(|v| v.is_empty() || !v.bytes().all(|c| c.is_ascii_digit()))
            {
                return Err("-t/--trim requires HEAD,TAIL for SE or H1,T1,H2,T2 for PE".into());
            }
            normalized.push(format!("--cut-r1={},{}", counts[0], counts[1]));
            if counts.len() == 4 {
                normalized.push(format!("--cut-r2={},{}", counts[2], counts[3]));
            }
        } else if matches!(key, "qualSys" | "outQualSys") {
            let phred = match value.as_str() {
                "1" => "64",
                "2" => "33",
                _ => return Err(format!("{flag} requires 1 (Phred+64) or 2 (Phred+33)")),
            };
            let target = if key == "qualSys" {
                "input-phred"
            } else {
                "output-phred"
            };
            normalized.push(format!("--{target}={phred}"));
        } else {
            normalized.push(format!("--{key}={value}"));
        }
        i += 1;
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::normalize;

    #[test]
    fn mixed_syntax_preserves_values_and_boolean_boundaries() {
        let args = [
            "--qc-only",
            "-1input.fq",
            "-T=16",
            "--sample-id",
            "-T",
            "--metrics=false",
        ];
        let result = normalize(&args.map(String::from)).unwrap();
        assert_eq!(
            result,
            [
                "--qc-only=true",
                "--r1=input.fq",
                "--threads=16",
                "--sample-id=-T",
                "--metrics=false"
            ]
        );
        assert_eq!(normalize(&["--r1".into(), "-".into()]).unwrap(), ["--r1=-"]);
    }

    #[test]
    fn familiar_composite_controls_have_explicit_translation() {
        let args = ["-t", "1,2,3,4", "-Q2", "--outQualSys=1", "-J"];
        assert_eq!(
            normalize(&args.map(String::from)).unwrap(),
            [
                "--cut-r1=1,2",
                "--cut-r2=3,4",
                "--input-phred=33",
                "--output-phred=64",
                "--adapter-action=trim"
            ]
        );
        assert!(normalize(&["-t1,2,3".into()]).is_err());
        assert!(normalize(&["-Q33".into()]).is_err());
        assert!(normalize(&["-J0.2".into()]).is_err());
        assert!(normalize(&["-c".into(), "100".into()])
            .unwrap_err()
            .contains("different meanings"));
    }

    #[test]
    fn duplicate_aliases_reach_the_same_duplicate_check() {
        let args = ["-1", "a.fq", "--r1", "b.fq"];
        assert!(crate::native_config::parse(&args.map(String::from))
            .unwrap_err()
            .contains("duplicate option --r1"));
        let args = ["-t", "1,2", "--cut-r1", "0,0"];
        assert!(crate::native_config::parse(&args.map(String::from))
            .unwrap_err()
            .contains("duplicate option --cut-r1"));
    }
}

#[cfg(test)]mod demux_boolean_tests{use super::*;
#[test]fn metadata_and_boolean_boundaries_are_demux_specific(){
 let args=["--help","--r1","blocked.fifo"].map(String::from);assert_eq!(normalize_demux(&args).unwrap(),["--help=true","--r1=blocked.fifo"]);assert!(normalize(&args).is_err());
 for flag in ["--help","--version","--metrics","--barcode-remove","--interleaved"]{let args=[flag,"--r1","x"].map(String::from);assert_eq!(normalize_demux(&args).unwrap()[0],format!("{flag}=true"));let args=[flag,"false","--r1","x"].map(String::from);assert_eq!(normalize_demux(&args).unwrap()[0],format!("{flag}=false"));}
 assert_eq!(normalize_demux(&["-h".into()]).unwrap(),["--help=true"]);assert_eq!(normalize_demux(&["-v".into()]).unwrap(),["--version=true"]);assert!(normalize(&["-h".into()]).is_err());assert_eq!(normalize_demux(&["--sample-id".into(),"--help".into()]).unwrap(),["--sample-id=--help"]);
}
}
