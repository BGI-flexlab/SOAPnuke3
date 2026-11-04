use std::path::PathBuf;

pub(crate) fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/fixtures/native").canonicalize().unwrap()
}

pub(crate) fn workspace(family: &str) -> PathBuf {
    let thread = std::thread::current();
    let name: String = thread.name().unwrap_or("native").chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect();
    static RUN: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    let run = RUN.get_or_init(|| {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.scratch/tmp/native");
        std::fs::create_dir_all(&base).unwrap();
        let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let run = base.join(format!("{}-{stamp}", std::process::id()));
        std::fs::create_dir(&run).unwrap();
        run.canonicalize().unwrap()
    });
    let root = run.join(name).join(family);
    let fresh = !root.exists();
    std::fs::create_dir_all(&root).unwrap();
    let root = root.canonicalize().unwrap();
    if fresh {
        if family == "header" {
            for name in ["catalogue-a.json", "catalogue-b.json", "literal.json"] {
                let text = std::fs::read_to_string(fixtures().join("header").join(name)).unwrap();
                std::fs::write(root.join(name), text.replace("{ROOT}", root.to_str().unwrap())).unwrap();
            }
        } else if family == "r4" {
            std::fs::create_dir(root.join("baseline")).unwrap();
            for (source, target) in [("normal-expected.json", "normal-expected.json"),
                ("normal-command.json", "baseline/command.json"),
                ("normal-report.json", "baseline/report.json")] {
                let text = std::fs::read_to_string(fixtures().join(source)).unwrap()
                    .replace("{ROOT}", root.to_str().unwrap())
                    .replace("{FIXTURES}", fixtures().to_str().unwrap());
                std::fs::write(root.join(target), text).unwrap();
            }
        }
    }
    root
}
