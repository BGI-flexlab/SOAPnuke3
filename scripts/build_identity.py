#!/usr/bin/env python3
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
out = root / '.scratch/build'
out.mkdir(parents=True, exist_ok=True)
paths = [root / 'soapnuke-rs/Cargo.toml', root / 'soapnuke-rs/Cargo.lock',
         root / 'soapnuke-rs/build.rs', *sorted((root / 'soapnuke-rs/src').glob('*'))]
source = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
          for p in sorted(paths) if p.is_file()}
(out / 'source-manifest.json').write_text(json.dumps(source, indent=2) + '\n')
dependencies = {}
for folder in ['vendor', 'soapnuke-rs/third_party']:
    for path in sorted((root / folder).rglob('*')):
        if path.is_file():
            dependencies[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
(out / 'dependency-manifest.json').write_text(json.dumps(dependencies, indent=2) + '\n')
