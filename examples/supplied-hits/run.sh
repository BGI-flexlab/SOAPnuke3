#!/bin/sh
set -eu
if [ "$#" -ne 1 ]; then
    echo 'Usage: sh run.sh NEW_OUTPUT_DIRECTORY' >&2
    exit 2
fi
fixture=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
out=$1
mkdir -- "$out"
"${SOAPNUKE:-SOAPnuke}" process --threads 1 \
    --r1 "$fixture/R1.fq" --r2 "$fixture/R2.fq" \
    --screen-method supplied-hits --screen-hits "$fixture/hits.tsv" \
    --screen-action remove --screen-pair any --screen-unscorable keep \
    --out1 "$out/clean.R1.fq" --out2 "$out/clean.R2.fq" --report "$out/report.json"
cmp "$out/clean.R1.fq" "$fixture/expected.R1.fq"
cmp "$out/clean.R2.fq" "$fixture/expected.R2.fq"
