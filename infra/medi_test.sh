#!/usr/bin/env bash
# infra/medi_test.sh
#
# End-to-end MEDI test: runs samples through MetaPhlAn + HUMAnN on AWS Batch,
# then MEDI (Kraken2 → Architeuthis → Bracken → food quantification) on this
# node (local executor). Validates both Batch and MEDI outputs.
#
# IMPORTANT — launch under screen (or tmux):
#   screen -dmS nf-medi -L -Logfile logs/medi-$(date +%Y%m%d-%H%M%S).log \
#       bash -lc 'bash infra/medi_test.sh; exec bash'
#
# Usage:
#   bash infra/medi_test.sh                    # fresh run
#   bash infra/medi_test.sh <project-name>     # resume into existing project

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
REGION=us-east-2
RUNS_BUCKET=gutz-nf-reads-profilers-runs
WORKDIR_BUCKET=gutz-nf-reads-profilers-workdir
SAMPLESHEET_LOCAL=assets/samplesheet-medi-food-hits-child.csv
SAMPLESHEET_S3="s3://${RUNS_BUCKET}/samplesheets/samplesheet-medi-food-hits-child.csv"
OUTDIR="s3://${RUNS_BUCKET}/results"
MEDI_DB_PATH=/mnt/scratch/ssddbs/medi_db
TMPDIR_TEST="/tmp/medi-test-$$"
PASS=0
FAIL=0

if [[ -n "${1:-}" ]]; then
    PROJECT="$1"
    RESUME_FLAG="-resume"
    echo "Resume mode: project=$PROJECT"
else
    PROJECT="medi-$(date +%Y%m%d-%H%M%S)"
    RESUME_FLAG=""
fi

cleanup() { rm -rf "$TMPDIR_TEST"; }
trap cleanup EXIT

check() {
    local name="$1"; shift
    if "$@"; then
        echo "  PASS: $name"
        (( PASS++ )) || true
    else
        echo "  FAIL: $name"
        (( FAIL++ )) || true
    fi
}

# ── Pre-flight ────────────────────────────────────────────────────────────────
echo "=== Pre-flight checks ==="

if [[ ! -f main.nf ]]; then
    echo "ERROR: main.nf not found. Run from repo root." >&2; exit 1
fi
if [[ ! -f "$SAMPLESHEET_LOCAL" ]]; then
    echo "ERROR: $SAMPLESHEET_LOCAL not found." >&2; exit 1
fi

# MEDI DB on local SSD — required for local executor MEDI processes
if [[ ! -f "${MEDI_DB_PATH}/hash.k2d" ]]; then
    echo "ERROR: MEDI DB not found at ${MEDI_DB_PATH}/hash.k2d" >&2
    echo "       Sync from s3://cjb-gutz-s3-demo/medi_db/ before running." >&2
    exit 1
fi
echo "  OK: MEDI DB present at $MEDI_DB_PATH"

if [[ -z "${STY:-}" && -z "${TMUX:-}" ]]; then
    echo "  WARNING: not in screen/tmux — SSH disconnect will kill Nextflow."
    echo "           Continuing in 5s..."
    sleep 5
fi

queue_state=$(aws batch describe-job-queues \
    --job-queues spot-queue --region "$REGION" \
    --query "jobQueues[0].state" --output text 2>/dev/null)
[[ "$queue_state" == "ENABLED" ]] || { echo "ERROR: spot-queue is '$queue_state'." >&2; exit 1; }
echo "  OK: spot-queue ENABLED"

ce_count=$(aws batch describe-compute-environments --region "$REGION" \
    --query "computeEnvironments[?state=='ENABLED' && status=='VALID'] | length(@)" \
    --output text 2>/dev/null)
[[ "$ce_count" -ge 2 ]] || { echo "ERROR: Only $ce_count CEs ENABLED/VALID." >&2; exit 1; }
echo "  OK: $ce_count compute environments ENABLED/VALID"

# Warn if FSR not enabled (cold Kraken2 load will take ~30 min instead of ~2 min)
fsr_enabled=$(aws ec2 describe-fast-snapshot-restores --region "$REGION" \
    --filters "Name=state,Values=enabled" \
    --query "length(FastSnapshotRestores)" --output text 2>/dev/null || echo 0)
if [[ "$fsr_enabled" -eq 0 ]]; then
    echo "  WARNING: no FSR snapshots in 'enabled' state — first Kraken2 job"
    echo "           will cold-load the 415 GB hash (~30 min). Run enable-fsr.sh first."
else
    echo "  OK: FSR enabled ($fsr_enabled AZ/snapshot pairs)"
fi

aws s3 ls "s3://${RUNS_BUCKET}/" --region "$REGION" > /dev/null 2>&1 || {
    echo "ERROR: Cannot access s3://${RUNS_BUCKET}/." >&2; exit 1
}
echo "  OK: S3 bucket accessible"

echo "  Uploading samplesheet..."
aws s3 cp "$SAMPLESHEET_LOCAL" "$SAMPLESHEET_S3" --region "$REGION" > /dev/null
echo "  OK: Samplesheet at $SAMPLESHEET_S3"

PRE_WORKDIR_BYTES=$(aws s3 ls "s3://${WORKDIR_BUCKET}/" --recursive --region "$REGION" --summarize 2>/dev/null \
    | awk '/Total Size:/{print $3}')
PRE_WORKDIR_BYTES=${PRE_WORKDIR_BYTES:-0}

# ── Run pipeline ──────────────────────────────────────────────────────────────
echo ""
echo "=== Running pipeline (project: $PROJECT) ==="
START_EPOCH=$(date -u +%s)
echo "  Start: $(date -u)"

set +e
nextflow run main.nf \
    -profile aws \
    -work-dir /mnt/scratch/nf-local-work \
    -bucket-dir s3://gutz-nf-reads-profilers-workdir/work \
    --input "$SAMPLESHEET_S3" \
    --project "$PROJECT" \
    --enable_medi \
    $RESUME_FLAG
NF_EXIT=$?
set -e

END_EPOCH=$(date -u +%s)
WALLCLOCK_SEC=$((END_EPOCH - START_EPOCH))
echo "  End: $(date -u)"
echo "  Wallclock: ${WALLCLOCK_SEC}s ($((WALLCLOCK_SEC / 60))m $((WALLCLOCK_SEC % 60))s)"

if [[ $NF_EXIT -ne 0 ]]; then
    echo "ERROR: Nextflow exited with code $NF_EXIT." >&2; exit 1
fi
echo "  OK: Nextflow completed"

# ── Validate outputs ──────────────────────────────────────────────────────────
echo ""
echo "=== Validating outputs ==="
mkdir -p "$TMPDIR_TEST"

# Parse samplesheet: sample name and study
SAMPLES=()
while IFS=, read -r sample _ _ _ study; do
    SAMPLES+=("$sample")
    STUDY="$study"
done < <(tail -n +2 "$SAMPLESHEET_LOCAL")

S3_BASE="${OUTDIR}/${PROJECT}/${STUDY}"

# Per-sample: Batch outputs (MetaPhlAn + HUMAnN) and MEDI filtered Kraken2
for SAMPLE in "${SAMPLES[@]}"; do
    echo "  --- Sample: $SAMPLE ---"

    BIOM="${TMPDIR_TEST}/${SAMPLE}_metaphlan.biom"
    if aws s3 cp "${S3_BASE}/taxa/${SAMPLE}_metaphlan.biom" "$BIOM" --region "$REGION" > /dev/null 2>&1; then
        biom_size=$(wc -c < "$BIOM")
        check "MetaPhlAn biom >1KB ($biom_size bytes)" test "$biom_size" -gt 1024
    else
        echo "  FAIL: MetaPhlAn biom missing"; (( FAIL++ )) || true
    fi

    GF="${TMPDIR_TEST}/${SAMPLE}_2_genefamilies.tsv"
    if aws s3 cp "${S3_BASE}/function/${SAMPLE}_2_genefamilies.tsv" "$GF" --region "$REGION" > /dev/null 2>&1; then
        gf_features=$(tail -n +2 "$GF" | grep -v '^UNMAPPED\|^UNINTEGRATED' | wc -l)
        check "genefamilies >50 features ($gf_features)" test "$gf_features" -gt 50
    else
        echo "  FAIL: genefamilies missing"; (( FAIL++ )) || true
    fi

    PA="${TMPDIR_TEST}/${SAMPLE}_4_pathabundance.tsv"
    if aws s3 cp "${S3_BASE}/function/${SAMPLE}_4_pathabundance.tsv" "$PA" --region "$REGION" > /dev/null 2>&1; then
        pa_features=$(tail -n +2 "$PA" | grep -v '^UNMAPPED\|^UNINTEGRATED' | wc -l)
        check "pathabundance >50 features ($pa_features)" test "$pa_features" -gt 50
    else
        echo "  FAIL: pathabundance missing"; (( FAIL++ )) || true
    fi

    K2="${TMPDIR_TEST}/${SAMPLE}_filtered.k2"
    if aws s3 cp "${S3_BASE}/medi/kraken2/${SAMPLE}_filtered.k2" "$K2" --region "$REGION" > /dev/null 2>&1; then
        k2_size=$(wc -c < "$K2")
        check "MEDI filtered k2 non-empty ($k2_size bytes)" test "$k2_size" -gt 0
    else
        echo "  FAIL: MEDI filtered.k2 missing"; (( FAIL++ )) || true
    fi
done

# Study-level: merged taxonomy counts and food quantification
echo "  --- Study: $STUDY ---"

for level in D G S; do
    COUNTS="${TMPDIR_TEST}/${level}_counts.csv"
    if aws s3 cp "${S3_BASE}/medi/${level}_counts.csv" "$COUNTS" --region "$REGION" > /dev/null 2>&1; then
        rows=$(tail -n +2 "$COUNTS" | wc -l)
        check "MEDI ${level}_counts.csv has rows ($rows)" test "$rows" -gt 0
    else
        echo "  FAIL: ${level}_counts.csv missing"; (( FAIL++ )) || true
    fi
done

FA="${TMPDIR_TEST}/food_abundance.csv"
if aws s3 cp "${S3_BASE}/medi/food_abundance.csv" "$FA" --region "$REGION" > /dev/null 2>&1; then
    fa_rows=$(tail -n +2 "$FA" | wc -l)
    check "food_abundance.csv has rows ($fa_rows)" test "$fa_rows" -gt 0
else
    echo "  FAIL: food_abundance.csv missing"; (( FAIL++ )) || true
fi

FC="${TMPDIR_TEST}/food_content.csv"
if aws s3 cp "${S3_BASE}/medi/food_content.csv" "$FC" --region "$REGION" > /dev/null 2>&1; then
    fc_rows=$(tail -n +2 "$FC" | wc -l)
    check "food_content.csv has rows ($fc_rows)" test "$fc_rows" -gt 0
else
    echo "  FAIL: food_content.csv missing"; (( FAIL++ )) || true
fi

# ── Metrics ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Metrics ==="

POST_WORKDIR_BYTES=$(aws s3 ls "s3://${WORKDIR_BUCKET}/" --recursive --region "$REGION" --summarize 2>/dev/null \
    | awk '/Total Size:/{print $3}')
POST_WORKDIR_BYTES=${POST_WORKDIR_BYTES:-0}
WORKDIR_DELTA=$((POST_WORKDIR_BYTES - PRE_WORKDIR_BYTES))

RESULTS_BYTES=$(aws s3 ls "${OUTDIR}/${PROJECT}/" --recursive --region "$REGION" --summarize 2>/dev/null \
    | awk '/Total Size:/{print $3}')
RESULTS_BYTES=${RESULTS_BYTES:-0}

fmt_bytes() {
    local b=$1
    if   (( b > 1073741824 )); then printf '%.2f GiB' "$(echo "$b / 1073741824" | bc -l)"
    elif (( b > 1048576 ));    then printf '%.2f MiB' "$(echo "$b / 1048576"    | bc -l)"
    elif (( b > 1024 ));       then printf '%.2f KiB' "$(echo "$b / 1024"       | bc -l)"
    else printf '%d B' "$b"
    fi
}

cat <<EOF

  Project:            $PROJECT
  Study:              $STUDY
  Samples:            ${#SAMPLES[@]}
  Wallclock:          ${WALLCLOCK_SEC}s ($((WALLCLOCK_SEC / 60))m $((WALLCLOCK_SEC % 60))s)
  S3 workdir delta:   $(fmt_bytes "$WORKDIR_DELTA")
  S3 results size:    $(fmt_bytes "$RESULTS_BYTES")
  Results (S3):       ${OUTDIR}/${PROJECT}/
  Reports (local):    results_local_logs/${PROJECT}/reports/

  Remember: run infra/packer/disable-fsr.sh after this run completes.
EOF

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "MEDI TEST RESULTS: ${PASS} passed, ${FAIL} failed"
echo "Project: ${PROJECT}"
echo "============================================"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
