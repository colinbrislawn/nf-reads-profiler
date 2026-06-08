#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# collate_metaphlan.sh — rebuild the independent-MetaPhlAn combine from published
# per-sample taxa/<id>_metaphlan.biom files.
#
# Standalone analog of bin/collate_medi.sh for the MetaPhlAn (profile_taxa) reduce.
# Replicates the `combine_metaphlan_tables` process exactly (biom outer-merge of
# every per-sample .biom into one study-level table). Use it to:
#   * recover study combines truncated by the skipCompleted merge bug (skipped
#     samples were dropped from the channel, so the published combine is short), and
#   * re-build the MetaPhlAn combine standalone without re-running profiling.
#
# It runs entirely outside Nextflow, so it never touches the NF S3 work cache.
#
# Usage:
#   bin/collate_metaphlan.sh <study_s3_uri> [--dry-run]
#
#   study_s3_uri  s3://<bucket>/results/<project>/<run>
#                 (contains taxa/<id>_metaphlan.biom)
#   --dry-run     run the reduce locally but do not upload results to S3.
#
# Env overrides: MPA_IMG, COLLATE_SCRATCH (default /mnt/scratch).
# ---------------------------------------------------------------------------
set -euo pipefail

STUDY_URI="${1:?usage: collate_metaphlan.sh <study_s3_uri> [--dry-run]}"
DRY_RUN=false
for arg in "$@"; do [ "$arg" = "--dry-run" ] && DRY_RUN=true; done

MPA_IMG="${MPA_IMG:-colinbrislawn/metaphlan:4.2.4}"

STUDY_URI="${STUDY_URI%/}"                 # strip trailing slash
RUN="$(basename "$STUDY_URI")"             # study/run id, used in output names
PROJECT_URI="$(dirname "$STUDY_URI")"      # combined_bioms live at project level

# Stage under a real on-disk path Docker can bind-mount (not a mountpoint-s3 FUSE
# path, and not /tmp which a user-private mount may block from the daemon).
source "$(dirname "${BASH_SOURCE[0]}")/_collate_lib.sh"
WORK="$(mktemp -d -p "${COLLATE_SCRATCH:-/mnt/scratch}" collate_metaphlan.XXXXXX)"
trap 'kill "${_MEM_PID:-}" 2>/dev/null; rm -rf "$WORK"' EXIT
start_mem_sampler collate_metaphlan
echo "[collate_metaphlan] study=$RUN  work=$WORK  img=$MPA_IMG  dry_run=$DRY_RUN"

# 1. Stage per-sample MetaPhlAn bioms from S3.
echo "[collate_metaphlan] downloading per-sample bioms from $STUDY_URI/taxa/"
aws s3 cp "$STUDY_URI/taxa/" "$WORK/taxa/" --recursive \
  --exclude '*' --include '*_metaphlan.biom' --only-show-errors
n_in=$(ls "$WORK/taxa/"*_metaphlan.biom 2>/dev/null | wc -l)
echo "  staged $n_in per-sample bioms"
[ "$n_in" -gt 0 ] || { echo "ERROR: no *_metaphlan.biom files under $STUDY_URI/taxa/"; exit 1; }

# 2. Merge: outer-join every per-sample biom into one study table. Mirrors the
#    python in the combine_metaphlan_tables process. Prints the merged sample count.
#    Script is written to a file (not piped on stdin — docker run without -i does
#    not forward stdin to the container).
cat > "$WORK/merge.py" <<'PY'
import glob, sys, biom
run = sys.argv[1]
files = sorted(glob.glob("taxa/*_metaphlan.biom"))
if len(files) == 1:
    table = biom.load_table(files[0])
else:
    table = biom.load_table(files[0])
    for f in files[1:]:
        table = table.merge(biom.load_table(f))
with biom.util.biom_open(f"{run}_metaphlan_combined.biom", "w") as fh:
    table.to_hdf5(fh, "merged metaphlan table")
print(f"[collate_metaphlan] merged table shape = {table.shape} (observations, samples)")
PY
docker run --rm --user "$(id -u):$(id -g)" -v "$WORK":/work -w /work "$MPA_IMG" python3 /work/merge.py "$RUN"

# 3. Sanity: merged sample count vs number of input bioms.
n_out=$(docker run --rm --user "$(id -u):$(id -g)" -v "$WORK":/work -w /work "$MPA_IMG" \
  python3 -c "import biom; print(biom.load_table('${RUN}_metaphlan_combined.biom').shape[1])")
echo "[collate_metaphlan] rebuilt ${RUN}_metaphlan_combined.biom samples=$n_out  (input bioms=$n_in)"
[ "$n_out" = "$n_in" ] || echo "WARNING: sample count mismatch — investigate before trusting outputs"

report_mem
if [ "$DRY_RUN" = true ]; then
  echo "[collate_metaphlan] --dry-run: output left in $WORK (not uploaded)"; trap - EXIT
  echo "$WORK"; exit 0
fi

# 4. Upload to both published locations (per-run combined_tables + project combined_bioms).
echo "[collate_metaphlan] uploading to S3"
aws s3 cp "$WORK/${RUN}_metaphlan_combined.biom" \
  "$STUDY_URI/combined_tables/${RUN}_metaphlan_combined.biom" --only-show-errors
aws s3 cp "$WORK/${RUN}_metaphlan_combined.biom" \
  "$PROJECT_URI/combined_bioms/metaphlan/${RUN}_metaphlan_combined.biom" --only-show-errors
echo "[collate_metaphlan] done — $RUN metaphlan combine republished with $n_out samples"
