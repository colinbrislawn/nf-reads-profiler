#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# medi_reduce.sh — rebuild the MEDI reduce from published per-sample Bracken
# .b2 files.
#
# MEDI is a map-reduce: the per-sample map (kraken → architeuthis → bracken)
# emits one .b2 per sample per level; the reduce merges them per study and
# quantifies food content. This script re-runs ONLY the reduce, reading the
# .b2 files the pipeline already published. Uses:
#
#   merge    (architeuthis merge)    .b2  → <level>_merged.csv
#   lineage  (architeuthis lineage)       → <level>_counts.csv
#   quantify (quantify.R)                 → food_abundance.csv, food_content.csv
#   biom     (medi_csv_to_biom.py)        → <run>_food_*.biom
#
# Use it to:
#   * recover runs truncated by the skipCompleted merge bug (skipped samples
#     were dropped from the channel, so the published merge is incomplete), and
#   * re-run the reduce standalone for any study without re-doing the map.
#
# The reduce's only inputs are the .b2 files + the taxonomy/food DB; the raw
# kraken2 .k2 files are NOT needed (which is why they are no longer published).
#
# Usage:
#   bin/medi_reduce.sh <study_s3_uri> [db_dir] [--dry-run]
#
#   study_s3_uri  s3://<bucket>/results/<project>/<run>
#                 (contains medi/bracken/{D,G,S}/*.b2)
#   db_dir        local MEDI db root (default below); needs taxonomy/ + food files.
#                 NOTE: must be a real on-disk path — Docker cannot bind-mount a
#                 mountpoint-s3 FUSE path, so stage db files to local disk first.
#   --dry-run     run the reduce locally but do not upload results to S3.
# ---------------------------------------------------------------------------
set -euo pipefail

STUDY_URI="${1:?usage: medi_reduce.sh <study_s3_uri> [db_dir] [--dry-run]}"
DB="${2:-/home/ubuntu/disk_dbs/referencedata/medi_db}"
DRY_RUN=false
for arg in "$@"; do [ "$arg" = "--dry-run" ] && DRY_RUN=true; done

# db_dir may have been given as the --dry-run flag; fall back to default.
case "$DB" in --dry-run) DB="/home/ubuntu/disk_dbs/referencedata/medi_db";; esac

MEDI_IMG="${MEDI_IMG:-colinbrislawn/medi:0.2.1}"
MPA_IMG="${MPA_IMG:-colinbrislawn/metaphlan:4.2.4}"
LEVELS=(D G S)

STUDY_URI="${STUDY_URI%/}"                 # strip trailing slash
RUN="$(basename "$STUDY_URI")"             # study/run id, used in biom names
PROJECT_URI="$(dirname "$STUDY_URI")"      # combined_bioms live at project level
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # for medi_csv_to_biom.py

[ -d "$DB/taxonomy" ] || { echo "ERROR: $DB/taxonomy not found (need names.dmp/nodes.dmp)"; exit 1; }
[ -f "$DB/food_matches.csv" ] || { echo "ERROR: $DB/food_matches.csv not found"; exit 1; }
[ -f "$DB/food_contents.csv.gz" ] || { echo "ERROR: $DB/food_contents.csv.gz not found"; exit 1; }

# Stage under $HOME (or MEDI_REDUCE_SCRATCH), not /tmp: the Docker daemon must be
# able to traverse the bind-mount source, which a user-private /tmp may block.
WORK="$(mktemp -d -p "${MEDI_REDUCE_SCRATCH:-$HOME}" medi_reduce.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
echo "[medi_reduce] study=$RUN  work=$WORK  db=$DB  dry_run=$DRY_RUN"

# 1. Stage per-sample .b2 files from S3 (preserving D/G/S subdirs).
echo "[medi_reduce] downloading .b2 files from $STUDY_URI/medi/bracken/"
aws s3 cp "$STUDY_URI/medi/bracken/" "$WORK/" --recursive --exclude '*' --include '*.b2' --only-show-errors
for l in "${LEVELS[@]}"; do
  n=$(ls "$WORK/$l/"*.b2 2>/dev/null | wc -l)
  echo "  $l: $n .b2 files"
  [ "$n" -gt 0 ] || { echo "ERROR: no .b2 files for level $l — nothing to reduce"; exit 1; }
done

# 2. Reduce: merge + lineage (per level) then quantify (across levels). medi image.
docker run --rm -v "$WORK":/work -v "$DB":/db:ro -w /work "$MEDI_IMG" bash -lc '
  set -euo pipefail
  for l in '"${LEVELS[*]}"'; do
    architeuthis merge   $l/*.b2 --out ${l}_merged.csv
    architeuthis lineage ${l}_merged.csv --data-dir /db/taxonomy --out ${l}_counts.csv
  done
  quantify.R /db/food_matches.csv /db/food_contents.csv.gz '"$(printf '%s_counts.csv ' "${LEVELS[@]}")"'
'

# 3. BIOM conversion (metaphlan image carries pandas/biom + we mount bin/).
docker run --rm -v "$WORK":/work -v "$SCRIPTS":/scripts:ro -w /work "$MPA_IMG" bash -lc '
  set -euo pipefail
  python3 /scripts/medi_csv_to_biom.py food_abundance.csv '"$RUN"'_food_abundance.biom --type abundance
  python3 /scripts/medi_csv_to_biom.py food_content.csv   '"$RUN"'_food_content.biom   --type content
'

# 4. Report sample counts (sanity vs the .b2 set).
sc_col=$(head -1 "$WORK/food_abundance.csv" | tr ',' '\n' | grep -nx sample_id | cut -d: -f1)
n_food=$(tail -n +2 "$WORK/food_abundance.csv" | cut -d',' -f"$sc_col" | sort -u | wc -l)
n_b2=$(ls "$WORK/S/"*.b2 | wc -l)
echo "[medi_reduce] rebuilt food_abundance samples=$n_food  (.b2 samples=$n_b2)"
[ "$n_food" = "$n_b2" ] || echo "WARNING: sample count mismatch — investigate before trusting outputs"

if [ "$DRY_RUN" = true ]; then
  echo "[medi_reduce] --dry-run: outputs left in $WORK (not uploaded)"; trap - EXIT
  echo "$WORK"; exit 0
fi

# 5. Upload reduce outputs to their published locations.
echo "[medi_reduce] uploading results to S3"
for l in "${LEVELS[@]}"; do
  aws s3 cp "$WORK/${l}_merged.csv" "$STUDY_URI/medi/merged/${l}_merged.csv" --only-show-errors
  aws s3 cp "$WORK/${l}_counts.csv" "$STUDY_URI/medi/${l}_counts.csv"         --only-show-errors
done
aws s3 cp "$WORK/food_abundance.csv" "$STUDY_URI/medi/food_abundance.csv" --only-show-errors
aws s3 cp "$WORK/food_content.csv"   "$STUDY_URI/medi/food_content.csv"   --only-show-errors
aws s3 cp "$WORK/${RUN}_food_abundance.biom"          "$PROJECT_URI/combined_bioms/medi/${RUN}_food_abundance.biom"          --only-show-errors
aws s3 cp "$WORK/${RUN}_food_content_nutrients.biom"  "$PROJECT_URI/combined_bioms/medi/${RUN}_food_content_nutrients.biom"  --only-show-errors
aws s3 cp "$WORK/${RUN}_food_content_compounds.biom"  "$PROJECT_URI/combined_bioms/medi/${RUN}_food_content_compounds.biom"  --only-show-errors
echo "[medi_reduce] done — $RUN reduce republished with $n_food samples"
