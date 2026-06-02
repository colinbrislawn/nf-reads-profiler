#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# collate_humann.sh — rebuild the HUMAnN study-level combines (+ HUMAnN-taxonomy)
# from published per-sample function/<id>_*.tsv files.
#
# Standalone analog of bin/collate_medi.sh for the non-MEDI table reduces.
# Replicates, for one study, the chain main.nf runs after per-sample profiling:
#   combine_humann_tables        humann_join_tables   -> <run>_<type>_combined.tsv
#                                  (type = genefamilies | reactions | pathabundance)
#   combine_humann_taxonomy_tables merge_metaphlan_tables.py
#                                                      -> <run>_humann_taxonomy_combined.tsv
#   split_stratified_tables      humann_split_stratified_table
#   convert_tables_to_biom       biom convert (+ empty-biom placeholder)
#                                  -> <run>_<type>_{stratified,unstratified}.biom
#                                  -> <run>_humann_taxonomy.biom
#
# It runs entirely outside Nextflow, so it never touches the NF S3 work cache.
# Use it to recover study combines truncated by the skipCompleted merge bug, or to
# rebuild the HUMAnN combines standalone without re-running profiling.
#
# Usage:
#   bin/collate_humann.sh <study_s3_uri> [--dry-run]
#
#   study_s3_uri  s3://<bucket>/results/<project>/<run>
#                 (contains function/<id>_{1_metaphlan_profile,2_genefamilies,
#                  3_reactions,4_pathabundance}.tsv)
#   --dry-run     run the reduce locally but do not upload results to S3.
#
# Env overrides: HUMANN_IMG, MPA_IMG, COLLATE_SCRATCH (default /mnt/scratch).
# ---------------------------------------------------------------------------
set -euo pipefail

STUDY_URI="${1:?usage: collate_humann.sh <study_s3_uri> [--dry-run]}"
DRY_RUN=false
for arg in "$@"; do [ "$arg" = "--dry-run" ] && DRY_RUN=true; done

HUMANN_IMG="${HUMANN_IMG:-barbarahelena/humann:4.0.3}"
MPA_IMG="${MPA_IMG:-colinbrislawn/metaphlan:4.2.4}"

# type -> per-sample file suffix (the substring humann_join_tables --file_name uses).
TYPES=(genefamilies reactions pathabundance)
declare -A SUFFIX=(
  [genefamilies]=_2_genefamilies.tsv
  [reactions]=_3_reactions.tsv
  [pathabundance]=_4_pathabundance.tsv
)
MPA_SUFFIX=_1_metaphlan_profile.tsv

STUDY_URI="${STUDY_URI%/}"                 # strip trailing slash
RUN="$(basename "$STUDY_URI")"             # study/run id, used in output names
PROJECT_URI="$(dirname "$STUDY_URI")"      # combined_bioms live at project level

# Stage under a real on-disk path Docker can bind-mount (not a mountpoint-s3 FUSE
# path, and not /tmp which a user-private mount may block from the daemon).
source "$(dirname "${BASH_SOURCE[0]}")/_collate_lib.sh"
WORK="$(mktemp -d -p "${COLLATE_SCRATCH:-/mnt/scratch}" collate_humann.XXXXXX)"
trap 'kill "${_MEM_PID:-}" 2>/dev/null; rm -rf "$WORK"' EXIT
start_mem_sampler collate_humann
mkdir -p "$WORK/function" "$WORK/out"
echo "[collate_humann] study=$RUN  work=$WORK  dry_run=$DRY_RUN"

# 1. Stage per-sample function tables from S3 (all four kinds in one flat dir).
echo "[collate_humann] downloading per-sample tables from $STUDY_URI/function/"
aws s3 cp "$STUDY_URI/function/" "$WORK/function/" --recursive --exclude '*' \
  --include "*${SUFFIX[genefamilies]}" --include "*${SUFFIX[reactions]}" \
  --include "*${SUFFIX[pathabundance]}" --include "*${MPA_SUFFIX}" --only-show-errors
for t in "${TYPES[@]}"; do
  n=$(ls "$WORK/function/"*"${SUFFIX[$t]}" 2>/dev/null | wc -l)
  echo "  $t: $n per-sample tables"
  [ "$n" -gt 0 ] || { echo "ERROR: no *${SUFFIX[$t]} files — nothing to combine"; exit 1; }
done
n_mpa=$(ls "$WORK/function/"*"${MPA_SUFFIX}" 2>/dev/null | wc -l)
echo "  humann_taxonomy: $n_mpa per-sample profiles"

# tobiom.py: replicates convert_tables_to_biom (has-data check, else empty placeholder).
cat > "$WORK/tobiom.py" <<'PY'
import sys, subprocess, h5py, numpy as np
inp, out, ttype = sys.argv[1], sys.argv[2], sys.argv[3]
def pos(v):
    try: return float(v) > 0
    except: return False
has_data = False
with open(inp) as f:
    for line in f:
        if line.startswith('#') or not line.strip():
            continue
        parts = line.strip().split('\t')
        if parts[0] in ('UNMAPPED', 'UNINTEGRATED'):
            continue
        if any(pos(v) for v in parts[1:] if v):
            has_data = True; break
if has_data:
    subprocess.run(['biom', 'convert', '--input-fp', inp, '--output-fp', out,
                    '--table-type', ttype, '--to-hdf5'], check=True)
else:
    with h5py.File(out, 'w') as f:
        f.attrs['id'] = 'None'; f.attrs['type'] = ttype
        f.attrs['format'] = 'Biological Observation Matrix 1.0.0'
        f.attrs['format_url'] = 'http://biom-format.org'
        f.attrs['generated_by'] = 'nf-reads-profiler'; f.attrs['creation_date'] = ''
        f.attrs['shape'] = np.array([0, 0], dtype=np.int32); f.attrs['nnz'] = np.int32(0)
        f.create_group('observation').create_dataset('ids', data=np.array([], dtype='S'))
        f.create_group('sample').create_dataset('ids', data=np.array([], dtype='S'))
print(f"  {out}: {'data' if has_data else 'empty-placeholder'}")
PY

# 2. HUMAnN container: join each type, split stratified/unstratified, convert to biom.
cat > "$WORK/humann_step.sh" <<EOF
set -euo pipefail
cd /work
for t in ${TYPES[*]}; do
  echo "[collate_humann] joining \$t ..."
  humann_join_tables -i function -o out/${RUN}_\${t}_combined.tsv --file_name \$t --verbose
  echo "[collate_humann] splitting \$t ..."
  humann_split_stratified_table -i out/${RUN}_\${t}_combined.tsv -o out
  # split emits out/${RUN}_\${t}_combined_{stratified,unstratified}.tsv
  for s in stratified unstratified; do
    python3 tobiom.py out/${RUN}_\${t}_combined_\${s}.tsv out/${RUN}_\${t}_\${s}.biom 'Function table'
  done
done
EOF
docker run --rm --user "$(id -u):$(id -g)" -v "$WORK":/work -w /work "$HUMANN_IMG" bash /work/humann_step.sh

# 3. MetaPhlAn container: combine HUMAnN-generated taxonomy profiles.
echo "[collate_humann] combining humann_taxonomy profiles ..."
docker run --rm --user "$(id -u):$(id -g)" -v "$WORK":/work -w /work "$MPA_IMG" bash -lc \
  "merge_metaphlan_tables.py function/*${MPA_SUFFIX} -o out/${RUN}_humann_taxonomy_combined.tsv --overwrite"

# 4. HUMAnN container: convert the taxonomy combine to biom (Taxon table, not split).
docker run --rm --user "$(id -u):$(id -g)" -v "$WORK":/work -w /work "$HUMANN_IMG" \
  python3 /work/tobiom.py "out/${RUN}_humann_taxonomy_combined.tsv" \
  "out/${RUN}_humann_taxonomy.biom" 'Taxon table'

# 5. Sanity: sample count in each combined tsv vs number of staged per-sample files.
echo "[collate_humann] sample counts (combined tsv header columns minus 1):"
for t in "${TYPES[@]}"; do
  ncol=$(head -1 "$WORK/out/${RUN}_${t}_combined.tsv" | awk -F'\t' '{print NF-1}')
  nin=$(ls "$WORK/function/"*"${SUFFIX[$t]}" | wc -l)
  echo "  $t: combined=$ncol  inputs=$nin"
  [ "$ncol" = "$nin" ] || echo "  WARNING: $t sample count mismatch"
done
# merge_metaphlan_tables.py prefixes a #mpa_... comment line; the column header is
# the first non-# line.
ntax=$(grep -v '^#' "$WORK/out/${RUN}_humann_taxonomy_combined.tsv" | head -1 | awk -F'\t' '{print NF-1}')
echo "  humann_taxonomy: combined=$ntax  inputs=$n_mpa"

report_mem
if [ "$DRY_RUN" = true ]; then
  echo "[collate_humann] --dry-run: outputs left in $WORK/out (not uploaded)"; trap - EXIT
  echo "$WORK/out"; ls -la "$WORK/out"; exit 0
fi

# 6. Upload to published locations.
#    combined_tables/ gets the per-run combined tsvs + all bioms.
#    combined_bioms/<type>/ gets the per-type bioms at project level.
echo "[collate_humann] uploading to S3"
CT="$STUDY_URI/combined_tables"
CB="$PROJECT_URI/combined_bioms"
for t in "${TYPES[@]}"; do
  aws s3 cp "$WORK/out/${RUN}_${t}_combined.tsv" "$CT/${RUN}_${t}_combined.tsv" --only-show-errors
  for s in stratified unstratified; do
    aws s3 cp "$WORK/out/${RUN}_${t}_${s}.biom" "$CT/${RUN}_${t}_${s}.biom"      --only-show-errors
    aws s3 cp "$WORK/out/${RUN}_${t}_${s}.biom" "$CB/${t}/${RUN}_${t}_${s}.biom" --only-show-errors
  done
done
aws s3 cp "$WORK/out/${RUN}_humann_taxonomy_combined.tsv" "$CT/${RUN}_humann_taxonomy_combined.tsv" --only-show-errors
aws s3 cp "$WORK/out/${RUN}_humann_taxonomy.biom" "$CT/${RUN}_humann_taxonomy.biom"                 --only-show-errors
aws s3 cp "$WORK/out/${RUN}_humann_taxonomy.biom" "$CB/humann_taxonomy/${RUN}_humann_taxonomy.biom" --only-show-errors
echo "[collate_humann] done — $RUN HUMAnN combines republished"
