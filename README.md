## Acknowledgement

This pipeline is based on the original [YAMP](https://github.com/alesssia/YAMP) repo. Modifications have been made to make use of our infrastructure more readily. If you're here for a more customizable and flexible pipeline, please consider taking a look at the original repo.

# nf-reads-profiler

Nextflow DSL2 pipeline for metagenomic read profiling. Core tools: MetaPhlAn4,
HUMAnN4, fastp, MultiQC. Optional MEDI subworkflow (Kraken2/Bracken/Architeuthis)
for food microbiome quantification. Runs on AWS Batch (primary) with local Docker
for development.

## Usage

### AWS Batch (production)

**Always launch inside `screen` — SSH disconnects and Claude Code client exits will
kill a foreground Nextflow process.**

```bash
# 1. Enable FSR so spot workers boot fast (bills $2.25/hr — run once before the pipeline)
FSR_CONFIRM=yes infra/packer/enable-fsr.sh
# Takes 15–30 min to reach 'enabled'; script polls and exits when ready.

# 2. Lock the MEDI Kraken2 hash into RAM — MEDI kraken runs in Docker on this
# head node; vmtouch warms the shared OS page cache so the container sees it instantly.
# -d daemonizes so it holds the lock while Nextflow runs.
vmtouch -dl /mnt/scratch/ssddbs/medi_db/hash.k2d

# 3. Start a named screen session and run the pipeline
screen -S nf-aws
nextflow run main.nf -profile aws \
  --input s3://gutz-nf-reads-profilers-runs/samplesheets/<name>.csv \
  --project <project_name> -resume

# 4. From another terminal: follow Nextflow's own log
tail -f .nextflow.log
grep "status: COMPLETED" .nextflow.log | grep -oP "name: \K\S+" | sort | uniq -c


# 5. After the pipeline finishes: release the lock and stop FSR billing
pkill vmtouch
infra/packer/disable-fsr.sh
```

`enable-fsr.sh` resolves the current worker AMI from SSM (`/nf-reads-profiler/ami-id`)
and enables FSR across all three `us-east-2` AZs. `disable-fsr.sh` is a kill-switch
that disables all FSR-enabled snapshots in the region — including any stale AMI snapshots
after a rollover. Minimum billing is 1 hour per enable-cycle regardless of how quickly
you disable.

Samplesheets live in `s3://gutz-nf-reads-profilers-runs/samplesheets/`. See
`samplesheets/slice.md` (also in that bucket) for how to build new slices.

### Local (Docker, dev/test)

```bash
# Basic test — small bundled data, no screen needed
nextflow run main.nf -profile test

# With MEDI food-microbiome quant (requires local SSD DBs at /mnt/scratch/ssddbs/)
screen -S nf-test
# Lock the Kraken2 hash into RAM before the first job (cold ~30 min; warm <1 min/sample).
# -d daemonizes so it holds the lock in the background while Nextflow runs.
vmtouch -dl /mnt/scratch/ssddbs/medi_db/hash.k2d
nextflow run main.nf -profile test_medi -resume
```

### Infrastructure scripts

| Script | Purpose |
|--------|---------|
| `infra/smoke-test.sh` | 2-sample end-to-end smoke test on AWS Batch |
| `infra/max005_test.sh` | 5-sample scaling baseline (I16); must run under screen |
| `infra/medi_test.sh` | Full MEDI end-to-end test; must run under screen |
| `infra/packer/enable-fsr.sh` | Enable EBS Fast Snapshot Restore so spot queue VMs dehydrate faster |
| `infra/packer/disable-fsr.sh` | Disable FSR after run to stop $0.75/AZ/hr billing |

## Output

All results land under `outdir/<project>/`. Three tiers: per-sample → per-study
combines → project-wide biom rollup. Layout below was verified against a real
2890-sample run (`diversigen-infant`).

```
outdir/<project>/<run>/
  ├── readcount/<id>_readcount.txt          # read count per sample
  ├── taxa/<id>_metaphlan.biom              # MetaPhlAn4 profile per sample
  ├── function/                             # HUMAnN4, per sample (skipped if --skipHumann)
  │     ├── <id>_0.log
  │     ├── <id>_1_metaphlan_profile.tsv    # HUMAnN-internal MetaPhlAn
  │     ├── <id>_2_genefamilies.tsv
  │     ├── <id>_3_reactions.tsv
  │     └── <id>_4_pathabundance.tsv
  ├── combined_tables/                      # per-study combines, TSV only (HUMAnN)
  │     └── <run>_<type>_combined.tsv       # type = reactions | pathabundance | humann_taxonomy
  │                                         #   (genefamilies_combined.tsv NOT published — too big, ~24 GB;
  │                                         #    reconstructable from its stratified/unstratified biom)
  ├── medi/                                 # only if --enable_medi
  │     ├── bracken/<lev>/<lev>_<id>.b2     # per-sample Bracken counts (D/G/S); .b2 only
  │     ├── food_abundance.csv, food_content.csv
  │     ├── <lev>_counts.csv                # D/G/S lineage-annotated counts (medi/ root)
  │     ├── merged/<lev>_merged.csv         # D/G/S merged
  │     ├── multiqc_report.html
  │     └── architeuthis/<id>_mapping.csv, mappings.csv   # only if --mapping (off by default)
  └── log/                                  # nf-profile-reads-Report_multiqc_report.html + _data/

outdir/<project>/combined_bioms/            # project-wide biom rollup, one dir per type
                                            # — the single home for ALL biom (no per-run copy)
  ├── metaphlan/<run>_metaphlan_combined.biom
  ├── genefamilies/<run>_genefamilies_{stratified,unstratified}.biom
  ├── pathabundance/<run>_pathabundance_{stratified,unstratified}.biom
  ├── reactions/<run>_reactions_{stratified,unstratified}.biom
  ├── humann_taxonomy/<run>_humann_taxonomy.biom
  ├── regrouped/                            # only if --humann_regroup (off by default)
  └── medi/<run>_food_abundance.biom, _food_content_nutrients.biom, _food_content_compounds.biom

outdir/<project>/reports/                   # timeline, report, trace (timestamped via params.ts)
```

Kraken2 intermediates (`.k2`/`.tsv`) and the Bracken `*_bracken.tsv` side-file are
**not** published — they flow through channels only (publishDir commented out in
`subworkflows/quant.nf`).

## Databases

Although the databases have been stored at the appropriate `/mnt/efs/databases` location mentioned in the config file. There might come a time when these need to be updated. Here is a quick view on how to do that.

### Metaphlan4

```{bash}
cd /mnt/efs/databases/Biobakery/Metaphlan/v4.0
docker container run \
    --volume $PWD:$PWD \
    --workdir $PWD \
    --rm \
    458432034220.dkr.ecr.us-west-2.amazonaws.com/biobakery/workflows:maf-20221028-a1 \
    metaphlan \
        --install \
        --nproc 4 \
        --bowtie2db .
```

### Humann3

This requires 3 databases.

#### Chocophlan

```{bash}
cd /mnt/efs/databases/Biobakery/Humann/v3.6
docker container run \
    --volume $PWD:$PWD \
    --workdir $PWD \
    --rm \
    458432034220.dkr.ecr.us-west-2.amazonaws.com/biobakery/workflows:maf-20221028-a1 \
        humann_databases \
        --download \
            chocophlan full .
```

This will create a subdirectory `chocophlan`, and download and extract the database here.

#### Uniref

```{bash}
cd /mnt/efs/databases/Biobakery/Humann/v3.6
docker container run \
    --volume $PWD:$PWD \
    --workdir $PWD \
    --rm \
    458432034220.dkr.ecr.us-west-2.amazonaws.com/biobakery/workflows:maf-20221028-a1 \
        humann_databases \
        --download \
        uniref uniref90_diamond .
```

This will create a subdirectory `uniref`, and download and extract the database here.

#### Utility Script Databases

```bash
cd /mnt/efs/databases/Biobakery/Humann/v3.6
docker container run \
    --volume $PWD:$PWD \
    --workdir $PWD \
    --rm \
    458432034220.dkr.ecr.us-west-2.amazonaws.com/biobakery/workflows:maf-20221028-a1 \
    humann_databases \
        --download \
        utility_mapping full .
```
