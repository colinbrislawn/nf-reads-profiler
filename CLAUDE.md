# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Nextflow pipeline (DSL2) for metagenomic read profiling. Originally forked from
[YAMP](https://github.com/alesssia/YAMP); now targeted at AWS Batch (primary) and
Azure Batch, with local Docker for dev. Core tools: MetaPhlAn4, HUMAnN4, fastp,
MultiQC. Optional MEDI subworkflow (Kraken2/Bracken/Architeuthis) for food
microbiome quantification.

## Running the pipeline

**Always use screen for any run that takes more than a few minutes.** Closing the
terminal (or the Claude Code client) kills the Nextflow process — screen keeps it
alive across SSH disconnects and client exits.

```bash
# ── Screen basics ──────────────────────────────────────────────────────────
screen -S nf-run          # start a named session
# Detach: Ctrl+A D   |   Reattach: screen -r nf-run   |   List: screen -ls

# ── Local — basic (Docker, small test data) ────────────────────────────────
nextflow run main.nf -profile test

# ── Local — with MEDI (I13); screen keeps it alive ─────────────────────────
screen -S nf-test
nextflow run main.nf -profile test_medi -resume
# Monitor from another terminal:
tail -f .nextflow.log

# ── AWS Batch — primary production path ────────────────────────────────────
# 1. Enable FSR so spot workers boot fast (bills $2.25/hr; run once before pipeline)
FSR_CONFIRM=yes infra/packer/enable-fsr.sh
# Polls until all 3 us-east-2 AZs reach 'enabled' (~15–30 min for a 150 GB snapshot)

# 2. Launch inside screen so SSH disconnect / Claude Code exit won't kill it:
screen -S nf-aws
nextflow run main.nf -profile aws \
  --input s3://gutz-nf-reads-profilers-runs/samplesheets/<name>.csv \
  --project <project_name> -resume
# Detach: Ctrl+A D  |  Reattach: screen -r nf-aws

# 3. From another terminal, tail Nextflow's own log:
tail -f .nextflow.log

# 4. After all runs are done for the day, stop FSR billing:
infra/packer/disable-fsr.sh
# Kill-switch: disables ALL FSR-enabled snapshots in us-east-2 (catches stale AMI rollovers too)
```

Profile-to-config mapping is in `nextflow.config`:
- `aws` → `conf/aws_batch.config` (s3 workDir, `awsbatch` executor, Graviton spot queue)
- `azure` → `conf/azurebatch.config`
- `test` → `conf/test.config` (local Docker, tiny `nreads`/`minreads`)
- `test_medi` → `conf/test_medi.config` (extends `test`; enables MEDI, sets ssddbs paths, disables cleanup)

## Samplesheet schema

Input is a CSV validated by `assets/schema_input.json` via the `nf-schema`
plugin. Each row has `fastq_1`, `fastq_2` (optional), and an SRA id column.
`main.nf` branches rows: rows with `fastq_1` set are treated as local files;
rows without local files but with an `[ESD]RR\d+` id go through `AWS_DOWNLOAD`
→ `FASTERQ_DUMP`. `single_end` is derived from whether `fastq_2` is present
(local) or the number of FASTQs produced (SRA).

## Code architecture

`main.nf` is the only top-level workflow. It wires three module files and one
subworkflow:

- `modules/data_handling.nf` — `AWS_DOWNLOAD`, `FASTERQ_DUMP` (SRA ingest from S3 → FASTQ).
- `modules/house_keeping.nf` — `count_reads`, `clean_reads` (fastp), `get_software_versions`, `MULTIQC`.
- `modules/community_characterisation.nf` — `profile_taxa` (MetaPhlAn4),
  `profile_function` (HUMAnN4), and HUMAnN table plumbing:
  `combine_humann_tables`, `combine_humann_taxonomy_tables`,
  `combine_metaphlan_tables`, `split_stratified_tables`, `convert_tables_to_biom`,
  `regroup_genefamilies`.
- `subworkflows/quant.nf` — `MEDI_QUANT` (Kraken2 → Architeuthis filter →
  Bracken → food-content quantification), gated on `params.enable_medi`.

Channel shape used throughout: `[ meta, reads_or_file ]`, where `meta` is a
map carrying at least `id` and `run` (study grouping key). Combines work by
dropping `id` from meta, tagging a `type` (`genefamilies`/`reactions`/
`pathabundance`/`metaphlan_profile`), then `groupTuple`-ing per study+type.
Stratification (`'stratified'`/`'unstratified'`) is stamped onto meta by the
main workflow *after* `split_stratified_tables` emits its two channels —
`main.nf` does these `.map` stamps inline, so splitter processes stay unaware
of stratification semantics.

Early-exit: `output_exists(meta)` in `main.nf` checks whether all three HUMAnN
TSVs already exist in `outdir/project/run/function/` — used to skip samples on
resume-style reruns.

The HUMAnN biom-conversion branch (`convert_tables_to_biom` →
`regroup_genefamilies`) is currently **commented out** in `main.nf`. The
workflow stops at combined TSVs + `split_stratified_tables`. Don't reintroduce
the biom steps without also re-enabling them in the workflow.

## Databases

All profiles expect pre-staged databases; nothing is downloaded at runtime.

| Param | Purpose |
|-------|---------|
| `direct_metaphlan_id` / `direct_metaphlan_db` | Standalone MetaPhlAn (newer DB, e.g. `mpa_vJan25_CHOCOPhlAnSGB_202503`) |
| `humann_metaphlan_index` / `humann_metaphlan_db` | MetaPhlAn DB matched to HUMAnN4 (e.g. `mpa_vOct22_CHOCOPhlAnSGB_202403`) |
| `humann_chocophlan` / `humann_uniref` / `humann_utilitymap` | HUMAnN4 nucleotide/protein/mapping DBs |
| `medi_db_path` / `medi_food_matches` / `medi_food_contents` | MEDI Kraken2+Bracken DB and food metadata |

Paths differ per profile:
- Local / `test_medi`: `/mnt/scratch/ssddbs/...` — synced from
  `s3://cjb-gutz-s3-demo` to the instance-store RAID at `/mnt/scratch/ssddbs/`
  (see `~/colin_notes_vm.md` sections 4–5). `docker.runOptions` in
  `nextflow.config` bind-mounts this into Docker. vJan25 was installed via
  `metaphlan --install` and is now in both ssddbs and S3.
- AWS: `/mnt/dbs/...` — pre-baked custom AMI (Packer, see `issues/I14-custom-ami-worker.md`).
  vJan25 baked in via `metaphlan --install` during AMI build (I21); vOct22
  synced from S3. Workers boot ready in seconds with no runtime sync.

`README.md` has the `docker run ... humann_databases --download` commands for
rebuilding HUMAnN4/MetaPhlAn DBs when versions bump.

## AWS Batch infra

Managed by a single CloudFormation template: `infra/batch-stack.yaml`. Stack
name `nf-reads-profiler-batch`, region `us-east-2`, account `730883236839`.
All compute is **Graviton (ARM64)** — runner and workers both. Two CEs behind
`spot-queue`: spot (primary) + on-demand (fallback). Two S3 buckets:

- `gutz-nf-reads-profilers-workdir` — Nextflow workDir, 30-day lifecycle, stack-managed.
- `gutz-nf-reads-profilers-runs` — samplesheets and results, `DeletionPolicy: Retain`.

Deploy, teardown, drift-recovery, and EFS setup steps are in `infra/readme.md`.
The `head-node-role` on the runner VM must have
`nf-reads-profiler-nextflow-runner-policy` attached; `conf/aws_batch.config`
references `nf-reads-profiler-batch-job-role` as `aws.batch.jobRole`.

Resource caps live in `conf/aws_batch.config` via `process.resourceLimits` —
retries won't blow past these (prevents runaway memory on retry storms).

## Key parameters

Defined in `nextflow.config`:

- `skipHumann` (default false) — skip functional profiling and all HUMAnN combine/split steps.
- `singleEnd`, `mergeReads`, `nreads` (32,000,000 cap), `minreads` (100,000 floor; samples below this are logged and dropped, not failed).
- `process_humann_tables`, `humann_regroups` (e.g. `"uniref90_ko,uniref90_rxn"`), `split_size` — used by the currently-disabled regroup branch.
- `humann_params` — passthrough (test profile sets `--bypass-translated-search`).
- MEDI: `enable_medi`, `confidence`, `consistency`, `entropy`, `multiplicity`, `read_length`, `threshold`, `batchsize`, `mapping`, plus fastp knobs (`trim_front`, `min_length`, `quality_threshold`).

Error strategy is profile-dependent. Azure uses `errorStrategy = 'ignore'`
with retries on labelled processes; AWS defaults to `maxRetries = 0` plus
`resourceLimits`. Failed samples are logged and skipped, not fatal.

## Output layout

```
outdir/<project>/<run>/
  ├── taxa/              # MetaPhlAn profiles
  ├── function/          # HUMAnN TSVs (genefamilies, pathabundance, pathcoverage)
  ├── medi/              # only if --enable_medi
  └── log/
outdir/<project>/reports/ # timeline, report, trace (timestamped via params.ts)
```

## Tests

`nf-test test` runs the nf-test suite (`tests/`, `nf-test.config`). There is
also a Python-side test harness under `tests/` for `bin/safe_cluster_process.py`
and friends — run with `python tests/run_integration_tests.py`. These cover
the (currently-disabled) HUMAnN split/regroup Python utilities, not the
Nextflow workflow itself.

## Scripts in `bin/`

Shipped on the Nextflow `PATH`. The table-processing scripts
(`safe_cluster_process.py`, `safe_regroup.py`, `process_humann_tables.sh`) are
only reached when the biom-conversion branch is re-enabled. The `scrape_*.sh`
helpers parse tool logs into MultiQC-custom-content TSVs; `medi_csv_to_biom.py`
converts MEDI CSV outputs.

## Custom agents and skills (`.claude/`)

**Agents** (spawn via `@agent-name` or the Agent tool):

- `batch-doctor` — read-only health check of the full Batch stack: CEs, queue,
  recent failures, launch template, S3 buckets, Nextflow logs. Produces a
  status table with WARN/FAIL callouts.
- `log-reader` — parses `.nextflow.log*` and fetches CloudWatch logs for
  failed Batch jobs. Produces a concise run report (succeeded/failed/aborted
  tasks, error messages, timing).

**Skills** (invoke via `/skill-name`):

- `deploy-stack` — validates the CloudFormation template, deploys, waits, and
  re-validates compute environments. Always shows the diff first.
- `preflight` — pre-flight checklist before a pipeline run: CEs valid, queue
  enabled, launch template UserData correct, S3 reachable, no stuck jobs.

## Debugging AWS Batch failures

When a pipeline run fails on AWS Batch, the diagnosis workflow is:

1. **Read the Nextflow log** — `grep 'ERROR\|FAIL' .nextflow.log` for the
   process name, exit code, and error summary.
2. **Get the Batch job log** — find the failed job's CloudWatch log stream in
   `/aws/batch/job` (log stream names follow
   `<job-def>/default/<job-id>`). The last few lines usually have the root
   cause.
3. **Check worker state** — if the error is a missing file/DB, the database
   may not be present. Currently this means the S3 sync didn't complete;
   after the custom AMI migration (see `issues/I14-custom-ami-worker.md`),
   it means the wrong AMI was used. SSH to the worker (if still running)
   and check `/var/log/nf-userdata.log` and `ls /mnt/dbs/`.
4. **Common failure modes**:
   - "database does not exist" → S3 sync race or wrong AMI (see
     `infra/readme.md` troubleshooting and `issues/I14-custom-ami-worker.md`).
   - "Essential container in task exited" → container OOM or command error;
     check CloudWatch logs for the specific error.
   - "Job killed by NF" → Nextflow aborted the run after a different task
     failed; find the original failure.
   - Jobs stuck in RUNNABLE → no capacity; check CE MaxvCpus and spot
     availability.

## Guardrails (`.claude/hooks/guardrails.sh`)

A PreToolUse hook that runs before every Bash, Write, and Edit call. Hard
blocks (exit 2) are non-negotiable; soft blocks prompt for confirmation.

**Hard blocks:**

| Category | What's blocked |
|----------|---------------|
| Docker destruction | `docker compose down -v`, `docker volume rm/prune`, `docker system prune` |
| Runaway EC2 | `aws ec2 run-instances`, `aws ec2 request-spot-instances` |
| Batch escalation | `update-compute-environment` with `maxvCpus` > 64 |
| CFN deletion | `aws cloudformation delete-stack` |
| Disk bombs | `dd if=`, `fallocate`, `mkfs` |
| Repo escape | `rm -r` outside the repo, Write/Edit to paths outside repo or `~/.claude/` |
| Git destruction | `git push --force` to main/master, `git reset --hard` on main/master |
| Secrets | Write to `.env`, `credentials`, `*.key`/`*_key.pem` files |

**Soft blocks (user confirmation prompt):**

| Category | What's prompted |
|----------|----------------|
| AWS pipeline launch | `nextflow run ... -profile aws` |

To override a hard block, the user must edit `guardrails.sh` directly.
