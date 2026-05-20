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

# 2. Start a named screen session and run the pipeline
screen -S nf-aws
nextflow run main.nf -profile aws \
  --input s3://gutz-nf-reads-profilers-runs/samplesheets/<name>.csv \
  --project <project_name> -resume

# 3. From another terminal: follow Nextflow's own log
tail -f .nextflow.log

# 4. After the pipeline finishes: stop FSR billing
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
