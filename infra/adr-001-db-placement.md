# ADR-001: Database Placement Strategy for AWS Batch Workers

**Status:** Superseded (2026-04-26)

**Date:** 2026-04-25

**Deciders:** Infrastructure Team

---

## Superseded — 2026-04-26

The 2-minute sync assumption proved wrong: actual sync time is 20+ minutes for
65 GiB / 30k objects on fresh Graviton spot instances. This exceeds the
10-minute reevaluation threshold stated in the Notes section below.

**New decision:** Pre-baked custom AMI (Packer) with databases + Miniconda/awscli
installed at image build time. Workers register with ECS in seconds instead of
waiting 20+ minutes for an S3 sync. See `issues/I14-custom-ami-worker.md` for
the full discussion (AMI vs EFS tradeoffs) and implementation plan.

**What stays the same:** Local gp3 SSD for I/O performance (the core rationale
of this ADR). `/mnt/dbs/` paths. `/opt/conda-aws/bin/aws` path. The bind mount
`['/mnt/dbs:/mnt/dbs']` in `conf/aws_batch.config`.

**What changes:** Boot-time S3 sync eliminated. UserData reduced to a health
check. AMI ID stored in SSM Parameter Store. AMI must be rebuilt (~30 min,
automated) when databases are updated (a few times per year).

---

---

## Context

The nf-reads-profiler pipeline requires access to large bioinformatics reference databases (~65 GB total) on each AWS Batch worker node. These databases must be available when jobs execute and accessed with minimal latency for random-read operations (e.g., Bowtie2 alignment, DIAMOND protein search).

Current databases synced per worker:
- MetaPhlAn (~20 GB)
- ChocoPhlAn (~42 GB)
- UniRef (~1.6 GB)
- Utility mapping (~2.7 GB)

Excluded: `referencedata/` (~1.37 TB) for MEDI Kraken2/Bracken — deferred to future decision.

Worker storage: 500 GB gp3 EBS root volume (`DeleteOnTermination: true`).

## Decision

**Implement per-worker S3-sync database placement:** Each AWS Batch worker syncs the database set from S3 to local `/mnt/dbs/` at boot via CloudFormation Launch Template UserData, using the command:

```bash
aws s3 sync s3://${DbSourceBucket} /mnt/dbs/ \
  --exclude "referencedata/*" \
  --quiet
```

The ECS agent is restarted after sync completes, ensuring it only accepts tasks once databases are available. The source bucket (`DbSourceBucket`) is parameterized in the CloudFormation stack.

## Rationale

1. **Performance:** Local gp3 SSD disk I/O is fastest for random-read bioinformatics workloads. Bowtie2 and DIAMOND require sub-millisecond seek latencies unavailable over network storage.

2. **Cost-effective:** ~2-minute sync overhead is <1% of typical HUMAnN wall time (~40–60 min). Zero per-worker ongoing charges (unlike EFS throughput or FSx).

3. **Operational simplicity:** S3 sync is reliable, well-tested, and requires no additional infrastructure (no NFS daemons, no mounted filesystems). Databases are versioned and deployed via standard S3 object lifecycle.

4. **Reversibility:** Easy to switch strategies — change UserData and redeploy the stack. No infrastructure lock-in.

## Alternatives Considered

| Alternative | Rationale for Rejection |
|---|---|
| **Pre-baked AMI** | Operational burden: AMI rebuild required on every database version bump. 2-minute savings does not justify image maintenance overhead and longer deployment cycles. |
| **EFS (NFS)** | NFS latency (microseconds roundtrip) compounds over millions of random seeks. DIAMOND's random-access pattern is particularly sensitive. Elastically-provisioned throughput remains slower than local SSD. Throughput charges accumulate at scale (hundreds of concurrent workers). |
| **FSx for Lustre** | $174/month minimum cost. Provides 18× overprovisioned capacity for 65 GB dataset. Network latency still impacts random-read performance vs. local disk. Overkill for static, read-only reference data. |
| **S3 Mountpoint FUSE** | Each file read becomes an S3 GET request. Latency kills alignment performance; incompatible with Bowtie2/DIAMOND access patterns. Not suitable for bioinformatics tools. |

## Consequences

### Positive

- **Minimal latency:** Local SSD I/O supports the random-read patterns of Bowtie2 and DIAMOND.
- **Predictable cost:** No per-GB throughput charges or minimum fees beyond EBS root volume.
- **Stateless workers:** Databases are ephemeral and rebuilt at boot; failed workers do not leave stale data.
- **Version management:** Databases are versioned as S3 objects; upgrades are performed by re-pushing to source bucket and redeploying workers.

### Negative

- **Boot overhead:** ~2-minute sync delay before tasks can begin. Scales with database size and S3 request concurrency.
- **Scale risk:** At hundreds of concurrent workers, all hitting the same S3 prefix may exceed request-rate limits or saturate egress bandwidth. Requires pilot instrumentation to validate.
- **No error gate:** If sync fails, ECS restarts anyway; tasks fail only when database files are missing at runtime. Error visibility depends on job logs.
- **Storage pressure:** 500 GB root volume must accommodate 65 GB databases plus pipeline working files. Future database growth (e.g., MEDI Kraken2) may require volume resize.

## Notes

- **Instrumentation:** Pilot runs will measure sync duration and S3 request patterns at scale (target: 50+ concurrent workers). If sync exceeds 10 minutes, alternative strategies (e.g., per-region pre-staged cache or request rate optimization) should be reevaluated.

- **Database versioning:** Source bucket (`cjb-gutz-s3-demo`, parameterized as `DbSourceBucket`) is maintained independently. Database version tags are managed externally (e.g., via S3 object tags or bucket structure). Pipeline `--humann_metaphlan_id` and `--direct_metaphlan_id` parameters specify which tool versions to invoke, not which S3 database version — database selection is implicit via bucket contents.

- **MEDI databases deferred:** Kraken2/Bracken (~1.37 TB) excluded from this sync. Future ADR required to decide strategy: pre-staged on regional filesystem, lazy-loaded on first use, or on-demand download per worker.

- **Error handling:** If UserData sync fails, the job will fail at the point of database access. Consider enhancing monitoring (e.g., CloudWatch metrics on sync success/failure) in future iterations.

- **ECS restart:** Restarting the ECS agent after sync ensures the container agent only accepts tasks once databases are confirmed available. This prevents race conditions where tasks are scheduled before sync completes.
