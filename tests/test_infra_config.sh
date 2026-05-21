#!/usr/bin/env bash
# tests/test_infra_config.sh
#
# Contract tests: verify that conf/aws_batch.config and infra/batch-stack.yaml
# stay in sync and contain the values required for the pipeline to work on AWS Batch.
#
# Usage:
#   bash tests/test_infra_config.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------

PASS=0
FAIL=0

run_test() {
    local name="$1"
    local fn="$2"
    if "$fn"; then
        echo "PASS: $name"
        (( PASS++ )) || true
    else
        echo "FAIL: $name"
        (( FAIL++ )) || true
    fi
}

# ---------------------------------------------------------------------------
# Infrastructure Config Tests (TC-02, TC-03, TC-05, TC-06, TC-09, TC-12)
# ---------------------------------------------------------------------------

# TC-02: resourceLimits.time >= 6h
#   Must NOT be 2.h (the broken default); accept 6.h, 8.h, 12.h, etc.
test_tc02_resourcelimits_time() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    if grep -q 'time:.*2\.h' "$cfg"; then
        echo "  reason: resourceLimits time is still 2.h (too short)" >&2
        return 1
    fi
    grep -qE 'time:[[:space:]]*(6|7|8|9|1[0-9]|2[0-9])\.h' "$cfg" || {
        echo "  reason: no resourceLimits time >= 6.h found in $cfg" >&2
        return 1
    }
}

# TC-03: profile_function memory == 60 GB
#   Must NOT be 32 GB (OOM) or 64 GB (RUNNABLE forever)
test_tc03_profile_function_memory() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    if ! grep -q "profile_function" "$cfg"; then
        echo "  reason: no withName:'profile_function' block found in $cfg" >&2
        return 1
    fi
    awk "/withName:[[:space:]]*'?\"?profile_function'?\"?/,/\}/" "$cfg" \
        | grep -qE "memory[[:space:]]*=[[:space:]]*'?\"?60[[:space:]]*(\.)?GB'?\"?" || {
        echo "  reason: profile_function block does not have memory = '60 GB' in $cfg" >&2
        return 1
    }
}

# TC-05: cleanup == false
#   Must NOT be cleanup = true (breaks debugging and -resume)
test_tc05_cleanup_false() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    if grep -qE '^[[:space:]]*cleanup[[:space:]]*=[[:space:]]*true' "$cfg"; then
        echo "  reason: cleanup = true found (must be false) in $cfg" >&2
        return 1
    fi
    grep -qE '^[[:space:]]*cleanup[[:space:]]*=[[:space:]]*false' "$cfg" || {
        echo "  reason: cleanup = false not found in $cfg" >&2
        return 1
    }
}

# TC-06: nreads == 32000000
#   Must NOT be 33333333 (old value)
test_tc06_nreads() {
    local cfg="$REPO_ROOT/nextflow.config"
    if grep -qE 'nreads[[:space:]]*=[[:space:]]*33333333' "$cfg"; then
        echo "  reason: nreads is still 33333333 (must be 32000000) in $cfg" >&2
        return 1
    fi
    grep -qE 'nreads[[:space:]]*=[[:space:]]*32000000' "$cfg" || {
        echo "  reason: nreads = 32000000 not found in $cfg" >&2
        return 1
    }
}

# TC-09: maxRetries == 0
test_tc09_maxretries() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    grep -qE 'maxRetries[[:space:]]*=[[:space:]]*0' "$cfg" || {
        echo "  reason: maxRetries = 0 not found in $cfg" >&2
        return 1
    }
}

# TC-12: profile_function withName block has time = '6h'
test_tc12_profile_function_time() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    if ! grep -q "profile_function" "$cfg"; then
        echo "  reason: no withName:'profile_function' block found in $cfg" >&2
        return 1
    fi
    awk "/withName:[[:space:]]*'?\"?profile_function'?\"?/,/\}/" "$cfg" \
        | grep -qE "time[[:space:]]*=[[:space:]]*'?\"?6h'?\"?" || {
        echo "  reason: profile_function block does not have time = '6h' in $cfg" >&2
        return 1
    }
}

# ---------------------------------------------------------------------------
# Contract Tests
# ---------------------------------------------------------------------------

# CT-01: executor block must have submitRateLimit = '10/s' and queueSize = 200
test_ct01_submit_rate_limit() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    grep -qE "submitRateLimit\s*=\s*'10/s'" "$cfg" || { echo "  missing: submitRateLimit = '10/s' in $cfg"; return 1; }
    grep -qE "queueSize\s*=\s*200" "$cfg"          || { echo "  missing: queueSize = 200 in $cfg"; return 1; }
}

# CT-02: aws.batch.volumes must contain the /mnt/dbs bind mount
test_ct02_mnt_dbs_volume() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    grep -qF "'/mnt/dbs:/mnt/dbs'" "$cfg" || { echo "  missing: volumes = ['/mnt/dbs:/mnt/dbs'] in $cfg"; return 1; }
}

# CT-03: aws.batch.jobRole must be set to an ARN containing 'nf-reads-profiler-batch-job-role'
test_ct03_job_role_arn() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    grep -qE "jobRole\s*=\s*'arn:aws:iam::[^']*nf-reads-profiler-batch-job-role'" "$cfg" || {
        echo "  missing or wrong: jobRole ARN containing 'nf-reads-profiler-batch-job-role' in $cfg"
        return 1
    }
}

# CT-04: process.queue in aws_batch.config must be 'spot-queue' AND
#         infra/batch-stack.yaml must define a JobQueue named 'spot-queue'
test_ct04_queue_name_matches_cfn() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    local cfn="$REPO_ROOT/infra/batch-stack.yaml"

    grep -qE "queue\s*=\s*'spot-queue'" "$cfg" || {
        echo "  missing: queue = 'spot-queue' in $cfg"
        return 1
    }
    grep -qE "JobQueueName:\s*spot-queue" "$cfn" || {
        echo "  missing: JobQueueName: spot-queue in $cfn"
        return 1
    }
}

# CT-05: role name in aws.batch.jobRole (aws_batch.config) must match
#         the BatchJobRole RoleName pattern in infra/batch-stack.yaml
#         Both must reference 'nf-reads-profiler-batch-job-role'
test_ct05_job_role_matches_cfn() {
    local cfg="$REPO_ROOT/conf/aws_batch.config"
    local cfn="$REPO_ROOT/infra/batch-stack.yaml"

    grep -qF "nf-reads-profiler-batch-job-role" "$cfg" || {
        echo "  missing: 'nf-reads-profiler-batch-job-role' in $cfg"
        return 1
    }
    # CFN uses !Sub "${ProjectTag}-batch-job-role"; ProjectTag default is 'nf-reads-profiler'
    grep -qE '\$\{ProjectTag\}-batch-job-role' "$cfn" || {
        echo "  missing: '\${ProjectTag}-batch-job-role' pattern in $cfn (BatchJobRole RoleName)"
        return 1
    }
}

run_test "TC-02: resourceLimits.time >= 6h (not 2.h)"        test_tc02_resourcelimits_time
run_test "TC-03: profile_function memory == 60 GB"            test_tc03_profile_function_memory
run_test "TC-05: cleanup == false"                            test_tc05_cleanup_false
run_test "TC-06: nreads == 32000000"                          test_tc06_nreads
run_test "TC-09: maxRetries == 0"                             test_tc09_maxretries
run_test "TC-12: profile_function withName block has time 6h" test_tc12_profile_function_time

run_test "CT-01: submitRateLimit and queueSize" test_ct01_submit_rate_limit
run_test "CT-02: /mnt/dbs bind mount in volumes" test_ct02_mnt_dbs_volume
run_test "CT-03: jobRole ARN references nf-reads-profiler-batch-job-role" test_ct03_job_role_arn
run_test "CT-04: queue name 'spot-queue' matches CFN JobQueue" test_ct04_queue_name_matches_cfn
run_test "CT-05: jobRole name matches CFN BatchJobRole pattern" test_ct05_job_role_matches_cfn

# ---------------------------------------------------------------------------
# Security Tests
# ---------------------------------------------------------------------------

# ST-01: humann_extraparams not injectable from samplesheet
#   humann_extraparams must only appear as params.humann_extraparams in
#   main.nf and modules/ — never as meta.humann_extraparams or constructed
#   from samplesheet fields.
test_st01_humann_extraparams_not_injectable() {
    local main_nf="$REPO_ROOT/main.nf"
    local modules_dir="$REPO_ROOT/modules"

    # Fail if any reference to meta.humann_extraparams exists
    if grep -rq 'meta\.humann_extraparams' "$main_nf" "$modules_dir"; then
        echo "  meta.humann_extraparams found — samplesheet injection possible"
        return 1
    fi

    # Confirm all occurrences are params.humann_extraparams (safe path)
    # Pass when at least one params.humann_extraparams reference exists AND
    # no unsafe references exist (checked above)
    grep -rq 'params\.humann_extraparams' "$main_nf" "$modules_dir" || {
        echo "  no params.humann_extraparams reference found in main.nf or modules/"
        return 1
    }
}

# ST-02: sra_accession regex in schema
#   assets/schema_input.json must have pattern "^[ESD]RR[0-9]+$"
test_st02_sra_accession_pattern() {
    local schema="$REPO_ROOT/assets/schema_input.json"

    [[ -f "$schema" ]] || { echo "  missing: $schema not found"; return 1; }
    grep -qF '"^[ESD]RR[0-9]+$"' "$schema" || {
        echo "  missing: pattern \"^[ESD]RR[0-9]+\$\" for sra_accession in $schema"
        return 1
    }
}

# ST-03: sample name pattern in schema
#   assets/schema_input.json must have pattern "^\\S+$" for the sample field
test_st03_sample_name_pattern() {
    local schema="$REPO_ROOT/assets/schema_input.json"

    [[ -f "$schema" ]] || { echo "  missing: $schema not found"; return 1; }
    grep -qF '"^\\S+$"' "$schema" || {
        echo "  missing: pattern \"^\\\\S+\$\" for sample in $schema"
        return 1
    }
}

run_test "ST-01: humann_extraparams only via params (not meta/samplesheet)" test_st01_humann_extraparams_not_injectable
run_test "ST-02: sra_accession has ^[ESD]RR[0-9]+\$ pattern in schema"     test_st02_sra_accession_pattern
run_test "ST-03: sample field has ^\\S+\$ pattern in schema"                test_st03_sample_name_pattern

# ---------------------------------------------------------------------------
# Schema Validation Tests
# ---------------------------------------------------------------------------

# TC-14: Samplesheet requires sample and study_accession
#   assets/schema_input.json "required" array must contain both fields
test_tc14_schema_required_fields() {
    local schema="$REPO_ROOT/assets/schema_input.json"

    [[ -f "$schema" ]] || { echo "  missing: $schema not found"; return 1; }

    grep -q '"sample"' "$schema" || {
        echo "  missing: \"sample\" not in schema required array in $schema"
        return 1
    }
    grep -q '"study_accession"' "$schema" || {
        echo "  missing: \"study_accession\" not in schema required array in $schema"
        return 1
    }
}

# TC-15: Samplesheet anyOf constraint
#   assets/schema_input.json must have an "anyOf" block requiring either
#   fastq_1 or sra_accession
test_tc15_schema_anyof_constraint() {
    local schema="$REPO_ROOT/assets/schema_input.json"

    [[ -f "$schema" ]] || { echo "  missing: $schema not found"; return 1; }

    grep -q '"anyOf"' "$schema" || {
        echo "  missing: no anyOf block found in $schema"
        return 1
    }
    grep -q '"fastq_1"' "$schema" || {
        echo "  missing: fastq_1 not referenced in schema anyOf in $schema"
        return 1
    }
    grep -q '"sra_accession"' "$schema" || {
        echo "  missing: sra_accession not referenced in schema anyOf in $schema"
        return 1
    }
}

run_test "TC-14: schema required[] contains sample and study_accession"  test_tc14_schema_required_fields
run_test "TC-15: schema anyOf requires fastq_1 or sra_accession"         test_tc15_schema_anyof_constraint

# ---------------------------------------------------------------------------
# Critical Bug Checks
# ---------------------------------------------------------------------------

# TC-07: output_exists filenames AND profile_function wiring (combined)
#   All four conditions must hold simultaneously — patching only some still fails.
test_tc07_output_exists_and_wiring() {
    local main="$REPO_ROOT/main.nf"
    local errors=0

    # Check output_exists uses the correct HUMAnN output filenames
    grep -q '_2_genefamilies.tsv' "$main"  || { echo "  missing: _2_genefamilies.tsv in output_exists" >&2; (( errors++ )); }
    grep -q '_3_reactions.tsv' "$main"     || { echo "  missing: _3_reactions.tsv in output_exists" >&2; (( errors++ )); }
    grep -q '_4_pathabundance.tsv' "$main" || { echo "  missing: _4_pathabundance.tsv in output_exists" >&2; (( errors++ )); }

    # Check wiring: profile_function must use ch_filtered_reads
    grep -q 'profile_function(ch_filtered_reads)' "$main" || { echo "  missing: profile_function(ch_filtered_reads)" >&2; (( errors++ )); }

    # Old broken patterns must be gone
    if grep -q '_pathcoverage\.tsv' "$main"; then
        echo "  stale: _pathcoverage.tsv still present in main.nf" >&2
        (( errors++ ))
    fi
    if grep 'profile_function(merged_reads)' "$main" | grep -qv '//'; then
        echo "  stale: profile_function(merged_reads) still present" >&2
        (( errors++ ))
    fi

    [ "$errors" -eq 0 ]
}

# TC-18: containerOptions — zero unescaped $(id instances
#   Every containerOptions line in community_characterisation.nf must use \$(id
test_tc18_container_options_escaping() {
    local modfile="$REPO_ROOT/modules/community_characterisation.nf"
    local unescaped_count
    unescaped_count=$(grep 'containerOptions' "$modfile" | grep -v '\\$(' | grep -c '$(id' || true)
    [ "$unescaped_count" -eq 0 ]
}

run_test "TC-07: output_exists filenames AND wiring (combined check)"     test_tc07_output_exists_and_wiring
run_test "TC-18: containerOptions — zero unescaped \$(id instances"       test_tc18_container_options_escaping

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

echo ""
echo "Results: $PASS passed, $FAIL failed"
if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
