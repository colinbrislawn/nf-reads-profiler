#!/usr/bin/env python3
"""Print a plain-text table of running EC2 nodes with CPU utilization (single CloudWatch call)."""

import json
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone

REGION = "us-east-2"
LOOKBACK_MIN = 10


def aws(*args):
    r = subprocess.run(["aws", "--region", REGION] + list(args), capture_output=True, text=True)
    if r.returncode != 0:
        print(f"ERROR: {r.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(r.stdout)


def main():
    instances = aws(
        "ec2", "describe-instances",
        "--filters", "Name=instance-state-name,Values=running",
        "--query", "Reservations[*].Instances[*].[InstanceId,InstanceType]",
        "--output", "json",
    )
    nodes = [row for group in instances for row in group]
    if not nodes:
        print("No running instances.")
        return

    # Resolve vCPU + RAM for each unique instance type
    types = list({itype for _, itype in nodes})
    type_data = aws(
        "ec2", "describe-instance-types",
        "--instance-types", *types,
        "--query", "InstanceTypes[*].[InstanceType,VCpuInfo.DefaultVCpus,MemoryInfo.SizeInMiB]",
        "--output", "json",
    )
    type_info = {row[0]: (row[1], row[2] // 1024) for row in type_data}

    # Build one batched CloudWatch query
    id_map = {}
    queries = []
    for iid, itype in nodes:
        mid = "m" + re.sub(r"[^a-z0-9]", "", iid)
        id_map[mid] = (iid, itype)
        queries.append({
            "Id": mid,
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/EC2",
                    "MetricName": "CPUUtilization",
                    "Dimensions": [{"Name": "InstanceId", "Value": iid}],
                },
                "Period": LOOKBACK_MIN * 60,
                "Stat": "Average",
            },
        })

    now = datetime.now(timezone.utc)
    result = aws(
        "cloudwatch", "get-metric-data",
        "--metric-data-queries", json.dumps(queries),
        "--start-time", (now - timedelta(minutes=LOOKBACK_MIN)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--end-time", now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--query", "MetricDataResults[*].[Id,Values[0]]",
        "--output", "json",
    )

    cpu_map = {}
    for mid, cpu in result:
        cpu_map[mid] = float(cpu) if cpu is not None else None

    rows = []
    for mid, (iid, itype) in id_map.items():
        vcpu, ram = type_info.get(itype, ("?", "?"))
        cpu = cpu_map.get(mid)
        cpu_str = f"{cpu:.1f}%" if cpu is not None else "N/A"
        rows.append([iid, itype, str(vcpu), str(ram), cpu_str])

    headers = ["Instance ID", "Type", "vCPUs", "RAM (GB)", "CPU %"]
    widths = [max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*row))


if __name__ == "__main__":
    main()
