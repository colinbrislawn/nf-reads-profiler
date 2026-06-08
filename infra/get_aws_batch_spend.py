#!/usr/bin/env python3
"""Daily EC2 spend — VM costs and other EC2 costs in separate plain-text tables."""

import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

LOOKBACK_DAYS = 7
S3_BUCKETS = [
    "cjb-gutz-s3-demo",
    "gutz-nf-reads-profilers-workdir",
    "gutz-nf-reads-profilers-runs",
]
S3_PRICE_PER_GB = 0.023  # us-east-2 standard storage, $/GB/month


def aws(*args):
    r = subprocess.run(["aws", "--region", "us-east-1"] + list(args), capture_output=True, text=True)
    if r.returncode != 0:
        print(f"ERROR: {r.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(r.stdout)


def query_daily(service, start, end):
    result = aws(
        "ce", "get-cost-and-usage",
        "--time-period", f"Start={start},End={end}",
        "--granularity", "DAILY",
        "--metrics", "UnblendedCost",
        "--filter", json.dumps({"Dimensions": {"Key": "SERVICE", "Values": [service]}}),
        "--group-by", "Type=DIMENSION,Key=USAGE_TYPE",
        "--output", "json",
    )
    by_date = {}
    all_groups = set()
    for period in result["ResultsByTime"]:
        date = period["TimePeriod"]["Start"]
        by_date[date] = {}
        for group in period["Groups"]:
            name = group["Keys"][0]
            cost = float(group["Metrics"]["UnblendedCost"]["Amount"])
            if cost >= 0.005:
                by_date[date][name] = cost
                all_groups.add(name)
    return by_date, sorted(all_groups, key=lambda c: -sum(d.get(c, 0) for d in by_date.values()))


def clean_label(usage_type):
    """USE2-SpotUsage:r8g.2xlarge -> Spot:r8g.2xlarge; USE2-EBS:VolumeUsage.gp3 -> EBS:VolumeUsage.gp3"""
    s = re.sub(r'^[A-Z0-9]+-', '', usage_type)
    s = s.replace("BoxUsage:", "On-Demand:")
    s = s.replace("SpotUsage:", "Spot:")
    return s


def print_table(title, by_date, cols):
    dates = sorted(by_date.keys())
    labels = ["Date"] + [clean_label(c) for c in cols] + ["Total"]

    rows = []
    for date in dates:
        row = by_date[date]
        vals = [row.get(c, 0.0) for c in cols]
        total = sum(vals)
        if total < 0.005:
            continue
        rows.append([date] + [f"${v:.2f}" for v in vals] + [f"${total:.2f}"])

    if not rows:
        return

    widths = [max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(labels)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)

    print(title)
    print(fmt.format(*labels))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*row))
    print()


def bucket_size_gb(bucket):
    r = subprocess.run(
        ["aws", "s3", "ls", "--summarize", "--recursive", f"s3://{bucket}", "--region", "us-east-2"],
        capture_output=True, text=True,
    )
    for line in r.stdout.splitlines():
        if "Total Size:" in line:
            return int(line.split()[-1]) / 1e9
    return None


def print_bucket_table():
    print("S3 Bucket Sizes and Estimated Monthly Cost  (CE cannot split by bucket — estimated at $0.023/GB)")
    with ThreadPoolExecutor(max_workers=len(S3_BUCKETS)) as ex:
        sizes = list(ex.map(bucket_size_gb, S3_BUCKETS))

    rows = []
    for bucket, size_gb in zip(S3_BUCKETS, sizes):
        if size_gb is None:
            rows.append([bucket, "error", "error"])
        else:
            rows.append([bucket, f"{size_gb:.0f} GB", f"${size_gb * S3_PRICE_PER_GB:.2f}/mo"])

    labels = ["Bucket", "Size", "Est. Monthly Cost"]
    widths = [max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(labels)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*labels))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*row))
    print()


def main():
    today = datetime.now(timezone.utc).date()
    # CE end date is exclusive. Data for a completed day is typically available
    # 8–14 h after midnight UTC, so yesterday's numbers are usually ready by morning.
    end = today  # exclusive → includes yesterday
    start = end - timedelta(days=LOOKBACK_DAYS)
    print(f"Cost Explorer data typically lags 8–14 h after midnight UTC. Showing {start} to {end - timedelta(days=1)} (complete days only).")
    print()

    compute_by_date, compute_cols = query_daily("Amazon Elastic Compute Cloud - Compute", start.isoformat(), end.isoformat())
    print_table("VM Costs (EC2 Compute)", compute_by_date, compute_cols)

    other_by_date, other_cols = query_daily("EC2 - Other", start.isoformat(), end.isoformat())
    print_table("Other EC2 Costs", other_by_date, other_cols)

    print_bucket_table()

    s3_by_date, s3_cols = query_daily("Amazon Simple Storage Service", start.isoformat(), end.isoformat())

    # S3 Summary: Storage vs Requests vs Other per day
    def s3_category(col):
        if "TimedStorage" in col:
            return "Storage"
        if "Requests" in col:
            return "Requests"
        return "Other"

    summary_cols = ["Storage", "Requests", "Other"]
    summary_by_date = {}
    for date, row in s3_by_date.items():
        buckets = {"Storage": 0.0, "Requests": 0.0, "Other": 0.0}
        for col, cost in row.items():
            buckets[s3_category(col)] += cost
        summary_by_date[date] = buckets
    print_table("S3 Summary", summary_by_date, summary_cols)

    # S3 Storage detail: Standard vs Intelligent-Tiering
    storage_cols = [c for c in s3_cols if "TimedStorage" in c]
    print_table("S3 Storage Detail", s3_by_date, storage_cols)

    # S3 Requests detail: Tier1 (PUT/LIST) vs Tier2 (GET)
    request_cols = [c for c in s3_cols if "Requests" in c]
    print_table("S3 Requests Detail", s3_by_date, request_cols)


if __name__ == "__main__":
    main()
