#!/usr/bin/env python3
"""Parallel scatter-gather wrapper around humann_join_tables.

`humann_join_tables` is single-threaded and parses every per-sample TSV serially,
so a large study (e.g. Diversigen, 1440 samples) pins one core for many minutes.
This splits the per-sample files into chunks, runs `humann_join_tables` on each
chunk concurrently, then joins the partial combined tables with a final
`humann_join_tables` — the same split -> parallel -> rejoin pattern as
bin/safe_cluster_process.py, but operating on the HUMAnN TSV join so the result is
identical to a single serial join (still humann_join_tables throughout, preserving
the row-union and stratified `taxon|` semantics the downstream split relies on).

Runs inside the HUMAnN container (needs humann_join_tables on PATH).

Usage:
  _humann_join_parallel.py <input_dir> <file_name> <output_tsv> [--threads N] [--chunk-size M]

  input_dir   directory of per-sample tables
  file_name   substring selecting this type's files (genefamilies|reactions|pathabundance)
  output_tsv  final combined table path
"""
import argparse, glob, math, os, subprocess, sys, tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed


def join_tables(input_dir, file_name, out_tsv):
    """One humann_join_tables call over every *file_name* file in input_dir."""
    subprocess.run(
        ["humann_join_tables", "-i", input_dir, "-o", out_tsv, "--file_name", file_name],
        check=True,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input_dir")
    ap.add_argument("file_name")
    ap.add_argument("output_tsv")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--chunk-size", type=int, default=200)
    a = ap.parse_args()

    files = sorted(
        f for f in glob.glob(os.path.join(a.input_dir, "*"))
        if a.file_name in os.path.basename(f)
    )
    if not files:
        sys.exit(f"[parallel_join] no files matching *{a.file_name}* in {a.input_dir}")

    # Balanced chunks with >= 2 files each: humann_join_tables names a column after
    # the *filename* (not the internal sample id) when a chunk has only ONE table, so
    # a lone-1 chunk would mislabel that sample. Pick k = ceil(n/chunk_size) chunks,
    # capped at n//2 so every chunk holds >= 2 files, then split as evenly as possible.
    n = len(files)
    k = max(1, math.ceil(n / a.chunk_size))
    if n >= 2:
        k = min(k, n // 2)
    base, extra = divmod(n, k)
    chunks, idx = [], 0
    for j in range(k):
        sz = base + (1 if j < extra else 0)
        chunks.append(files[idx:idx + sz]); idx += sz
    print(f"[parallel_join] {a.file_name}: {n} files -> {len(chunks)} balanced chunk(s) "
          f"(sizes {min(len(c) for c in chunks)}-{max(len(c) for c in chunks)}), "
          f"{a.threads} threads", flush=True)

    # Single chunk: a plain serial join — byte-identical to the original behaviour.
    if len(chunks) == 1:
        join_tables(a.input_dir, a.file_name, a.output_tsv)
        return

    out_dir = os.path.dirname(os.path.abspath(a.output_tsv))
    with tempfile.TemporaryDirectory(dir=out_dir) as td:
        def do_chunk(i_files):
            i, fs = i_files
            cdir = os.path.join(td, f"chunk_{i}")
            os.makedirs(cdir, exist_ok=True)
            for f in fs:
                os.symlink(os.path.abspath(f), os.path.join(cdir, os.path.basename(f)))
            partial = os.path.join(td, f"partial_{i:04d}.tsv")
            join_tables(cdir, a.file_name, partial)
            return partial

        partials = []
        with ThreadPoolExecutor(max_workers=a.threads) as ex:
            futs = {ex.submit(do_chunk, (i, fs)): i for i, fs in enumerate(chunks)}
            for fut in as_completed(futs):
                partials.append(fut.result())  # raises if any chunk join failed

        # Final gather: join the partial combined tables (their names share 'partial_';
        # the chunk_* subdirs are not scanned — humann_join_tables lists files, not dirs).
        join_tables(td, "partial_", a.output_tsv)
        print(f"[parallel_join] {a.file_name}: gathered {len(partials)} partials -> "
              f"{os.path.basename(a.output_tsv)}", flush=True)


if __name__ == "__main__":
    main()
