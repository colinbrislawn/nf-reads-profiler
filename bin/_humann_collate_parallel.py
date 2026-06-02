#!/usr/bin/env python3
"""Parallel, scale-friendly HUMAnN collate for one table type.

Supersedes the join-only _humann_join_parallel.py. Each chunk runs the FULL
per-type pipeline — humann_join_tables -> humann_split_stratified_table ->
biom convert — on its ~chunk_size samples, in parallel; then the per-chunk bioms
are concatenated with safe_cluster_process.join_biom_files (biom Table.concat).

Why: at scale (1440+ samples) the dominant cost is `biom convert` on the full
tens-of-GB combined table (single-threaded, ~20 min, ~25 GB RSS each). Converting
small per-chunk tables in parallel and concatenating avoids that entirely. Same
split -> parallel -> rejoin pattern as bin/safe_cluster_process.py, whose
join_biom_files we reuse directly.

Per-chunk split is exact: stratification is per-feature (taxon `|`), independent of
samples, so concatenating per-chunk stratified/unstratified bioms (union of
observations, samples concatenated) equals splitting+converting the full table.

Outputs for <out_prefix>:
  <out_prefix>_combined.tsv       humann_join_tables over the partial combined TSVs
  <out_prefix>_stratified.biom    concat of per-chunk stratified bioms
  <out_prefix>_unstratified.biom  concat of per-chunk unstratified bioms

Runs inside the HUMAnN container (needs humann_join_tables, humann_split_stratified_table,
biom, and biom/scipy python libs — all present in barbarahelena/humann:4.0.3).
"""
import argparse, glob, math, os, shutil, subprocess, sys, tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import h5py
import numpy as np
from biom.util import biom_open
from safe_cluster_process import join_biom_files  # reuse the existing concat helper

TABLE_TYPE = "Function table"


def run(cmd):
    subprocess.run(cmd, check=True)


def has_data(tsv):
    """True if any non-UNMAPPED/UNINTEGRATED feature has a positive value."""
    with open(tsv) as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if parts[0] in ("UNMAPPED", "UNINTEGRATED"):
                continue
            for v in parts[1:]:
                try:
                    if float(v) > 0:
                        return True
                except ValueError:
                    pass
    return False


def write_empty_biom(out):
    """Placeholder matching convert_tables_to_biom's empty branch."""
    with h5py.File(out, "w") as f:
        f.attrs["id"] = "None"
        f.attrs["type"] = TABLE_TYPE
        f.attrs["format"] = "Biological Observation Matrix 1.0.0"
        f.attrs["format_url"] = "http://biom-format.org"
        f.attrs["generated_by"] = "nf-reads-profiler"
        f.attrs["creation_date"] = ""
        f.attrs["shape"] = np.array([0, 0], dtype=np.int32)
        f.attrs["nnz"] = np.int32(0)
        f.create_group("observation").create_dataset("ids", data=np.array([], dtype="S"))
        f.create_group("sample").create_dataset("ids", data=np.array([], dtype="S"))


def balanced_chunks(files, chunk_size):
    """k = ceil(n/chunk_size) chunks, capped at n//2 so each holds >= 2 files
    (humann_join_tables names a 1-file chunk after the filename, not the sample id)."""
    n = len(files)
    k = max(1, math.ceil(n / chunk_size))
    if n >= 2:
        k = min(k, n // 2)
    base, extra = divmod(n, k)
    chunks, idx = [], 0
    for j in range(k):
        sz = base + (1 if j < extra else 0)
        chunks.append(files[idx:idx + sz])
        idx += sz
    return chunks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input_dir")
    ap.add_argument("file_name")
    ap.add_argument("out_prefix")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--chunk-size", type=int, default=150)
    a = ap.parse_args()

    files = sorted(
        f for f in glob.glob(os.path.join(a.input_dir, "*"))
        if a.file_name in os.path.basename(f)
    )
    if not files:
        sys.exit(f"[collate_parallel] no files matching *{a.file_name}* in {a.input_dir}")

    chunks = balanced_chunks(files, a.chunk_size)
    print(f"[collate_parallel] {a.file_name}: {len(files)} files -> {len(chunks)} chunk(s) "
          f"(sizes {min(map(len, chunks))}-{max(map(len, chunks))}), {a.threads} threads",
          flush=True)

    out_dir = os.path.dirname(os.path.abspath(a.out_prefix)) or "."
    td = tempfile.mkdtemp(dir=out_dir, prefix="collate_")
    comb_dir = os.path.join(td, "comb"); os.makedirs(comb_dir)
    split_dir = os.path.join(td, "split"); os.makedirs(split_dir)

    def do_chunk(idx_files):
        i, fs = idx_files
        cdir = os.path.join(td, f"in_{i:04d}"); os.makedirs(cdir)
        for f in fs:
            os.symlink(os.path.abspath(f), os.path.join(cdir, os.path.basename(f)))
        comb = os.path.join(comb_dir, f"comb_{i:04d}.tsv")
        run(["humann_join_tables", "-i", cdir, "-o", comb, "--file_name", a.file_name])
        run(["humann_split_stratified_table", "-i", comb, "-o", split_dir])
        stem = f"comb_{i:04d}"
        result = {"comb": comb, "stratified": None, "unstratified": None}
        for strat in ("stratified", "unstratified"):
            tsv = os.path.join(split_dir, f"{stem}_{strat}.tsv")
            if os.path.exists(tsv) and has_data(tsv):
                biom = os.path.join(td, f"{strat}_{i:04d}.biom")
                run(["biom", "convert", "--input-fp", tsv, "--output-fp", biom,
                     "--table-type", TABLE_TYPE, "--to-hdf5"])
                result[strat] = biom
        return result

    try:
        results = []
        with ThreadPoolExecutor(max_workers=a.threads) as ex:
            futs = {ex.submit(do_chunk, (i, fs)): i for i, fs in enumerate(chunks)}
            for fut in as_completed(futs):
                results.append(fut.result())  # re-raises on any chunk failure

        # Gather 1: combined TSV (published) — join the partial combined tables. Their
        # names share 'comb_' and live in comb_dir alone (split TSVs are elsewhere).
        run(["humann_join_tables", "-i", comb_dir, "-o", f"{a.out_prefix}_combined.tsv",
             "--file_name", "comb_"])

        # Gather 2: each stratification biom = concat of the per-chunk bioms.
        for strat in ("stratified", "unstratified"):
            bioms = [r[strat] for r in results if r[strat]]
            out = f"{a.out_prefix}_{strat}.biom"
            if bioms:
                joined = join_biom_files(bioms)
                with biom_open(out, "w") as f:
                    joined.to_hdf5(f, f"{strat} table")
                print(f"[collate_parallel] {a.file_name} {strat}: concat {len(bioms)} chunk bioms "
                      f"-> {joined.shape[0]} obs x {joined.shape[1]} samples", flush=True)
            else:
                write_empty_biom(out)
                print(f"[collate_parallel] {a.file_name} {strat}: all chunks empty -> placeholder",
                      flush=True)
    finally:
        shutil.rmtree(td, ignore_errors=True)


if __name__ == "__main__":
    main()
