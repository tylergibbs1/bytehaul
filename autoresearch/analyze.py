#!/usr/bin/env python3
"""Analyze autoresearch results and show progress over time."""

import csv
import sys
from pathlib import Path

TSV = Path(__file__).parent / "results.tsv"

def load_results():
    if not TSV.exists():
        print("No results.tsv found. Run bench.sh --baseline first.")
        sys.exit(1)
    with open(TSV) as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader)

def main():
    results = load_results()
    if not results:
        print("No results yet.")
        return

    # Find baseline
    baseline = next((r for r in results if r["name"] == "baseline"), results[0])
    base_500 = float(baseline["throughput_500mb"])

    print(f"{'#':>3}  {'Name':<30}  {'500MB MB/s':>12}  {'Δ%':>8}  {'100MB':>10}  {'Dir':>10}  {'Kept':>5}")
    print("─" * 95)

    best_500 = base_500
    for r in results:
        t500 = float(r["throughput_500mb"])
        t100 = float(r["throughput_100mb"])
        tdir = float(r["throughput_dir"])
        delta = ((t500 - base_500) / base_500 * 100) if base_500 > 0 else 0
        kept = r.get("kept", "?")

        marker = ""
        if t500 > best_500 and r["name"] != "baseline":
            marker = " ★"
            best_500 = t500

        print(f"{r['experiment']:>3}  {r['name']:<30}  {t500:>12.2f}  {delta:>+7.1f}%  {t100:>10.2f}  {tdir:>10.2f}  {kept:>5}{marker}")

    print()
    print(f"Baseline: {base_500:.2f} MB/s")
    print(f"Best:     {best_500:.2f} MB/s ({(best_500 - base_500) / base_500 * 100:+.1f}%)")
    print(f"Total experiments: {len(results)}")
    kept_count = sum(1 for r in results if r.get("kept") == "yes" and r["name"] != "baseline")
    print(f"Kept: {kept_count} / {len(results) - 1}")

if __name__ == "__main__":
    main()
