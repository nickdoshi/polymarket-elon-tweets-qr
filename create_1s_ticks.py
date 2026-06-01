"""
Create elon_ticks_1s.parquet — 1-second-bucketed orderbook + trade data.

Why the single-pass approach OOMs:
  1.36 B rows × ~1 event/35 sec per token → ~1.36 B distinct (token, second) groups.
  DuckDB's hash table for that many groups is ~100 GB → spills to disk → disk full.

Solution: process BATCH_SIZE condition_ids at a time.
  Each batch = ~450 K rows → hash table fits comfortably in RAM, no disk spill.
  Each batch writes one small staging parquet; all are merged at the end.

Input:  elon_tweet_ticks.parquet  (kept intact)
Output: elon_ticks_1s.parquet

Usage:
    python create_1s_ticks.py
"""

import duckdb, time
from pathlib import Path

BASE       = Path("/Users/nick/Desktop/intern/POLYMARKET")
SRC        = BASE / "elon_tweet_ticks.parquet"
DST        = BASE / "elon_ticks_1s.parquet"
STAGE_DIR  = BASE / "_1s_stage"
BATCH_SIZE = 50   # condition_ids per batch — reduce to 20 if still OOM

# ── helpers ────────────────────────────────────────────────────────────────
def copy_union(sources: list[Path], dest: Path, conn) -> None:
    """Stream-copy a list of parquets into dest (no sort, no dedup)."""
    file_list = "[" + ", ".join(f"'{p}'" for p in sources) + "]"
    conn.execute(f"""
    COPY (SELECT * FROM read_parquet({file_list}))
    TO '{dest}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """)

# ── connect ────────────────────────────────────────────────────────────────
conn = duckdb.connect()
conn.execute(
    "SET memory_limit='6GB'; "
    "SET preserve_insertion_order=false; "
    "SET threads=4;"
)

print(f"Source: {SRC.name}  ({SRC.stat().st_size / 1e9:.2f} GB)")

# ── get all condition_ids ──────────────────────────────────────────────────
print("Reading distinct condition_ids …")
all_cids = conn.execute(
    f"SELECT DISTINCT condition_id FROM read_parquet('{SRC}') ORDER BY condition_id"
).fetchdf()["condition_id"].tolist()

batches   = [all_cids[i : i + BATCH_SIZE] for i in range(0, len(all_cids), BATCH_SIZE)]
n_batches = len(batches)
print(f"condition_ids: {len(all_cids)}  →  {n_batches} batches × {BATCH_SIZE}")

STAGE_DIR.mkdir(exist_ok=True)

# ── per-batch aggregation ──────────────────────────────────────────────────
print(f"\nProcessing batches …  (each pass reads {SRC.stat().st_size/1e9:.1f} GB)")
t_total = time.time()

for bi, batch in enumerate(batches):
    out = STAGE_DIR / f"batch_{bi:04d}.parquet"
    if out.exists():
        print(f"  [{bi+1}/{n_batches}] skip (already done)")
        continue

    ids_sql = "(" + ", ".join(f"'{c}'" for c in batch) + ")"
    t0 = time.time()

    conn.execute(f"""
    COPY (
        SELECT
            DATE_TRUNC('second', timestamp)                                  AS timestamp,
            condition_id,
            asset_id,
            arg_max(best_bid,  timestamp)                                    AS best_bid,
            arg_max(best_ask,  timestamp)                                    AS best_ask,
            SUM(CASE WHEN event_type = 'last_trade_price'
                     THEN size ELSE 0 END)                                   AS trade_size,
            MAX(CASE WHEN event_type = 'last_trade_price'
                     THEN price END)                                         AS trade_price,
            COUNT(*)                                                         AS n_events,
            SUM(CASE WHEN event_type = 'price_change'
                     THEN 1 ELSE 0 END)                                      AS n_quotes,
            SUM(CASE WHEN event_type = 'last_trade_price'
                     THEN 1 ELSE 0 END)                                      AS n_trades,
            arg_max(source_version, timestamp)                               AS source_version
        FROM   read_parquet('{SRC}')
        WHERE  event_type   IN ('price_change', 'last_trade_price')
          AND  condition_id IN {ids_sql}
        GROUP  BY DATE_TRUNC('second', timestamp), condition_id, asset_id
    ) TO '{out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """)

    elapsed  = time.time() - t0
    done_so_far = bi + 1
    avg_s    = (time.time() - t_total) / done_so_far
    eta_min  = (n_batches - done_so_far) * avg_s / 60
    print(f"  [{done_so_far}/{n_batches}] {out.name}  {elapsed:.0f}s  ETA {eta_min:.0f} min")

print(f"\nAll batches done in {(time.time()-t_total)/60:.1f} min")

# ── merge staging files ────────────────────────────────────────────────────
batch_files  = sorted(STAGE_DIR.glob("batch_*.parquet"))
MERGE_CHUNK  = 200
TMP          = BASE / "_1s_tmp.parquet"

print(f"Merging {len(batch_files)} batch files …")

if len(batch_files) <= MERGE_CHUNK:
    copy_union(batch_files, TMP, conn)
else:
    INTER_DIR = BASE / "_1s_inter"
    INTER_DIR.mkdir(exist_ok=True)
    inter_files = []
    n_chunks = (len(batch_files) + MERGE_CHUNK - 1) // MERGE_CHUNK

    for ci in range(0, len(batch_files), MERGE_CHUNK):
        chunk = batch_files[ci : ci + MERGE_CHUNK]
        inter = INTER_DIR / f"inter_{ci // MERGE_CHUNK:04d}.parquet"
        copy_union(chunk, inter, conn)
        inter_files.append(inter)
        print(f"  merge chunk {ci // MERGE_CHUNK + 1}/{n_chunks} → {inter.name}")

    copy_union(inter_files, TMP, conn)

    for f in inter_files:
        f.unlink()
    INTER_DIR.rmdir()

# ── finalise ───────────────────────────────────────────────────────────────
n    = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{TMP}')").fetchone()[0]
span = conn.execute(
    f"SELECT MIN(timestamp)::VARCHAR, MAX(timestamp)::VARCHAR FROM read_parquet('{TMP}')"
).fetchone()
conn.close()

TMP.replace(DST)
for f in batch_files:
    f.unlink()
try:
    STAGE_DIR.rmdir()
except OSError:
    pass

print(f"\n{'='*55}")
print(f"  Output:     {DST.name}")
print(f"  Rows:       {n:,}")
print(f"  Size:       {DST.stat().st_size / 1e6:.0f} MB")
print(f"  Date range: {span[0]}  →  {span[1]}")
print(f"  Original:   {SRC.name}  {SRC.stat().st_size / 1e6:.0f} MB  (intact)")
print(f"{'='*55}")
print("Set SOURCE = '1s' in data_pipeline.ipynb to use this file.")
