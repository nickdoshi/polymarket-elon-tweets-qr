"""
Elon Tweet Market — Archive Scraper
Streams through ALL v1 and v2 parquet files, extracts only Elon tweet market rows,
appends to a master parquet, then deletes the downloaded raw file to save disk space.

Strategy:
  - DuckDB does HTTP range-reads with predicate pushdown — only downloads relevant
    row groups for our condition IDs, not full 400 MB files.
  - Batches of extracted rows are written to a staging/ subdirectory (one small
    parquet per batch). A final merge step produces elon_tweet_ticks.parquet.
    This avoids the O(n²) rewrite problem of appending to a single growing file.
  - Resumes from a compact checkpoint (JSON with completed URL set + total row count).
  - Pre-downloaded files in data/ are used in-place (no re-download).

Usage:
    python scrape_elon_tweets.py

Output:
    elon_tweet_ticks.parquet          — master parquet (created at end / on --merge)
    staging/batch_NNNN.parquet        — incremental write buffers (auto-cleaned)
    scrape_progress.json              — resume checkpoint
"""

import json, re, time
from datetime import datetime, timedelta
from pathlib import Path
import duckdb
import pandas as pd

# ── Config ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR    = Path("/Users/nick/Desktop/intern/POLYMARKET")
MASTER_FILE   = OUTPUT_DIR / "elon_tweet_ticks.parquet"
STAGING_DIR   = OUTPUT_DIR / "staging"
PROGRESS_FILE = OUTPUT_DIR / "scrape_progress.json"
LOCAL_DATA_DIR = OUTPUT_DIR / "data"   # pre-downloaded raw parquets (skip re-download)

V1_BASE = "https://r2.pmxt.dev/polymarket_orderbook_"
V2_BASE = "https://r2v2.pmxt.dev/polymarket_orderbook_"

# Keep these event types; skip tick_size_change (not useful for price/volume analysis)
EVENT_TYPES_KEEP = ("price_change", "last_trade_price", "book")

# Flush a batch file every N source files that contained data
BATCH_FLUSH_EVERY = 25

# ── Load condition IDs ─────────────────────────────────────────────────────────
def load_condition_ids() -> list[str]:
    """
    Merge condition IDs from:
      1. market_ids.json   — from PMXT API (current/active markets)
      2. all_elon_cids_2026.json — from Polymarket CLOB API (historical markets)
    Run fetch_market_ids.py to regenerate these files if needed.
    """
    ids: set[str] = set()

    api_file = OUTPUT_DIR / "market_ids.json"
    if api_file.exists():
        for m in json.loads(api_file.read_text()):
            if m.get("contract_address"):
                ids.add(m["contract_address"])

    clob_file = OUTPUT_DIR / "all_elon_cids_2026.json"
    if clob_file.exists():
        data = json.loads(clob_file.read_text())
        # supports both "condition_ids" and "condition_ids_2026" keys
        ids.update(data.get("condition_ids", []))
        ids.update(data.get("condition_ids_2026", []))

    if not ids:
        raise RuntimeError(
            "No condition IDs found.\n"
            "  Run: python fetch_market_ids.py\n"
            "to generate market_ids.json and all_elon_cids_2026.json."
        )

    print(f"Loaded {len(ids)} distinct condition IDs.")
    return sorted(ids)   # sorted for reproducibility

# ── Generate URL schedule ──────────────────────────────────────────────────────
def generate_url_schedule() -> list[tuple[str, str, datetime]]:
    """
    Returns list of (url, archive_version, datetime) tuples covering:
      v1: Feb 21 2026 T16 → Apr 16 2026 T06  (base: r2.pmxt.dev)
      v2: Apr 13 2026 T19 → (now - 5 days)   (base: r2v2.pmxt.dev)
    Gaps and 404s are silently skipped in the main loop.
    """
    schedule: list[tuple[str, str, datetime]] = []

    # v1 window
    v1_start = datetime(2026, 2, 21, 16)
    v1_end   = datetime(2026, 4, 16, 7)   # exclusive upper bound
    curr = v1_start
    while curr < v1_end:
        schedule.append((f"{V1_BASE}{curr.strftime('%Y-%m-%dT%H')}.parquet", "v1", curr))
        curr += timedelta(hours=1)

    # v2 window — upper bound = today minus 5-day lag (archive is ~5 days behind)
    v2_start = datetime(2026, 4, 13, 19)
    v2_end   = datetime.utcnow() - timedelta(days=5)
    v2_end   = v2_end.replace(minute=0, second=0, microsecond=0)
    curr = v2_start
    while curr < v2_end:
        schedule.append((f"{V2_BASE}{curr.strftime('%Y-%m-%dT%H')}.parquet", "v2", curr))
        curr += timedelta(hours=1)

    n_v1 = sum(1 for _, v, _ in schedule if v == "v1")
    n_v2 = sum(1 for _, v, _ in schedule if v == "v2")
    print(f"URL schedule: {len(schedule)} total  ({n_v1} v1 + {n_v2} v2)")
    return schedule

# ── Progress checkpoint ────────────────────────────────────────────────────────
def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"completed_urls": [], "total_rows": 0, "errors": [], "batch_count": 0}

def save_progress(progress: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))

# ── Extract rows from one source file ─────────────────────────────────────────
def extract_from_source(
    source: str,
    ids_sql: str,
    conn: duckdb.DuckDBPyConnection,
    version: str,       # "v1" or "v2" — passed in from the schedule, no extra HTTP call
) -> pd.DataFrame:
    """
    Handles both archive versions:

    v1 schema (r2.pmxt.dev):
        timestamp_received, timestamp_created_at, market_id (VARCHAR),
        update_type, data (JSON blob)
      JSON data fields: token_id, change_price, change_size, change_side,
                        best_bid, best_ask, bids, asks

    v2 schema (r2v2.pmxt.dev):
        timestamp_received, timestamp, market (fixed_size_binary), event_type,
        asset_id, price, size, side, best_bid, best_ask, bids, asks,
        transaction_hash, ...

    Output: unified DataFrame with columns:
        timestamp_received, timestamp, condition_id, asset_id, event_type,
        price, size, side, best_bid, best_ask, bids, asks,
        transaction_hash, source_version
    """
    event_types_sql = str(tuple(EVENT_TYPES_KEEP))

    if version == "v1":
        q = f"""
        SELECT
            timestamp_received,
            timestamp_created_at                                         AS timestamp,
            market_id                                                    AS condition_id,
            json_extract_string(data, '$.token_id')                      AS asset_id,
            update_type                                                   AS event_type,
            COALESCE(
                TRY_CAST(json_extract_string(data, '$.change_price') AS DOUBLE),
                TRY_CAST(json_extract_string(data, '$.price')        AS DOUBLE)
            )                                                            AS price,
            COALESCE(
                TRY_CAST(json_extract_string(data, '$.change_size')  AS DOUBLE),
                TRY_CAST(json_extract_string(data, '$.size')         AS DOUBLE)
            )                                                            AS size,
            COALESCE(
                json_extract_string(data, '$.change_side'),
                json_extract_string(data, '$.side')
            )                                                            AS side,
            TRY_CAST(json_extract_string(data, '$.best_bid') AS DOUBLE) AS best_bid,
            TRY_CAST(json_extract_string(data, '$.best_ask') AS DOUBLE) AS best_ask,
            json_extract_string(data, '$.bids')                         AS bids,
            json_extract_string(data, '$.asks')                         AS asks,
            NULL::VARCHAR                                               AS transaction_hash,
            'v1'::VARCHAR                                               AS source_version
        FROM read_parquet('{source}')
        WHERE market_id IN {ids_sql}
          AND update_type IN {event_types_sql}
        """
    else:
        q = f"""
        SELECT
            timestamp_received,
            timestamp,
            market::VARCHAR             AS condition_id,
            asset_id,
            event_type,
            CAST(price    AS DOUBLE)    AS price,
            CAST(size     AS DOUBLE)    AS size,
            side,
            CAST(best_bid AS DOUBLE)    AS best_bid,
            CAST(best_ask AS DOUBLE)    AS best_ask,
            bids,
            asks,
            transaction_hash,
            'v2'::VARCHAR               AS source_version
        FROM read_parquet('{source}')
        WHERE market::VARCHAR IN {ids_sql}
          AND event_type IN {event_types_sql}
        """
    return conn.execute(q).fetchdf()

# ── Staging writer ─────────────────────────────────────────────────────────────
def write_batch(batch_df: pd.DataFrame, batch_num: int) -> Path:
    """Write a batch DataFrame to staging/batch_NNNN.parquet."""
    STAGING_DIR.mkdir(exist_ok=True)
    out = STAGING_DIR / f"batch_{batch_num:04d}.parquet"
    conn = duckdb.connect()
    conn.execute(f"""
    COPY (SELECT * FROM batch_df ORDER BY condition_id, asset_id, timestamp)
    TO '{out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """)
    conn.close()
    return out

# ── Final merge ────────────────────────────────────────────────────────────────
def merge_staging_to_master() -> None:
    """
    Merge all staging/batch_*.parquet files (plus any existing master) into
    elon_tweet_ticks.parquet. Deduplicates on (condition_id, asset_id, timestamp_received).

    Uses an explicit file list (not glob inside list) so DuckDB expands correctly.
    Writes to a temp file first to avoid reading-from and writing-to the same path.
    """
    batch_files = sorted(STAGING_DIR.glob("batch_*.parquet"))
    if not batch_files:
        print("No staging batches to merge.")
        return

    # Build explicit file list — never pass a glob string inside a DuckDB list literal
    all_sources: list[Path] = list(batch_files)
    if MASTER_FILE.exists():
        all_sources.append(MASTER_FILE)

    file_list_sql = "[" + ", ".join(f"'{p}'" for p in all_sources) + "]"
    tmp_out = OUTPUT_DIR / "_merge_tmp.parquet"

    print(f"\nMerging {len(batch_files)} batch(es)"
          f"{' + existing master' if MASTER_FILE.exists() else ''} → {MASTER_FILE.name} ...")

    conn = duckdb.connect()
    conn.execute("SET threads=8; SET memory_limit='4GB';")

    # Write deduplicated result to temp file (avoids clobbering master while reading it)
    conn.execute(f"""
    COPY (
        SELECT DISTINCT ON (condition_id, asset_id, timestamp_received) *
        FROM read_parquet({file_list_sql}, union_by_name=true)
        ORDER BY condition_id, asset_id, timestamp_received
    ) TO '{tmp_out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """)
    row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_out}')").fetchone()[0]
    conn.close()

    # Atomic replace
    tmp_out.replace(MASTER_FILE)
    size_mb = MASTER_FILE.stat().st_size / 1e6
    print(f"Master parquet: {row_count:,} rows  {size_mb:.1f} MB")

    # Clean up staging batches
    for f in batch_files:
        f.unlink()
    print("Staging directory cleaned.")

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    condition_ids = load_condition_ids()
    ids_sql = "(" + ", ".join(f"'{c}'" for c in condition_ids) + ")"

    schedule = generate_url_schedule()
    progress = load_progress()
    completed_set = set(progress["completed_urls"])

    # Build local file map: filename → full path (to use in-place instead of HTTP)
    local_map: dict[str, Path] = {}
    if LOCAL_DATA_DIR.exists():
        for p in LOCAL_DATA_DIR.glob("*.parquet"):
            local_map[p.name] = p
        print(f"Found {len(local_map)} pre-downloaded files in {LOCAL_DATA_DIR}/")

    pending = [(url, ver, dt) for url, ver, dt in schedule if url not in completed_set]
    print(f"\nAlready done: {len(completed_set)}  |  Remaining: {len(pending)}")

    # Shared DuckDB connection for all HTTP reads (metadata cache is per-connection)
    conn = duckdb.connect()
    conn.execute("SET enable_http_metadata_cache=true; SET threads=4; SET memory_limit='2GB';")

    batch_dfs: list[pd.DataFrame] = []
    batch_num  = progress["batch_count"]
    total_rows = progress["total_rows"]
    files_done = 0
    data_files = 0   # files that actually had tweet data

    try:
        for url, version, dt in pending:
            # Use local file if available, else HTTP
            fname = Path(url).name
            source = str(local_map[fname]) if fname in local_map else url

            try:
                df = extract_from_source(source, ids_sql, conn, version)
            except Exception as e:
                err_str = str(e)
                if "404" in err_str or "Not Found" in err_str:
                    # Normal — archive has gaps
                    pass
                else:
                    print(f"  ✗ {dt.strftime('%Y-%m-%dT%H')} [{version}]  {err_str[:80]}")
                    progress["errors"].append({"url": url, "dt": dt.isoformat(), "error": err_str})
                # Mark as completed so we don't retry on resume
                completed_set.add(url)
                progress["completed_urls"].append(url)
                continue

            row_count = len(df)
            completed_set.add(url)
            progress["completed_urls"].append(url)
            files_done += 1

            if row_count > 0:
                batch_dfs.append(df)
                total_rows += row_count
                data_files += 1
                print(f"  ✓ {dt.strftime('%Y-%m-%dT%H')} [{version}]  "
                      f"+{row_count:,} rows  (running total: {total_rows:,})")

            # Flush batch when enough data files have accumulated
            if data_files >= BATCH_FLUSH_EVERY and batch_dfs:
                combined = pd.concat(batch_dfs, ignore_index=True)
                batch_path = write_batch(combined, batch_num)
                batch_num += 1
                batch_dfs = []
                data_files = 0
                progress["batch_count"] = batch_num
                progress["total_rows"]  = total_rows
                size_mb = batch_path.stat().st_size / 1e6
                print(f"\n  → Wrote {batch_path.name}  ({size_mb:.1f} MB)  "
                      f"[batches so far: {batch_num}]\n")
                save_progress(progress)

    except KeyboardInterrupt:
        print("\n⚠ Interrupted — saving progress checkpoint...")

    finally:
        # Flush any remaining rows
        if batch_dfs:
            combined = pd.concat(batch_dfs, ignore_index=True)
            write_batch(combined, batch_num)
            batch_num += 1
            progress["batch_count"] = batch_num

        progress["total_rows"] = total_rows
        save_progress(progress)
        conn.close()

    # Merge all staging batches → master parquet
    merge_staging_to_master()

    remaining = [u for u, _, _ in schedule if u not in completed_set]
    print(f"\n{'='*60}")
    print(f"Files processed this run:  {files_done}")
    print(f"Total rows (all runs):     {total_rows:,}")
    print(f"Files still pending:       {len(remaining)}")
    print(f"Errors:                    {len(progress['errors'])}")
    if MASTER_FILE.exists():
        print(f"Master parquet size:       {MASTER_FILE.stat().st_size / 1e6:.1f} MB")

if __name__ == "__main__":
    import sys
    if "--merge-only" in sys.argv:
        # Just merge existing staging batches without scraping
        merge_staging_to_master()
    else:
        main()
