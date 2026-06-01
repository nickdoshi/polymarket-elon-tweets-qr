"""
Elon Tweet Market — Archive Scraper
Streams through ALL v1 and v2 parquet files, extracts only Elon tweet market rows,
writes results directly to staging parquets (no pandas accumulation), then merges.

Strategy:
  - DuckDB does HTTP range-reads with predicate pushdown — only downloads relevant
    row groups for our condition IDs, not full 400 MB files.
  - Each source file that has matching rows is written straight to a staging parquet
    via DuckDB COPY — no pandas DataFrame is ever materialised in Python memory.
    Peak RAM is bounded by DuckDB's memory_limit (2 GB), not by file count.
  - A final merge step produces elon_tweet_ticks.parquet.
  - Resumes from a compact checkpoint (JSON with completed URL set + total row count).
  - Pre-downloaded files in data/ are used in-place (no re-download).

Usage:
    python scrape_elon_tweets.py
    python scrape_elon_tweets.py --merge-only

Output:
    elon_tweet_ticks.parquet          — master parquet (created at end / on --merge)
    staging/src_NNNNNN.parquet        — per-source write buffers (auto-cleaned)
    scrape_progress.json              — resume checkpoint
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
import duckdb

# ── Config ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR     = Path("/Users/nick/Desktop/intern/POLYMARKET")
MASTER_FILE    = OUTPUT_DIR / "elon_tweet_ticks.parquet"
STAGING_DIR    = OUTPUT_DIR / "staging"
PROGRESS_FILE  = OUTPUT_DIR / "scrape_progress.json"
LOCAL_DATA_DIR = OUTPUT_DIR / "data"   # pre-downloaded raw parquets (skip re-download)

V1_BASE = "https://r2.pmxt.dev/polymarket_orderbook_"
V2_BASE = "https://r2v2.pmxt.dev/polymarket_orderbook_"

EVENT_TYPES_KEEP = ("price_change", "last_trade_price", "book")

# ── Load condition IDs ─────────────────────────────────────────────────────────
def load_condition_ids() -> list[str]:
    """
    Merge condition IDs from:
      1. market_ids.json          — from PMXT API (current/active markets)
      2. all_elon_cids_2026.json  — from Polymarket CLOB API (historical markets)
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
        ids.update(data.get("condition_ids", []))
        ids.update(data.get("condition_ids_2026", []))

    if not ids:
        raise RuntimeError(
            "No condition IDs found.\n"
            "  Run: python fetch_market_ids.py\n"
            "to generate market_ids.json and all_elon_cids_2026.json."
        )

    print(f"Loaded {len(ids)} distinct condition IDs.")
    return sorted(ids)

# ── Generate URL schedule ──────────────────────────────────────────────────────
def generate_url_schedule() -> list[tuple[str, str, datetime]]:
    """
    Returns list of (url, archive_version, datetime) tuples covering:
      v1: Feb 21 2026 T16 → Apr 16 2026 T06  (base: r2.pmxt.dev)
      v2: Apr 13 2026 T19 → (now - 5 days)   (base: r2v2.pmxt.dev)
    Gaps and 404s are silently skipped in the main loop.
    """
    schedule: list[tuple[str, str, datetime]] = []

    v1_start = datetime(2026, 2, 21, 16)
    v1_end   = datetime(2026, 4, 16, 7)
    curr = v1_start
    while curr < v1_end:
        schedule.append((f"{V1_BASE}{curr.strftime('%Y-%m-%dT%H')}.parquet", "v1", curr))
        curr += timedelta(hours=1)

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
    return {"completed_urls": [], "total_rows": 0, "errors": [], "staging_count": 0}

def save_progress(progress: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))

# ── Schema detection ───────────────────────────────────────────────────────────
def detect_schema(source: str, conn: duckdb.DuckDBPyConnection) -> str:
    """
    Inspect column names to determine actual file schema.
    Returns 'v1', 'v2', or 'unknown'.
    DuckDB with HTTP metadata cache makes this a metadata-only read (parquet footer).
    """
    cols = {row[0] for row in conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{source}') LIMIT 0"
    ).fetchall()}
    if "market_id" in cols:
        return "v1"
    if "market" in cols:
        return "v2"
    return "unknown"

# ── Extract directly to parquet (no pandas) ────────────────────────────────────
def extract_and_write(
    source: str,
    ids_sql: str,
    conn: duckdb.DuckDBPyConnection,
    out_path: Path,
) -> int:
    """
    Detect schema, build query, write matching rows directly to a parquet file
    using DuckDB COPY — no pandas DataFrame is ever created in Python memory.
    Returns the number of rows written (0 = no data, out_path not created).
    """
    actual = detect_schema(source, conn)
    if actual == "unknown":
        return 0

    event_types_sql = str(tuple(EVENT_TYPES_KEEP))

    if actual == "v1":
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
    else:  # v2
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

    STAGING_DIR.mkdir(exist_ok=True)
    row_count = conn.execute(f"""
    COPY ({q}) TO '{out_path}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """).fetchone()[0]

    if row_count == 0:
        out_path.unlink(missing_ok=True)

    return row_count

# ── Final merge ────────────────────────────────────────────────────────────────
MERGE_CHUNK_SIZE = 200  # files per intermediate merge pass

def _master_schema_compatible() -> bool:
    """True if the existing master has 'condition_id' (new schema), not just 'market'."""
    if not MASTER_FILE.exists():
        return False
    conn = duckdb.connect()
    cols = {r[0] for r in conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{MASTER_FILE}') LIMIT 0"
    ).fetchall()}
    conn.close()
    return "condition_id" in cols


def _copy_files(sources: list[Path], dest: Path, conn: duckdb.DuckDBPyConnection) -> None:
    """Stream-copy a list of parquet files into dest — no sort, no dedup, O(1) disk."""
    file_list = "[" + ", ".join(f"'{p}'" for p in sources) + "]"
    conn.execute(f"""
    COPY (SELECT * FROM read_parquet({file_list}, union_by_name=true))
    TO '{dest}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """)


def merge_staging_to_master() -> None:
    """
    Merge all staging/src_*.parquet files into elon_tweet_ticks.parquet.

    Design choices to avoid OOM / disk-full:
      - No ORDER BY or DISTINCT ON: those force a full sort of every row onto
        temp disk (~50 GB for 500 M rows). Streaming COPY is O(1) temp disk.
      - Chunked merge: with 2000+ files a single read_parquet([...]) opens too
        many file handles. We first merge chunks of MERGE_CHUNK_SIZE into
        small intermediates, then do a final merge of those intermediates.
      - Old master excluded if its schema predates the 'condition_id' rename
        (it has a 'market' column instead — incompatible; the staging files
        cover the same time range anyway).
    """
    batch_files = sorted(STAGING_DIR.glob("src_*.parquet"))
    if not batch_files:
        print("No staging files to merge.")
        return

    include_master = _master_schema_compatible()
    all_sources = list(batch_files) + ([MASTER_FILE] if include_master else [])

    tmp_out   = OUTPUT_DIR / "_merge_tmp.parquet"
    inter_dir = OUTPUT_DIR / "_merge_inter"

    print(f"\nMerging {len(batch_files)} staging file(s)"
          f"{' + existing master' if include_master else ' (old master skipped — schema mismatch)'}"
          f" → {MASTER_FILE.name} ...")

    conn = duckdb.connect()
    conn.execute(
        "SET threads=2; "
        "SET memory_limit='4GB'; "
        "SET preserve_insertion_order=false;"
    )

    inter_files: list[Path] = []
    try:
        if len(all_sources) <= MERGE_CHUNK_SIZE:
            _copy_files(all_sources, tmp_out, conn)
        else:
            # Pass 1: merge each chunk → one intermediate file
            inter_dir.mkdir(exist_ok=True)
            n_chunks = (len(all_sources) + MERGE_CHUNK_SIZE - 1) // MERGE_CHUNK_SIZE
            for i in range(0, len(all_sources), MERGE_CHUNK_SIZE):
                chunk = all_sources[i : i + MERGE_CHUNK_SIZE]
                inter = inter_dir / f"inter_{i // MERGE_CHUNK_SIZE:04d}.parquet"
                _copy_files(chunk, inter, conn)
                inter_files.append(inter)
                print(f"  chunk {i // MERGE_CHUNK_SIZE + 1}/{n_chunks}: "
                      f"{len(chunk)} files → {inter.name}")

            # Pass 2: merge all intermediates into final output
            _copy_files(inter_files, tmp_out, conn)

        row_count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{tmp_out}')"
        ).fetchone()[0]

    finally:
        conn.close()
        for f in inter_files:
            if f.exists():
                f.unlink()
        if inter_dir.exists():
            try:
                inter_dir.rmdir()
            except OSError:
                pass

    tmp_out.replace(MASTER_FILE)
    size_mb = MASTER_FILE.stat().st_size / 1e6
    print(f"Master parquet: {row_count:,} rows  {size_mb:.1f} MB")

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

    local_map: dict[str, Path] = {}
    if LOCAL_DATA_DIR.exists():
        for p in LOCAL_DATA_DIR.glob("*.parquet"):
            local_map[p.name] = p
        print(f"Found {len(local_map)} pre-downloaded files in {LOCAL_DATA_DIR}/")

    pending = [(url, ver, dt) for url, ver, dt in schedule if url not in completed_set]
    print(f"\nAlready done: {len(completed_set)}  |  Remaining: {len(pending)}")

    # Single connection for all reads — HTTP metadata cache is per-connection
    # memory_limit bounds DuckDB's internal buffers; no pandas accumulation occurs
    conn = duckdb.connect()
    conn.execute("SET enable_http_metadata_cache=true; SET threads=4; SET memory_limit='2GB';")

    staging_count = progress.get("staging_count", 0)
    total_rows    = progress["total_rows"]
    files_done    = 0

    try:
        for url, version, dt in pending:
            fname  = Path(url).name
            source = str(local_map[fname]) if fname in local_map else url
            out_path = STAGING_DIR / f"src_{staging_count:06d}.parquet"

            try:
                row_count = extract_and_write(source, ids_sql, conn, out_path)
            except Exception as e:
                err_str = str(e)
                if "404" in err_str or "Not Found" in err_str:
                    pass
                else:
                    print(f"  ✗ {dt.strftime('%Y-%m-%dT%H')} [{version}]  {err_str[:80]}")
                    progress["errors"].append({"url": url, "dt": dt.isoformat(), "error": err_str})
                completed_set.add(url)
                progress["completed_urls"].append(url)
                continue

            completed_set.add(url)
            progress["completed_urls"].append(url)
            files_done += 1

            if row_count > 0:
                staging_count += 1
                total_rows    += row_count
                print(f"  ✓ {dt.strftime('%Y-%m-%dT%H')} [{version}]  "
                      f"+{row_count:,} rows  (running total: {total_rows:,})")

            # Save checkpoint periodically (every 50 files processed)
            if files_done % 50 == 0:
                progress["staging_count"] = staging_count
                progress["total_rows"]    = total_rows
                save_progress(progress)

    except KeyboardInterrupt:
        print("\n⚠ Interrupted — saving progress checkpoint...")

    finally:
        progress["staging_count"] = staging_count
        progress["total_rows"]    = total_rows
        save_progress(progress)
        conn.close()

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
        merge_staging_to_master()
    else:
        main()
