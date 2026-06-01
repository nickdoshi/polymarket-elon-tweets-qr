# CLAUDE.md — Polymarket Elon Tweet QR Project

## Machine constraints
- RAM: 16 GB total (leave ~4 GB for OS; DuckDB gets max 3–6 GB)
- Disk: ~460 GB, typically 85–90% full (≈ 50–60 GB free)
- Python: system python3 via anaconda base

## Critical DuckDB rules

### Never use ORDER BY inside a COPY statement
```python
# WRONG — sorts the full dataset to disk, fills 50+ GB of temp space
conn.execute("COPY (SELECT * FROM big_table ORDER BY col) TO 'out.parquet' …")

# RIGHT — stream straight through, sort at query time if needed
conn.execute("COPY (SELECT * FROM big_table) TO 'out.parquet' …")
```

### Never load large parquets into pandas
```python
# WRONG — 1.36 B rows × 14 cols crashes the kernel
df = conn.execute("SELECT * FROM read_parquet('elon_tweet_ticks.parquet')").df()

# RIGHT — aggregate in DuckDB, pull only the small result into pandas
ohlcv_df = conn.execute("SELECT …GROUP BY hour, condition_id…").df()
```

### Always set preserve_insertion_order=false for large aggregations
```python
conn.execute("SET preserve_insertion_order=false;")
```
This lets DuckDB use sort-merge aggregation instead of hash aggregation, halving memory use.

### Merging many parquet files — use chunks, no dedup sort
```python
# WRONG — DISTINCT ON + ORDER BY on 2000 files fills temp disk
"COPY (SELECT DISTINCT ON (a,b,c) * FROM read_parquet([…]) ORDER BY a,b,c) TO …"

# RIGHT — streaming union in batches of ~200 files
def _copy_files(sources, dest, conn):
    file_list = "[" + ", ".join(f"'{p}'" for p in sources) + "]"
    conn.execute(f"COPY (SELECT * FROM read_parquet({file_list}, union_by_name=true)) TO '{dest}' …")
```

### Batching large GROUP BY operations
When a single-pass GROUP BY would produce > ~50 M output rows (or input > ~500 M rows), process in batches of ~50 condition_ids at a time. Each batch writes a small staging parquet; merge them at the end.

## Data pipeline architecture

```
fetch_market_ids.py          → market_ids.json, all_elon_cids_2026.json
scrape_elon_tweets.py        → staging/src_NNNNNN.parquet → elon_tweet_ticks.parquet
create_1s_ticks.py           → elon_ticks_1s.parquet   (preferred source for notebooks)
data_pipeline.ipynb          → ohlcv_df  (hourly candles, fits in RAM)
```

### elon_tweet_ticks.parquet (master, ~10 GB)
1.36 B rows, Feb 2026 – present. Unified schema from v1 + v2 archive:
`timestamp_received, timestamp, condition_id, asset_id, event_type, price, size, side, best_bid, best_ask, bids, asks, transaction_hash, source_version`

99%+ of rows are `price_change` events — each limit order add/cancel fires one.
`book` and `last_trade_price` events are < 0.1% of rows.

### elon_ticks_1s.parquet (derived, smaller)
1-second bucketed, `price_change` + `last_trade_price` only.
Schema: `timestamp, condition_id, asset_id, best_bid, best_ask, trade_size, trade_price, n_events, n_quotes, n_trades, source_version`
Use `SOURCE = '1s'` in data_pipeline.ipynb.

### Archive schema versions
| Version | Columns | Identified by |
|---------|---------|---------------|
| v1 | `market_id`, `update_type`, `data` (JSON blob) | `"market_id" in cols` |
| v2 | `market` (BLOB), `event_type`, `asset_id`, … | `"market" in cols` |
| unknown | neither | skip silently, return empty |

Always call `detect_schema(source, conn)` before querying — early Feb 2026 files have a third layout.

## Notebook conventions

### SOURCE options in data_pipeline.ipynb
- `'1s'` — recommended, use `elon_ticks_1s.parquet`
- `'master'` — use `elon_tweet_ticks.parquet` (slow, full history)
- `'staging'` — use `staging/src_*.parquet` (only during active scrape)

### YES token identification
1. Check `known_sides` temp table (from `market_ids.json`) — authoritative
2. For unknowns: infer YES = lower average mid-price token per condition_id
   (works because most specific tweet-count buckets have P(YES) < 0.5)

### PCA on implied distribution — use CLR transform
Bucket YES prices form a probability simplex (sum to 1). Standard PCA on raw prices is wrong.
```python
# Correct: Centered Log-Ratio transform before PCA
def clr_transform(prices, eps=1e-8):
    p = np.clip(prices, eps, 1 - eps)
    log_p = np.log(p)
    return log_p - log_p.mean(axis=1, keepdims=True)

# Max useful PCs = min(N_buckets - 1, N_timestamps - 1)
# (CLR output has rank N-1 due to simplex constraint)
```

## Scraper behaviour
- Checkpoint in `scrape_progress.json` — safe to interrupt/resume
- Staging files in `staging/src_NNNNNN.parquet` — one per source hour with data
- `--merge-only` flag re-merges existing staging without re-scraping
- Old master skipped if schema has `market` column instead of `condition_id`
- v1 URL window: Feb 21 – Apr 16 2026 (r2.pmxt.dev)
- v2 URL window: Apr 13 2026 – now minus 5 days (r2v2.pmxt.dev)
