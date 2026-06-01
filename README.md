# Polymarket — Elon Musk Tweet Count QR

Quantitative research toolkit for Polymarket's **Elon Musk tweet count** markets. Scrapes full tick-level orderbook history from the PMXT archive, normalises v1 and v2 schemas into a single parquet, and provides analysis scripts for finding alpha.

---

## Markets covered

| Type | Format | Example |
|---|---|---|
| **Monthly** | "Elon Musk # tweets in [Month] 2026?" | 26–66 binary outcome buckets (0–19, 20–39 … 2000+) |
| **Weekly** | "Elon Musk # tweets [date] – [date]?" | 10–26 binary outcome buckets |

Each market is **multi-outcome**: exactly one bucket resolves YES, all others resolve NO.  
Coverage: Jan 2026 → present (~4,246 condition IDs for 2026 alone).

---

## Scripts

### `fetch_market_ids.py`
Run once before scraping. Pulls all known condition IDs from two sources:

```bash
python fetch_market_ids.py [--pmxt-key pmxt_live_...]
```

Outputs:
- `market_ids.json` — active market metadata from the PMXT API (slugs, token IDs, live prices)
- `all_elon_cids_2026.json` — all historical condition IDs from the Polymarket CLOB API (6,800+ across all time, 4,246 for 2026)

---

### `scrape_elon_tweets.py`
Streams all ~2,285 hourly parquets from the PMXT archive (v1 + v2), filters only Elon tweet market rows using DuckDB's HTTP predicate pushdown, and writes a master parquet.

```bash
python scrape_elon_tweets.py          # full scrape
python scrape_elon_tweets.py          # re-run to resume from checkpoint
python scrape_elon_tweets.py --merge-only  # re-merge staging without re-scraping
```

- Uses `data/` for any pre-downloaded raw parquets (skips re-download)
- Writes batches to `staging/batch_NNNN.parquet`, merges at end into `elon_tweet_ticks.parquet`
- Progress checkpoint in `scrape_progress.json` (safe to interrupt and resume)

**Archive sources:**

| Version | Base URL | Date range | Schema |
|---|---|---|---|
| v1 | `https://r2.pmxt.dev/` | Feb 21 – Apr 16 2026 | 5 cols: `market_id`, `update_type`, `data` (JSON blob) |
| v2 | `https://r2v2.pmxt.dev/` | Apr 13 2026 – ~5 days ago | 16 flat cols |

---

### `elon_tweet_qr.py`
QR analysis using the PMXT API for live + OHLCV data.

```bash
python elon_tweet_qr.py
```

Outputs `elon_tweet_qr.png` — a 6-panel dashboard.

---

## Output parquet schema

**File:** `elon_tweet_ticks.parquet`  
**Sorted by:** `condition_id`, `asset_id`, `timestamp_received`  
**Compression:** Zstandard, 100k row-groups  

| Column | Type | Notes |
|---|---|---|
| `timestamp_received` | TIMESTAMPTZ | When the PMXT feed received the event |
| `timestamp` | TIMESTAMPTZ | Exchange-side event timestamp |
| `condition_id` | VARCHAR | Polymarket condition ID (`0x` + 64 hex chars). Identifies the **market** (e.g. "Elon tweets in May 2026"). Many outcomes share the same condition_id. |
| `asset_id` | VARCHAR | CLOB token ID (decimal string). Identifies a specific **outcome** (YES token for one bucket). Join with `market_ids.json` → `yes_token` to map to a bucket label. |
| `event_type` | VARCHAR | `price_change` (99%+), `last_trade_price`, `book` |
| `price` | DOUBLE | Trade/change price, 0–1 scale (raw probability) |
| `size` | DOUBLE | Order/trade size in USDC |
| `side` | VARCHAR | `BUY` or `SELL` |
| `best_bid` | DOUBLE | Best bid at time of event, 0–1 scale |
| `best_ask` | DOUBLE | Best ask at time of event, 0–1 scale |
| `bids` | VARCHAR | JSON depth `[["price","size"],...]` — only populated on `book` events |
| `asks` | VARCHAR | JSON depth `[["price","size"],...]` — only populated on `book` events |
| `transaction_hash` | VARCHAR | On-chain tx hash — only populated on `last_trade_price` events |
| `source_version` | VARCHAR | `v1` or `v2` — which archive the row came from |

### Quick DuckDB query

```python
import duckdb
conn = duckdb.connect()

# Mid-price time series for a specific outcome (asset_id)
conn.execute("""
    SELECT
        timestamp,
        condition_id,
        asset_id,
        (best_bid + best_ask) / 2   AS mid_price,
        best_ask - best_bid          AS spread,
        price,
        size
    FROM 'elon_tweet_ticks.parquet'
    WHERE event_type = 'price_change'
      AND asset_id = '<your_token_id>'
    ORDER BY timestamp
""").df()
```

### Mapping `asset_id` → bucket label

```python
import json, pandas as pd

markets = pd.read_json('market_ids.json')
# yes_token = YES outcome token; no_token = NO outcome token
# slug contains the bucket range: e.g. "elon-musk-of-tweets-may-2026-160-179"
token_to_bucket = dict(zip(markets['yes_token'], markets['slug']))
```

---

## Price format note

> **PMXT OHLCV endpoint returns prices in decimal-odds format** (`1 / probability`), not raw probability.  
> Convert: `prob = 1 / ohlcv_close`  
>  
> The flat `best_bid` / `best_ask` / `price` columns in the parquet are **raw probability** (0–1 scale), matching `market.yes.price` from `fetch_markets()`.

---

## Cross-market arb angle

The same Elon tweet count period is covered by both a **monthly** market and several overlapping **weekly** markets. The joint weekly distributions should be consistent with the monthly. When they diverge, there is an implied arb:

```
sum(weekly_implied_means) ≈ monthly_implied_mean
```

The parquet enables you to track this spread over time at tick resolution.

---

## Dependencies

```bash
pip install pmxt duckdb pandas numpy matplotlib seaborn scipy statsmodels
```

---

## Ideas

Don't predict Elon Tweet Intensity/Cadence
PCA - main component should be market intensity
    - spreads between front/back month

Try predict principal component

Features
- Tweets? - hard to match up to the current data set
- Returns for each of the markets/buckets
- 