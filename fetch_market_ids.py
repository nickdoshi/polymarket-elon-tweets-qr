"""
Fetch Elon tweet market condition IDs from two sources:
  1. PMXT API  → market_ids.json          (current/active markets)
  2. Polymarket CLOB API → all_elon_cids_2026.json  (all historical markets)

Run this ONCE before running scrape_elon_tweets.py.

Usage:
    python fetch_market_ids.py [--pmxt-key pmxt_live_...]
"""

import json, re, sys, time, urllib.request
from collections import defaultdict
from pathlib import Path

OUTPUT_DIR = Path("/Users/nick/Desktop/intern/POLYMARKET")

# ── 1. PMXT API ────────────────────────────────────────────────────────────────
def fetch_pmxt_ids(api_key: str | None = None) -> list[dict]:
    """Fetch current tweet market metadata from PMXT API."""
    try:
        import pmxt
        api = pmxt.Polymarket(pmxt_api_key=api_key) if api_key else pmxt.Polymarket()
        raw = api.fetch_markets(query="elon musk", limit=200)
        tweet_markets = [m for m in raw if "tweet" in m.slug.lower()]
        print(f"PMXT API: {len(tweet_markets)} tweet market outcomes")

        rows = []
        for m in tweet_markets:
            if not m.yes:
                continue
            rows.append({
                "slug":             m.slug,
                "title":            m.title,
                "market_id":        m.market_id,
                "contract_address": m.contract_address,
                "event_id":         m.event_id,
                "yes_token":        m.yes.metadata.get("clobTokenId"),
                "no_token":         m.no.metadata.get("clobTokenId") if m.no else None,
                "live_price":       m.yes.price,
                "resolution_date":  str(m.resolution_date),
                "status":           m.status,
            })

        out = OUTPUT_DIR / "market_ids.json"
        out.write_text(json.dumps(rows, indent=2))
        print(f"Saved {len(rows)} markets to {out.name}")
        return rows

    except Exception as e:
        print(f"PMXT fetch error: {e}")
        return []

# ── 2. Polymarket CLOB API ─────────────────────────────────────────────────────
def fetch_clob_ids() -> list[dict]:
    """
    Page through Polymarket CLOB API to find ALL historical Elon tweet markets.
    Returns list of market dicts with condition_id, market_slug, end_date_iso.
    """
    print("\nQuerying Polymarket CLOB API for historical Elon tweet markets...")
    all_markets: list[dict] = []
    next_cursor: str | None = None
    page = 0

    while True:
        url = "https://clob.polymarket.com/markets?keyword=elon+tweets&limit=100"
        if next_cursor:
            url += f"&next_cursor={next_cursor}"

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
        except Exception as e:
            print(f"  CLOB page {page} error: {e}")
            break

        batch = [
            m for m in data.get("data", [])
            if "tweet" in (m.get("market_slug", "") + m.get("question", "")).lower()
        ]
        all_markets.extend(batch)
        next_cursor = data.get("next_cursor")
        page += 1

        if page % 10 == 0:
            print(f"  Page {page}: {len(all_markets)} total so far")

        # Stop conditions:
        #  - no cursor returned
        #  - empty page (no more data)
        #  - "LTE=" is base64 for "-1" — Polymarket's end-of-pagination sentinel
        if not next_cursor or not data.get("data") or next_cursor in ("LTE=", "LTE", "-1"):
            break
        time.sleep(0.15)

    print(f"CLOB API: {len(all_markets)} total Elon tweet outcomes across all time")

    # Group by period for a quick summary
    by_period: dict[str, int] = defaultdict(int)
    for m in all_markets:
        slug = m.get("market_slug", "")
        end  = m.get("end_date_iso", "")[:7]   # YYYY-MM
        by_period[end] += 1
    for period, cnt in sorted(by_period.items()):
        print(f"  {period}: {cnt} outcomes")

    # Extract all condition IDs (all time + 2026-only subsets)
    all_cids   = sorted({m["condition_id"] for m in all_markets if m.get("condition_id")})
    cids_2026  = sorted({
        m["condition_id"] for m in all_markets
        if m.get("condition_id") and "2026" in (m.get("end_date_iso") or "")
    })
    print(f"\nDistinct condition IDs (all time): {len(all_cids)}")
    print(f"Distinct condition IDs (2026):     {len(cids_2026)}")

    out = OUTPUT_DIR / "all_elon_cids_2026.json"
    out.write_text(json.dumps({
        "condition_ids":      all_cids,     # ALL historical — safe to use for v1+v2 scan
        "condition_ids_2026": cids_2026,    # 2026-only subset
        "markets":            all_markets,
    }, indent=2))
    print(f"Saved to {out.name}")
    return all_markets

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    api_key = None
    if "--pmxt-key" in sys.argv:
        idx = sys.argv.index("--pmxt-key")
        api_key = sys.argv[idx + 1]

    fetch_pmxt_ids(api_key)
    fetch_clob_ids()

    print("\nDone. Ready to run: python scrape_elon_tweets.py")
