"""
Elon Musk Tweet Count Market — Quantitative Research
Polymarket (via PMXT API)

NOTE: PMXT OHLCV prices are in DECIMAL ODDS format (1/probability),
      not raw probabilities. Winner = lowest odds price (→1.0 at resolution).
      market.yes.price from fetch_markets() IS raw probability (0–1).
"""

import pmxt
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
from collections import defaultdict
import re
import time
import warnings
warnings.filterwarnings("ignore")

# ── Config ─────────────────────────────────────────────────────────────────────
API_KEY   = "pmxt_30879f8af5731b646bd065fe65964504444f679eca2c2585e5a1f03880b3984a"
OHLCV_RES = "1h"   # resolution for weekly; monthly auto-falls back to 6h/1d
OUTPUT    = "/Users/nick/Desktop/intern/POLYMARKET/elon_tweet_qr.png"

# Convert decimal-odds OHLCV close → probability (clip to sensible range)
def odds_to_prob(x):
    return np.clip(1.0 / np.clip(x, 1e-6, 1e6), 0.0, 1.0)

# ── 1. Fetch markets ───────────────────────────────────────────────────────────
print("=== Fetching markets ===")
api = pmxt.Polymarket(pmxt_api_key=API_KEY)
raw = api.fetch_markets(query="elon musk", limit=200)
tweet_markets = [m for m in raw if "tweet" in m.slug.lower()]
print(f"  {len(tweet_markets)} tweet market outcomes")

# ── 2. Parse bucket lo/hi from slug ──────────────────────────────────────────
def parse_bucket(slug):
    m = re.search(r'-(\d+)-(\d+)$', slug)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r'-(\d+)plus$', slug)
    if m:
        return int(m.group(1)), 9999
    return None, None

# ── 3. Group by event ─────────────────────────────────────────────────────────
events = defaultdict(list)
for m in tweet_markets:
    events[m.event_id].append(m)

event_meta = {}
for eid, ms in events.items():
    m0 = ms[0]
    is_weekly = bool(re.search(
        r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-\d{1,2}-'
        r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
        m0.slug, re.I))
    event_meta[eid] = {
        "title": m0.title.split(" - ")[0],
        "resolution_date": m0.resolution_date,
        "status": m0.status,
        "type": "weekly" if is_weekly else "monthly",
        "n_outcomes": len(ms),
    }
    print(f"  Event [{event_meta[eid]['type']}]: {event_meta[eid]['title']}"
          f"  → {m0.resolution_date}  ({len(ms)} outcomes)")

# ── 4. Current live price snapshot from fetch_markets ─────────────────────────
# market.yes.price is raw probability (0–1)
live_rows = []
for m in tweet_markets:
    lo, hi = parse_bucket(m.slug)
    if lo is None:
        continue
    live_rows.append({
        "event_id":   m.event_id,
        "bucket_lo":  lo,
        "bucket_hi":  hi,
        "bucket_mid": (lo + min(hi, 9999)) / 2,
        "live_prob":  getattr(m.yes, "price", np.nan) or 0,
        "live_vol":   getattr(m, "volume_24h", 0) or 0,
    })
live_df = pd.DataFrame(live_rows)

# ── 5. Fetch OHLCV history for all outcomes ───────────────────────────────────
print("\n=== Fetching OHLCV ===")
all_candles = []

for eid, ms in events.items():
    meta = event_meta[eid]
    res_for_type = "6h" if meta["type"] == "monthly" else OHLCV_RES
    print(f"\n  {meta['title']}  ({len(ms)} outcomes)...")

    for market in ms:
        if market.yes is None:
            continue
        lo, hi = parse_bucket(market.slug)
        if lo is None:
            continue

        candles = None
        for try_res in [res_for_type, "6h", "1d"]:
            try:
                candles = api.fetch_ohlcv(market.yes, resolution=try_res, limit=500)
                if candles:
                    break
            except Exception:
                pass
            time.sleep(0.1)

        if not candles:
            continue

        for c in candles:
            prob = odds_to_prob(c.close)
            all_candles.append({
                "event_id":        eid,
                "event_title":     meta["title"],
                "event_type":      meta["type"],
                "resolution_date": meta["resolution_date"],
                "bucket_lo":       lo,
                "bucket_hi":       hi,
                "bucket_mid":      (lo + min(hi, 9999)) / 2,
                "ts":              pd.Timestamp(c.timestamp, unit="ms", tz="UTC"),
                "prob":            prob,
                "vol":             c.volume or 0,
            })

        last_prob = odds_to_prob(candles[-1].close)
        print(f"    [{lo}-{hi}]  {len(candles)} candles  last_prob={last_prob:.3f}")

ohlcv = pd.DataFrame(all_candles)
print(f"\nTotal OHLCV rows: {len(ohlcv)}")

# ── 6. Per-event analysis ──────────────────────────────────────────────────────
print("\n=== Analysis per event ===")

# Latest OHLCV prob per (event, bucket)
ohlcv_latest = (
    ohlcv.sort_values("ts")
         .groupby(["event_id", "event_title", "bucket_lo", "bucket_hi", "bucket_mid"])
         .last()[["prob", "vol"]]
         .reset_index()
)
# First OHLCV prob (opening price)
ohlcv_first = (
    ohlcv.sort_values("ts")
         .groupby(["event_id", "bucket_lo"])
         .first()[["prob"]]
         .rename(columns={"prob": "open_prob"})
         .reset_index()
)
snapshot = ohlcv_latest.merge(ohlcv_first, on=["event_id", "bucket_lo"])
snapshot["prob_change"] = snapshot["prob"] - snapshot["open_prob"]

# Merge with live prices
snapshot = snapshot.merge(
    live_df[["event_id", "bucket_lo", "live_prob"]],
    on=["event_id", "bucket_lo"], how="left"
)

# ── 7. Print implied distributions & identify alpha ──────────────────────────
print("\n=== Implied distributions (live market prices) ===")
for eid in events:
    sub = live_df[live_df["event_id"] == eid].sort_values("bucket_lo")
    if sub.empty:
        continue
    meta = event_meta[eid]
    total = sub["live_prob"].sum()
    winner_row = sub.loc[sub["live_prob"].idxmax()]
    print(f"\n{meta['title']} [{meta['type']}]  (sum={total:.3f})")
    for _, r in sub.iterrows():
        bar = "█" * int(r["live_prob"] * 60)
        hi_s = str(int(r.bucket_hi)) if r.bucket_hi < 9999 else "∞"
        marker = " ◄ LEADING" if r.bucket_lo == winner_row.bucket_lo else ""
        print(f"  {int(r.bucket_lo):5d}-{hi_s:<6}  {r.live_prob:.4f}  {bar}{marker}")

# ── 8. Alpha signal: opening bias ─────────────────────────────────────────────
print("\n=== Opening vs. current probability (top movers) ===")
movers = snapshot.sort_values("prob_change", key=abs, ascending=False).head(15)
for _, r in movers.iterrows():
    hi_s = str(int(r.bucket_hi)) if r.bucket_hi < 9999 else "∞"
    arrow = "▲" if r.prob_change > 0 else "▼"
    print(f"  {r.event_title[:28]:<28} [{int(r.bucket_lo)}-{hi_s:<5}]  "
          f"open={r.open_prob:.3f} → ohlcv_last={r.prob:.3f}  {arrow}{abs(r.prob_change):.3f}")

# ── 9. Implied tweet count stats ──────────────────────────────────────────────
print("\n=== Implied tweet count (mean ± σ) from live prices ===")
for eid in events:
    sub = live_df[live_df["event_id"] == eid].sort_values("bucket_lo")
    if sub.empty:
        continue
    probs = sub["live_prob"].values
    total = probs.sum()
    if total < 0.05:
        continue
    p_norm = probs / total
    mids = sub["bucket_mid"].values
    mu = (mids * p_norm).sum()
    sigma = np.sqrt(((mids - mu)**2 * p_norm).sum())
    meta = event_meta[eid]
    print(f"  {meta['title']}:  μ={mu:.0f} tweets  σ={sigma:.0f}  "
          f"(market sums to {total:.3f})")

# ── 10. Entropy decay (market certainty over time) ────────────────────────────
print("\n=== Market entropy per event ===")
if not ohlcv.empty:
    def entropy_at_ts(grp):
        p = grp["prob"].clip(1e-9)
        p = p / p.sum()
        return -(p * np.log(p)).sum()

    entropy_ts = (
        ohlcv.groupby(["event_id", "event_title", "ts"])
             .apply(entropy_at_ts)
             .reset_index(name="entropy")
    )
    for eid in entropy_ts["event_id"].unique():
        sub = entropy_ts[entropy_ts["event_id"] == eid].sort_values("ts")
        if len(sub) < 2:
            continue
        delta = sub["entropy"].iloc[-1] - sub["entropy"].iloc[0]
        direction = "▼ (converging)" if delta < 0 else "▲ (diverging)"
        print(f"  {event_meta[eid]['title']}: start={sub['entropy'].iloc[0]:.2f}  "
              f"end={sub['entropy'].iloc[-1]:.2f}  Δ={delta:+.2f} {direction}")

# ── 11. Plots ──────────────────────────────────────────────────────────────────
print("\n=== Generating plots ===")
sns.set_theme(style="darkgrid")
fig = plt.figure(figsize=(22, 26))
gs = gridspec.GridSpec(4, 2, figure=fig, hspace=0.5, wspace=0.35)

ordered_eids = sorted(events.keys(),
                      key=lambda e: event_meta[e]["resolution_date"] or pd.Timestamp.max.tz_localize("UTC"))
# Priority: show weekly short-term first, then monthly
weekly_eids  = [e for e in ordered_eids if event_meta[e]["type"] == "weekly"]
monthly_eids = [e for e in ordered_eids if event_meta[e]["type"] == "monthly"]
plot_eids    = (weekly_eids + monthly_eids)[:4]

COLORS = sns.color_palette("tab10", 12)

# Row 0: Live probability distributions
for col, eid in enumerate(plot_eids[:2]):
    ax = fig.add_subplot(gs[0, col])
    sub = live_df[live_df["event_id"] == eid].sort_values("bucket_lo")
    meta = event_meta[eid]
    total = sub["live_prob"].sum()
    mids  = sub["bucket_mid"].values
    probs = sub["live_prob"].values / max(total, 1e-9)
    colors = ["#e74c3c" if p == max(probs) else "#3498db" for p in probs]
    ax.bar(mids, probs, width=(mids[1]-mids[0]) * 0.85 if len(mids) > 1 else 15,
           color=colors, edgecolor="white", linewidth=0.4)
    mu = (mids * probs).sum()
    ax.axvline(mu, color="orange", linewidth=1.5, linestyle="--", label=f"μ={mu:.0f}")
    ax.set_title(f"{meta['title']}\nLive implied PDF  (resolves {str(meta['resolution_date'])[:10]})",
                 fontsize=9, fontweight="bold")
    ax.set_xlabel("Tweets (bucket midpoint)")
    ax.set_ylabel("Probability")
    ax.legend(fontsize=8)

# Row 1: Price trajectory (prob over time) for top buckets per event
for col, eid in enumerate(plot_eids[:2]):
    ax = fig.add_subplot(gs[1, col])
    meta = event_meta[eid]
    if ohlcv.empty:
        ax.text(0.5, 0.5, "No OHLCV data", ha="center", va="center",
                transform=ax.transAxes)
        continue
    sub_ohlcv = ohlcv[ohlcv["event_id"] == eid]
    top_los = (
        sub_ohlcv.groupby("bucket_lo")["prob"].max()
                 .sort_values(ascending=False)
                 .head(5)
                 .index.tolist()
    )
    for i, lo_val in enumerate(top_los):
        grp = sub_ohlcv[sub_ohlcv["bucket_lo"] == lo_val].sort_values("ts")
        hi_val = grp["bucket_hi"].iloc[0]
        hi_s = str(int(hi_val)) if hi_val < 9999 else "∞"
        ax.plot(grp["ts"], grp["prob"], label=f"{int(lo_val)}-{hi_s}",
                color=COLORS[i], linewidth=1.5)
    ax.set_title(f"{meta['title']}\nProb. trajectory (top 5 buckets)", fontsize=9, fontweight="bold")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Probability")
    ax.legend(fontsize=7, ncol=2)
    ax.tick_params(axis="x", rotation=25)

# Row 2 left: Opening prob vs current prob scatter
ax_scat = fig.add_subplot(gs[2, 0])
type_colors = {"weekly": "#2980b9", "monthly": "#c0392b"}
for _, row in snapshot.iterrows():
    etype = event_meta[row["event_id"]]["type"]
    ax_scat.scatter(row["open_prob"], row["prob"], s=25,
                    color=type_colors.get(etype, "gray"), alpha=0.55)
lo_lim = min(snapshot["open_prob"].min(), snapshot["prob"].min()) * 0.9
hi_lim = max(snapshot["open_prob"].max(), snapshot["prob"].max()) * 1.1
ax_scat.plot([lo_lim, hi_lim], [lo_lim, hi_lim], "k--", lw=0.8)
ax_scat.set_title("Opening prob vs. OHLCV-last prob\n(each dot = one bucket/event)", fontweight="bold")
ax_scat.set_xlabel("Opening probability")
ax_scat.set_ylabel("Current OHLCV probability")
from matplotlib.patches import Patch
ax_scat.legend(handles=[Patch(color=c, label=t) for t, c in type_colors.items()], fontsize=8)

# Row 2 right: Volume by bucket (all events)
ax_vol = fig.add_subplot(gs[2, 1])
vol_agg = ohlcv.groupby("bucket_lo")["vol"].sum().reset_index()
ax_vol.bar(vol_agg["bucket_lo"], vol_agg["vol"],
           width=15, color="#8e44ad", edgecolor="white", linewidth=0.3)
ax_vol.set_title("Cumulative OHLCV volume by bucket\n(all events)", fontweight="bold")
ax_vol.set_xlabel("Bucket low (tweet count)")
ax_vol.set_ylabel("Volume")

# Row 3 left: Entropy over time
ax_ent = fig.add_subplot(gs[3, 0])
if not ohlcv.empty:
    for i, eid in enumerate(plot_eids[:4]):
        sub_e = entropy_ts[entropy_ts["event_id"] == eid].sort_values("ts") if 'entropy_ts' in dir() else pd.DataFrame()
        if len(sub_e) > 1:
            label = event_meta[eid]["title"].replace("Elon Musk", "EM").replace("# tweets", "#tw")[:30]
            ax_ent.plot(sub_e["ts"], sub_e["entropy"], label=label,
                        color=COLORS[i], linewidth=1.5)
ax_ent.set_title("Shannon entropy over time\n(↓ = market converging to answer)", fontweight="bold")
ax_ent.set_xlabel("Time (UTC)")
ax_ent.set_ylabel("Entropy (nats)")
ax_ent.legend(fontsize=7)
ax_ent.tick_params(axis="x", rotation=25)

# Row 3 right: Implied μ per event (bar chart)
ax_mu = fig.add_subplot(gs[3, 1])
mu_rows = []
for eid in events:
    sub = live_df[live_df["event_id"] == eid].sort_values("bucket_lo")
    probs = sub["live_prob"].values
    total = probs.sum()
    if total < 0.05:
        continue
    p_n = probs / total
    mu = (sub["bucket_mid"].values * p_n).sum()
    sigma = np.sqrt(((sub["bucket_mid"].values - mu)**2 * p_n).sum())
    mu_rows.append({
        "title": event_meta[eid]["title"].replace("Elon Musk ", "").replace("musk ", ""),
        "mu": mu, "sigma": sigma,
        "type": event_meta[eid]["type"],
        "res": str(event_meta[eid]["resolution_date"])[:10],
    })
if mu_rows:
    mu_df = pd.DataFrame(mu_rows).sort_values("mu")
    bar_colors = [type_colors.get(t, "gray") for t in mu_df["type"]]
    bars = ax_mu.barh(mu_df["title"] + "\n" + mu_df["res"], mu_df["mu"],
                      xerr=mu_df["sigma"], color=bar_colors, capsize=4, height=0.6)
    ax_mu.set_title("Implied mean tweet count ± 1σ\n(from live market prices)", fontweight="bold")
    ax_mu.set_xlabel("Tweets")
    ax_mu.legend(handles=[Patch(color=c, label=t) for t, c in type_colors.items()], fontsize=8)

plt.suptitle("Elon Musk Tweet Count Markets — QR Dashboard\nPMXT / Polymarket  |  " +
             pd.Timestamp.now().strftime("%Y-%m-%d %H:%M UTC"),
             fontsize=13, fontweight="bold", y=1.0)

plt.savefig(OUTPUT, dpi=150, bbox_inches="tight", facecolor="white")
print(f"\nPlot saved: {OUTPUT}")

# ── 12. Alpha summary ─────────────────────────────────────────────────────────
print("\n" + "="*65)
print("ALPHA SIGNALS SUMMARY")
print("="*65)

print("\n[ SUM-OF-PROBABILITIES CHECK ]")
print("  If sum > 1.0 → market is overpriced in aggregate (sellers have edge)")
print("  If sum < 1.0 → market is underpriced (buyers have edge)\n")
for eid in events:
    sub = live_df[live_df["event_id"] == eid]
    total = sub["live_prob"].sum()
    edge = "OVER" if total > 1.05 else ("UNDER" if total < 0.95 else "FAIR")
    print(f"  {event_meta[eid]['title']}: sum={total:.3f}  [{edge}]")

print("\n[ LARGEST OPENING MISPRICING (OHLCV open → OHLCV last) ]")
snap_sorted = snapshot.sort_values("prob_change", key=abs, ascending=False).head(8)
for _, r in snap_sorted.iterrows():
    hi_s = str(int(r.bucket_hi)) if r.bucket_hi < 9999 else "∞"
    arrow = "▲" if r.prob_change > 0 else "▼"
    print(f"  {r.event_title[:26]:<26} [{int(r.bucket_lo)}-{hi_s:<5}]  "
          f"open={r.open_prob:.3f} → {r.prob:.3f}  {arrow}{abs(r.prob_change):.3f}")

print("\n[ CURRENT LEADING BUCKETS — LIVE PRICES ]")
for eid in ordered_eids:
    sub = live_df[live_df["event_id"] == eid].sort_values("live_prob", ascending=False)
    if sub.empty or sub["live_prob"].max() < 0.01:
        continue
    top3 = sub.head(3)
    meta = event_meta[eid]
    print(f"\n  {meta['title']} [{meta['type']}]  (res: {str(meta['resolution_date'])[:10]})")
    for _, r in top3.iterrows():
        hi_s = str(int(r.bucket_hi)) if r.bucket_hi < 9999 else "∞"
        bar = "█" * int(r.live_prob * 50)
        print(f"    [{int(r.bucket_lo):4d}-{hi_s:<6}]  {r.live_prob:.4f}  {bar}")

print("\nDone.")
