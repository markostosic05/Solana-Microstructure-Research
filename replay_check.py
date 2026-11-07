#!/usr/bin/env python3
"""
Replay check for the graduation paper trader: did we MEASURE the candle rules wrongly live, or did
the market change since the backtest?

For pools the paper trader already decided (rules v1.1), it waits until the pool is 30+ minutes old,
downloads its COMPLETE first-20-minute 1-minute candles, and recomputes the three candle rules
(vol_first2, ret_to_decision, busiest_share) exactly like the trader does (same shared function).

  live fail rate  ~ replay fail rate  -> measurement is fine; the coins themselves differ from the
                                         backtest period (market changed / regime)
  replay ~ backtest, live much higher -> live candles were incomplete at minute 12 (measurement bug)

Liquidity and buy/sell come from the early snapshot and can't be replayed - they're reused as logged.

Run from the dexbank folder (next to paper.db and rules_v1_1.json):
    python replay_check.py                      # 200 pools, 2 calls/min (safe next to the paper trader)
    python replay_check.py --calls-per-min 8    # faster - only when the paper trader is NOT running
    python replay_check.py --report             # just print the comparison
Resumable: results go to replay.db; Ctrl+C any time and rerun.
"""

import argparse
import hashlib
import json
import logging
import random
import sqlite3
import time
from pathlib import Path

import numpy as np

from paper_trader import Api, candle_features

log = logging.getLogger("paper")
RULES = ("vol_first2", "ret_to_decision", "busiest_share")


def fails_of(f, e):
    out = set()
    if f["vol_first2"] is None or not (e["vol_first2_usd"][0] <= f["vol_first2"] <= e["vol_first2_usd"][1]):
        out.add("vol_first2")
    if f["ret_to_decision"] is None or not (e["ret_to_decision"][0] <= f["ret_to_decision"] <= e["ret_to_decision"][1]):
        out.add("ret_to_decision")
    if f["busiest_share"] is None or f["busiest_share"] >= e["busiest_minute_share_max"]:
        out.add("busiest_share")
    return out


def init(path):
    db = sqlite3.connect(path)
    db.execute("""CREATE TABLE IF NOT EXISTS replay (
        pool TEXT PRIMARY KEY, ts REAL, n_rows INTEGER, vol_first2 REAL, ret_to_decision REAL,
        busiest_share REAL, n_candles INTEGER, active_recent INTEGER, error TEXT)""")
    db.commit()
    return db


def run(args, rules, h):
    src = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True, timeout=30)
    out = init(args.out)
    done = {r[0] for r in out.execute("SELECT pool FROM replay")}
    now = time.time()
    rows = src.execute("""SELECT d.pool, p.created_ts FROM decisions d JOIN pools p ON p.pool = d.pool
                          WHERE d.rules_hash=? AND p.created_ts < ?
                            AND d.fail_reasons NOT LIKE '%candles_skipped%'""", (h, now - 30 * 60)).fetchall()
    todo = [r for r in rows if r[0] not in done]
    random.Random(7).shuffle(todo)                      # random sample, same order every run
    todo = todo[:max(0, args.n - len(done))]
    log.info("%d decided pools old enough, %d already replayed, %d to fetch (~%.0f min at %.0f calls/min)",
             len(rows), len(done), len(todo), len(todo) / args.calls_per_min, args.calls_per_min)
    api = Api(args.calls_per_min)
    T = rules["decision_minute"]
    for i, (pool, created) in enumerate(todo, 1):
        g0 = created - created % 60
        data = api.get(f"/networks/solana/pools/{pool}/ohlcv/minute",
                       {"aggregate": 1, "limit": 30, "currency": "usd", "token": "base",
                        "before_timestamp": int(g0 + 21 * 60)})
        if data is None:
            out.execute("INSERT OR REPLACE INTO replay (pool, ts, error) VALUES (?,?,?)", (pool, time.time(), "no data"))
        else:
            lst = ((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
            f = candle_features(lst, created, T)
            out.execute("INSERT OR REPLACE INTO replay VALUES (?,?,?,?,?,?,?,?,NULL)",
                        (pool, time.time(), len(lst), f["vol_first2"], f["ret_to_decision"], f["busiest_share"],
                         f["n_candles"], int(f["active_recent"])))
        out.commit()
        if i % 10 == 0 or i == len(todo):
            log.info("replayed %d / %d", i, len(todo))
    src.close()


def report(args, rules, h):
    out = init(args.out)
    out.execute("ATTACH DATABASE ? AS live", (str(args.db),))
    rows = out.execute("""SELECT d.pool, d.fail_reasons, d.vol_first2, d.ret_to_decision, d.busiest_share, d.n_candles,
                                 p.early_liq, r.vol_first2, r.ret_to_decision, r.busiest_share, r.n_candles
                          FROM replay r JOIN live.decisions d ON d.pool = r.pool JOIN live.pools p ON p.pool = r.pool
                          WHERE d.rules_hash=? AND r.error IS NULL""", (h,)).fetchall()
    errs = out.execute("SELECT COUNT(*) FROM replay WHERE error IS NOT NULL").fetchone()[0]
    print(f"\nReplay check - rules {rules['version']} (hash {h}): {len(rows)} pools replayed ({errs} without data)")
    if not rows:
        print("Nothing replayed yet - run without --report first.")
        return
    e, bt = rules["entry"], rules.get("backtest_fail_rates", {})
    real = [r for r in rows if (r[6] or 0) >= 5000]
    junk = [r for r in rows if (r[6] or 0) < 5000]
    for label, grp in (("ALL replayed pools", rows), ("REAL pools (early liquidity >= $5k)", real),
                       ("JUNK pools (early liquidity < $5k)", junk)):
        if not grp:
            continue
        print(f"\n--- {label}: n={len(grp)} ---")
        print(f"  {'rule':<16} {'live fail':>9} {'replay fail':>11} {'backtest':>9}   live->replay flips")
        L = [set(x for x in (r[1] or "").split(",") if x) for r in grp]
        R = [fails_of(dict(vol_first2=r[7], ret_to_decision=r[8], busiest_share=r[9]), e) for r in grp]
        for k in RULES:
            lf = np.mean([k in s for s in L])
            rf = np.mean([k in s for s in R])
            to_pass = sum(k in a and k not in b for a, b in zip(L, R))
            to_fail = sum(k not in a and k in b for a, b in zip(L, R))
            print(f"  {k:<16} {lf:>9.0%} {rf:>11.0%} {bt.get(k, float('nan')):>9.0%}   "
                  f"fail->pass {to_pass:>3}, pass->fail {to_fail:>3}")
        lc = np.median([r[5] or 0 for r in grp])
        rc = np.median([r[10] or 0 for r in grp])
        print(f"  minutes with trades (0-10): live median {lc:.0f}, replay median {rc:.0f}"
              + ("   <- live saw FEWER candles: data wasn't complete at decision time" if rc - lc >= 1 else ""))
        for k, i, j in (("vol_first2", 2, 7), ("ret_to_decision", 3, 8), ("busiest_share", 4, 9)):
            d = [(r[j] - r[i]) for r in grp if r[i] is not None and r[j] is not None]
            if d:
                print(f"  {k:<16} replay minus live: median {np.median(d):+.3g}, "
                      f"same within 1%: {np.mean([abs(x) <= 0.01 * max(abs(r[i]), 1e-9) for x, r in zip(d, [r for r in grp if r[i] is not None and r[j] is not None])]):.0%}")
        # how many would have passed ALL rules with replayed candles + the live liquidity / buy-sell result
        other = [set(x for x in (r[1] or "").split(",") if x) - set(RULES) for r in grp]
        n_pass = sum(not (a | b) for a, b in zip(R, other))
        n_live = sum(not s for s in L)
        print(f"  would pass ALL rules: live {n_live}, with replayed candles {n_pass}")
    print("\nHOW TO READ: compare 'live fail' with 'replay fail' on REAL pools.")
    print("  close (within ~5 points)   -> the live measurement is right; the coins differ from the backtest period.")
    print("  replay much lower than live -> live candles were incomplete at minute 12 -> fix the measurement.")
    print("  replay ~ live but both >> backtest -> market changed (or the backtest sample was unusual).\n")


def main():
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", default=str(here / "rules_v1_1.json"))
    ap.add_argument("--db", default=str(here / "paper.db"))
    ap.add_argument("--out", default=str(here / "replay.db"))
    ap.add_argument("--n", type=int, default=200, help="how many pools to replay in total")
    ap.add_argument("--calls-per-min", type=float, default=2)
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    rules = json.loads(Path(args.rules).read_text())
    h = hashlib.sha1(json.dumps(rules, sort_keys=True).encode()).hexdigest()[:10]
    if not args.report:
        try:
            run(args, rules, h)
        except KeyboardInterrupt:
            print("\nstopped - rerun to continue")
    report(args, rules, h)


if __name__ == "__main__":
    main()
