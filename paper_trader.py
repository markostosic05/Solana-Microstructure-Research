#!/usr/bin/env python3
"""
Live PAPER trader for pump.fun graduations (no money, no wallet - it only records decisions).

What it does, live:
  1. Watches GeckoTerminal for new PumpSwap pools (= pump.fun graduations).
  2. At minute 10 after each graduation it checks the FROZEN rules in rules_v1.json and logs the
     decision for EVERY pool (pass or fail, with all feature values) - no survivorship.
  3. If the rules pass it opens a paper position at the live price, then polls every minute and
     closes it under 5 exit variants side by side (same entry, different exits):
        time        sell at max hold (30 min)
        trail       sell when price is 30% below its best since entry
        tp50/tp100  fixed 2:1 risk:reward - take profit at +50% / +100%, stop at -25% / -50%
        activity    sell when trades in the last 5 min fall under 25% of their peak (or to zero)
        liquidity   sell when liquidity drops 20% more than the price move explains, or < $1,000
        combined    whichever of trail / activity / liquidity / time fires first
  4. Checks price + liquidity of EVERY graduation at minutes 10, 20, 40, 60, 120, 360, 1440
     (when do pools get drained?).
Results are priced at several sizes with direct-PumpSwap fees and with Axiom fees.

If the PC was off/asleep while a position was open, that position is marked 'interrupted' and
left out of the results (honest). Ctrl+C any time; rerun to continue.

Holder logging: if helius_key.txt is next to this script, every decision also gets a Helius holder
snapshot (biggest wallet vs pool, top-10 share, dev) - stored in the 'holders' table, logged only.

Run (from the dexbank folder, next to rules_v1_1.json):
    python paper_trader.py              # run live
    python paper_trader.py --report     # results so far (works while it runs, in a 2nd terminal)
Database: paper.db next to this script.
"""

import argparse
import hashlib
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import requests

GT = "https://api.geckoterminal.com/api/v2"
VARIANTS = ["time", "trail", "activity", "liquidity", "combined", "tp50", "tp100"]
# fixed risk:reward exits (2:1), all with the same 30-min max hold. (take-profit, stop-loss)
TP_SL = {"tp50": (0.50, 0.25), "tp100": (1.00, 0.50)}
CHECKPOINTS = [10, 20, 40, 60, 120, 360, 1440]
# pump.fun's published PumpSwap fees (total per side) by market cap in SOL. Between 3,440 and 98,000 SOL
# they "scale down gradually" - interpolated here. Source: pump.fun help centre, fees article (Nov 2026).
PUMPSWAP_FEE_TIERS = [(0, 0.0125), (420, 0.0120), (1470, 0.0115), (2460, 0.0110), (3440, 0.0110), (98000, 0.0030)]


def pumpswap_fee(mcap_sol):
    """Fee per side on PumpSwap for a coin of this market cap (SOL). Unknown -> the small-coin fee."""
    if not mcap_sol or mcap_sol <= 0:
        return PUMPSWAP_FEE_TIERS[0][1]
    if mcap_sol >= 98000:
        return 0.0030
    if mcap_sol >= 3440:                                  # log-interpolate the undocumented middle
        import math
        t = (math.log(mcap_sol) - math.log(3440)) / (math.log(98000) - math.log(3440))
        return 0.0110 + t * (0.0030 - 0.0110)
    fee = PUMPSWAP_FEE_TIERS[0][1]
    for lo, f in PUMPSWAP_FEE_TIERS:
        if mcap_sol >= lo:
            fee = f
    return fee


REAL_FEE_GRAD = 0.0125        # fresh graduations are always in the smallest tier
DS_LINK = "https://dexscreener.com/solana/"
log = logging.getLogger("paper")


# --------------------------------------------------------------------------- #
class Api:
    def __init__(self, per_min=15):
        self.s = requests.Session()
        self.s.headers.update({"Accept": "application/json;version=20230302", "User-Agent": "paper-trader/0.1"})
        self.gap, self.next_ok, self.calls, self.r429 = 60.0 / per_min, 0.0, 0, 0
        self.base_gap, self.ok_streak = self.gap, 0

    def get(self, path, params=None):
        for attempt in range(3):
            now = time.monotonic()
            if self.next_ok > now:
                time.sleep(self.next_ok - now)
            self.next_ok = max(now, self.next_ok) + self.gap
            self.calls += 1
            try:
                r = self.s.get(GT + path, params=params, timeout=20)
            except requests.RequestException as e:
                log.warning("network error: %s", e)
                time.sleep(3 * (attempt + 1))
                continue
            if r.status_code == 429:
                self.r429 += 1
                self.ok_streak = 0
                self.gap = min(self.gap * 1.25, 20.0)          # back off: fewer calls per minute
                log.warning("429 -> waiting 20s, now %.1f calls/min", 60 / self.gap)
                time.sleep(20)
                continue
            self.ok_streak += 1
            if self.ok_streak >= 10 and self.gap > self.base_gap:
                self.gap = max(self.base_gap, self.gap * 0.9)   # recover (3 -> 8 calls/min in ~100 calls)
                self.ok_streak = 0
            if r.status_code != 200:
                log.warning("HTTP %s on %s", r.status_code, path)
                return None
            try:
                return r.json()
            except ValueError:
                return None
        return None


def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def iso_ts(s):
    from datetime import datetime
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


# --------------------------------------------------------------------------- #
def init_db(path):
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
    CREATE TABLE IF NOT EXISTS pools (
        pool TEXT PRIMARY KEY, mint TEXT, name TEXT, created_ts REAL, seen_ts REAL,
        status TEXT,                 -- pending | decided | missed
        next_cp INTEGER DEFAULT 0);  -- index into CHECKPOINTS
    CREATE TABLE IF NOT EXISTS decisions (
        pool TEXT PRIMARY KEY, ts REAL, minute REAL, rules_hash TEXT, passed INTEGER, fail_reasons TEXT,
        vol_first2 REAL, ret_to_decision REAL, liquidity REAL, busiest_share REAL, buy_sell_ratio REAL,
        price REAL, fdv REAL, n_candles INTEGER);
    CREATE TABLE IF NOT EXISTS positions (
        pool TEXT, variant TEXT, entry_ts REAL, entry_price REAL, entry_liq REAL,
        peak_price REAL, peak_tx5 REAL, last_poll_ts REAL,
        exit_ts REAL, exit_price REAL, exit_liq REAL, exit_reason TEXT,
        status TEXT,                 -- open | closed | interrupted
        PRIMARY KEY (pool, variant));
    CREATE TABLE IF NOT EXISTS checkpoints (
        pool TEXT, target_min INTEGER, ts REAL, price REAL, liquidity REAL, tx5 INTEGER, vol5 REAL,
        PRIMARY KEY (pool, target_min));
    CREATE TABLE IF NOT EXISTS runs (start_ts REAL, rules_hash TEXT);
    """)
    for col in ("early_ts REAL", "early_liq REAL", "early_buys5 INTEGER", "early_sells5 INTEGER"):
        try:
            db.execute(f"ALTER TABLE pools ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass                                   # column already there
    db.commit()
    return db


def attrs_of(item):
    a = item.get("attributes") or {}
    tx = (a.get("transactions") or {}).get("m5") or {}
    return dict(price=fnum(a.get("base_token_price_usd")), liq=fnum(a.get("reserve_in_usd")),
                fdv=fnum(a.get("fdv_usd")), buys5=tx.get("buys") or 0, sells5=tx.get("sells") or 0,
                vol5=fnum((a.get("volume_usd") or {}).get("m5")))


def fetch_multi(api, pools):
    out = {}
    for i in range(0, len(pools), 30):
        batch = pools[i:i + 30]
        data = api.get(f"/networks/solana/pools/multi/{','.join(batch)}")
        for it in (data or {}).get("data") or []:
            addr = (it.get("attributes") or {}).get("address")
            if addr:
                out[addr] = attrs_of(it)
    return out


def candle_features(lst, created, T):
    """Candle-based entry features from GeckoTerminal 1-minute OHLCV rows (shared with replay_check.py)."""
    g0 = created - created % 60
    c = np.full(T + 1, np.nan)
    v = np.zeros(T + 1)
    for row in lst:
        try:
            m = int((int(row[0]) - g0) // 60)
            if 0 <= m <= T:
                c[m] = float(row[4])
                v[m] += float(row[5])
        except (TypeError, ValueError, IndexError):
            pass
    # same ghost filter as the backtest: a >3x jump needs 2 of the next 5 trades to confirm
    idx = np.flatnonzero((v > 0) & np.isfinite(c) & (c > 0))
    ok, last = np.zeros(T + 1, bool), None
    for j, i in enumerate(idx):
        if last is None or c[i] <= 3 * last or (c[idx[j + 1:j + 6]] >= c[i] / 2).sum() >= 2:
            ok[i], last = True, c[i]
    p = np.where(ok, c, np.nan)
    first = p[np.isfinite(p)]
    pT = next((p[k] for k in range(T, -1, -1) if np.isfinite(p[k])), np.nan)
    vol_T = v.sum()
    return dict(
        vol_first2=float(v[:2].sum()),
        ret_to_decision=float(pT / first[0] - 1) if len(first) and np.isfinite(pT) else None,
        busiest_share=float(v.max() / vol_T) if vol_T > 0 else None,
        n_candles=int((v > 0).sum()),
        active_recent=bool((v[max(T - 2, 0):] > 0).any()))


# --------------------------------------------------------------------------- #
class Trader:
    def __init__(self, db, api, rules, clock=time.time):
        self.db, self.api, self.r, self.clock = db, api, rules, clock
        self.hash = hashlib.sha1(json.dumps(rules, sort_keys=True).encode()).hexdigest()[:10]
        self.verbose = True        # print every graduation, decision and paper trade with a link
        self.seen = set()          # every pool address seen in the feed (any DEX) -> stop paging early
        self.last_try = {}         # pool -> last decision attempt (don't hammer the API)
        self.helius = None         # optional holder logging (helius.py) - logged only, never a filter

    # ---------- 1. discovery ----------
    def discover(self, max_pages=8):
        u, new = self.r["universe"], 0
        for page in range(1, max_pages + 1):
            data = self.api.get(f"/networks/{u['network']}/new_pools", {"page": page, "include": "base_token,dex"})
            items = (data or {}).get("data") or []
            if not items:
                break
            known = 0
            for it in items:
                a, rel = it.get("attributes") or {}, it.get("relationships") or {}
                pool = a.get("address")
                dex = ((rel.get("dex") or {}).get("data") or {}).get("id")
                mint = (((rel.get("base_token") or {}).get("data") or {}).get("id") or "").split("_", 1)[-1]
                created = iso_ts(a.get("pool_created_at"))
                if not pool or created is None:
                    continue
                if pool in self.seen:
                    known += 1
                    continue
                self.seen.add(pool)
                if self.db.execute("SELECT 1 FROM pools WHERE pool=?", (pool,)).fetchone():
                    continue
                if dex != u["dex"] or not mint.endswith(u["mint_suffix"]):
                    continue
                self.db.execute("INSERT INTO pools (pool, mint, name, created_ts, seen_ts, status, next_cp) "
                                "VALUES (?,?,?,?,?,'pending',0)",
                                (pool, mint, a.get("name"), created, self.clock()))
                new += 1
                if self.verbose:
                    log.info("NEW GRAD   %-22s graduated %.1f min ago  %s%s", str(a.get("name"))[:22],
                             (self.clock() - created) / 60, DS_LINK, pool)
            self.db.commit()
            # stop paging once we reach pools older than the decision window
            oldest = min((iso_ts((x.get("attributes") or {}).get("pool_created_at")) or 0) for x in items)
            if known or self.clock() - oldest > (self.r["decision_minute"] + 5) * 60:
                break
        if new:
            log.info("discovered %d new graduations", new)

    # ---------- 1b. early snapshot (the backtest measured liquidity + buys/sells at ~minute 2) ----------
    def early_snapshots(self):
        lo, hi = self.r.get("measurement", {}).get("early_snapshot_min", [1.5, 5.0])
        now = self.clock()
        rows = self.db.execute("SELECT pool, created_ts FROM pools WHERE status='pending' AND early_ts IS NULL").fetchall()
        due = [p for p, c in rows if lo * 60 <= now - c <= hi * 60]
        if not due:
            return
        attrs = fetch_multi(self.api, due)
        for pool in due:
            a = attrs.get(pool)
            if a:
                self.db.execute("UPDATE pools SET early_ts=?, early_liq=?, early_buys5=?, early_sells5=? WHERE pool=?",
                                (now, a["liq"], a["buys5"], a["sells5"], pool))
        self.db.commit()

    # ---------- 2. decisions ----------
    def features(self, pool, created, attrs):
        data = self.api.get(f"/networks/solana/pools/{pool}/ohlcv/minute",
                            {"aggregate": 1, "limit": 40, "currency": "usd", "token": "base",
                             "before_timestamp": int(self.clock())})
        lst = (((data or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
        f = candle_features(lst, created, self.r["decision_minute"])
        f.update(liquidity=attrs.get("liq"), buy_sell_ratio=attrs["buys5"] / max(attrs["sells5"], 1),
                 price=attrs.get("price"), fdv=attrs.get("fdv"))
        return f

    def check(self, f):
        e, fails = self.r["entry"], []
        if f["vol_first2"] is None or not (e["vol_first2_usd"][0] <= f["vol_first2"] <= e["vol_first2_usd"][1]):
            fails.append("vol_first2")
        if f["ret_to_decision"] is None or not (e["ret_to_decision"][0] <= f["ret_to_decision"] <= e["ret_to_decision"][1]):
            fails.append("ret_to_decision")
        if f["liquidity"] is None or f["liquidity"] < e["liquidity_usd_min"]:
            fails.append("liquidity")
        if f["busiest_share"] is None or f["busiest_share"] >= e["busiest_minute_share_max"]:
            fails.append("busiest_share")
        if f["buy_sell_ratio"] is None:
            fails.append("no_early_snapshot")
        elif f["buy_sell_ratio"] < e["buy_sell_ratio_5m_min"]:
            fails.append("buy_sell_ratio")
        if not f["active_recent"]:
            fails.append("not_trading")
        if not f["price"]:
            fails.append("no_price")
        return fails

    def decide_due(self):
        now, T, lag = self.clock(), self.r["decision_minute"], self.r["max_decision_lag_min"]
        start = self.r.get("measurement", {}).get("decide_after_minute", T + 1)
        rows = self.db.execute("SELECT pool, created_ts FROM pools WHERE status='pending'").fetchall()
        due, missed = [], []
        for pool, created in sorted(rows, key=lambda r: r[1]):
            g0 = created - created % 60
            if now >= g0 + (start + lag) * 60:
                missed.append(pool)
            elif now >= g0 + start * 60 and now - self.last_try.get(pool, 0) >= 30 and len(due) < 4:
                due.append((pool, created))
                self.last_try[pool] = now
        for pool in missed:
            self.db.execute("UPDATE pools SET status='missed' WHERE pool=?", (pool,))
            self.last_try.pop(pool, None)
        if not due:
            self.db.commit()
            return
        attrs = fetch_multi(self.api, [p for p, _ in due])
        for pool, created in due:
            a = attrs.get(pool)
            if not a:
                continue                      # try again next tick (until it counts as missed)
            meas = self.r.get("measurement", {})
            el, eb, es, mint = self.db.execute("SELECT early_liq, early_buys5, early_sells5, mint FROM pools "
                                               "WHERE pool=?", (pool,)).fetchone()
            e = self.r["entry"]
            early_fails = []
            if meas.get("liquidity_from") == "early" and (el is None or el < e["liquidity_usd_min"]):
                early_fails.append("liquidity")
            if meas.get("buy_sell_from") == "early":
                if eb is None:
                    early_fails.append("no_early_snapshot")
                elif eb / max(es, 1) < e["buy_sell_ratio_5m_min"]:
                    early_fails.append("buy_sell_ratio")
            if early_fails:
                # it fails whatever the candles say -> save the API call (candle rules are not evaluated)
                f = dict(vol_first2=None, ret_to_decision=None, busiest_share=None, n_candles=None,
                         liquidity=el, buy_sell_ratio=(eb / max(es, 1)) if eb is not None else None,
                         price=a.get("price"), fdv=a.get("fdv"))
                fails = early_fails + ["candles_skipped"]
            else:
                f = self.features(pool, created, a)
                if meas.get("liquidity_from") == "early":
                    f["liquidity"] = el
                if meas.get("buy_sell_from") == "early":
                    f["buy_sell_ratio"] = (eb / max(es, 1)) if eb is not None else None
                fails = self.check(f)
            if self.helius and (el or 0) >= 5000:     # skip junk pools; dev lookup for passes + 1 in 4 others
                self.helius.submit("grad", pool, mint, a.get("price"), a.get("liq"), now,
                                   want_dev=(not fails) or int(hashlib.sha1(pool.encode()).hexdigest(), 16) % 4 == 0)
            self.db.execute("INSERT OR REPLACE INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (pool, now, (now - created) / 60, self.hash, int(not fails), ",".join(fails),
                             f["vol_first2"], f["ret_to_decision"], f["liquidity"], f["busiest_share"],
                             f["buy_sell_ratio"], f["price"], f["fdv"], f["n_candles"]))
            self.db.execute("UPDATE pools SET status='decided' WHERE pool=?", (pool,))
            nm = self.db.execute("SELECT name FROM pools WHERE pool=?", (pool,)).fetchone()[0]
            if fails and self.verbose:
                log.info("SKIP       %-22s %s  %s%s", str(nm)[:22], ",".join(fails), DS_LINK, pool)
            if not fails:
                for var in VARIANTS:
                    self.db.execute("INSERT OR IGNORE INTO positions VALUES (?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL,'open')",
                                    (pool, var, now, f["price"], a["liq"], f["price"],
                                     a["buys5"] + a["sells5"], now))
                log.info("PAPER BUY  %-22s price %.3g  liq $%.0f  %s%s", str(nm)[:22], f["price"], a["liq"] or 0,
                         DS_LINK, pool)
        self.db.commit()

    # ---------- 3. open positions ----------
    def poll_positions(self):
        now, x = self.clock(), self.r["exits"]
        rows = self.db.execute("SELECT pool, variant, entry_ts, entry_price, entry_liq, peak_price, peak_tx5, "
                               "last_poll_ts FROM positions WHERE status='open'").fetchall()
        if not rows:
            return
        attrs = fetch_multi(self.api, sorted({r[0] for r in rows}))
        for pool, var, ets, ep, el, peak, peak_tx, last_poll in rows:
            if now - last_poll > 300:         # unchecked for 5+ min (PC off/asleep): can't know what happened
                self.db.execute("UPDATE positions SET status='interrupted', exit_ts=? WHERE pool=? AND variant=?",
                                (now, pool, var))
                continue
            a = attrs.get(pool)
            held = (now - ets) / 60
            if not a or not a["price"]:
                if held >= x["max_hold_min"]:
                    self.close(pool, var, now, None, None, "no_data_at_exit")
                continue
            price, liq, tx5 = a["price"], a["liq"] or 0.0, a["buys5"] + a["sells5"]
            peak, peak_tx = max(peak or price, price), max(peak_tx or 0, tx5)
            reasons = {}
            if held >= x["max_hold_min"]:
                reasons["time"] = "time"
            if price <= peak * (1 - x["trail_frac"]):
                reasons["trail"] = "trail"
            if held >= x["activity_min_hold_min"] and (tx5 == 0 or tx5 <= x["activity_floor_frac"] * peak_tx):
                reasons["activity"] = "activity"
            expected = (el or 0) * np.sqrt(price / ep) if ep else 0
            if liq < x["drained_usd"] or (expected > 0 and liq < (1 - x["liquidity_drop_frac"]) * expected):
                reasons["liquidity"] = "liquidity"
            for name, (tp, sl) in TP_SL.items():
                if price >= ep * (1 + tp):
                    reasons[name] = "take_profit"
                elif price <= ep * (1 - sl):
                    reasons[name] = "stop_loss"
            fire = {"tp50": reasons.get("tp50") or reasons.get("time"),
                    "tp100": reasons.get("tp100") or reasons.get("time"),
                    "time": reasons.get("time"), "trail": reasons.get("trail") or reasons.get("time"),
                    "activity": reasons.get("activity") or reasons.get("time"),
                    "liquidity": reasons.get("liquidity") or reasons.get("time"),
                    "combined": next((reasons[k] for k in ("liquidity", "activity", "trail", "time") if k in reasons), None)}
            if fire[var]:
                self.close(pool, var, now, price, liq, fire[var])
            else:
                self.db.execute("UPDATE positions SET peak_price=?, peak_tx5=?, last_poll_ts=? WHERE pool=? AND variant=?",
                                (peak, peak_tx, now, pool, var))
        self.db.commit()

    def close(self, pool, var, now, price, liq, reason):
        if var == "combined" and self.verbose:
            row = self.db.execute("SELECT p.entry_price, p.entry_ts, p.entry_liq, l.name FROM positions p "
                                  "JOIN pools l ON l.pool=p.pool WHERE p.pool=? AND p.variant=?", (pool, var)).fetchone()
            if row:
                ep, ets, el, nm = row
                c = self.r["costs"]
                size = c["sizes_usd"][min(1, len(c["sizes_usd"]) - 1)]
                net = net_return(size, ep, el, price, liq, c["fee_per_side"]["direct"], c["tx_usd"])
                ch = f"price {(price / ep - 1) * 100:+.1f}%" if price and ep else "price n/a"
                real = f"real ${size:g} trade {net * 100:+.1f}%" if net is not None else "real n/a"
                warn = "  << POOL DRAINED" if liq is not None and liq < self.r["exits"]["drained_usd"] else ""
                log.info("PAPER SELL %-22s %s | %s after %.0f min (exit: %s)%s  %s%s", str(nm)[:22], ch, real,
                         (now - ets) / 60, reason, warn, DS_LINK, pool)
        self.db.execute("UPDATE positions SET status='closed', exit_ts=?, exit_price=?, exit_liq=?, exit_reason=?, "
                        "last_poll_ts=? WHERE pool=? AND variant=?", (now, price, liq, reason, now, pool, var))

    # ---------- 4. checkpoints for every graduation ----------
    def checkpoints_due(self):
        now = self.clock()
        rows = self.db.execute("SELECT pool, created_ts, next_cp FROM pools WHERE next_cp < ?",
                               (len(CHECKPOINTS),)).fetchall()
        due = [(p, c, k) for p, c, k in rows if now >= c + CHECKPOINTS[k] * 60]
        if not due:
            return
        attrs = fetch_multi(self.api, [p for p, _, _ in due][:300])
        for pool, created, k in due[:300]:
            age = (now - created) / 60
            a = attrs.get(pool)
            if a:
                self.db.execute("INSERT OR REPLACE INTO checkpoints VALUES (?,?,?,?,?,?,?)",
                                (pool, CHECKPOINTS[k], now, a["price"], a["liq"], a["buys5"] + a["sells5"], a["vol5"]))
            nxt = k + 1
            while nxt < len(CHECKPOINTS) and age >= CHECKPOINTS[nxt] + 30:
                nxt += 1                      # skip checkpoints we already slept through
            self.db.execute("UPDATE pools SET next_cp=? WHERE pool=?", (nxt, pool))
        self.db.commit()


# --------------------------------------------------------------------------- #
def net_return(size, ep, el, xp, xl, fee, tx):
    """Constant-product round trip: buy `size` USD at entry, sell everything at exit."""
    if not ep or not el or el <= 0:
        return None
    if xp is None or not xl or xl <= 0:
        return -1.0
    r_in, r_out = el / 2, xl / 2
    spend = size * (1 - fee)
    val = spend * r_in / (r_in + spend) * xp / ep
    got = val * r_out / (r_out + val) * (1 - fee)
    return (got - 2 * tx) / size - 1


def report(db, rules, h):
    c = rules["costs"]
    print(f"\nRules {rules.get('version')} (hash {h}) - decisions made under other rule versions are left out.")
    n_pools = db.execute("SELECT COUNT(*) FROM pools").fetchone()[0]
    st = dict(db.execute("SELECT status, COUNT(*) FROM pools GROUP BY 1").fetchall())
    dec = db.execute("SELECT COUNT(*), SUM(passed) FROM decisions WHERE rules_hash=?", (h,)).fetchone()
    print(f"\nGraduations seen: {n_pools} | decided: {st.get('decided', 0)} | missed decision window: "
          f"{st.get('missed', 0)} | waiting: {st.get('pending', 0)}")
    print(f"Rules passed: {dec[1] or 0} of {dec[0]} decisions ({(dec[1] or 0) / max(dec[0], 1):.1%})")
    fails, n_cand = {}, 0
    for (fr,) in db.execute("SELECT fail_reasons FROM decisions WHERE rules_hash=?", (h,)):
        fs = [f for f in (fr or "").split(",") if f]
        n_cand += "candles_skipped" not in fs
        for f in fs:
            fails[f] = fails.get(f, 0) + 1
    if fails:
        print("Most common reasons to skip: " + ", ".join(f"{k} {v}" for k, v in sorted(fails.items(), key=lambda z: -z[1])))
    ref = rules.get("backtest_fail_rates")
    if ref and dec[0]:
        print("Fail rate per rule, live vs backtest (big gaps = measured differently or the market changed):")
        if fails.get("candles_skipped"):
            print(f"  (candle rules are only checked when liquidity and buy/sell pass: {n_cand} of {dec[0]} pools)")
        for k, v in ref.items():
            if k == "all_pass":
                print(f"  {'PASS ALL':<16} live {(dec[1] or 0) / dec[0]:5.1%}   backtest {v:5.1%}")
            else:
                den = dec[0] if k in ("liquidity", "buy_sell_ratio") else max(n_cand, 1)
                print(f"  {k:<16} live {fails.get(k, 0) / den:5.0%}   backtest {v:5.0%}")

    rows = db.execute("SELECT p.variant, p.entry_price, p.entry_liq, p.exit_price, p.exit_liq, p.exit_reason, "
                      "p.entry_ts, p.exit_ts, p.status FROM positions p JOIN decisions d ON d.pool = p.pool "
                      "WHERE d.rules_hash=?", (h,)).fetchall()
    print(f"\n=== Paper trades by exit variant (closed only; interrupted ones excluded) ===")
    hdr = f"{'variant':<10} {'n':>4} {'gross med':>9} {'hold med':>8} |"
    for s in c["sizes_usd"]:
        hdr += f" ${s:g} direct: med / win / mean |"
    hdr += f" ${c['sizes_usd'][1]:g} via Axiom med | ${c['sizes_usd'][1]:g} REAL fee {REAL_FEE_GRAD:.2%}: med / mean"
    print(hdr)
    for var in VARIANTS:
        closed = [r for r in rows if r[0] == var and r[8] == "closed"]
        inter = sum(1 for r in rows if r[0] == var and r[8] == "interrupted")
        if not closed:
            print(f"{var:<10} {0:>4}   (interrupted: {inter})")
            continue
        gross = [(r[3] / r[1] - 1) if r[3] else -1.0 for r in closed]
        hold = [(r[7] - r[6]) / 60 for r in closed]
        line = f"{var:<10} {len(closed):>4} {np.median(gross) * 100:>+8.1f}% {np.median(hold):>7.1f}m |"
        for s in c["sizes_usd"]:
            rr = [net_return(s, r[1], r[2], r[3], r[4], c["fee_per_side"]["direct"], c["tx_usd"]) for r in closed]
            rr = np.array([x for x in rr if x is not None])
            line += f" {np.median(rr) * 100:+6.1f}% / {(rr > 0).mean():4.0%} / {np.clip(rr, None, 9).mean() * 100:+6.1f}% |"
        ra = [net_return(c["sizes_usd"][1], r[1], r[2], r[3], r[4], c["fee_per_side"]["axiom"], c["tx_usd"]) for r in closed]
        ra = np.array([x for x in ra if x is not None])
        line += f" {np.median(ra) * 100:+6.1f}%"
        rf = np.array([x for x in (net_return(c["sizes_usd"][1], r[1], r[2], r[3], r[4], REAL_FEE_GRAD, c["tx_usd"])
                                   for r in closed) if x is not None])
        line += f" | {np.median(rf) * 100:+6.1f}% / {np.clip(rf, None, 9).mean() * 100:+6.1f}%"
        if inter:
            line += f"   (interrupted: {inter})"
        print(line)
    reasons = db.execute("SELECT exit_reason, COUNT(*) FROM positions WHERE variant='combined' AND status='closed' "
                         "GROUP BY 1").fetchall()
    if reasons:
        print("combined exits fired by: " + ", ".join(f"{k} {v}" for k, v in reasons))

    print("\n=== Drain timing for ALL graduations (share with liquidity < $1,000 at each checkpoint) ===")
    for m in CHECKPOINTS:
        r = db.execute("SELECT COUNT(*), SUM(liquidity < 1000), AVG(tx5 = 0) FROM checkpoints WHERE target_min=?",
                       (m,)).fetchone()
        if r[0]:
            print(f"  minute {m:>5}: n={r[0]:>5}  drained {r[1] / r[0]:5.1%}  no trades in last 5 min {r[2]:5.1%}")
    print("\nFEES: 'direct' uses the rules file's 0.3%. pump.fun's published PumpSwap fee for fresh coins is 1.25% per")
    print("side - the REAL fee column is the honest one. Via Axiom you pay Axiom's fee ON TOP of that.")
    print("\nBacktest expectation (OOS, gross 30-min hold): median about +42% with all 5 rules - small sample,")
    print("probably optimistic. The paper results above are the honest test. Give it 100+ closed trades.\n")


def main():
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", default=str(here / "rules_v1_1.json"))
    ap.add_argument("--db", default=str(here / "paper.db"))
    ap.add_argument("--xs-rules", default="rules_xs_v1.json,rules_xs_young_v1.json",
                    help="comma-separated cross-sectional momentum rule files (missing files are skipped)")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--calls-per-min", type=float, default=8)
    ap.add_argument("--quiet", action="store_true", help="don't print every graduation/decision")
    ap.add_argument("--no-helius", action="store_true", help="don't log holders even if helius_key.txt exists")
    ap.add_argument("--no-whales", action="store_true", help="don't run the whale watcher")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    if not Path(args.rules).exists():
        sys.exit(f"Could not find {args.rules} - put rules_v1_1.json next to this script")
    rules = json.loads(Path(args.rules).read_text())
    db = init_db(args.db)
    xs_sets = []                                     # [(rules, hash), ...]
    for name in [x.strip() for x in args.xs_rules.split(",") if x.strip()]:
        fp = Path(name) if Path(name).is_absolute() else here / name
        if fp.exists():
            r = json.loads(fp.read_text())
            xs_sets.append((r, hashlib.sha1(json.dumps(r, sort_keys=True).encode()).hexdigest()[:10]))
    if args.report:
        print("\n========== GRADUATION STRATEGY ==========")
        report(db, rules, hashlib.sha1(json.dumps(rules, sort_keys=True).encode()).hexdigest()[:10])
        if xs_sets:
            from xs_momentum import XSMomentum, report_xs
            for r, hh in xs_sets:
                XSMomentum(db, None, r, hh)                  # makes sure the tables exist
                report_xs(db, r, hh)
        from helius import report_holders
        report_holders(db, hashlib.sha1(json.dumps(rules, sort_keys=True).encode()).hexdigest()[:10])
        from whale import report_whales
        report_whales(db)
        return
    api = Api(args.calls_per_min)
    tr = Trader(db, api, rules)
    tr.verbose = not args.quiet
    xss = []
    if xs_sets:
        from xs_momentum import XSMomentum
        shared = {}                                  # one trending list + price cache for all rule sets
        for r, hh in xs_sets:
            xss.append(XSMomentum(db, api, r, hh, verbose=not args.quiet, shared=shared))
            log.info("cross-sectional momentum ON (%s, hash %s): horizons %s", r["version"], hh,
                     ", ".join(r["horizons"]))
    hel = None
    if not args.no_helius:
        from helius import Helius
        hel = Helius(args.db, here / "helius_key.txt")
        if hel.ok:
            log.info("Helius holder logging ON (logged only, not a filter)")
            if not args.no_whales:
                try:
                    from whale import WhaleWatcher
                    hel.watcher = WhaleWatcher(args.db, hel)
                    log.info("whale watcher ON (top 3 wallets of each coin, 60 min)")
                except ImportError:
                    log.info("whale watcher OFF - install it with:  pip install websockets")
        else:
            log.info("Helius holder logging OFF (no helius_key.txt next to the script)")
            hel = None
    tr.helius = hel
    for x in xss:
        x.helius = hel
    db.execute("INSERT INTO runs VALUES (?,?)", (time.time(), tr.hash))
    db.commit()
    log.info("paper trading with rules %s (hash %s) -> %s  (Ctrl+C to stop)", rules["version"], tr.hash, args.db)
    nxt = {"disc": 0.0, "pos": 0.0, "xs": 0.0, "last_pos": time.time()}
    next_status = time.time() + 600

    def service():
        """Open positions come first: check them whenever a minute has passed."""
        t = time.time()
        if t >= nxt["pos"]:
            if t - nxt["last_pos"] > 150:
                log.warning("positions were last checked %.0f s ago - the loop is falling behind", t - nxt["last_pos"])
            tr.poll_positions()
            if xss:                                  # one price fetch for every XS set's open positions
                open_pools = [r[0] for r in db.execute("SELECT DISTINCT pool FROM xs_positions WHERE status='open'")]
                if open_pools:
                    xss[0].attrs(open_pools)
            for x in xss:
                x.poll()
            nxt["pos"], nxt["last_pos"] = time.time() + 60, time.time()

    try:
        while True:
            now = time.time()
            service()
            if now >= nxt["disc"]:
                tr.discover()
                nxt["disc"] = time.time() + 60
            service()
            tr.early_snapshots()
            service()
            tr.decide_due()
            service()
            tr.checkpoints_due()
            service()
            round_start = time.time()
            for x in xss:
                x.maybe_decide(round_start)
                service()
            now = time.time()
            if now >= next_status:
                o = db.execute("SELECT COUNT(DISTINCT pool) FROM positions WHERE status='open'").fetchone()[0]
                cl = db.execute("SELECT COUNT(*) FROM positions WHERE variant='combined' AND status='closed'").fetchone()[0]
                d = db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
                log.info("status: %d decisions | %d open paper trades | %d closed | API calls %d (429s %d, now %.1f/min)"
                         "%s", d, o, cl, api.calls, api.r429, 60 / api.gap,
                         (f" | holder snapshots {hel.done} (Helius calls {hel.calls}, errors {hel.errors})" if hel else "")
                         + (f" | whales: watching {len(hel.watcher.active)}, events {hel.watcher.events}"
                            f"{'' if hel.watcher.connected else ' (DISCONNECTED)'}" if hel and hel.watcher else ""))
                next_status = now + 600
            time.sleep(3)
    except KeyboardInterrupt:
        log.info("stopped (rerun to continue; open positions will be marked interrupted)")
    finally:
        db.commit()
        db.close()


if __name__ == "__main__":
    main()
