#!/usr/bin/env python3
"""
NEAR-GRADUATION paper trader (no money, no wallet - it only records decisions).

Idea: buy pump.fun coins that are close to finishing their bonding curve, ride the last part of the
curve, and sell at/after graduation (or earlier). The curve can't be rugged - you can always sell back
into it - and the last stretch is steep: from 80% progress to graduation is ~x2.4, from 90% ~x1.6,
from 95% ~x1.3. The open question is how many coins actually finish, and this measures it.

Data:
  PumpPortal websocket (free): every new pump.fun coin + every graduation, in real time
  Helius: reads each coin's bonding curve (progress, price) - 100 coins per call
  GeckoTerminal: only the PumpSwap price 1 and 5 minutes after a graduation

Entries: when a coin's progress crosses 80%, 90% or 95% (each threshold is a separate test).
Exits (each a separate paper position, same entry):
  grad_best   sell the moment it graduates, at the curve's final price  (BEST CASE - bots are faster)
  grad_1m     hold to graduation, sell on PumpSwap ~1 minute after
  grad_5m     hold to graduation, sell on PumpSwap ~5 minutes after
  tp25_sl25   sell on the curve at +25% or -25%; if it graduates first, like grad_1m
  stall10     sell on the curve if progress hasn't made a new high for 10 minutes; graduates -> like grad_1m
  All: if no graduation within 60 minutes, sell on the curve.
Fees: pump.fun's published 1.25% per side on the curve and on small PumpSwap coins, plus a network fee.
Sizes are in SOL (0.5 and 2.5 SOL, roughly $100 and $500).

Run (from the dexbank folder, next to helius_key.txt; can run next to paper_trader.py):
    python neargrad.py              # live
    python neargrad.py --report     # results so far (works while it runs, in another terminal)
Database: neargrad.db.  Needs: pip install websockets  (already done for the whale watcher)
"""

import argparse
import asyncio
import json
import logging
import sqlite3
import time
from pathlib import Path

import numpy as np
import requests

from helius import parse_curve, curve_address

log = logging.getLogger("neargrad")
PP_URL = "wss://pumpportal.fun/api/data"
THRESHOLDS = [0.80, 0.90, 0.95]
VARIANTS = ["grad_best", "grad_1m", "grad_5m", "tp25_sl25", "stall10"]
AFTER_GRAD_1M = {"grad_1m", "tp25_sl25", "stall10"}
CURVE_FEE = 0.0125          # pump.fun: 1.25% per trade on the bonding curve
SWAP_FEE = 0.0125           # PumpSwap, coins under 420 SOL market cap (all fresh graduates)
TX_SOL = 0.001              # network + priority fee per transaction (assumption)
SIZES_SOL = [0.5, 2.5]
STD_K = 30e9 * 1_073_000_000e6          # standard SOL curve: virtual SOL x virtual tokens (raw units)
DS = "https://dexscreener.com/solana/"


# ---------------------------------------------------------------------- fills (all in SOL)
def curve_buy(sol, vs, vt):
    """Tokens (raw) received for `sol` on a curve with virtual reserves vs (lamports), vt (raw)."""
    s = sol * (1 - CURVE_FEE) * 1e9
    return vt - vs * vt / (vs + s)


def curve_sell(tokens, vs, vt):
    out = vs - vs * vt / (vt + tokens)
    return out / 1e9 * (1 - CURVE_FEE)


def swap_sell(tokens, r_sol, price_sol):
    """Sell raw tokens into a constant-product pool with r_sol SOL reserve at price_sol (SOL per raw token)."""
    if not r_sol or not price_sol or r_sol <= 0:
        return 0.0
    b = r_sol / price_sol
    return r_sol * tokens / (b + tokens) * (1 - SWAP_FEE)


def trade_result(size, row):
    """Net return of one paper position at `size` SOL, or None if unpriced."""
    vs, vt, kind, a, b = row["entry_vs"], row["entry_vt"], row["exit_kind"], row["exit_a"], row["exit_b"]
    if not vs or not vt or kind is None:
        return None
    tok = curve_buy(size, vs, vt)
    if kind == "curve":
        got = curve_sell(tok, a, b)
    elif kind == "swap":
        got = swap_sell(tok, a, b)
    else:
        return None
    return (got - 2 * TX_SOL) / size - 1


# ---------------------------------------------------------------------- the trader
class NearGrad:
    MIN = 60.0                      # seconds per "minute" (the test speeds this up)

    def __init__(self, db_path, key_path, gt_calls_per_min=3, verbose=True):
        self.db = sqlite3.connect(db_path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        init_db(self.db)
        key = Path(key_path).read_text().strip() if Path(key_path).exists() else ""
        if not key:
            raise SystemExit("helius_key.txt not found next to the script")
        self._rpc_url = f"https://mainnet.helius-rpc.com/?api-key={key}"
        self.http = requests.Session()
        self.gt_per_min, self.verbose = gt_calls_per_min, verbose
        self.coins = {}                 # mint -> dict (watched coins)
        self.post = []                  # [(due_ts, mint, label, tries)]
        self.helius_calls = self.helius_errors = self.ws_msgs = self.new_coins = self.migrations = 0
        self.connected = False
        self._gt = None
        self._resume()

    def now(self):
        return time.time()

    # ------------------------------------------------------------ startup
    def _resume(self):
        now = self.now()
        # positions whose coin wasn't checked for 5+ minutes can't be trusted
        n = self.db.execute("""UPDATE positions SET status='interrupted', exit_ts=?
                               WHERE status IN ('open','awaiting_pool')""", (now,)).rowcount
        self.db.commit()
        if n:
            log.info("%d paper positions from the last run marked interrupted", n)
        for row in self.db.execute("""SELECT mint, name, curve, created_ts, max_progress, max_progress_ts, dev
                                      FROM coins WHERE status='watch' AND created_ts > ?""", (now - 24 * 60 * self.MIN,)):
            mint, name, curve, created, mp, mpt, dev = row
            self.coins[mint] = dict(mint=mint, name=name, curve=curve, created=created, max_p=mp or 0.0,
                                    max_p_ts=mpt or created, next_poll=now, last=None, dev=dev, crossed=set(
                                        r[0] for r in self.db.execute("SELECT threshold FROM crossings WHERE mint=?",
                                                                      (mint,))), last_tick=0.0)

    # ------------------------------------------------------------ PumpPortal
    async def ws_task(self):
        try:
            from websockets.asyncio.client import connect
        except ImportError:
            from websockets import connect
        from whale import _ssl_context
        extra = {"ssl": _ssl_context()} if PP_URL.startswith("wss://") else {}
        backoff = 2
        while True:
            try:
                async with connect(PP_URL, open_timeout=20, ping_interval=20, ping_timeout=20,
                                   max_size=2 ** 22, **extra) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    await ws.send(json.dumps({"method": "subscribeMigration"}))
                    self.connected, backoff = True, 2
                    async for raw in ws:
                        self.ws_msgs += 1
                        try:
                            self.on_message(json.loads(raw))
                        except Exception as e:
                            log.warning("bad PumpPortal message (%s)", type(e).__name__)
            except Exception as e:
                self.connected = False
                log.warning("PumpPortal connection lost (%s), reconnecting in %ds", type(e).__name__, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def on_message(self, m):
        if not isinstance(m, dict) or not m.get("mint"):
            return                                       # e.g. "successfully subscribed"
        tx = (m.get("txType") or "").lower()
        if tx == "create":
            self.add_coin(m)
        else:                                            # the only other stream we subscribe to: migrations
            self.migrations += 1
            if self.db.execute("SELECT COUNT(*) FROM raw_msgs").fetchone()[0] < 20:
                self.db.execute("INSERT INTO raw_msgs VALUES (?,?)", (self.now(), json.dumps(m)[:2000]))
            self.on_graduated(m["mint"], source="pumpportal")
            self.db.commit()

    def add_coin(self, m):
        mint, now = m["mint"], self.now()
        if mint in self.coins:
            return
        curve = m.get("bondingCurveKey") or curve_address(mint)
        c = dict(mint=mint, name=m.get("symbol") or m.get("name"), curve=curve, created=now, max_p=0.0,
                 max_p_ts=now, next_poll=now + self.MIN / 3, last=None, dev=m.get("traderPublicKey"),
                 crossed=set(), last_tick=0.0)
        self.coins[mint] = c
        self.new_coins += 1
        self.db.execute("""INSERT OR IGNORE INTO coins (mint, name, curve, created_ts, dev, dev_initial_buy_sol,
                           status, max_progress, max_progress_ts) VALUES (?,?,?,?,?,?,'watch',0,?)""",
                        (mint, c["name"], curve, now, c["dev"], m.get("solAmount"), now))
        self.db.commit()

    # ------------------------------------------------------------ polling the curves
    async def poll_task(self):
        loop = asyncio.get_running_loop()
        while True:                                       # one polling cycle every 10 s, batches of 100 coins
            t0 = self.now()
            due = sorted((c for c in self.coins.values() if c["next_poll"] <= t0 + self.MIN / 12),
                         key=lambda c: c["next_poll"])
            for i in range(0, len(due), 100):
                batch = due[i:i + 100]
                try:
                    vals = await loop.run_in_executor(None, self.rpc, "getMultipleAccounts",
                                                      [[c["curve"] for c in batch], {"encoding": "base64"}])
                    vals = (vals or {}).get("value") or []
                except Exception as e:
                    self.helius_errors += 1
                    log.warning("Helius error (%s)", type(e).__name__)
                    continue
                for c, v in zip(batch, vals):
                    if c["mint"] in self.coins:
                        self.update_coin(c, v)
                self.db.commit()
            await asyncio.sleep(max(0.0, self.MIN / 6 - (self.now() - t0)))

    def rpc(self, method, params):
        self.helius_calls += 1
        r = self.http.post(self._rpc_url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                           timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        body = r.json()
        if "error" in body:
            raise RuntimeError("rpc error")
        return body.get("result")

    def update_coin(self, c, v):
        now = self.now()
        age = (now - c["created"]) / self.MIN
        st = parse_curve(v["data"][0]) if v and v.get("data") else None
        if st is None:
            c["misses"] = c.get("misses", 0) + 1
            if c["misses"] >= 3:
                self.drop(c, "no_curve_account")
            else:
                c["next_poll"] = now + self.MIN / 3
            return
        if abs(st["virtual_sol"] * st["virtual_tokens"] / STD_K - 1) > 0.02:
            self.drop(c, "not_a_SOL_curve")                   # non-standard curve (other quote token)
            return
        c["last"], c["last_ts"] = st, now
        p = st["progress"]
        if p > c["max_p"] + 1e-6:
            c["max_p"], c["max_p_ts"] = p, now
        self.db.execute("UPDATE coins SET progress=?, max_progress=?, max_progress_ts=?, last_poll_ts=? WHERE mint=?",
                        (p, c["max_p"], c["max_p_ts"], now, c["mint"]))
        if p >= 0.5 and now - c["last_tick"] >= self.MIN / 4:
            self.db.execute("INSERT INTO ticks VALUES (?,?,?,?,?)", (c["mint"], now, p, st["price_sol"], st["real_sol_ui"]))
            c["last_tick"] = now
        if st["complete"]:
            self.on_graduated(c["mint"], source="curve")
            return
        # entries
        for thr in THRESHOLDS:
            if p >= thr and thr not in c["crossed"]:
                c["crossed"].add(thr)
                self.db.execute("INSERT OR IGNORE INTO crossings VALUES (?,?,?,?,?,?)",
                                (c["mint"], thr, now, p, st["price_sol"], age))
                for var in VARIANTS:
                    self.db.execute("""INSERT OR IGNORE INTO positions (mint, threshold, variant, entry_ts, entry_progress,
                        entry_price, entry_vs, entry_vt, status) VALUES (?,?,?,?,?,?,?,?,'open')""",
                                    (c["mint"], thr, var, now, p, st["price_sol"], st["virtual_sol"],
                                     st["virtual_tokens"]))
                if self.verbose:
                    log.info("PAPER BUY  %-12s crossed %2.0f%% (now %.1f%%, age %.1f min)  %s%s",
                             str(c["name"])[:12], thr * 100, p * 100, age, DS, c["mint"])
        self.manage_on_curve(c, st)
        # next poll
        has_pos = self.db.execute("SELECT 1 FROM positions WHERE mint=? AND status='open' LIMIT 1",
                                  (c["mint"],)).fetchone() is not None
        nxt = self.interval(p, age, has_pos)
        if nxt is None:
            self.drop(c, "inactive")
        else:
            c["next_poll"] = now + nxt

    def interval(self, p, age, has_pos):
        M = self.MIN
        if has_pos or p >= 0.6:
            return M / 6                                   # every cycle (10 s)
        if age < 3:
            return M / 3                                   # every 20 s
        if (p < 0.03) or (p < 0.10 and age >= 15) or (p < 0.30 and age >= 60) or age > 24 * 60:
            return None
        return M / 3 if age < 15 else M

    def drop(self, c, why):
        self.db.execute("UPDATE coins SET status='dropped', drop_reason=? WHERE mint=?", (why, c["mint"]))
        self.coins.pop(c["mint"], None)

    def manage_on_curve(self, c, st):
        now = self.now()
        rows = self.db.execute("SELECT threshold, variant, entry_ts, entry_price FROM positions "
                               "WHERE mint=? AND status='open'", (c["mint"],)).fetchall()
        for thr, var, ets, ep in rows:
            reason = None
            if (now - ets) / self.MIN >= 60:
                reason = "no_graduation_60m"
            elif var == "tp25_sl25" and ep:
                if st["price_sol"] >= ep * 1.25:
                    reason = "take_profit"
                elif st["price_sol"] <= ep * 0.75:
                    reason = "stop_loss"
            elif var == "stall10" and (now - c["max_p_ts"]) / self.MIN >= 10:
                reason = "stalled"
            if reason:
                self.close(c["mint"], thr, var, now, "curve", st["virtual_sol"], st["virtual_tokens"], reason, st["progress"])

    def close(self, mint, thr, var, now, kind, a, b, reason, progress=None, status="closed"):
        self.db.execute("""UPDATE positions SET status=?, exit_ts=?, exit_kind=?, exit_a=?, exit_b=?, exit_reason=?,
                           exit_progress=? WHERE mint=? AND threshold=? AND variant=?""",
                        (status, now, kind, a, b, reason, progress, mint, thr, var))

    # ------------------------------------------------------------ graduation
    def on_graduated(self, mint, source):
        now = self.now()
        row = self.db.execute("SELECT status, grad_ts FROM coins WHERE mint=?", (mint,)).fetchone()
        if not row or row[1]:
            if row is None:
                self.db.execute("INSERT OR IGNORE INTO coins (mint, status, grad_ts, grad_source) VALUES (?,?,?,?)",
                                (mint, "graduated_unseen", now, source))
            return
        c = self.coins.pop(mint, None)
        st = (c or {}).get("last")
        self.db.execute("UPDATE coins SET status='graduated', grad_ts=?, grad_source=?, progress=1, max_progress=1 "
                        "WHERE mint=?", (now, source, mint))
        open_rows = self.db.execute("SELECT threshold, variant FROM positions WHERE mint=? AND status='open'",
                                    (mint,)).fetchall()
        if not open_rows:
            return
        # best case: sell at the curve's final price into a pool holding the curve's SOL (~85 SOL)
        final = st
        if final and not final["complete"]:
            # last poll was before completion: completion price = curve price with all real tokens sold
            vt_end = final["virtual_tokens"] - final["real_tokens"]
            k = final["virtual_sol"] * final["virtual_tokens"]
            final = dict(price_sol=(k / vt_end / 1e9) / (vt_end / 1e6), real_sol_ui=(k / vt_end - final["virtual_sol"]
                                                                                      ) / 1e9 + final["real_sol_ui"])
        for thr, var in open_rows:
            if var == "grad_best" and final:
                self.close(mint, thr, var, now, "swap", final["real_sol_ui"], final["price_sol"] / 1e6,
                           "graduated", 1.0)
            else:
                self.db.execute("UPDATE positions SET status='awaiting_pool', grad_ts=? WHERE mint=? AND threshold=? "
                                "AND variant=?", (now, mint, thr, var))
        self.post.append((now + self.MIN, mint, "1m", 0))
        self.post.append((now + 5 * self.MIN, mint, "5m", 0))
        if self.verbose:
            log.info("GRADUATED  %-12s (%s) - %d paper positions selling on PumpSwap at +1/+5 min",
                     str((c or {}).get("name") or mint[:8])[:12], source, len(open_rows))

    async def post_task(self):
        loop = asyncio.get_running_loop()
        while True:
            now = self.now()
            due = [x for x in self.post if x[0] <= now]
            if due:
                self.post = [x for x in self.post if x[0] > now]
                for ts, mint, label, tries in due:
                    try:
                        price_sol, r_sol = await loop.run_in_executor(None, self.pool_price, mint)
                    except Exception as e:
                        log.warning("post-graduation price failed (%s)", type(e).__name__)
                        price_sol = r_sol = None
                    vars_ = AFTER_GRAD_1M | {"grad_1m"} if label == "1m" else {"grad_5m"}
                    if price_sol:
                        for thr, var, gts in self.db.execute(
                                "SELECT threshold, variant, grad_ts FROM positions WHERE mint=? AND status='awaiting_pool'",
                                (mint,)).fetchall():
                            if var in vars_:
                                self.close(mint, thr, var, now, "swap", r_sol, price_sol, f"sold_{label}", 1.0)
                                self.db.execute("UPDATE positions SET exit_delay_min=? WHERE mint=? AND threshold=? "
                                                "AND variant=?", ((now - gts) / self.MIN, mint, thr, var))
                    elif tries < 3:
                        self.post.append((now + self.MIN / 2, mint, label, tries + 1))
                    else:
                        for thr, var in self.db.execute("SELECT threshold, variant FROM positions WHERE mint=? AND "
                                                        "status='awaiting_pool'", (mint,)).fetchall():
                            if var in vars_:
                                self.close(mint, thr, var, now, None, None, None, "no_pool_price", status="unpriced")
                    self.db.commit()
            await asyncio.sleep(min(2.0, self.MIN / 30))

    def pool_price(self, mint):
        """PumpSwap price (SOL per raw token) and SOL reserve, from GeckoTerminal."""
        if self._gt is None:
            from paper_trader import Api
            self._gt = Api(self.gt_per_min)
        d = self._gt.get(f"/networks/solana/tokens/{mint}/pools", {"page": 1, "include": "dex"})
        for it in (d or {}).get("data") or []:
            dex = (((it.get("relationships") or {}).get("dex") or {}).get("data") or {}).get("id")
            a = it.get("attributes") or {}
            if dex != "pumpswap":
                continue
            p, q, liq = (float(a.get(k) or 0) for k in ("base_token_price_usd", "quote_token_price_usd", "reserve_in_usd"))
            if p > 0 and q > 20 and liq > 0:
                return p / q / 1e6, liq / 2 / q
        return None, None

    # ------------------------------------------------------------ status
    async def status_task(self):
        while True:
            await asyncio.sleep(5 * self.MIN)
            o = self.db.execute("SELECT COUNT(DISTINCT mint) FROM positions WHERE status IN ('open','awaiting_pool')"
                                ).fetchone()[0]
            cl = self.db.execute("SELECT COUNT(*) FROM positions WHERE status='closed' AND variant='grad_1m'").fetchone()[0]
            hot = sum(1 for c in self.coins.values() if c["max_p"] >= 0.5)
            log.info("status: PumpPortal %s | new coins %d, graduations %d | watching %d (%d above 50%%) | "
                     "open coins %d | closed (grad_1m) %d | Helius calls %d (errors %d)",
                     "connected" if self.connected else "DISCONNECTED", self.new_coins, self.migrations,
                     len(self.coins), hot, o, cl, self.helius_calls, self.helius_errors)

    async def run(self):
        await asyncio.gather(self.ws_task(), self.poll_task(), self.post_task(), self.status_task())


# ---------------------------------------------------------------------- db + report
def init_db(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS coins (
        mint TEXT PRIMARY KEY, name TEXT, curve TEXT, created_ts REAL, dev TEXT, dev_initial_buy_sol REAL,
        status TEXT, drop_reason TEXT, progress REAL, max_progress REAL, max_progress_ts REAL, last_poll_ts REAL,
        grad_ts REAL, grad_source TEXT);
    CREATE TABLE IF NOT EXISTS crossings (mint TEXT, threshold REAL, ts REAL, progress REAL, price_sol REAL,
        age_min REAL, PRIMARY KEY (mint, threshold));
    CREATE TABLE IF NOT EXISTS positions (
        mint TEXT, threshold REAL, variant TEXT, entry_ts REAL, entry_progress REAL, entry_price REAL,
        entry_vs REAL, entry_vt REAL, status TEXT, grad_ts REAL, exit_ts REAL, exit_kind TEXT, exit_a REAL,
        exit_b REAL, exit_reason TEXT, exit_progress REAL, exit_delay_min REAL,
        PRIMARY KEY (mint, threshold, variant));
    CREATE TABLE IF NOT EXISTS ticks (mint TEXT, ts REAL, progress REAL, price_sol REAL, real_sol REAL);
    CREATE INDEX IF NOT EXISTS idx_ticks ON ticks(mint, ts);
    CREATE TABLE IF NOT EXISTS raw_msgs (ts REAL, msg TEXT);
    """)
    db.commit()


def report(db, minute=60.0):
    init_db(db)
    now = time.time()
    st = dict(db.execute("SELECT status, COUNT(*) FROM coins GROUP BY 1").fetchall())
    print("\n========== NEAR-GRADUATION (pump.fun bonding curve) ==========")
    print(f"coins seen {sum(st.values())} | still watched {st.get('watch', 0)} | graduated {st.get('graduated', 0)} "
          f"(+{st.get('graduated_unseen', 0)} we never saw being created) | dropped as inactive {st.get('dropped', 0)}")
    print("\nHow many coins that reached X% went on to graduate (coins resolved: graduated, dropped, or 2h+ old):")
    print(f"  {'reached':>8} {'coins':>6} {'graduated':>10} {'time to graduate (median)':>27}")
    for thr in (0.5, 0.7, 0.8, 0.9, 0.95):
        r = db.execute("""SELECT status, grad_ts, created_ts FROM coins WHERE max_progress >= ?
                          AND (status IN ('graduated','dropped') OR created_ts < ?)""", (thr, now - 2 * 3600)).fetchall()
        if not r:
            continue
        g = [x for x in r if x[0] == "graduated"]
        tt = db.execute("""SELECT c.grad_ts - x.ts FROM crossings x JOIN coins c ON c.mint = x.mint
                           WHERE x.threshold=? AND c.grad_ts IS NOT NULL""", (thr,)).fetchall()
        tts = f"{np.median([t[0] for t in tt]) / minute:.1f} min after crossing" if tt else "-"
        print(f"  {thr:>8.0%} {len(r):>6} {len(g) / len(r):>10.0%} {tts:>27}")
    late = db.execute("SELECT threshold, AVG(progress - threshold), COUNT(*) FROM crossings GROUP BY 1").fetchall()
    if late:
        print("  detection lag (how far past the threshold we noticed): " +
              ", ".join(f"{t:.0%}: +{d * 100:.1f} pts (n={n})" for t, d, n in late))

    rows = [dict(zip([d[0] for d in db.execute("SELECT * FROM positions LIMIT 0").description], r))
            for r in db.execute("SELECT * FROM positions")]
    print("\nPaper trades (real fees: 1.25% per side + 0.001 SOL per tx). 'won %' = net > 0.")
    hdr = f"  {'entry':>6} {'exit':<10} {'n':>4} {'graduated':>9} |"
    for s in SIZES_SOL:
        hdr += f" {s:g} SOL: med / won / mean |"
    print(hdr)
    for thr in THRESHOLDS:
        for var in VARIANTS:
            sel = [r for r in rows if abs(r["threshold"] - thr) < 1e-9 and r["variant"] == var and r["status"] == "closed"]
            other = {k: sum(1 for r in rows if abs(r["threshold"] - thr) < 1e-9 and r["variant"] == var and r["status"] == k)
                     for k in ("open", "awaiting_pool", "interrupted", "unpriced")}
            if not sel:
                continue
            gr = np.mean([r["exit_kind"] == "swap" for r in sel])
            line = f"  {thr:>6.0%} {var:<10} {len(sel):>4} {gr:>9.0%} |"
            for s in SIZES_SOL:
                rr = np.array([x for x in (trade_result(s, r) for r in sel) if x is not None])
                line += f" {np.median(rr) * 100:+6.1f}% / {(rr > 0).mean():4.0%} / {np.clip(rr, None, 9).mean() * 100:+6.1f}% |"
            extra = ", ".join(f"{k} {v}" for k, v in other.items() if v)
            print(line + (f"  ({extra})" if extra else ""))
    d = db.execute("SELECT AVG(exit_delay_min) FROM positions WHERE exit_reason='sold_1m'").fetchone()[0]
    if d:
        print(f"  ('1m' sells happened on average {d:.1f} min after graduation - GeckoTerminal needs time to list the pool)")
    reasons = db.execute("SELECT variant, exit_reason, COUNT(*) FROM positions WHERE status='closed' GROUP BY 1, 2").fetchall()
    if reasons:
        print("  exit reasons: " + "; ".join(f"{v} {r} {n}" for v, r, n in reasons))
    print("\nREAD: grad_best is a best case (bots sell first at graduation). The honest columns are grad_1m / grad_5m")
    print("and the curve exits. A real edge needs a positive MEDIAN or a mean that clearly survives at 2.5 SOL.\n")


def main():
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(here / "neargrad.db"))
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    if args.report:
        report(sqlite3.connect(args.db))
        return
    ng = NearGrad(args.db, here / "helius_key.txt", verbose=not args.quiet)
    log.info("near-graduation paper trader started -> %s  (Ctrl+C to stop)", args.db)
    try:
        asyncio.run(ng.run())
    except KeyboardInterrupt:
        log.info("stopped (rerun to continue; open positions will be marked interrupted)")
    finally:
        ng.db.commit()


if __name__ == "__main__":
    main()
