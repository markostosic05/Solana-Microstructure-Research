"""
Cross-sectional momentum (paper) - used by paper_trader.py when rules_xs_v1.json is present.

Every 5 minutes (5m horizon) and every 15 minutes (15m horizon):
  1. Take Solana's trending pools (GeckoTerminal, top 60 by 5-minute trending).
  2. Keep the ones passing the filters (liquidity, age, buys >= sells, not a major).
  3. Rank them by their return over the horizon (5m or 15m).
  4. Paper-buy: top1 (biggest winner), top10pct (top 10%), bottom10pct (biggest losers),
     control (a random coin from the middle). Each with its exit variants:
       hold_h / guard_h   hold for the horizon (guard = early exit on liquidity pull / dead trading)
       tp50_rr1, tp100_rr2, tp200_rr3   close everything at +50% / +100% / +200%, stop -50%
The spread 'top 10% minus bottom 10%' tells whether the ranking carries information at all.
"""

import json
import logging
import math
import random
import time

import numpy as np

log = logging.getLogger("paper")
DS_LINK = "https://dexscreener.com/solana/"


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


def parse_item(it):
    a = it.get("attributes") or {}
    tx = (a.get("transactions") or {}).get("m5") or {}
    pc = a.get("price_change_percentage") or {}
    rel = it.get("relationships") or {}
    mint = (((rel.get("base_token") or {}).get("data") or {}).get("id") or "").split("_", 1)[-1] or None
    dex = ((rel.get("dex") or {}).get("data") or {}).get("id")
    return dict(pool=a.get("address"), mint=mint, dex=dex, quote_price=fnum(a.get("quote_token_price_usd")),
                name=a.get("name"), price=fnum(a.get("base_token_price_usd")),
                liq=fnum(a.get("reserve_in_usd")), fdv=fnum(a.get("fdv_usd")),
                buys5=tx.get("buys") or 0, sells5=tx.get("sells") or 0,
                pc5=fnum(pc.get("m5")), pc15=fnum(pc.get("m15")), created=iso_ts(a.get("pool_created_at")))


def fee_estimate(dex, fdv, quote_price, sol_usd=None):
    """Estimated fee per side. PumpSwap: pump.fun's published market-cap tiers. Other DEXes: assumed 0.3%."""
    if dex != "pumpswap":
        return 0.003
    from paper_trader import pumpswap_fee
    sol = quote_price if quote_price and quote_price > 20 else sol_usd     # quote is SOL (not a $1 stablecoin)
    return pumpswap_fee(fdv / sol if fdv and sol else None)


def net_legs(size, ep, el, legs, fee, tx):
    """Round trip with partial exits. legs = [(fraction, price, liquidity), ...]."""
    if not ep or not el or el <= 0:
        return None
    r_in = el / 2
    spend = size * (1 - fee)
    tv = spend * r_in / (r_in + spend)
    got = -tx
    for frac, p, liq in legs:
        if not p or not liq or liq <= 0:
            got -= tx
            continue
        val = tv * frac * p / ep
        r_out = liq / 2
        got += val * r_out / (r_out + val) * (1 - fee) - tx
    return got / size - 1


class XSMomentum:
    def __init__(self, db, api, rules, rules_hash, clock=time.time, verbose=True, shared=None):
        self.db, self.api, self.r, self.hash, self.clock, self.verbose = db, api, rules, rules_hash, clock, verbose
        self.next_dec = {}
        # one trending list + price cache shared by all XS rule sets (saves API calls)
        self.shared = shared if shared is not None else {}
        self.shared.setdefault("uni", {"ts": 0.0, "items": [], "use_duration": True, "sol_usd": None})
        self.cache = self.shared.setdefault("cache", {})      # pool -> (ts, attrs)
        self.tag = rules.get("tag", "XS")
        self.helius = None                       # optional holder logging (set by paper_trader)
        db.executescript("""
        CREATE TABLE IF NOT EXISTS xs_prices (ts REAL, pool TEXT, price REAL);
        CREATE INDEX IF NOT EXISTS idx_xs_prices ON xs_prices(pool, ts);
        CREATE TABLE IF NOT EXISTS xs_decisions (
            ts REAL, horizon TEXT, rules_hash TEXT, n_universe INTEGER, n_eligible INTEGER, ranking TEXT);
        CREATE TABLE IF NOT EXISTS xs_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, decision_ts REAL, horizon TEXT, book TEXT, rank INTEGER,
            pool TEXT, name TEXT, variant TEXT, rules_hash TEXT, signal REAL,
            entry_ts REAL, entry_price REAL, entry_liq REAL, peak REAL, peak_tx5 REAL,
            remaining REAL, next_level INTEGER, legs TEXT, last_poll_ts REAL, exit_ts REAL, exit_reason TEXT,
            status TEXT);
        """)
        for col in ("dex TEXT", "fee_est REAL", "age_min REAL"):
            try:
                db.execute(f"ALTER TABLE xs_positions ADD COLUMN {col}")
            except Exception:
                pass                                         # already there
        db.commit()

    # ------------------------------------------------------------------ data
    def fetch_universe(self):
        now, U = self.clock(), self.shared["uni"]
        if now - U["ts"] < 50:
            return U["items"]
        items = []
        for page in range(1, self.r.get("pages", 3) + 1):
            params = {"page": page, "include": "base_token,dex"}
            if U["use_duration"]:
                params["duration"] = self.r.get("trending_duration", "5m")
            data = self.api.get(f"/networks/{self.r['network']}/trending_pools", params)
            if data is None and U["use_duration"] and page == 1:
                log.warning("trending 'duration' not accepted - using the default trending list")
                U["use_duration"] = False
                params.pop("duration", None)
                data = self.api.get(f"/networks/{self.r['network']}/trending_pools", params)
            lst = (data or {}).get("data") or []
            if not lst:
                break
            items.extend(lst)
        uni = [u for u in (parse_item(x) for x in items) if u["pool"] and u["price"]]
        for rank, u in enumerate(uni, 1):
            u["trend_rank"] = rank
            self.cache[u["pool"]] = (now, u)
            self.db.execute("INSERT INTO xs_prices VALUES (?,?,?)", (now, u["pool"], u["price"]))
            if u["quote_price"] and u["quote_price"] > 20:
                U["sol_usd"] = u["quote_price"]
        self.db.execute("DELETE FROM xs_prices WHERE ts < ?", (now - 3 * 3600,))
        self.db.commit()
        U["items"], U["ts"] = uni, self.clock()          # timestamp when the download FINISHED
        return uni

    def attrs(self, pools):
        """Latest attrs per pool, reusing anything fetched in the last 50 s."""
        from paper_trader import fetch_multi
        now = self.clock()
        need = [p for p in pools if now - self.cache.get(p, (0, None))[0] > 50]
        if need:
            fresh = fetch_multi(self.api, need)
            for p, a in fresh.items():
                self.cache[p] = (now, a)
        return {p: self.cache[p][1] for p in pools if p in self.cache and now - self.cache[p][0] <= 120}

    def own_return(self, pool, price, minutes):
        now = self.clock()
        row = self.db.execute("SELECT price FROM xs_prices WHERE pool=? AND ts BETWEEN ? AND ? "
                              "ORDER BY ABS(ts - ?) LIMIT 1",
                              (pool, now - (minutes + 3) * 60, now - (minutes - 3) * 60, now - minutes * 60)).fetchone()
        return price / row[0] - 1 if row and row[0] else None

    # ------------------------------------------------------------------ decisions
    def maybe_decide(self, now=None):
        """now = when the main loop started this round (so a slow decision by another XS set
        doesn't make this one think it slept through its slot)."""
        now = self.clock() if now is None else now
        for name, mins in self.r["horizons"].items():
            step = mins * 60
            if name not in self.next_dec:
                self.next_dec[name] = math.floor(now / step) * step + step
                continue
            if now >= self.next_dec[name]:
                if now - self.next_dec[name] < 120:      # skip slots we slept through
                    self.decide(name, mins)
                self.next_dec[name] = math.floor(now / step) * step + step

    def decide(self, hname, mins):
        uni = self.fetch_universe()
        f, now = self.r["filters"], self.clock()
        elig = []
        for u in uni:
            if (u["liq"] or 0) < f["liquidity_usd_min"]:
                continue
            if u["created"] is None or (now - u["created"]) / 60 < f["pool_age_min"]:
                continue
            if f.get("pool_age_max_min") is not None and (now - u["created"]) / 60 > f["pool_age_max_min"]:
                continue
            if u["buys5"] < f["buy_sell_ratio_5m_min"] * max(u["sells5"], 1):
                continue
            if u["fdv"] is not None and u["fdv"] > f["fdv_usd_max"]:
                continue
            if mins == 5:
                sig = u["pc5"] / 100 if u["pc5"] is not None else self.own_return(u["pool"], u["price"], 5)
            else:
                sig = u["pc15"] / 100 if u["pc15"] is not None else self.own_return(u["pool"], u["price"], mins)
            if sig is None:
                continue
            elig.append(dict(u, signal=sig))
        elig.sort(key=lambda z: -z["signal"])
        n = len(elig)
        self.db.execute("INSERT INTO xs_decisions VALUES (?,?,?,?,?,?)",
                        (now, hname, self.hash, len(uni), n,
                         json.dumps([(e["pool"], round(e["signal"], 4)) for e in elig])))
        if n < 4:
            self.db.commit()
            if self.verbose:
                log.info(self.tag + " %-3s only %d eligible coins - no trades this slot", hname, n)
            return
        k = max(1, int(math.ceil(0.10 * n)))
        top, bottom = elig[:k], elig[-k:]
        middle = elig[k:n - k] or elig
        rng = random.Random(int(now) ^ mins)
        books = {"top1": [elig[0]], "top10pct": top, "bottom10pct": bottom, "control": [rng.choice(middle)]}
        for book, picks in books.items():
            for c in picks:
                rank = elig.index(c) + 1
                for var in self.r["books"][book]["variants"]:
                    self.db.execute(
                        "INSERT INTO xs_positions (decision_ts, horizon, book, rank, pool, name, variant, rules_hash, "
                        "signal, entry_ts, entry_price, entry_liq, peak, peak_tx5, remaining, next_level, legs, "
                        "last_poll_ts, status, dex, fee_est, age_min) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1.0,0,'[]',?,'open',?,?,?)",
                        (now, hname, book, rank, c["pool"], c["name"], var, self.hash, c["signal"], now, c["price"],
                         c["liq"], c["price"], c["buys5"] + c["sells5"], now, c.get("dex"),
                         fee_estimate(c.get("dex"), c.get("fdv"), c.get("quote_price"),
                                      self.shared["uni"]["sol_usd"]),
                         (now - c["created"]) / 60 if c.get("created") else None))
        self.db.commit()
        if self.helius:
            for c in {c["pool"]: c for picks in books.values() for c in picks}.values():
                self.helius.submit("xs", c["pool"], c.get("mint"), c["price"], c["liq"], now,
                                   want_dev=bool(c.get("mint") and c["mint"].endswith("pump")))
        if self.verbose:
            w = elig[0]
            log.info(self.tag + " %-3s %d eligible | top winner %-18s %+.1f%% (liq $%.0f) %s%s | bottom %+.1f%%", hname, n,
                     str(w["name"])[:18], w["signal"] * 100, w["liq"] or 0, DS_LINK, w["pool"],
                     bottom[-1]["signal"] * 100)

    # ------------------------------------------------------------------ positions
    def poll(self):
        now = self.clock()
        rows = self.db.execute("SELECT id, horizon, variant, entry_ts, entry_price, entry_liq, peak, peak_tx5, "
                               "remaining, next_level, legs, last_poll_ts, pool FROM xs_positions "
                               "WHERE status='open' AND rules_hash=?", (self.hash,)).fetchall()
        if not rows:
            return
        attrs = self.attrs(sorted({r[12] for r in rows}))
        g = self.r["guard"]
        for (pid, hname, var, ets, ep, el, peak, peak_tx, rem, lvl, legs_s, last_poll, pool) in rows:
            if now - last_poll > 300:
                self.db.execute("UPDATE xs_positions SET status='interrupted', exit_ts=? WHERE id=?", (now, pid))
                continue
            spec = self.r["variants"][var]
            hold = self.r["horizons"][hname] if spec["hold"] == "h" else spec["hold"]
            held = (now - ets) / 60
            legs = json.loads(legs_s)
            a = attrs.get(pool)
            if not a or not a.get("price"):
                if held >= hold:
                    legs.append([rem, None, None])
                    self.finish(pid, now, legs, "no_data_at_exit")
                else:
                    self.db.execute("UPDATE xs_positions SET last_poll_ts=? WHERE id=?", (now, pid))
                continue
            price, liq = a["price"], a.get("liq") or 0.0
            tx5 = (a.get("buys5") or 0) + (a.get("sells5") or 0)
            peak, peak_tx = max(peak or price, price), max(peak_tx or 0, tx5)
            reason = None
            # ladder take-profits (each level sells 1/3 of the ORIGINAL position)
            ladder = spec.get("ladder")
            if ladder:
                while lvl < len(ladder) and price >= ep * (1 + ladder[lvl]) and rem > 1e-9:
                    part = min(1.0 / len(ladder), rem)
                    legs.append([part, price, liq])
                    rem -= part
                    lvl += 1
                if rem <= 1e-9:
                    reason = "all_targets"
                elif price <= ep * (1 - spec["stop"]):
                    reason = "stop_loss"
            if reason is None and spec.get("tp") is not None:          # full close at target or stop
                if price >= ep * (1 + spec["tp"]):
                    reason = "take_profit"
                elif price <= ep * (1 - spec["stop"]):
                    reason = "stop_loss"
            if reason is None and spec.get("guard") and held >= g["min_hold_min"]:
                expected = el * np.sqrt(price / ep) if el and ep else 0
                if liq < g["drained_usd"] or (expected and liq < (1 - g["liquidity_drop_frac"]) * expected) or tx5 == 0:
                    reason = "guard"
            if reason is None and held >= hold:
                reason = "time"
            if reason:
                if rem > 1e-9:
                    legs.append([rem, price, liq])
                self.finish(pid, now, legs, reason)
            else:
                self.db.execute("UPDATE xs_positions SET peak=?, peak_tx5=?, remaining=?, next_level=?, legs=?, "
                                "last_poll_ts=? WHERE id=?", (peak, peak_tx, rem, lvl, json.dumps(legs), now, pid))
        self.db.commit()

    def finish(self, pid, now, legs, reason):
        self.db.execute("UPDATE xs_positions SET status='closed', exit_ts=?, exit_reason=?, legs=?, remaining=0, "
                        "last_poll_ts=? WHERE id=?", (now, reason, json.dumps(legs), now, pid))


# ---------------------------------------------------------------------- report
def report_xs(db, rules, h):
    try:
        rows = db.execute("SELECT horizon, book, variant, entry_price, entry_liq, legs, status, entry_ts, fee_est, dex "
                          "FROM xs_positions WHERE rules_hash=?", (h,)).fetchall()
        last_start = db.execute("SELECT MAX(start_ts) FROM runs").fetchone()[0] or 0
        decs = db.execute("SELECT horizon, COUNT(*), AVG(n_eligible) FROM xs_decisions WHERE rules_hash=? "
                          "GROUP BY 1", (h,)).fetchall()
    except Exception:
        return
    c = rules["costs"]
    print(f"\n========== CROSS-SECTIONAL MOMENTUM - {rules.get('tag', 'XS')} ({rules.get('version')}, hash {h}) ==========")
    if rules.get("note"):
        print("  " + rules["note"])
    for hz, nd, avg in decs:
        print(f"  {hz}: {nd} ranking slots, avg {avg or 0:.1f} eligible coins per slot")

    def stats(sel, size):
        rr = [net_legs(size, r[3], r[4], json.loads(r[5]), c["fee_per_side"], c["tx_usd"]) for r in sel]
        rr = np.array([x for x in rr if x is not None])
        return rr

    def gross(sel):
        out = []
        for r in sel:
            legs = json.loads(r[5])
            out.append(sum(f * (p / r[3] if p else 0.0) for f, p, _ in legs) - 1 if r[3] else np.nan)
        return np.array(out)

    def real(sel, size=100):
        rr = [net_legs(size, r[3], r[4], json.loads(r[5]), r[8], c["tx_usd"]) for r in sel if r[8] is not None]
        return np.array([x for x in rr if x is not None])

    hdr = f"  {'horizon':<7} {'book':<12} {'exit':<11} {'n':>4} {'gross med':>9} |"
    for s in c["sizes_usd"]:
        hdr += f" ${s:g}: med / win / capped mean |"
    hdr += " $100 REAL fees: n / med / win / capped mean |"
    print(hdr)
    for hz in rules["horizons"]:
        for book, spec in rules["books"].items():
            for var in spec["variants"]:
                sel = [r for r in rows if r[0] == hz and r[1] == book and r[2] == var and r[6] == "closed"]
                inter = sum(1 for r in rows if r[0] == hz and r[1] == book and r[2] == var and r[6] == "interrupted")
                if not sel:
                    continue
                gr = gross(sel)
                line = f"  {hz:<7} {book:<12} {var:<11} {len(sel):>4} {np.nanmedian(gr) * 100:>+8.1f}% |"
                for s in c["sizes_usd"]:
                    rr = stats(sel, s)
                    line += f" {np.median(rr) * 100:+6.1f}% / {(rr > 0).mean():4.0%} / {np.clip(rr, None, 9).mean() * 100:+6.1f}% |"
                rf = real(sel)
                line += (f" {len(rf):>4} / {np.median(rf) * 100:+6.1f}% / {(rf > 0).mean():4.0%} / "
                         f"{np.clip(rf, None, 9).mean() * 100:+6.1f}% |" if len(rf) else "    - (older trades) |")
                drained = sum(1 for r in sel if any((l[2] or 0) < 1000 for l in json.loads(r[5])))
                if drained:
                    line += f" drained exits {drained}"
                if inter:
                    new = sum(1 for r in rows if r[0] == hz and r[1] == book and r[2] == var
                              and r[6] == "interrupted" and r[7] >= last_start)
                    line += f" (interrupted {inter}, since restart {new})"
                print(line)
        top = [r for r in rows if r[0] == hz and r[1] == "top10pct" and r[2] == "hold_h" and r[6] == "closed"]
        bot = [r for r in rows if r[0] == hz and r[1] == "bottom10pct" and r[2] == "hold_h" and r[6] == "closed"]
        ctl = [r for r in rows if r[0] == hz and r[1] == "control" and r[2] == "hold_h" and r[6] == "closed"]
        if top and bot:
            def honest(sel):        # drained exit = you couldn't sell -> -100%; cap single wins at +300%
                return np.array([-1.0 if any((l[2] or 0) < 1000 for l in json.loads(r[5])) else min(g, 3.0)
                                 for r, g in zip(sel, gross(sel)) if np.isfinite(g)])
            ht, hb = honest(top), honest(bot)
            hc = honest(ctl) if ctl else np.array([np.nan])
            print(f"  {hz} SPREAD (gross, hold {hz}; drained exits = -100%): MEDIAN winners {np.median(ht) * 100:+.2f}% | "
                  f"control {np.median(hc) * 100:+.2f}% | losers {np.median(hb) * 100:+.2f}%   MEAN winners "
                  f"{ht.mean() * 100:+.2f}% | control {np.nanmean(hc) * 100:+.2f}% | losers {hb.mean() * 100:+.2f}%")
    print("  READ: momentum is real only if winners beat BOTH the control and the losers, consistently.")
    print("  REAL fees = each pool's actual DEX fee (PumpSwap 1.25% per side for small coins, others ~0.3%);")
    print("  only trades opened with this version have it.")
    print("  'drained exits' = sold into a pool with < $1,000 liquidity: the gross % is meaningless there")
    print("  (a near-empty pool shows absurd prices); the $ columns are the honest result.")
