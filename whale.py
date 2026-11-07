"""
Whale watcher (paper/research only - it never trades).

After each Helius holder snapshot, it watches the TOP 3 WALLETS' token accounts of that coin for 60
minutes over a Helius websocket. A message only arrives when one of those wallets' balance changes,
so it costs almost nothing. For every change it logs:
  when (minutes after our decision), how much of their bag moved, how big that is versus the pool,
  and whether it was a SELL into the pool or just a TRANSFER to another wallet (checked by looking
  at the transaction: a sale touches the pool).

What we want to learn (python paper_trader.py --report -> WHALES section):
  - how often a top wallet sells within the hour, and how soon
  - is the first sell the WHOLE bag (no warning) or a CHUNK (first sell = warning to get out)?
  - time between the first and second sell = how long we'd have to exit after the warning
Needs:  pip install websockets
"""

import asyncio
import json
import logging
import queue
import sqlite3
import threading
import time

import requests

log = logging.getLogger("paper")

TABLES = """
CREATE TABLE IF NOT EXISTS whale_watch (
    token_account TEXT, pool TEXT, mint TEXT, context TEXT, ref_ts REAL, wallet TEXT, rank INTEGER,
    start_ts REAL, end_ts REAL, start_amount REAL, pool_tokens REAL, top1_vs_pool REAL, status TEXT,
    PRIMARY KEY (token_account, ref_ts));
CREATE TABLE IF NOT EXISTS whale_events (
    ts REAL, token_account TEXT, pool TEXT, mint TEXT, context TEXT, ref_ts REAL, wallet TEXT, rank INTEGER,
    before REAL, after REAL, delta REAL, frac_of_bag REAL, frac_of_pool REAL, kind TEXT, sig TEXT);
CREATE INDEX IF NOT EXISTS idx_whale_events ON whale_events(pool, ref_ts);
"""


def ensure_tables(db):
    db.executescript(TABLES)
    db.commit()


def _ssl_context():
    """Certificate checking stays ON. Uses the same certificate list as requests (certifi), and turns off
    Python 3.13's extra-strict X.509 rule that rejects some valid certificate chains on Windows."""
    import ssl
    try:
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        ctx = ssl.create_default_context()
    if hasattr(ssl, "VERIFY_X509_STRICT"):
        ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    return ctx


def _amount(value):
    """Token amount from an accountNotification value (None/empty = account closed = 0)."""
    if not value or not value.get("lamports"):
        return 0.0
    try:
        return float(value["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"] or 0)
    except (TypeError, KeyError):
        return None


class WhaleWatcher:
    def __init__(self, db_path, helius, watch_min=60, top_n=3, max_subs=150):
        import websockets                                     # noqa: F401  (fail early if missing)
        self.db_path, self.watch_s, self.top_n, self.max_subs = db_path, watch_min * 60, top_n, max_subs
        self._ws_url = helius._url.replace("https://", "wss://")
        self._http_url = helius._url
        self.req = queue.Queue()
        self.active = {}          # token_account -> watch dict
        self.subs = {}            # subscription id -> token_account
        self.events = self.watched = self.skipped = self.reconnects = 0
        self.connected = False
        with sqlite3.connect(db_path, timeout=30) as db:
            ensure_tables(db)
        threading.Thread(target=self._thread, daemon=True, name="whales").start()

    def __repr__(self):
        return f"<WhaleWatcher watching={len(self.active)} events={self.events}>"

    # called from the Helius thread after a snapshot
    def add(self, context, pool, mint, ref_ts, vault_account, pool_tokens, top1_vs_pool, wallets):
        for rank, (acct, wallet, amount) in enumerate(wallets[:self.top_n], 1):
            self.req.put(dict(token_account=acct, pool=pool, mint=mint, context=context, ref_ts=ref_ts,
                              wallet=wallet, rank=rank, amount=amount, pool_tokens=pool_tokens,
                              top1_vs_pool=top1_vs_pool, vault=vault_account))

    # ------------------------------------------------------------------ thread / event loop
    def _thread(self):
        self.db = sqlite3.connect(self.db_path, timeout=30)
        self.http = requests.Session()
        # anything left 'watching' from an earlier run was cut short
        self.db.execute("UPDATE whale_watch SET status='cut_short' WHERE status='watching'")
        self.db.commit()
        asyncio.run(self._main())

    async def _main(self):
        try:
            from websockets.asyncio.client import connect
        except ImportError:                                   # older websockets
            from websockets import connect
        backoff = 2
        extra = {"ssl": _ssl_context()} if self._ws_url.startswith("wss://") else {}
        while True:
            try:
                async with connect(self._ws_url, ping_interval=20, ping_timeout=20, max_size=2 ** 22,
                                   open_timeout=20, **extra) as ws:
                    self.connected, backoff = True, 2
                    self.subs.clear()
                    for acct in list(self.active):            # (re)subscribe everything still in its window
                        self.active[acct]["sub"] = None       # old subscription ids died with the old connection
                        await self._subscribe(ws, acct)
                    await self._loop(ws)
            except Exception as e:                            # log only the type: messages can contain the URL
                self.connected = False
                self.reconnects += 1
                why = getattr(e, "verify_message", None) or getattr(e, "reason", None)   # SSL detail, never the URL
                log.warning("whale watcher: connection lost (%s%s), reconnecting in %ds", type(e).__name__,
                            f": {why}" if isinstance(why, str) else "", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _subscribe(self, ws, acct):
        w = self.active[acct]
        w["req_id"] = rid = (hash(acct) & 0x7FFFFFFF) or 1
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, "method": "accountSubscribe",
                                  "params": [acct, {"encoding": "jsonParsed", "commitment": "confirmed"}]}))

    async def _loop(self, ws):
        next_expire = 0.0
        while True:
            # new watch requests
            while True:
                try:
                    w = self.req.get_nowait()
                except queue.Empty:
                    break
                if w["token_account"] in self.active:
                    continue
                if len(self.active) >= self.max_subs:
                    self.skipped += 1
                    continue
                w.update(start_ts=time.time(), last=w["amount"], sub=None, sells=0)
                self.active[w["token_account"]] = w
                self.watched += 1
                self.db.execute("INSERT OR REPLACE INTO whale_watch VALUES (?,?,?,?,?,?,?,?,NULL,?,?,?,'watching')",
                                (w["token_account"], w["pool"], w["mint"], w["context"], w["ref_ts"], w["wallet"],
                                 w["rank"], w["start_ts"], w["amount"], w["pool_tokens"], w["top1_vs_pool"]))
                self.db.commit()
                await self._subscribe(ws, w["token_account"])
            # expire finished watches
            now = time.time()
            if now >= next_expire:
                next_expire = now + 5
                for acct, w in list(self.active.items()):
                    if now - w["start_ts"] >= self.watch_s:
                        if w.get("sub") is not None:
                            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "accountUnsubscribe",
                                                      "params": [w["sub"]]}))
                            self.subs.pop(w["sub"], None)
                        self.db.execute("UPDATE whale_watch SET status='done', end_ts=? WHERE token_account=? "
                                        "AND ref_ts=?", (now, acct, w["ref_ts"]))
                        del self.active[acct]
                self.db.commit()
            # messages
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            msg = json.loads(raw)
            if "id" in msg and "result" in msg and isinstance(msg["result"], int):
                for acct, w in self.active.items():           # subscription confirmed
                    if w.get("req_id") == msg["id"] and w.get("sub") is None:
                        w["sub"] = msg["result"]
                        self.subs[msg["result"]] = acct
                        break
            elif msg.get("method") == "accountNotification":
                p = msg.get("params") or {}
                acct = self.subs.get(p.get("subscription"))
                if acct and acct in self.active:
                    amt = _amount((p.get("result") or {}).get("value"))
                    if amt is not None:
                        await self._change(self.active[acct], amt)

    async def _change(self, w, amt):
        before = w["last"]
        if before is None or abs(amt - before) <= 1e-9 * max(before, 1):
            return
        w["last"] = amt
        delta = amt - before
        kind, sig = "buy" if delta > 0 else "decrease", None
        if delta < 0:
            kind, sig = await asyncio.get_running_loop().run_in_executor(None, self._classify, w)
        self.events += 1
        bag = w["amount"] or before or 1.0
        self.db.execute("INSERT INTO whale_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (time.time(), w["token_account"], w["pool"], w["mint"], w["context"], w["ref_ts"],
                         w["wallet"], w["rank"], before, amt, delta, abs(delta) / bag,
                         abs(delta) / w["pool_tokens"] if w["pool_tokens"] else None, kind, sig))
        self.db.commit()
        if kind == "sell":
            log.info("WHALE SELL  #%d wallet sold %.0f%% of its bag (= %.0f%% of pool tokens) %s min after decision"
                     "  https://dexscreener.com/solana/%s", w["rank"], 100 * abs(delta) / bag,
                     100 * abs(delta) / w["pool_tokens"] if w["pool_tokens"] else float("nan"),
                     f"{(time.time() - w['ref_ts']) / 60:.1f}", w["pool"])

    # ------------------------------------------------------------------ sell or transfer?
    def _rpc(self, method, params):
        r = self.http.post(self._http_url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                           timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        return r.json().get("result")

    def _classify(self, w):
        """A sale touches the pool (or its token vault); a plain transfer doesn't."""
        try:
            for attempt in range(3):
                sigs = self._rpc("getSignaturesForAddress", [w["token_account"],
                                                             {"limit": 1, "commitment": "confirmed"}]) or []
                if sigs:
                    break
                time.sleep(2)
            if not sigs:
                return "decrease", None
            sig = sigs[0]["signature"]
            tx = self._rpc("getTransaction", [sig, {"encoding": "json", "maxSupportedTransactionVersion": 0,
                                                    "commitment": "confirmed"}]) or {}
            keys = set(((tx.get("transaction") or {}).get("message") or {}).get("accountKeys") or [])
            loaded = (tx.get("meta") or {}).get("loadedAddresses") or {}
            keys |= set(loaded.get("writable") or []) | set(loaded.get("readonly") or [])
            if w["pool"] in keys or (w.get("vault") and w["vault"] in keys):
                return "sell", sig
            return "transfer", sig
        except Exception as e:
            log.debug("whale classify failed (%s)", type(e).__name__)
            return "decrease", None


# ---------------------------------------------------------------------- report
def report_whales(db):
    import numpy as np
    ensure_tables(db)
    w = db.execute("SELECT COUNT(*), COUNT(DISTINCT pool || ref_ts), SUM(status='done') FROM whale_watch").fetchone()
    print("\n========== WHALES (top 3 wallets watched for 60 min after each decision) ==========")
    if not w[0]:
        print("  nothing watched yet (needs helius_key.txt and: pip install websockets)")
        return
    print(f"  watched {w[0]} wallets on {w[1]} coins ({w[2] or 0} finished the full hour)")
    kinds = dict(db.execute("SELECT kind, COUNT(*) FROM whale_events GROUP BY 1").fetchall())
    print("  balance changes: " + ", ".join(f"{k} {v}" for k, v in sorted(kinds.items())))
    # per coin (finished watches only): first sell by any watched wallet
    coins = db.execute("""SELECT pool, ref_ts, context, MAX(top1_vs_pool) FROM whale_watch
                          WHERE status='done' GROUP BY pool, ref_ts""").fetchall()
    if not coins:
        return
    for ctx in ("grad", "xs"):
        grp = [c for c in coins if c[2] == ctx]
        if not grp:
            continue
        firsts, gaps, whole, impact = [], [], [], []
        for pool, ref, _, _ in grp:
            ev = db.execute("""SELECT ts, wallet, frac_of_bag, frac_of_pool FROM whale_events
                               WHERE pool=? AND ref_ts=? AND kind='sell' ORDER BY ts""", (pool, ref)).fetchall()
            if not ev:
                continue
            firsts.append((ev[0][0] - ref) / 60)
            whole.append(ev[0][2] >= 0.9)
            if ev[0][3] is not None:
                impact.append(1 / (1 + ev[0][3]) ** 2 - 1)
            same = [e for e in ev[1:] if e[1] == ev[0][1]]
            if same:
                gaps.append((same[0][0] - ev[0][0]) / 60)
        n = len(grp)
        print(f"\n  {ctx}: {n} coins watched a full hour | a top-3 wallet SOLD into the pool: "
              f"{len(firsts)} ({len(firsts) / n:.0%})")
        if firsts:
            print(f"    first sell: median {np.median(firsts):.1f} min after decision | it was the WHOLE bag (>=90%): "
                  f"{np.mean(whole):.0%}" + (f" | est. price hit of that sell: median {np.median(impact) * 100:+.0f}%"
                                             if impact else ""))
            ln = len(whole) - sum(whole)
            if gaps:
                print(f"    same wallet sold again: {len(gaps)} times, median {np.median(gaps):.1f} min later "
                      f"(= the time you'd have after the first chunk)")
            elif ln:
                print(f"    {ln} first sells were chunks, but no second sell inside the hour yet")
        for lo, hi, lab in ((0, 0.3, "low  <0.3"), (0.3, 1.0, "mid 0.3-1"), (1.0, 1e18, "high >1  ")):
            sub = [c for c in grp if c[3] is not None and lo <= c[3] < hi]
            if sub:
                s = sum(1 for pool, ref, _, _ in sub if db.execute(
                    "SELECT 1 FROM whale_events WHERE pool=? AND ref_ts=? AND kind='sell' LIMIT 1",
                    (pool, ref)).fetchone())
                print(f"    dump risk {lab}: {len(sub):>4} coins, top wallet sold within the hour: {s / len(sub):.0%}")
