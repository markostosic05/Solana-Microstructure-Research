"""
Holder / dev snapshots via Helius (Solana RPC) - LOGGED ONLY, never used as a filter (rules stay frozen).

Runs in a background thread, so a slow Helius call can never delay the trader's price checks.

For a token at decision time it records:
  supply          total token supply
  pool_tokens     tokens in the pool (its token account if found among the 20 largest, else liquidity/2/price)
  program_share   share of supply held by PROGRAM accounts (pools, lockers, bonding curves) - not people
  top1_share      biggest WALLET's share of supply (program accounts excluded)
  top10_share     top-10 wallets' share of supply
  top1_vs_pool    biggest wallet's tokens / tokens in the pool  -> DUMP RISK, the number to watch.
                  If that wallet sells everything into the pool, price falls to about 1/(1+x)^2:
                  0.3 -> ~59% of price,  0.5 -> ~44%,  1.0 -> ~25%,  2.0 -> ~11%
  dev, dev_share  creator wallet (signer of the token's first transaction), best effort, and its share
                  if it is still among the 20 largest accounts (dev_in_top20 = 0 -> holds less than #20)

Key: helius_key.txt next to the scripts, containing only the key. The key is never printed or logged.
Cost: ~4 credits per snapshot, +2-3 when the dev is looked up. Free plan = 1M credits/month.
"""

import json
import logging
import queue
import sqlite3
import threading
import time
from pathlib import Path

import requests

log = logging.getLogger("paper")

# ---------------------------------------------------------------- wallet vs program address
# A normal wallet address is a point on the ed25519 curve; program-derived addresses (pool vaults'
# owners, lockers, bonding curves) are deliberately OFF the curve. That tells people from programs.
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_P = 2 ** 255 - 19
_D = (-121665 * pow(121666, _P - 2, _P)) % _P
_I = pow(2, (_P - 1) // 4, _P)


def b58decode(s):
    n = 0
    for ch in s:
        n = n * 58 + _B58.index(ch)
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\0" * (len(s) - len(s.lstrip("1"))) + raw


def b58encode(b):
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    return "1" * (len(b) - len(b.lstrip(b"\0"))) + out


def is_wallet(addr):
    """True if addr is on the ed25519 curve (a keypair = a person/bot), False for a program address."""
    try:
        b = b58decode(addr)
    except (ValueError, TypeError):
        return None
    if len(b) != 32:
        return None
    return _on_curve(b)


def find_program_address(seeds, program_id):
    """Solana PDA derivation (same as the SDKs): first bump from 255 down that gives an off-curve point."""
    import hashlib
    pid = b58decode(program_id)
    for bump in range(255, -1, -1):
        d = hashlib.sha256(b"".join(seeds) + bytes([bump]) + pid + b"ProgramDerivedAddress").digest()
        if not _on_curve(d):
            return b58encode(d), bump
    raise ValueError("no PDA")


# ---------------------------------------------------------------- pump.fun bonding curve
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CURVE_REAL_TOKENS_START = 793_100_000 * 10 ** 6        # tokens sold on the curve until graduation (6 decimals)


def curve_address(mint):
    return find_program_address([b"bonding-curve", b58decode(mint)], PUMP_PROGRAM)[0]


def parse_curve(data_b64):
    """pump.fun bonding-curve account -> dict(progress 0..1, complete, price_sol, creator, ...)."""
    import base64
    import struct
    raw = base64.b64decode(data_b64)
    if len(raw) < 49:
        return None
    vt, vs, rt, rs, supply = struct.unpack_from("<QQQQQ", raw, 8)
    complete = bool(raw[48])
    creator = b58encode(raw[49:81]) if len(raw) >= 81 else None
    return dict(virtual_tokens=vt, virtual_sol=vs, real_tokens=rt, real_sol=rs, supply=supply, complete=complete,
                creator=creator, real_sol_ui=rs / 1e9,
                progress=1.0 if complete else max(0.0, min(1.0, 1 - rt / CURVE_REAL_TOKENS_START)),
                price_sol=(vs / 1e9) / (vt / 1e6) if vt else None)


def _on_curve(b):
    y = int.from_bytes(b, "little")
    sign, y = y >> 255, y & ((1 << 255) - 1)
    if y >= _P:
        return False
    u, v = (y * y - 1) % _P, (_D * y * y + 1) % _P
    x2 = u * pow(v, _P - 2, _P) % _P
    x = pow(x2, (_P + 3) // 8, _P)
    if (x * x - x2) % _P:
        x = x * _I % _P
    if (x * x - x2) % _P:
        return False
    return not (x == 0 and sign == 1)


COLS = ("ts", "ref_ts", "context", "pool", "mint", "supply", "pool_tokens", "vault_found", "program_share",
        "top1_share", "top10_share", "top1_vs_pool", "top1_owner", "dev", "dev_share", "dev_in_top20", "error")


class Helius:
    def __init__(self, db_path, key_path, max_per_sec=4.0, backlog=60):
        key = Path(key_path).read_text().strip() if Path(key_path).exists() else ""
        self.ok = bool(key)
        self._url = f"https://mainnet.helius-rpc.com/?api-key={key}"
        self.db_path = db_path
        self.gap, self.next_ok = 1.0 / max_per_sec, 0.0
        self.calls = self.errors = self.done = self.dropped = 0
        self.recent = {}                         # mint -> last snapshot time (30-min cache)
        self.watcher = None                      # optional WhaleWatcher (whale.py)
        self.q = queue.Queue(maxsize=backlog)
        with sqlite3.connect(db_path, timeout=30) as db:
            ensure_table(db)
        if self.ok:
            threading.Thread(target=self._worker, daemon=True, name="helius").start()

    def __repr__(self):                          # never show the URL (it contains the key)
        return f"<Helius ok={self.ok} calls={self.calls}>"

    # ---------------------------------------------------------- public: queue a snapshot (never blocks)
    def submit(self, context, pool, mint, price, liq, ref_ts, want_dev=True, min_gap_s=1800):
        if not self.ok or not mint:
            return False
        now = time.time()
        if now - self.recent.get(mint, 0) < min_gap_s:
            return False
        self.recent[mint] = now
        try:
            self.q.put_nowait((context, pool, mint, price, liq, ref_ts, want_dev))
            return True
        except queue.Full:
            self.dropped += 1
            return False

    # ---------------------------------------------------------- worker
    def _worker(self):
        self.s = requests.Session()
        db = sqlite3.connect(self.db_path, timeout=30)
        while True:
            job = self.q.get()
            try:
                row = self.snapshot(*job)
                db.execute(f"INSERT INTO holders ({','.join(COLS)}) VALUES ({','.join('?' * len(COLS))})",
                           tuple(row[k] for k in COLS))
                db.commit()
                self.done += 1
                if self.watcher and not row["error"] and row.get("_wallets"):
                    self.watcher.add(row["context"], row["pool"], row["mint"], row["ref_ts"], row.get("_vault"),
                                     row["pool_tokens"], row["top1_vs_pool"], row["_wallets"])
            except Exception as e:                       # never let the thread die
                log.warning("Helius snapshot not saved (%s)", type(e).__name__)

    def rpc(self, method, params):
        for attempt in range(3):
            now = time.monotonic()
            if self.next_ok > now:
                time.sleep(self.next_ok - now)
            self.next_ok = max(now, self.next_ok) + self.gap
            self.calls += 1
            try:
                r = self.s.post(self._url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                                timeout=25)
            except requests.RequestException as e:
                self.errors += 1                          # log only the error TYPE: the message has the URL
                log.warning("Helius network error on %s (%s)", method, type(e).__name__)
                time.sleep(3 * (attempt + 1))
                continue
            if r.status_code == 429:
                time.sleep(3 * (attempt + 1))
                continue
            if r.status_code in (401, 403):
                self.ok = False
                log.warning("Helius refused the key (HTTP %s) - holder logging switched off. Check helius_key.txt",
                            r.status_code)
                raise RuntimeError(f"HTTP {r.status_code}")
            if r.status_code != 200:
                self.errors += 1
                raise RuntimeError(f"HTTP {r.status_code} on {method}")
            body = r.json()
            if "error" in body:
                raise RuntimeError(f"{method}: {str(body['error'].get('message', body['error']))[:120]}")
            return body.get("result")
        raise RuntimeError(f"{method}: no response")

    def creator(self, mint, max_pages=2):
        """Signer of the mint's first transaction. None if history is too long to page through cheaply."""
        before, oldest = None, None
        for _ in range(max_pages):
            opts = {"limit": 1000}
            if before:
                opts["before"] = before
            sigs = self.rpc("getSignaturesForAddress", [mint, opts]) or []
            if not sigs:
                break
            oldest = sigs[-1]["signature"]
            if len(sigs) < 1000:
                break
            before = oldest
        else:
            return None
        if not oldest:
            return None
        tx = self.rpc("getTransaction", [oldest, {"encoding": "json", "maxSupportedTransactionVersion": 0,
                                                  "commitment": "confirmed"}])
        keys = (((tx or {}).get("transaction") or {}).get("message") or {}).get("accountKeys") or []
        return keys[0] if keys else None

    def snapshot(self, context, pool, mint, price, liq, ref_ts, want_dev=True):
        row = {k: None for k in COLS}
        row.update(ts=time.time(), ref_ts=ref_ts, context=context, pool=pool, mint=mint, vault_found=0)
        try:
            supply = float(self.rpc("getTokenSupply", [mint])["value"]["uiAmount"] or 0)
            largest = self.rpc("getTokenLargestAccounts", [mint])["value"] or []
            accts = [a["address"] for a in largest]
            info = (self.rpc("getMultipleAccounts", [accts, {"encoding": "jsonParsed"}])["value"] or []) if accts else []
            amounts = [float(a.get("uiAmount") or 0) for a in largest]
            owners = []
            for i in range(len(amounts)):
                try:
                    owners.append(info[i]["data"]["parsed"]["info"]["owner"])
                except (TypeError, KeyError, IndexError):
                    owners.append(None)
            est = (liq / 2 / price) if liq and price else None
            # the pool's token account: owned by the pool itself, or a program-owned account holding ~the pool stock
            vault = next((i for i, o in enumerate(owners) if o and o == pool), None)
            if vault is None and est:
                near = [i for i, x in enumerate(amounts)
                        if abs(x - est) / est < 0.15 and owners[i] and is_wallet(owners[i]) is False]
                vault = near[0] if near else None
            pool_tokens = amounts[vault] if vault is not None else est
            wallets, program, biggest_acct = {}, 0.0, {}
            for i, (o, x) in enumerate(zip(owners, amounts)):
                if i == vault:
                    continue
                if o and is_wallet(o) is False:
                    program += x                         # other pools, lockers, bonding curves...
                    continue
                key = o or accts[i]
                wallets[key] = wallets.get(key, 0.0) + x
                if x > biggest_acct.get(key, ("", -1))[1]:
                    biggest_acct[key] = (accts[i], x)
            ranked = sorted(wallets.items(), key=lambda kv: -kv[1])
            # for the whale watcher: (token account, wallet, amount) of the biggest wallets
            row["_wallets"] = [(biggest_acct[k][0], k, biggest_acct[k][1]) for k, _ in ranked[:5] if k in biggest_acct]
            row["_vault"] = accts[vault] if vault is not None else None
            top1_owner, top1 = ranked[0] if ranked else (None, 0.0)
            row.update(supply=supply, pool_tokens=pool_tokens, vault_found=int(vault is not None),
                       program_share=program / supply if supply else None,
                       top1_share=top1 / supply if supply else None,
                       top10_share=sum(x for _, x in ranked[:10]) / supply if supply else None,
                       top1_vs_pool=top1 / pool_tokens if pool_tokens else None, top1_owner=top1_owner)
            if want_dev:
                dev = self.creator(mint)
                if dev:
                    d = wallets.get(dev)
                    row.update(dev=dev, dev_in_top20=int(d is not None),
                               dev_share=d / supply if (d is not None and supply) else None)
        except Exception as e:
            row["error"] = str(e)[:200]
        return row


def ensure_table(db):
    db.execute(f"""CREATE TABLE IF NOT EXISTS holders (
        ts REAL, ref_ts REAL, context TEXT, pool TEXT, mint TEXT, supply REAL, pool_tokens REAL,
        vault_found INTEGER, program_share REAL, top1_share REAL, top10_share REAL, top1_vs_pool REAL,
        top1_owner TEXT, dev TEXT, dev_share REAL, dev_in_top20 INTEGER, error TEXT)""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_holders ON holders(pool, ref_ts)")
    db.commit()


# ---------------------------------------------------------------- report
def _risk_rows(rows, label):
    import numpy as np
    print(f"  {label}")
    for lo, hi, lab in ((0, 0.3, "low  <0.3 "), (0.3, 1.0, "mid 0.3-1 "), (1.0, 1e18, "high >1   ")):
        g = np.array([r for r, v in rows if lo <= v < hi])
        if len(g):
            print(f"    dump risk {lab} n={len(g):>4}  median {np.median(g) * 100:+6.1f}%  mean "
                  f"{np.clip(g, None, 9).mean() * 100:+6.1f}%  crashed >50%: {(g < -0.5).mean():4.0%}")


def report_holders(db, grad_hash=None):
    import numpy as np
    ensure_table(db)
    n = db.execute("SELECT context, COUNT(*), SUM(error IS NOT NULL) FROM holders GROUP BY 1").fetchall()
    print("\n========== HOLDERS (Helius) - logged only, not used to decide ==========")
    if not n:
        print("  no snapshots yet (helius_key.txt next to the script? it only snapshots new decisions)")
        return
    for ctx, cnt, err in n:
        print(f"  {ctx}: {cnt} snapshots ({err or 0} with errors)")
    for ctx in ("grad", "xs"):
        r = db.execute("SELECT top1_vs_pool, top10_share, dev_in_top20, dev, program_share FROM holders "
                       "WHERE context=? AND error IS NULL AND top1_vs_pool IS NOT NULL", (ctx,)).fetchall()
        if not r:
            continue
        v = np.array([x[0] for x in r])
        t10 = [x[1] for x in r if x[1] is not None]
        dv = [x[2] for x in r if x[3]]
        print(f"  {ctx}: biggest wallet vs pool tokens  median {np.median(v):.2f} | >0.5: {(v > 0.5).mean():.0%} | "
              f">1: {(v > 1).mean():.0%} | top-10 wallets median {np.median(t10):.0%} of supply"
              + (f" | dev found {len(dv)}, still in top 20: {np.mean(dv):.0%}" if dv else ""))

    # XS: winners' result by dump risk (hold for the horizon, gross)
    rows = db.execute("""SELECT p.entry_price, p.legs, h.top1_vs_pool FROM xs_positions p
        JOIN holders h ON h.pool = p.pool AND h.context='xs' AND ABS(h.ref_ts - p.decision_ts) < 1800
        WHERE p.status='closed' AND p.book IN ('top1','top10pct') AND p.variant='hold_h'
          AND h.error IS NULL AND h.top1_vs_pool IS NOT NULL""").fetchall() if _has(db, "xs_positions") else []
    res = []
    for ep, legs, v in rows:
        if ep:
            res.append((sum(f * (p / ep if p else 0) for f, p, _ in json.loads(legs or "[]")) - 1, v))
    if res:
        _risk_rows(res, "XS winners (hold for the horizon, gross), split by the biggest wallet's dump risk:")
    # graduations: every decided pool, price at minute 60 vs decision price, by dump risk
    q = """SELECT d.price, c.price, h.top1_vs_pool FROM decisions d
        JOIN holders h ON h.pool = d.pool AND h.context='grad' AND h.error IS NULL AND h.top1_vs_pool IS NOT NULL
        LEFT JOIN checkpoints c ON c.pool = d.pool AND c.target_min = 60"""
    res = [((c / p - 1) if c else -1.0, v) for p, c, v in db.execute(q + (" WHERE d.rules_hash=?" if grad_hash else ""),
                                                                     (grad_hash,) if grad_hash else ())
           if p and c is not None]
    if res:
        _risk_rows(res, "Graduations (all decided, pass or fail): price at minute 60 vs minute 12, by dump risk:")


def _has(db, table):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None
