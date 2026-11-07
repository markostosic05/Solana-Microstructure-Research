#!/usr/bin/env python3
"""
One-off probe for the NEAR-GRADUATION idea: where can we find pump.fun coins that are still on the
bonding curve, and can we read their progress? Takes ~2-3 minutes. It changes nothing, trades nothing.

Run from the dexbank folder (can run while paper_trader.py is running - it is gentle with the API):
    python probe_curve.py
Then paste me the whole output.
"""

import asyncio
import json
import time
from pathlib import Path

import requests

from helius import curve_address, parse_curve
from paper_trader import Api, fnum, iso_ts

HERE = Path(__file__).resolve().parent


def line(t=""):
    print(t, flush=True)


def gt_part(api):
    line("=== 1. GeckoTerminal: which DEX ids look like pump.fun? ===")
    pump_ids = []
    for page in range(1, 6):
        d = api.get("/networks/solana/dexes", {"page": page})
        items = (d or {}).get("data") or []
        if not items:
            break
        for it in items:
            did, name = it.get("id"), (it.get("attributes") or {}).get("name")
            if "pump" in (did or "").lower() or "pump" in (name or "").lower():
                pump_ids.append(did)
                line(f"  found dex id '{did}' ({name})")
    if not pump_ids:
        line("  none found in the first 5 pages")

    samples = []
    line("\n=== 2. Pools on the curve DEX (anything pump-like that isn't PumpSwap) ===")
    for did in [d for d in pump_ids if d != "pumpswap"]:
        for sort in ("h24_tx_count_desc", "h24_volume_usd_desc"):
            d = api.get(f"/networks/solana/dexes/{did}/pools", {"page": 1, "sort": sort, "include": "base_token"})
            items = (d or {}).get("data") or []
            line(f"  {did} sort={sort}: {len(items)} pools" + ("" if items else "  (call failed or empty)"))
            for it in items[:20]:
                a, rel = it.get("attributes") or {}, it.get("relationships") or {}
                mint = (((rel.get("base_token") or {}).get("data") or {}).get("id") or "").split("_", 1)[-1]
                samples.append(dict(dex=did, pool=a.get("address"), mint=mint, name=a.get("name"),
                                    price=fnum(a.get("base_token_price_usd")), fdv=fnum(a.get("fdv_usd")),
                                    quote=fnum(a.get("quote_token_price_usd")),
                                    age=(time.time() - (iso_ts(a.get("pool_created_at")) or time.time())) / 60))
            for s in samples[-len(items):][:5]:
                line(f"    {str(s['name'])[:24]:<24} fdv ${s['fdv'] or 0:>10,.0f}  age {s['age']:7.1f} min  "
                     f"mint ..{s['mint'][-8:]}")

    line("\n=== 3. Do curve coins show up in the lists we already use? (dex ids by count) ===")
    for path, params in (("/networks/solana/new_pools", {"page": 1, "include": "dex"}),
                         ("/networks/solana/trending_pools", {"page": 1, "include": "dex", "duration": "5m"})):
        d = api.get(path, params)
        cnt = {}
        for it in (d or {}).get("data") or []:
            dx = (((it.get("relationships") or {}).get("dex") or {}).get("data") or {}).get("id")
            cnt[dx] = cnt.get(dx, 0) + 1
        line(f"  {path.split('/')[-1]:<15} {dict(sorted(cnt.items(), key=lambda kv: -kv[1]))}")
    return samples


def helius_part(samples):
    line("\n=== 4. Helius: read the bonding curve of the sampled coins ===")
    kp = HERE / "helius_key.txt"
    if not kp.exists():
        line("  helius_key.txt not found - skipped")
        return
    url = f"https://mainnet.helius-rpc.com/?api-key={kp.read_text().strip()}"
    uniq = {s["mint"]: s for s in samples if s["mint"].endswith("pump")}      # same coin can appear twice
    pump = list(uniq.values())[:40]
    if not pump:
        line("  no pump.fun mints among the samples - nothing to read")
        return
    curves = [curve_address(s["mint"]) for s in pump]
    try:
        r = requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
                                     "params": [curves, {"encoding": "base64"}]}, timeout=30)
        vals = r.json()["result"]["value"]
    except Exception as e:
        line(f"  Helius call failed ({type(e).__name__})")
        return
    found, near = 0, 0
    for s, cv, v in zip(pump, curves, vals):
        if not v:
            line(f"  {str(s['name'])[:22]:<22} no curve account")
            continue
        c = parse_curve(v["data"][0])
        if not c:
            continue
        found += 1
        near += (not c["complete"]) and c["progress"] >= 0.7
        usd = c["price_sol"] * s["quote"] if (c["price_sol"] and s["quote"] and s["quote"] > 20) else None
        same = "yes" if cv == s["pool"] else "no"
        line(f"  {str(s['name'])[:22]:<22} progress {c['progress']:6.1%}  complete {str(c['complete']):<5} "
             f"SOL in curve {c['real_sol_ui']:6.1f}  price check: curve ${usd or 0:.3g} vs GT ${s['price'] or 0:.3g}  "
             f"GT pool = curve account: {same}")
    line(f"  -> read {found} curves, {near} of them between 70% and 100% (not yet graduated)")


async def pumpportal_part(seconds=20):
    line("\n=== 5. Plan B: PumpPortal free websocket (new coins + live curve data) ===")
    try:
        from websockets.asyncio.client import connect
    except ImportError:
        try:
            from websockets import connect
        except ImportError:
            line("  websockets not installed - skipped")
            return
    from whale import _ssl_context
    got = []
    try:
        async with connect("wss://pumpportal.fun/api/data", ssl=_ssl_context(), open_timeout=15) as ws:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            end = time.time() + seconds
            while time.time() < end and len(got) < 5:
                try:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=max(0.1, end - time.time())))
                except asyncio.TimeoutError:
                    break
                if isinstance(msg, dict) and msg.get("mint"):
                    got.append(msg)
    except Exception as e:
        line(f"  could not connect ({type(e).__name__})")
        return
    line(f"  received {len(got)} new-coin messages in {seconds}s")
    if got:
        line("  fields: " + ", ".join(sorted(got[0].keys())))
        for m in got[:3]:
            line(f"    {str(m.get('name'))[:20]:<20} vSolInBondingCurve={m.get('vSolInBondingCurve')} "
                 f"marketCapSol={m.get('marketCapSol')}")


def main():
    line(f"probe started {time.strftime('%H:%M:%S')}")
    api = Api(4)                                     # gentle: 4 calls/min, fine next to the paper trader
    samples = []
    try:
        samples = gt_part(api)
    except Exception as e:
        line(f"  GeckoTerminal part failed ({type(e).__name__}: {e})")
    try:
        helius_part(samples)
    except Exception as e:
        line(f"  Helius part failed ({type(e).__name__})")
    try:
        asyncio.run(pumpportal_part())
    except Exception as e:
        line(f"  PumpPortal part failed ({type(e).__name__})")
    line(f"\nGeckoTerminal calls used: {api.calls} (429s: {api.r429}). Done - paste me everything above.")


if __name__ == "__main__":
    main()
