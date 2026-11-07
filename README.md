# Solana memecoin market-microstructure research

A live research platform that tests trading ideas on Solana memecoins **with paper trades only**: data pipelines, frozen rules, realistic costs, and honest verdicts. Most ideas failed, and the write-up says why.

Built in Python over Oct–Nov 2026 on public data: [GeckoTerminal](https://www.geckoterminal.com/) (prices, pools), [Helius](https://www.helius.dev/) (Solana RPC and websockets), and [PumpPortal](https://pumpportal.fun/) (real-time new tokens and graduations).

## What it does

| Component | File | Purpose |
| --- | --- | --- |
| Graduation paper trader | `paper_trader.py` | Watches pump.fun tokens graduating to PumpSwap, applies 5 frozen entry rules at minute 12, paper-trades 7 exit variants, logs price checkpoints up to 24h |
| Cross-sectional momentum | `xs_momentum.py` + `rules_xs_*.json` | Every 5 and 15 minutes, ranks trending tokens and paper-buys the top winner, top 10%, bottom 10% and a random control |
| Holder snapshots | `helius.py` | Logs holder concentration at each decision: biggest wallet vs pool depth (dump risk), top-10 share, creator wallet. Tells wallets from program accounts with an ed25519 on-curve check |
| Whale watcher | `whale.py` | Watches the top 3 wallets of each token for an hour over websockets; classifies each balance change as a sell, a transfer or a buy |
| Near-graduation trader | `neargrad.py` | Decodes pump.fun bonding-curve accounts on-chain to track progress, paper-buys at 80/90/95%, compares exits |
| Replay check | `replay_check.py` | Recomputes live decisions from complete historical candles, to separate measurement errors from market changes |

Engineering: adaptive API rate limiting, crash-safe restarts, background threads for slow calls, and simulation tests with fake APIs for every component (they caught several real bugs before going live).

## Findings

**1. Graduation strategy: the backtest edge did not survive.** An event study on historical graduations produced 5 entry rules (about 5% of tokens passing). Live, only 1 of 750 passed. The replay check matched live measurements 98–100%, so the measurement was right and the market had changed. The strategy was retired.

**2. Fees were the hidden killer.** pump.fun's published PumpSwap fee for small tokens is 1.25% per side, not the ~0.3% often assumed, so a round trip costs about 2.5%.

**3. Momentum is real but too small.** Over 400+ paper trades, top 5-minute winners beat a random control by about **1.2% gross per 5 minutes**, which turns negative after real fees.

**4. Holder concentration predicts crashes.** Among trending tokens, those whose biggest wallet held more than the pool's whole token stock crashed more than 50% about **13% of the time, vs about 3%** for low-concentration tokens.

**5. "Exit on the first whale sell" doesn't work.** Watching 1,998 top wallets on 693 tokens: on fresh graduations, **43% of first sells were the whole position at once**, so there's no warning; on older tokens, top wallets trade constantly in small pieces, so a sell carries no signal.

**6. Near-graduation tokens graduate often, but the losers lose almost everything.** Of tokens reaching 80%, 90% and 95% of the bonding curve, **72%, 82% and 95% graduated**, usually within 1–2 minutes. But tokens that stalled were dumped back down the curve (−80% to −90%), and graduated tokens often dumped within a minute of migrating.

**Overall:** on fresh memecoins, fast bots, insiders and fees take most of the available edge. The useful output is the method and the measurements, not a strategy.

## Method

- Rules frozen in JSON files and identified by hash, so results are never mixed across rule versions
- In-sample / out-of-sample splits and placebo tests in the backtests
- Ghost-print filter: a price jump over 3x needs 2 of the next 5 trades to confirm it
- Real costs: venue fees, constant-product price impact at several trade sizes, network fees
- Positions interrupted by downtime are excluded, never guessed

## Run

```
pip install requests numpy websockets
python paper_trader.py            # graduation + XS momentum + holders + whales
python paper_trader.py --report
python neargrad.py                # near-graduation trader (separate terminal)
python neargrad.py --report
```

Holder logging, whales and the near-graduation trader need a free Helius API key in `helius_key.txt` next to the scripts. That file is excluded by `.gitignore`. Never commit it.

Paper trading only: nothing here places real orders or holds a wallet.
