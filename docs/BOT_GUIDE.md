# SOL-SNIPER — Complete Guide

Everything the bot does, how to start and stop it, how to pick a trading source, and what each filter, exit and setting means.

> **Risk:** this bot trades real SOL on mainnet when `wallet.live: true`. Memecoins can go to zero in seconds. Run small sizes and never use money you can't afford to lose.

---

## 1. What the bot is

A Solana memecoin trading bot for pump.fun and PumpSwap coins, with a web dashboard. It:

- **finds coins** from one or more *sources* (new launches, 5-minute momentum, caller copy-trading, and so on);
- **filters them** with hard rules (age, 5m/1m change, liquidity, optional RugCheck);
- **buys automatically** through the route you choose (Lightning, Local, Pump curve or Jupiter);
- **sells automatically** with take-profits, stops, trails and caller-exit rules;
- **records everything** in a ledger with net PnL after fees, and can email you on open and close.

---

## 2. Starting and stopping

### Start (from the `sol-sniper` folder)

```bash
venv/bin/python -m src.main --mode live-feed --config config/sniper.yaml --duration 0
```

| Option | Meaning |
|---|---|
| `--mode live-feed` | Normal running mode: live chain feed plus dashboard. Other modes: `demo`, `paper`, `live-rpc`, `live`. |
| `--config config/sniper.yaml` | The settings file (section 3). |
| `--duration 0` | Run until stopped. A number runs that many seconds. |
| `--no-ui` | No dashboard. |
| `--max-decodes N` | Cap on transaction decodes per minute (default 30). |

Then open the dashboard at **http://localhost:8080**; on a server, use `http://<server-ip>:8080`. The page talks to the bot over port **8765**.

### Stop and restart

| Action | How | What it does |
|---|---|---|
| **STOP** button | Dashboard → Bot card | Stops scans and new buys. The process and dashboard keep running, and open trades are still managed. **START** resumes. |
| **RESTART** button | Dashboard → Bot card | Restarts the process with the same settings. **It also deletes the session logs in `logs/`.** |
| **AUTO TRADE ON/OFF** | Dashboard → Bot card | Master switch for automatic buys. |
| Full stop | `Ctrl+C` in the terminal, or kill the process | Ends everything. If Ctrl+C hangs, kill it: `pkill -9 -f "src.main --mode live-feed"`. |

> Before a full stop, check **Open positions** on the dashboard. A full stop does not sell open trades.

### First-time setup

1. Python venv with the requirements: `python -m venv venv && venv/bin/pip install -r requirements.txt`.
2. Fill in `config/sniper.yaml`: RPC, Jupiter key, PumpPortal key, email (section 3).
3. Start the bot, then go to **Dashboard → Wallet → Change wallet** and upload your Solana keypair JSON. It's saved as `logs/active-wallet.json` and never leaves the machine.
4. Fund the wallet. Keep at least **0.05 SOL**: each buy needs the buy size plus about 0.002 SOL for token-account rent plus fees.

---

## 3. Configuration files — where every setting lives

| File | What it holds | Who changes it |
|---|---|---|
| `config/sniper.yaml` | Keys, RPCs, live/paper, default source, built-in exit styles | You, by editing; then restart |
| `logs/runtime_state.json` | Everything set on the **Live controls** and **Settings** pages | The dashboard; survives restarts |
| `sqlitebase/sniper.db` | Open trades, hold list, ledger (newest 100 rows), saved settings | The bot |
| `logs/active-wallet.json` | The trading wallet keypair | Dashboard → Change wallet |
| `logs/openholds_watch.json` | Callers you picked on OpenHolds | OpenHolds page |

**Precedence:** a shell `export` of the same name overrides `sniper.yaml`. Settings saved from the dashboard override the yaml defaults on the next start.

### `config/sniper.yaml` sections

| Section | Key | Use |
|---|---|---|
| `wallet` | `live` | `true` = real trades, `false` = paper. |
| | `keypair_path` | Wallet file. Leave as is if you upload it on the dashboard. |
| | `allow_launch` | Lets the Launch source buy. |
| `rpc` | `primary` | **Your paid RPC** (Flux). Buys, sells, and the live price read when public nodes are busy. Not the caller stream (bandwidth cost). |
| | `urls` | Free public RPCs. Used only for scans and history; they rate-limit hard. |
| `jupiter` | `api_key` | Jupiter swaps and quotes (fallback route and dust checks). |
| `pumpportal` | `api_key` | Lightning trading **and** the real-time caller trade feed. The key's wallet needs at least 0.02 SOL or the feed is refused. |
| | `route` | Default buy/sell route: `local`, `lightning`, `pump`, `jupiter`. |
| `jito` | `enabled` | Jito bundle sending. |
| `email` | `resend_api_key`, `from_email`, `to_email` | Open and close emails through Resend. All three must be set. |
| `trading` | `mode` | `paper` or `live`. |
| | `filter_mode` | Default source at first start (section 4). |
| | `max_position_sol` | Buy size. |
| | `max_concurrent` | Maximum open trades. |
| | `min_liquidity_sol` | Skip coins below this liquidity. |
| | `exit_style` | `ath`, `plan` or `classic` (section 6). |
| `exit_styles` | `ath`, `plan`, `classic` | Built-in exit plans. The dashboard can't overwrite them. |
| `dayscalp` | scan, liquidity, volume, 5m range, exits | Dayscalp source rules. |
| `launch` | exits | Launch source exit plan, kept separate from Dayscalp. |

---

## 4. Sources — where coins come from

A **source** is a way of finding coins to buy. Each one has an **Enabled** switch on the **Settings** page, and only enabled sources place buys. You pick the main *profile* in **Live controls → filter mode**.

| Source | Finds coins from | Buys when | Best for |
|---|---|---|---|
| **Launch** (`launch`) | pump.fun create feed: brand-new coins | Coin is 1–15 s old and its early candle is in range (the log shows `buy if 1s +0.10% to +2.0% OR 5m +0.10% to +2% equals 1h`), plus the 5m/1m floor and cap | Sniping the first seconds of a launch |
| **Dayscalp** (`dayscalp`) | DexScreener pump.fun list ranked by 5-minute change | 5m change +25% to +1000%, 5m volume at least $1,000, liquidity at least $25k, buy-side pressure, no blacklisted word (scam/test/rug), optional RugCheck | Riding 5-minute momentum on established coins |
| **Predict** (`predict`) | pump.fun live tape (Predict page) | Tape says OPEN: 5m +4% to +30%, 1h at least -8%, buyers at least 60% of volume and more buyers than sellers, volume at least $300 | Tape-reading entries |
| **Callout** (`callout`) | The **CallOuts** page: coins that tracked callers have open | Caller row is OPEN and its 5m is present and positive. The next coin opens only when the newest Callout trade reaches *Callout next trade %* (default -25%) | Following callers' calls |
| **OpenCopy** (`opencopy`) | The **OpenHolds** page: the callers *you* picked | Coin opened inside the *auto-open window* (default 1m), mint starts or ends with `pump`, caller's unrealized PnL above 0, and the caller's % between 0 and +5% | Copying specific wallets in real time |
| `strict` / `scalp` / `pump_curve` | Older filter profiles | `strict`: mint and freeze authority renounced only. `scalp`: tighter scalp numbers. `pump_curve`: paper only | Testing |

### How to change source

1. **Settings** page → turn **Enabled** on for the source(s) you want and off for the rest.
2. **Dashboard → Live controls → filter mode** → pick the matching profile, for example `opencopy (OpenHolds only)`.
3. Press **Apply settings**.
4. For **OpenCopy**: open **OpenHolds**, search a caller, click the match to add them, and tick **Open existing** if you also want coins they opened in the last hour.

> Launch and Predict don't share a buy slot. Predict never runs during Launch or Dayscalp.

---

## 5. Filters — the rules that can stop a buy

| Filter | Where to set it | Applies to | Effect |
|---|---|---|---|
| **5m / 1m floor** (default -6%) | Settings → Both sources | Launch, Dayscalp | Skip if the 5m **or** the 1m is below this. A missing number also skips. |
| **5m / 1m cap** (default +200%) | Settings → Both sources | Launch, Dayscalp | Skip if either is above this (too late). |
| **Min liquidity** | Live controls | All | Skip thin pools. |
| **Max concurrent** | Live controls | All | No new buy while this many trades are open. |
| **RugCheck** | Settings → Dayscalp | Dayscalp (never Launch) | When on, the RugCheck score must be Good (0–21) or the coin is skipped. Off until you turn it on. |
| **Blacklist keywords** | `sniper.yaml → dayscalp` | Dayscalp | Skips names containing scam, test or rug. |
| **OpenHolds auto-open window** (default 1m) | Settings → Callout | OpenCopy | Auto-buy only picks a coin the caller opened within this window. A clicked **Buy** has no age limit. |
| **Caller PnL 0–5%** | Built in | OpenCopy | Copies only while the caller is slightly up, not after the move has already run. |
| **Bait-buy filter** | Built in | OpenCopy | A caller "buy" under 0.01 SOL is ignored; these are spam entries. |
| **Dust / frozen check** | Built in | All | Coins Jupiter can't sell, or whose tokens the creator froze, are flagged DUST and never re-bought or restored. |

---

## 6. Exits — how and when it sells

### Exit styles (Live controls → Exit style)

| Style | Plan |
|---|---|
| **ath** (default) | Sell half at **+7%**, a quarter at **+25%**, the last quarter at **+75%**. Stop **-6%**. 35% trail from the high. Time stop: after 45 s, if the high is still under +3%. |
| **plan** | Half at **+25%**, trail 30%, stop **-12%**, time stop 60 s. |
| **classic** | Half at **+30%**, the rest at **+80%**, stop **-35%**, time stop 300 s. |
| *custom* | Type a new name, set the numbers, press **Apply**. Pick it again later from the list. |

### Live controls fields

| Field | Meaning |
|---|---|
| Max position (SOL) | Buy size. |
| Hard stop | Sell everything at this loss. For example, -0.30 is -30%; 0 is off. |
| First / second / third bank % and fraction | Take-profit levels and how much to sell at each. |
| Runner floor | Lowest the last part may fall to after the banks; 0 is breakeven. |
| Fallback arm | Touched this %, then fell back under it before the sale filled: sell 100%. |
| Trail starts at / drop from high | Trailing stop; 0 is off. |
| Time stop | Seconds before a flat trade is sold; 0 is off. |
| Max tip SOL | Cap on the priority/Jito tip. |

### Copy-trading exits (Settings → Callout, Copy speed & protection)

| Setting | Default | Meaning |
|---|---|---|
| **Follow caller exit** | On | When the live stream sees the caller sell the whole coin, sell your copy in full at once. |
| **Copy stop-loss %** | -15 | Callout and OpenCopy trades sell in full at this % since buy; 0 is off. |
| **Dropoff** | Off | On: caller sold means sell now. Off: under +1% the trade is kept; at +1% or more it sells. |
| **Caller drop recovery %** | +7 | After the caller sold and the trade went negative, sell when it gets back to this. |
| **Callout/OpenCopy trailing** | Off | Use the Live controls trail for copy trades. |
| **Callout sell indication** | 0 (off) | Sell a Callout trade by age, for example `5m` or `1h`. |

### Other sell controls

| Control | Where | Meaning |
|---|---|---|
| **Fast pace sell** | Settings | On: sell 100% as soon as Since Buy reaches the margin (default +5%), for every source. |
| **Sell now (-1)** | Dashboard | Immediately sells every open trade of the selected source, even at a loss. |
| **Sell** button | OpenHolds and Open positions rows | Sells that one coin now. |

---

## 7. Buy and sell routes (Settings → Buy and sell route)

| Route | How it works | Speed |
|---|---|---|
| **Lightning** | PumpPortal builds **and sends** the transaction in one call. Needs the PumpPortal key. | Fastest |
| **Local** | PumpPortal builds it, the bot signs it, your paid RPC sends it. | Fast |
| **Pump curve** | The bot builds the bonding-curve transaction itself. Curve coins only. | Medium |
| **Jupiter** | Quote, then build, sign and send. Rate-limited. | Slowest |

The selected route runs first. If it returns no signature, the bot falls back (Jupiter last). A rejected simulation spends no fee; a transaction that fails on-chain does cost the network fee.

---

## 8. Live caller data and prices (OpenHolds)

- **Caller stream.** Two free feeds run at the same time and each trade is counted once:
  - **PumpPortal** (funded key). Fastest, about 0.1 s ahead, but it skips some trades, and on custom-curve coins its SOL amount is wrong. Its price is used only for coins where it has matched the chain.
  - **Public Solana logs** (`api.mainnet-beta`) on callers' wallets, coins you hold, caller-opened coins, then the most active coins (40 by default; set `SOL_SNIPER_STREAM_RPC_MINTS`). These are exact on-chain amounts, and in testing they arrived about 0.18 s before the paid RPC.
  - The paid RPC websocket is **off**. It used about 18 KB per trade, which was 12 GB in 5 hours. Turn it on only with `SOL_SNIPER_STREAM_PAID=1`.
  - The price poller skips coins the stream priced in the last 2 s.
- **Prices match pump.fun.** Live prices come from each trade's actual swap rate. Coins on custom curves, and newer PumpSwap pools, are scaled to pump.fun's own price after two matching checks. In testing, the live rows matched pump.fun within about 1% (median 0.08%).
- **What gets live prices.** The 48 most active coins get a 0.2 s price read; the top 150 get instant trade updates. Ranking puts coins the bot holds first, then the most recently traded. Dead coins keep pump.fun's API value; with no trades, it's still correct.
- **Flashing.** A cell flashes green or red each time its value changes.
- **Real-time caller stream switch** (Settings → Copy speed & protection) turns the stream off; it then uses the polled books only. Takes effect after a restart.

---

## 9. Dashboard pages

| Page | What it shows |
|---|---|
| **Dashboard** | Bot controls, pipeline counters, performance, Sell now, **Live controls**, win rate, daily / live / 30-day / total PnL, live chart, activity, verdict mix, action log, wallet panel (balance, coins, Change wallet), held token, open positions. |
| **Trade Ledger** | Every buy and sell with SOL in and out, fees and tip, cost, **net PnL after fees**, % since buy, ATH, reason and transaction link. Export to Excel. |
| **Trade History** | Event-by-event trade log: buy, partial sells, sell, with why each happened. Export to Excel. |
| **Execution History** | The wallet's latest 50 on-chain transactions, read from the chain itself. |
| **All Coins** | Every mint seen, with its last verdict. |
| **Decoded Tokens** | Every scanned coin, newest first, with verdict, liquidity, ATH, authorities and the reason. |
| **CallOuts** | Tracked callers' open coins (the Callout source list). Open bot trades are pinned on top. |
| **OpenHolds** | Your picked callers' open coins, live, with Buy and Sell per row (the OpenCopy source). |
| **Live Trades** | A card per open trade, removed when it sells or goes to dust. |
| **Predict** | Live pump.fun tape with an OPEN / CLOSE / FADE / WAIT call and confidence per coin. |
| **Settings** | Route, fast pace sell, 5m floor and cap, source switches (Launch, Predict, Dayscalp, Callout), RugCheck, copy protection. |

**Bot card buttons:** START · STOP · RESTART · AUTO TRADE ON/OFF · **CLEAR SESSION**. CLEAR SESSION resets session PnL and stats and deletes session logs and your OpenHolds caller picks. Open trades are dropped from the book **without selling**, so close them first.

---

## 10. Reading common log lines

| Log line | Meaning |
|---|---|
| `CALLER STREAM connected — PumpPortal` | Fast caller feed is up. |
| `PumpPortal refused the trade feed` / `sent no trades in 20s` | Key not funded or the feed is silent; the bot moved to the RPC feed. |
| `OPENHOLDS <coin> — new open … Buying.` | A copy buy was triggered. |
| `BUY live OK … sol=…` / `SELL live OK … sol_out=…` | Buy or sell confirmed on-chain. |
| `BUY downsize 0.0150 → 0.0122` | Wallet too small for the full size; it bought less. |
| `COPY STOP … hit -15%` | Copy stop-loss fired. |
| `CALLOUT TOUCH … target +33%` | Take-profit reached. |
| `DUST …` / `FLAG DUST` | Coin is worthless or unsellable; left in the wallet and never retried. |
| `LEDGER no sale booked … PnL unchanged` | Normal: a dust coin isn't counted as a sale. |
| `PRICE SCALE <coin> live x…` | That coin's live price was scaled to match pump.fun. |
| `HTTP 429` | A free public RPC or pump.fun is rate-limiting. Trading uses the paid RPC. |
| `TX sell … Custom 6022` / `Custom 6025` | The sell was rejected; usually there were no tokens left to sell. |
| `AccountFrozen (0x11)` | The coin's creator froze holders' tokens (scam). Unsellable. |

---

## 11. Tips for better results

- **Pick callers who actually win.** Speed only makes a profit if the caller's own trades make one; a fast copy of a losing caller just loses faster. Watch a caller's closed trades on pump.fun before adding them.
- **Keep the wallet funded** (0.05 SOL or more) so buys aren't cut down by rent and fees.
- **Fund the PumpPortal key** with at least 0.02 SOL for the fastest caller feed and Lightning route.
- **Stop width is a trade-off.** A -15% copy stop can sell into a 2–3 second dip; a wider one (-25%) avoids that but loses more on real dumps.
- **Use Clear Session sparingly.** It wipes your caller picks and drops open trades without selling them.
- **Start small.** Raise `max_position_sol` only after a run of winning sessions.

---

## 12. Deploying to a server

1. Copy `deploy/caller_stream_update.tar.gz` and `deploy/install_caller_stream.sh` into the server's sol-sniper folder.
2. On the server: `bash install_caller_stream.sh caller_stream_update.tar.gz`. This backs up the old files to `backup-<date>/` and checks that the new ones compile.
3. Restart the bot. To roll back: `cp -r backup-<date>/src .` and restart.
