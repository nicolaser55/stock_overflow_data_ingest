# VIX Probe (read-only)

Finds out what IBKR offers for `VIX` and `VIX3M` **before** a download pipeline is built. It writes nothing to the raw or staging
folders. It only asks IBKR questions and saves the answers as two small CSV files in `probe_output/` (ignored by git).

## Files (all new; nothing existing is overwritten)

| File | Role |
|---|---|
| `ingest/index_probe.py` | `ProbeApp` (a subclass of `IbkrApp` with contract details, head timestamp and free-form bar requests), the plan, the run, the diagnosis |
| `scripts/probe_ibkr_vix.py` | Command line script |
| `tests/test_vix_probe.py` | Tests against a simulated IBKR server (no connection) |
| `VIX_PROBE.md` | This file |

## Run it

1. Start IB Gateway, log in, and check that the API is enabled on port 4001 (live) or 4002 (paper).
2. Open a terminal in the workspace folder and run:

```
venv-ingest\Scripts\python tests\test_vix_probe.py
venv-ingest\Scripts\python scripts\probe_ibkr_vix.py --list
venv-ingest\Scripts\python scripts\probe_ibkr_vix.py
```

The first command tests the probe on a simulated server. The second lists the 36 requests without connecting. The third runs
them (1 to 2 minutes; the screen now shows the IBKR error code and text of every failed request). Other options: `--symbols VIX`, `--dates 2010-05-06 2012-08-01`, `--port 4002`, `--no-control`, `--no-extended`.

## What it asks, per index

1. **Contract details** (IND, CBOE, USD): does the contract exist; trading hours, time zone, exchanges.
2. **Earliest data point** (reqHeadTimeStamp).
   **Latest bars without an end date** (daily and 1-minute). Only counts and times are reported, never prices (the latest bars are inside the untouched window).
3. **Daily bars and 1-minute bars** on six sample sessions: 2005-01-04, 2007-12-04 (VIX3M start), 2008-10-10, 2015-08-24, 2020-03-16, 2026-05-13.
4. **1-minute bars including extended hours** on the last sample date (is the index also calculated before 09:30?).
5. **Control**: SPY on the last sample date. If SPY fails too, the problem is the connection, not VIX.

Sample dates from 2026-05-14 on (the untouched window of the research) are refused unless `--allow-untouched-window` is given.

## Reading the answer

The screen shows one line per request and a **Verdict**. The CSV has the details (`error_code`, `error_text`, first and last bar,
missing minutes, volume information). Diagnoses:

| Diagnosis | Meaning |
|---|---|
| data returned | IBKR answered with bars (or contract / timestamp) |
| market data permission missing | The account lacks a subscription for this index. Look up the exact package in Client Portal before buying anything |
| no data for this window | IBKR has nothing for that date and data type (before the history starts, or not offered) |
| IBKR error 10314 | IBKR rejected the end date of a request. The probe then retries in other end formats (`end_format` column: `us_eastern`, `america_new_york`, `utc`, `no_timezone`) and the verdict names the one that worked |
| contract not found | Wrong symbol, security type or exchange (`--exchange` changes the exchange) |
| pacing violation / no answer before the timeout | Run again later |

## Not verified

The probe has only been tested against a simulated server. How IBKR actually answers for `VIX` / `VIX3M` with your account
(permissions, history depth, bars per day, volume) is exactly what the first real run will show.

## Alignment check (second probe)

The first run found that the index 1-minute bars start at 09:31 (SPY: 09:30). `scripts/probe_index_alignment.py` checks whether the index bars
are labelled like SPY's, because a one-minute shift would put a minute of the future into a decision at minute t.

| File | Role |
|---|---|
| `ingest/index_alignment.py` | The two checks, the run and the verdict |
| `scripts/probe_index_alignment.py` | Command line script |
| `tests/test_index_alignment.py` | Tests against a simulated server (aligned, +1 shift, -1 delay) |

```
venv-ingest\Scripts\python tests\test_index_alignment.py
venv-ingest\Scripts\python scripts\probe_index_alignment.py --list
venv-ingest\Scripts\python scripts\probe_index_alignment.py
```

1. **Daily vs minute:** per session, does the daily open equal the open of the first minute bar, the daily close the close of the last minute bar,
   and the daily high / low the max / min of the minute highs / lows? Which bar carries the daily open and close?
2. **Lag vs SPY:** the correlation of the index's 1-minute return at label t with SPY's return at label t + lag (-3 .. +3), pooled over six volatile
   sessions (2008-10-10, 2011-08-08, 2015-08-24, 2018-02-05, 2020-03-16, 2024-08-05). Lag +k means the index bar labelled t holds the move of the SPY bar
   labelled t + k (future information at decision minute t). Lag -k means the index is late (stale).

30 requests, about 1 minute. Output: a verdict on screen and three CSV files in `probe_output/`. Dates from 2026-05-14 on are refused.
