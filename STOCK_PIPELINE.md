# Stock pipeline: AAPL daily and 1-minute bars

Downloads the 1-minute and daily bars of a US stock (AAPL) from IBKR into **its own** staging and raw folders. The SPY pipeline is not changed: nothing here reads or writes the SPY folders.
It adds no new download or merge engine: the 1-minute bars use the SPY engine, the daily bars use the index (VIX) engine, with a stock contract and other folders.

## Folders (under the data root, next to the SPY ones)

| Purpose | Path |
|---|---|
| Raw (add-only), one per stock and bar size | `store01_rawzone/ibkr_aapl_1min/`, `store01_rawzone/ibkr_aapl_daily/` |
| Staging, same names + `_staging` | `store01_rawzone/ibkr_aapl_1min_staging/`, `store01_rawzone/ibkr_aapl_daily_staging/` |
| Logs (inside each staging folder) | `download_log.csv`, `merge_log.csv`, `merged/` (1-minute files already added to raw) |

The research pipeline reads every CSV of the SPY raw folder only, so stock files can never be picked up by accident. Wiring AAPL into the research pipeline is a separate, later step.

## Run

```
python scripts/download_ibkr_stock.py --list                          # plan only, no connection
python scripts/download_ibkr_stock.py --dates 2024-08-05 2025-04-09   # small trial first (1-minute and daily)
python scripts/stock_data_status.py                                   # what is where, what is missing
python scripts/check_stock_daily_vs_minute.py                         # daily bar vs the aggregated 1-minute bars of the same dates (prices, volume ratio)
python scripts/download_ibkr_stock.py --date1 2024-01-02              # everything missing from that date
python scripts/download_ibkr_stock.py                                 # everything missing after the last date present (nothing present: from the start date in ingest/stock_config.py)
python scripts/download_ibkr_stock.py --from-start                    # also fills gaps anywhere in the history
python scripts/merge_stock_staging_into_raw.py                        # dry run
python scripts/merge_stock_staging_into_raw.py --apply
```

Options: `--symbols`, `--bars 1min daily`, `--date1`, `--date2`, `--dates`, `--redownload`, `--port`, `--client-id`, `--max-in-flight`.
Repeating a run is safe: finished requests are saved, present dates are skipped. IB Gateway must be running and logged in (port 4001 live).

## What it does

- **1-minute** (the SPY engine, `ingest/ibkr_download.py`): one request per NYSE session, the session checked minute by minute against the NYSE schedule (390 bars, fewer on early closes), a partial
  session requested up to 3 times and then saved and flagged `partial`, empty / error sessions never saved. One staging file per session `ohlcv_data_YYYYMMDD.csv`.
- **Daily** (the index engine, `ingest/index_download.py`): one request per calendar year of missing sessions (UTC end = last wanted date + 3 days, or now; duration `2 Y`; bars outside the wanted
  dates dropped). One staging file per year `ohlcv_data_YYYY.csv`; a later run adds rows and never drops rows already staged. The timestamp is midnight New York of the date (a label, not a time).
  A missing date makes the year `partial` (retried, saved, flagged).
- **Format**: the SPY raw columns (`timestamp, open, high, low, close, volume, created_ts, date`). Unlike the indices, the **volume is kept**.
- **Finished sessions**: the SPY rule, a session is downloadable 5 minutes after its close.
- **Merge** (add-only, dry run by default): 1-minute files are copied as new day files after the SPY checks (closed NYSE session, every minute present unless `--include-partial`, candlestick
  sanity, bad ticks counted and reported); a date already in raw is skipped. Daily files are merged into the raw year file with existing rows winning (conflicts counted, backup first, into the
  sibling folder `ibkr_aapl_daily_backup_<time>/`). The raw folder is created by the first `--apply`.
- A stock is added with one entry in `STOCK_SPEC_DICT` (`ingest/stock_config.py`): symbol, folder name part, primary exchange, first date to request. Everything else follows from it.
  The 1-minute check assumes a regular NYSE / NASDAQ session (regular trading hours only, `useRTH = 1`).

## Not verified on the real server (first run: use the small trial)

- **The start date.** `start_date_str = 2007-01-03` in `STOCK_SPEC_DICT` is an assumption, not a fact about IBKR. How far back IBKR holds AAPL 1-minute bars is unknown until a trial
  (`--dates` with an old date). A session before IBKR's first bar comes back `no_data` and is requested 3 times, at least 16 s apart, so check a few old dates before a full run.
  An explicit `--date1` / `--dates` is never moved to the start date.
- **Split adjustment.** As far as I know IBKR returns split-adjusted history as of the day of the request (TRADES bars are not dividend-adjusted). If so, files downloaded before a future split stay on the
  old price basis while later downloads are on the new one, because raw files are add-only. AAPL split 7:1 in 2014 and 4:1 in 2020, so a history downloaded now is on the post-2020 basis. Check
  it: compare a downloaded 2019 close with a known unadjusted price, and after any future AAPL split re-download with `--redownload` into a clean folder before using the data. A daily
  `--redownload` of dates already in raw followed by the merge dry run shows `conflict` counts when the values changed.
- **Volume.** The unit of IBKR's stock volume (shares or round lots of 100) is not checked here. It is the same convention as the SPY raw files because the same engine and request are used.
  `scripts/check_stock_daily_vs_minute.py` prints the daily bar next to the aggregated 1-minute bars of the same dates: a volume ratio near 1 and equal prices mean the two downloads agree; a ratio near
  100 or 0.01, or different prices, is a finding to look into.
- The pacing: about 2 requests per second at most, so the full 1-minute history (about 4,900 sessions from 2007) takes roughly 1 hour or more.
- All behaviour above was tested only against the simulated server (`tests/test_stock_pipeline.py`).

## Research notes

- Dates from 2026-05-14 are staged normally; the untouched window of the research must not be evaluated on them.
- A single stock is one more time series: any use of AAPL in a model needs its own check for look-ahead (daily bars are labelled by date, 1-minute bars by the start of the minute) and for survivorship
  and selection (one stock chosen after the fact).
