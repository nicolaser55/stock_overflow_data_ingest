# Index pipeline: VIX and VIX3M, daily and 1-minute bars

Downloads VIX and VIX3M from IBKR into their **own** staging and raw folders. The SPY pipeline is not changed: nothing here reads or writes the SPY folders.

## Folders (under the data root, next to the SPY ones)

| Purpose | Path |
|---|---|
| Raw (add-only), one per index and bar size | `store01_rawzone/ibkr_VIX_ohlcv_data/{vix,vix3m}_{1min,daily}/` |
| Staging, same four names | `store01_rawzone/ibkr_VIX_ohlcv_data_incoming/{vix,vix3m}_{1min,daily}/` |
| Logs | `ibkr_VIX_ohlcv_data_incoming/download_log.csv`, `merge_log.csv` |

The research pipeline reads every CSV of the SPY raw folder only, so index files can never be picked up by accident. Wiring the index data into the research
pipeline is a separate, later step.

## Run

```
python scripts/download_ibkr_index.py --list                        # plan only, no connection
python scripts/download_ibkr_index.py --symbols VIX --dates 2024-08-05 2025-04-09     # small trial first
python scripts/download_ibkr_index.py                              # everything missing after the last date present (nothing present: from 2005-10-03 / 2009-08-12)
python scripts/download_ibkr_index.py --from-start                 # also fills gaps anywhere in the history
python scripts/merge_index_staging_into_raw.py                     # dry run
python scripts/merge_index_staging_into_raw.py --apply
```

Options: `--symbols`, `--bars 1min daily`, `--date1`, `--date2`, `--dates`, `--redownload`, `--port`, `--client-id`, `--max-in-flight`.
Repeating a run is safe: finished requests are saved, present dates are skipped.

## What it does

- **1-minute**: one request per NYSE session (UTC end `YYYYMMDD-21:00:00`/`22:00:00` = 17:00 New York, duration `1 D`, TRADES, regular hours). One staging file per session
  `ohlcv_data_YYYYMMDD.csv`.
- **Daily**: one request per calendar year of missing sessions (end = last wanted date + 3 days, or now; duration `2 Y`; bars outside the wanted dates are dropped). One staging file
  per year `ohlcv_data_YYYY.csv`; a later run adds rows to the staged file and never drops rows already there. The timestamp is midnight New York of the date (a label, not a time).
- **Format**: the SPY raw columns (`timestamp, open, high, low, close, volume, created_ts, date`). IBKR's unset volume (2**127 - 1) is written as an empty cell; 0 and negative volumes are kept as received.
- **Checks**: a 1-minute session is "partial" only if minutes are missing inside 09:31-15:59. The first bar, last bar, missing minutes and bars of another date are logged, not judged,
  because the session window changed over the years. A partial session is requested up to 3 times, then saved and flagged. Empty / error sessions are not saved.
- **Finished sessions**: every session before today, and today only from 17:00 New York.
- **Merge** (add-only, dry run by default): 1-minute files are copied as new day files after the checks (positive prices, high/low consistent, no duplicate minutes, no bars of another date,
  no core-hour gaps unless `--include-partial`); a date already in raw is skipped. Daily files are merged into the raw year file with existing rows winning (conflicts counted, backup first).
- The download log also records the ibapi and IB Gateway server version of each attempt.

## Not verified on the real server (first run: use the small trial)

- The 17:00 New York end of 1-minute requests (the probe used 16:30). If a trial session returns fewer bars than the probe did, or bars of another date, the log's `first_bar_str`, `last_bar_str` and `extra_str` show it.
- Whether the daily bar of the end date itself is returned (the +3 day margin avoids relying on it).
- Real request speed: at most about 2 requests per second, so the full 1-minute history takes roughly 1 hour per index or more.
- All behaviour above was tested only against the simulated server (`tests/test_index_pipeline.py`).

## Research notes

- Dates from 2026-05-14 are staged normally; the untouched window of the research must not be evaluated on them.
- The label convention of the 1-minute index bars (start or end of the minute) is still not proven; see `VIX_PROBE.md` and the project doc `claude/VIX_PROBE_RESULTS_2026-10-06.md`.
