# Stock Overflow: Data Ingest Workspace

Downloads SPY 1-minute bars from IBKR into the shared data folder of the Stock Overflow research project
(`//100.123.162.2/stock_overflow_data/`), checks them, and maintains the raw folder that the research pipeline reads
(`store01_rawzone/ibkr_spy_1min/`). Shared code (NYSE schedule, file helpers, data quality, bad tick rule) comes from the
research workspace [`stock_overflow_workspace`](https://github.com/nicolaser55/stock_overflow_workspace) (package `so`),
installed in the same venv.

**Rewritten on 2026-10-06** from the previous folder (root modules + `ibkr_data_stream.ipynb`). What changed and why is in
§6; the old notebooks are kept unchanged in `notebooks/legacy/`.

## 1. Layout

```
stock_overflow_data_ingest/
├── README.md                    this guide
├── pyproject.toml               makes ingest/ importable from any folder (pip install -e .)
├── setup_venv.py, requirements/ environment (Python 3.12.0, versions pinned to the research workspace)
├── ingest/                      THE PACKAGE
│   ├── config.py                every constant: data folders, IBKR connection, contract, request / pacing settings
│   ├── ibkr_client.py           IBKR connection: one result per request, callbacks for ibapi 9.81 and 10.x
│   ├── ibkr_download.py         session selection, concurrent download, minute-by-minute check, staging files, log
│   ├── ibkr_stream.py           live 1-minute bars during the session (never merged)
│   ├── sessions.py              New York clock, NYSE sessions, session checks, issue report
│   ├── raw_files.py             file names, text-preserving read / write, roll-up rules, merge engine
│   ├── raw_maintenance.py       merge staging -> raw, organize, clean orphans, rebuild, find issues
│   └── gcs_file_management.py   Google Cloud Storage helpers
├── scripts/                     command line tools (every tool that writes is a dry run unless --apply)
├── notebooks/                   step01 download, step02 live stream, step03 raw data check; legacy/ (old notebooks)
└── tests/                       simulated IBKR server + plain-assert suites (python tests/run_all_tests.py)
```

Data folders (`ingest/config.py`, root overridable with the environment variable `SO_INGEST_DATA_PATH`):

| Folder | Content | Written by |
|---|---|---|
| `store01_rawzone/ibkr_spy_1min/` | raw minute bars read by the research pipeline | `merge_staging_into_raw.py` (add-only), `organize` / `clean` / `rebuild` |
| `store01_rawzone/ibkr_spy_1min_staging/` | **staging**: new downloads, `download_log.csv`, `merge_log.csv` | `download_ibkr_ohlcv.py`, notebook step01 |
| `.../ibkr_spy_1min_staging/merged/` | staging files already added to raw (record) | `merge_staging_into_raw.py --apply` |
| `.../ibkr_spy_1min_staging/live/` | live stream bars, `ohlcv_live_YYYYMMDD.csv` (never merged) | notebook step02 |
| `store01_rawzone/ibkr_spy_1min_backup_YYYYMMDD_HHMMSS/` | byte-for-byte copies of every file changed or removed | every `--apply` that changes a raw file |
| `store01_rawzone/ibkr_vix_family/` | raw VIX / VIX3M bars (`{vix,vix3m}_{1min,daily}/`) | `merge_index_staging_into_raw.py` (add-only) |
| `store01_rawzone/ibkr_vix_family_staging/` | **staging** for index downloads, `download_log.csv`, `merge_log.csv` | `download_ibkr_index.py` |

**Renamed on 2026-10-08:** `ibkr_SPY_ohlcv_data/` → `ibkr_spy_1min/`, `ibkr_SPY_ohlcv_data_incoming/` → `ibkr_spy_1min_staging/`, `ibkr_VIX_ohlcv_data/` → `ibkr_vix_family/`, `ibkr_VIX_ohlcv_data_incoming/` → `ibkr_vix_family_staging/`. Sibling backups use the raw folder name plus `_backup_YYYYMMDD_HHMMSS`. The SPY raw leaf is `so.paths.LOCAL_OHLCV_DATA_FILE_PATH_STR` in the research workspace; that constant is switched to `ibkr_spy_1min` when the folders on the share are renamed.

## 2. Setup (once per machine)

You need Python 3.12, the TWS API (for the ibapi 10.45.1 source) and a clone of `stock_overflow_workspace`.

```
python setup_venv.py --name venv-ingest --requirements venv_ingest_requirements.txt --workspace C:/Users/nico/Desktop/stock_overflow_workspace --ibapi-source "C:/TWS API/source/pythonclient" --kernel
venv-ingest\Scripts\python tests\run_all_tests.py
```

`--workspace` defaults to a sibling folder named `stock_overflow_workspace`; `--ibapi-source` defaults to
`C:/TWS API/source/pythonclient` when it exists. The research workspace is installed in editable mode: after a `git pull`
there, restart the kernel and the ingest uses the new `so` code.

IB Gateway: enable the API (Configure → Settings → API → Settings), port 4001 (live) or 4002 (paper). The downloader,
the live stream and the notebooks use different client ids (1, 2, 3) so they can be connected at the same time.

## 3. Use

```
# 1. Download every closed session after the last one present in raw + staging (or --start / --end / --dates)
venv-ingest\Scripts\python scripts\download_ibkr_ohlcv.py
# 2. Check the staged sessions (dry run), then add them to the raw folder
venv-ingest\Scripts\python scripts\merge_staging_into_raw.py
venv-ingest\Scripts\python scripts\merge_staging_into_raw.py --apply
# 3. Optional: roll up the day files of closed months (dry run, then --apply)
venv-ingest\Scripts\python scripts\organize_ohlcv_data.py
```

Then, in `stock_overflow_workspace` on nicodesktop: `python scripts/fix_raw_bad_ticks.py` (dry run) if the merge
reported bad ticks, and `pipeline/step00_data_quality_check.ipynb`. The same steps are in `notebooks/step01` and `step03`.

| Script | Does | Writes |
|---|---|---|
| `download_ibkr_ohlcv.py` | downloads sessions into staging (`--list` only lists them) | staging only |
| `merge_staging_into_raw.py` | checks staged days (closed session, every minute, candlestick sanity, bad ticks, not already in raw) and adds them as new day files | `--apply` |
| `organize_ohlcv_data.py` | day files of closed months → month file; day / month files of closed years → year file | `--apply` |
| `clean_ohlcv_data.py` | folds left-over day / month files into the month / year file that already exists | `--apply` |
| `rebuild_ohlcv_data.py` | rewrites a folder into the organized layout, into `<raw>_rebuilt/` by default | `--apply` |
| `find_ohlcv_data_issues.py` | missing sessions, missing / extra minutes, duplicates, bars on non-session dates | never (`--save` a CSV) |
| `rename_data_folders.py` | moves the previous raw and staging folders to the names in the table above | `--apply` (dry run writes a manifest only) |
| `gcs_mount.py` | mounts the GCS bucket with rclone (key path in `SO_GCS_KEY_PATH`) | – |

## 4. Rules that protect the raw data

1. **Staging first.** Downloads never go into the raw folder. `merge_staging_into_raw.py` is **add-only**: it never changes
   an existing raw file and never adds a date the raw folder already has (a re-download cannot undo the bad tick
   corrections of `records/bad_tick_corrections.csv`).
2. **Dry run by default.** Every tool that writes needs `--apply`.
3. **Text is preserved.** Files are read and written as text: prices, volumes and timestamps keep their exact characters
   (the old scripts re-wrote every timestamp as UTC text).
4. **Existing rows win.** When two files hold the same minute, the row already in the target file is kept; conflicting
   values are counted and listed.
5. **Backups, atomic writes, all or nothing.** Every changed file is copied into `<raw>_backup_YYYYMMDD_HHMMSS/`; files
   are written to `*.tmp` and renamed; a roll-up first moves its sources into the backup (if the share refuses the
   delete, nothing has changed yet), then writes and re-reads the target, and restores everything if that fails.

## 5. Research notes

- The research pipeline reads **every CSV** of the raw folder. A merge changes its input: re-run pipeline step 00 and the
  bad tick check afterwards. Sessions after 2026-08-13 are after the research's untouched window
  (`docs/RESEARCH_STATE_*` in the research workspace); whether and when they enter the raw folder is your decision, which
  is why they wait in staging.
- `download_log.csv` records every attempt (status, bars, missing minutes, IBKR error, request strings, ibapi version):
  the provenance of each staged session. `merge_log.csv` records what was added to raw and when.
- IBKR does not have every minute of every day: 2007-07-02 is missing and 2009-07-27 / 2013-12-23 are partial in the
  existing raw data. A partial session is retried 3 times, then staged and flagged `partial`; it is merged only with
  `--include-partial`.

## 6. What changed (2026-10-06)

| Old | New | Why |
|---|---|---|
| `datetime_utils.py`, `local_file_management.py`, `data_quality.py` (copies) | `so.core.*` from the research workspace | one source of truth (`local_file_management` was already behind the repo: 3 functions missing; the other two were identical) |
| `ibkr_data_stream.ipynb` (TestApp, `get_date_ohlcv_pdf`) | `ingest/ibkr_client.py`, `ingest/ibkr_download.py`, `notebooks/step01`, `scripts/download_ibkr_ohlcv.py` | bugs below; several requests in flight |
| live streaming cells of the same notebook | `ingest/ibkr_stream.py`, `notebooks/step02` | bugs below |
| `organize_ohlcv_data.py`, `clean_ohlcv_data.py`, `rebuild_ohlcv_data.py`, `find_ohlcv_data_issues.py` | `ingest/raw_maintenance.py` + `scripts/` | shared merge engine, rules of §4, real New York clock |
| `ohlcv_data_quality_code.ipynb` | `notebooks/step03_raw_data_check.ipynb` | |
| `GCS_file_management.py`, `GCS_mount.py`, `GCS_key.json` | `ingest/gcs_file_management.py`, `scripts/gcs_mount.py`; key outside the workspace (`SO_GCS_KEY_PATH`) | the key was inside the folder (and the shared zip): **rotate it** |
| `yf_data_ingestion.ipynb`, `timescale_db.ipynb` | `notebooks/legacy/` | not maintained; their broken `local_file_management` imports fixed; TimescaleDB password from `SO_TIMESCALE_PASSWORD` |
| `requirements.txt` (pandas 3.0.5, ...), `venv_main_requirements.txt` | `requirements/venv_ingest_requirements.txt` | versions pinned to the research workspace; ibapi 10.45.1 from the TWS source |

Bugs fixed (each one is covered by `tests/test_ingest.py`):

1. **Partial days saved as complete.** After the 10 s timeout, the bars received so far were saved as the day's file, and
   access mode `"I"` then skipped that date forever. Now only a request that IBKR finished is saved, every session is
   checked minute by minute, and timeouts / errors are retried (60 s timeout, 3 attempts).
2. **IBKR errors ignored.** An error (no data, pacing violation, no security definition, ...) never released the wait:
   each failed day waited the full timeout, and pacing violations were never backed off.
3. **Answers mixed between requests.** All bars went into one shared list, so a late answer to a timed-out request was
   appended to the next request's bars. Results are now kept per request id.
4. **Error 366 after every request**, caused by `cancelHistoricalData` after `historicalDataEnd`.
5. **Wrong "now".** `convert_to_ny_ts(datetime.now())` labelled the laptop's clock (Riyadh, UTC+3) as New York time:
   7 hours off. Month / year cutoffs could be wrong near midnight, and `find_ohlcv_data_issues.py` could expect bars of a
   session still trading.
6. **Raw files re-written by pandas** in organize / clean / rebuild (timestamps turned into UTC text, number formatting
   could change), against the byte-for-byte convention of the bad tick correction.
7. **Unsafe defaults.** `clean_ohlcv_data.py` and `rebuild_ohlcv_data.py` had `DRY_RUN = False`; the rebuild deleted
   "stale" files by default.
8. **Orphans.** A failed delete after a roll-up left the same data in two files (read twice by the pipeline). Roll-ups are
   now all or nothing.
9. **Live stream.** A leftover `time.sleep(5)  # TESTING` advanced the query minute every 5 s (requesting minutes that had
   not happened); a missed poll lost its minute; the start logic used a global `ny_datetime_now` instead of its argument.
10. **Issue report** did not flag bars on dates that are not sessions; it now does, and it is vectorized.

## 7. Known limitations

- **Not tested against a live IBKR connection** (only against the simulated server of `tests/fake_ibkr.py`, with the
  callback layouts of ibapi 9.81 and 10.45). Run one small download first, e.g.
  `scripts/download_ibkr_ohlcv.py --dates 2026-08-14 --redownload`, and compare it with what you expect.
- The pacing defaults (4 in flight, ≥ 0.5 s apart) follow IBKR's published rules; if the log shows pacing violations,
  lower `--max-in-flight`.
- The research workspace says the raw files on nicodesktop are protected against deletion. Then `organize` / `clean`
  cannot delete day files: they stop with "rolled_back" and change nothing. The add-only merge is unaffected.
- The tools print emoji. When their output is redirected to a file on Windows, set `PYTHONIOENCODING=utf-8` (as the
  research workspace's test runner does).

## 8. Tests and conventions

`python tests/run_all_tests.py` (about a minute; no IBKR connection, temporary folders only). Conventions are those of the
research workspace (`.cursor/rules/code-conventions.mdc` there): an uppercase comment before every code line, a
`# FUNCTION:` header and an Args / Returns docstring per function, the `_pdf` / `_str` / `_list` / `_dict` / `_in`
suffixes, module docstrings after the imports, CRLF line endings for `.py` and `.md`.
