"""
Stock Pipeline Tests (simulated IBKR server, temporary folders, no connection, no real data folder)

Run from the workspace root:   python tests/test_stock_pipeline.py   (or python tests/run_all_tests.py)

What is verified (AAPL; the engines themselves are covered by test_ingest.py and test_index_pipeline.py):
    1. Folders and contract: a raw and a staging folder per stock and bar size, in the raw zone next to the SPY folders and inside none of them, the log paths,
       the STK / SMART / NASDAQ / USD contract.
    2. Planning: the start date of the stock when nothing is present, the day after the last date present, an explicit first date that is never moved to the
       start date, present dates skipped, --from-start filling gaps, the closed-session rule (SPY rule), dates that are not sessions, one daily request per year.
    3. The 1-minute download: the AAPL contract on the wire, a complete session, a gap (retried, saved and flagged partial), an empty session (not saved), the
       volume kept, the SPY file format and download log, nothing written to the SPY folders.
    4. The daily download: the AAPL contract, a year chunk with a missing date (partial), the volume kept (not blanked), the second run adding the date.
    5. The merge: dry run changes nothing and creates no raw folder; 1-minute files added to a raw folder that does not exist yet (add-only, a date already in
       raw is skipped, price issues / partial refused, --include-partial); daily files merged into the raw year file with existing rows winning, conflicts counted
       and a sibling backup folder; the merge logs.
    6. The status: the folder table, the missing sessions, the log problems; the stock folders are not reported as "other folders".
    7. The scripts: --list needs no connection, the merge dry run and the status run on empty folders.
"""

import os
import sys
import tempfile
import subprocess
from decimal import Decimal

# ADD THE TESTS FOLDER TO THE PATH AND IMPORT THE SIMULATED SERVERS (THIS ALSO POINTS THE DATA ROOT TO A TEMPORARY FOLDER)
TESTS_PATH_STR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_PATH_STR)
from test_vix_probe import passed
from test_index_pipeline import FakeIndexApp
from fake_ibkr import FakeIbkrApp

import pandas as pd
# IMPORT THE STOCK PIPELINE
from ingest import config
from ingest import stock_config
from ingest.ibkr_download import DOWNLOAD_LOG_COL_STR_LIST
from ingest.index_download import INDEX_DOWNLOAD_LOG_COL_STR_LIST
from ingest.index_merge import INDEX_MERGE_LOG_COL_STR_LIST
from ingest.index_status import get_other_folder_pdf
from ingest.raw_maintenance import MERGE_LOG_COL_STR_LIST
from ingest.stock_download import get_stock_session_pdf, get_stock_daily_task_list, download_stock_minute_pdf, download_stock_daily_pdf
from ingest.stock_merge import merge_stock_staging_into_raw_pdf
from ingest.stock_status import get_stock_status_pdf, get_stock_missing_date_list, get_stock_log_problem_pdf

# DEFINE THE CLOCK OF THE TESTS (A TUESDAY EVENING, AFTER THE CLOSE)
NOW_TS = pd.Timestamp("2026-10-06 18:00", tz=config.NY_TZ_STR)


# CLASS: SIMULATED 1-MINUTE SERVER THAT RECORDS THE CONTRACT OF EVERY REQUEST
class FakeStockMinuteApp(FakeIbkrApp):
    """
    FakeIbkrApp (SPY engine server) that remembers (symbol, secType, exchange, primaryExchange, currency) of every historical request.
    """

    # METHOD: INITIALIZE
    def __init__(self, *args, **kwargs):
        FakeIbkrApp.__init__(self, *args, **kwargs)
        self.contract_list = []

    # METHOD: RECORD THE CONTRACT AND SIMULATE THE REQUEST
    def reqHistoricalData(self, reqId, contract, *args):
        self.contract_list.append((contract.symbol, contract.secType, contract.exchange, contract.primaryExchange, contract.currency))
        FakeIbkrApp.reqHistoricalData(self, reqId, contract, *args)


# CLASS: SIMULATED DAILY SERVER (INDEX ENGINE SERVER) WITH A REAL STOCK VOLUME
class FakeStockDailyApp(FakeIndexApp):
    """
    FakeIndexApp whose daily bars carry a volume of 12,345,678 shares instead of IBKR's unset value.
    """

    # CALLBACK: RECEIVE ONE HISTORICAL BAR WITH A REAL VOLUME
    def historicalData(self, reqId, bar):
        bar.volume = Decimal(12345678)
        FakeIndexApp.historicalData(self, reqId, bar)


# FUNCTION: BUILD A CONNECTED APPLICATION
def get_app(app_class_in, *args_in):
    app = app_class_in(*args_in)
    assert app.connect_app(), "handshake failed"
    return app


# FUNCTION: DEFINE FOLDERS OF A TEST
def get_folder_tuple(name_str_in):
    base_str = tempfile.mkdtemp(prefix=f"so_stock_{name_str_in}_").replace("\\", "/")
    return f"{base_str}/raw/", f"{base_str}/staging/"


# FUNCTION: WRITE A 1-MINUTE SESSION FILE (09:30 - 15:59, THE FORMAT OF THE SPY RAW FILES)
def write_session_file(folder_str_in, date_str_in, price_list_in=(100.0, 101.0, 99.0, 100.5), skip_list_in=()):
    os.makedirs(folder_str_in, exist_ok=True)
    minute_index = pd.date_range(f"{date_str_in} 09:30", f"{date_str_in} 15:59", freq="1min", tz=config.NY_TZ_STR)
    minute_index = minute_index[[ts.strftime("%H:%M") not in skip_list_in for ts in minute_index]]
    pd.DataFrame({"timestamp": [ts.isoformat(sep=" ") for ts in minute_index], "open": str(price_list_in[0]), "high": str(price_list_in[1]), "low": str(price_list_in[2]),
                  "close": str(price_list_in[3]), "volume": "1000", "created_ts": "", "date": date_str_in}).to_csv(f"{folder_str_in}ohlcv_data_{date_str_in.replace('-', '')}.csv", index=False)


# TEST: FOLDERS AND CONTRACT
def test_folders_and_contract():
    rawzone_str = os.path.dirname(config.RAW_OHLCV_PATH_STR.rstrip("/")) + "/"
    assert stock_config.STOCK_RAWZONE_PATH_STR == rawzone_str
    folder_str_list = [stock_config.get_stock_folder_path_str("AAPL", bar_kind_str, staging_bool) for bar_kind_str in ["1min", "daily"] for staging_bool in [False, True]]
    assert folder_str_list == [f"{rawzone_str}ibkr_aapl_1min/", f"{rawzone_str}ibkr_aapl_1min_staging/", f"{rawzone_str}ibkr_aapl_daily/", f"{rawzone_str}ibkr_aapl_daily_staging/"]
    # NONE OF THEM IS THE SPY FOLDER OR INSIDE IT (THE RESEARCH PIPELINE READS EVERY CSV OF THE SPY RAW FOLDER)
    assert all(not folder_str.startswith(config.RAW_OHLCV_PATH_STR) and not folder_str.startswith(config.STAGING_OHLCV_PATH_STR) for folder_str in folder_str_list)
    assert stock_config.get_stock_log_path_str("AAPL", "1min", "download") == f"{rawzone_str}ibkr_aapl_1min_staging/download_log.csv"
    assert stock_config.get_stock_log_path_str("AAPL", "daily", "merge") == f"{rawzone_str}ibkr_aapl_daily_staging/merge_log.csv"
    assert stock_config.get_stock_folder_name_set() == {"ibkr_aapl_1min", "ibkr_aapl_1min_staging", "ibkr_aapl_daily", "ibkr_aapl_daily_staging"}
    contract = stock_config.get_stock_contract("AAPL")
    assert (contract.symbol, contract.secType, contract.exchange, contract.primaryExchange, contract.currency) == ("AAPL", "STK", "SMART", "NASDAQ", "USD")
    assert stock_config.STOCK_BAR_KIND_LIST == ["1min", "daily"]
    passed("folders and contract: 4 folders next to (not inside) the SPY ones, log paths, AAPL STK / SMART / NASDAQ / USD")


# TEST: PLANNING
def test_planning():
    raw_str, staging_str = get_folder_tuple("plan")

    def plan(bar_kind_str_in="1min", now_ts_in=NOW_TS, **kwargs_in):
        return get_stock_session_pdf("AAPL", bar_kind_str_in, raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=now_ts_in, alert_in=False, **kwargs_in)

    # NOTHING PRESENT: START AT THE START DATE OF THE STOCK (2007-01-03; 2007-01-02 WAS A DAY OF MOURNING, NO SESSION)
    assert [str(d) for d in plan(date2_in="2007-01-09")["date"]] == ["2007-01-03", "2007-01-04", "2007-01-05", "2007-01-08", "2007-01-09"]
    # AN EXPLICIT FIRST DATE IS NEVER MOVED TO THE START DATE
    assert [str(d) for d in plan(date1_in="2006-12-28", date2_in="2007-01-03")["date"]] == ["2006-12-28", "2006-12-29", "2007-01-03"]
    # THE CLOSED-SESSION RULE: A SESSION OF TODAY IS DOWNLOADABLE FROM 5 MINUTES AFTER THE CLOSE
    assert [str(d) for d in plan(date1_in="2026-10-05")["date"]] == ["2026-10-05", "2026-10-06"]
    assert [str(d) for d in plan(date1_in="2026-10-05", now_ts_in=pd.Timestamp("2026-10-06 13:00", tz=config.NY_TZ_STR))["date"]] == ["2026-10-05"]
    # A DAY THAT IS NOT A SESSION IS SKIPPED (2026-09-07 IS LABOR DAY)
    assert [str(d) for d in plan(date_list_in=["2026-09-07", "2026-09-08"])["date"]] == ["2026-09-08"]
    # THE SESSION COLUMNS ARE THOSE THE SPY ENGINE EXPECTS
    assert list(plan(date_list_in=["2026-09-08"]).columns) == ["date", "first_bar_ts", "last_bar_ts", "bar_count_int"] and plan(date_list_in=["2026-09-08"])["bar_count_int"].iloc[0] == 390
    # PRESENT DATES (RAW OR STAGING) ARE SKIPPED, THE DEFAULT START IS THE DAY AFTER THE LAST DATE PRESENT, --from-start FILLS GAPS, --redownload REQUESTS PRESENT DATES
    write_session_file(raw_str, "2026-10-01")
    write_session_file(staging_str, "2026-10-05")
    assert [str(d) for d in plan()["date"]] == ["2026-10-06"]
    assert [str(d) for d in plan(date1_in="2026-09-30", date2_in="2026-10-06")["date"]] == ["2026-09-30", "2026-10-02", "2026-10-06"]
    assert str(plan(from_start_bool_in=True)["date"].min()) == "2007-01-03" and len(plan(from_start_bool_in=True)) > 4900
    assert [str(d) for d in plan(date1_in="2026-10-01", redownload_bool_in=True)["date"]] == ["2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06"]
    # THE DAILY REQUESTS: ONE PER CALENDAR YEAR, END = LAST WANTED DATE + 3 DAYS (UTC), 2 Y, THE AAPL CONTRACT
    daily_pdf = get_stock_session_pdf("AAPL", "daily", date1_in="2025-12-29", date2_in="2026-01-05", raw_path_str_in=raw_str, staging_path_str_in=staging_str,
                                      now_ny_ts_in=NOW_TS, alert_in=False)
    task_list = get_stock_daily_task_list("AAPL", daily_pdf, staging_str, NOW_TS)
    assert [(task["label_str"], len(task["wanted_date_list"]), task["end_datetime_str"], task["duration_str"], task["bar_size_str"]) for task in task_list] == \
           [("2025", 3, "20260103-05:00:00", "2 Y", "1 day"), ("2026", 2, "20260108-05:00:00", "2 Y", "1 day")]
    assert task_list[0]["file_path_str"] == f"{staging_str}ohlcv_data_2025.csv" and task_list[0]["symbol_str"] == "AAPL"
    assert (task_list[0]["contract"].symbol, task_list[0]["contract"].secType, task_list[0]["contract"].primaryExchange) == ("AAPL", "STK", "NASDAQ")
    assert get_stock_daily_task_list("AAPL", daily_pdf.iloc[0:0], staging_str, NOW_TS) == []
    passed("planning: start date, explicit dates, closed rule, present dates, --from-start, --redownload, one daily request per year")


# TEST: THE 1-MINUTE DOWNLOAD
def test_minute_download():
    staging_str = stock_config.get_stock_folder_path_str("AAPL", "1min", True)
    log_str = stock_config.get_stock_log_path_str("AAPL", "1min", "download")
    date_list = ["2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17"]
    app = get_app(FakeStockMinuteApp, {"2026-09-15": ["partial"], "2026-09-16": ["empty"], "2026-09-17": ["error", "ok"]})
    session_pdf = get_stock_session_pdf("AAPL", "1min", date_list_in=date_list, now_ny_ts_in=NOW_TS, alert_in=False)
    summary_pdf = download_stock_minute_pdf(app, "AAPL", session_pdf, timeout_seconds_in=5, pacing_backoff_seconds_in=0.05, same_request_wait_seconds_in=0.05,
                                            min_interval_seconds_in=0, alert_in=False).set_index(session_pdf["date"].astype(str))
    app.disconnect_app()
    # EVERY REQUEST WAS FOR THE AAPL STOCK ON SMART WITH THE NASDAQ PRIMARY EXCHANGE
    assert app.contract_list and set(app.contract_list) == {("AAPL", "STK", "SMART", "NASDAQ", "USD")}
    # THE OUTCOMES: COMPLETE; PARTIAL SAVED AND FLAGGED AFTER 3 ATTEMPTS; EMPTY NEVER SAVED; ERROR THEN OK
    assert summary_pdf.loc["2026-09-14", ["status_str", "saved_bool", "attempt_int"]].tolist() == ["complete", True, 1]
    assert summary_pdf.loc["2026-09-15", ["status_str", "saved_bool", "attempt_int", "missing_count_int"]].tolist() == ["partial", True, 3, 30]
    assert summary_pdf.loc["2026-09-16", ["status_str", "saved_bool", "attempt_int"]].tolist() == ["empty", False, 3]
    assert summary_pdf.loc["2026-09-17", ["status_str", "saved_bool", "attempt_int"]].tolist() == ["complete", True, 2]
    # THE FILES ARE IN THE STAGING FOLDER OF THE STOCK, IN THE SPY FORMAT, THE VOLUME KEPT
    assert sorted(f for f in os.listdir(staging_str)) == ["download_log.csv", "ohlcv_data_20260914.csv", "ohlcv_data_20260915.csv", "ohlcv_data_20260917.csv"]
    text_pdf = pd.read_csv(f"{staging_str}ohlcv_data_20260914.csv", dtype=str, keep_default_na=False)
    assert list(text_pdf.columns) == config.OHLCV_COL_STR_LIST and len(text_pdf) == 390
    assert text_pdf["timestamp"].iloc[0] == "2026-09-14 09:30:00-04:00" and text_pdf["timestamp"].iloc[-1] == "2026-09-14 15:59:00-04:00"
    assert (text_pdf["volume"] != "").all() and text_pdf["volume"].iloc[0] == "1030" and set(text_pdf["date"]) == {"2026-09-14"}
    # THE LOG IS THE SPY DOWNLOAD LOG, ONE ROW PER ATTEMPT
    log_pdf = pd.read_csv(log_str, dtype=str, keep_default_na=False)
    assert list(log_pdf.columns) == DOWNLOAD_LOG_COL_STR_LIST and len(log_pdf) == 1 + 3 + 3 + 2
    # NOTHING WAS WRITTEN TO THE SPY FOLDERS
    assert not os.path.exists(config.RAW_OHLCV_PATH_STR) and not os.path.exists(config.STAGING_OHLCV_PATH_STR)
    # A SECOND PLAN REQUESTS ONLY THE SESSION THAT WAS NOT SAVED
    assert [str(d) for d in get_stock_session_pdf("AAPL", "1min", date_list_in=date_list, now_ny_ts_in=NOW_TS, alert_in=False)["date"]] == ["2026-09-16"]
    passed("1-minute download: AAPL contract, complete / partial / empty / error then ok, volume kept, SPY file format and log, SPY folders untouched")


# TEST: THE DAILY DOWNLOAD
def test_daily_download():
    raw_str, staging_str = get_folder_tuple("daily")
    log_str = f"{staging_str}download_log.csv"
    # 2026-09-01 .. 2026-09-18 HAS 13 SESSIONS (LABOR DAY 09-07); THE SERVER LACKS 09-10 AND HAS A BAR ON LABOR DAY (NOT A SESSION)
    app = get_app(FakeStockDailyApp, None, {"2026-09-10"})
    session_pdf = get_stock_session_pdf("AAPL", "daily", date1_in="2026-09-01", date2_in="2026-09-18", raw_path_str_in=raw_str, staging_path_str_in=staging_str,
                                        now_ny_ts_in=NOW_TS, alert_in=False)
    task_list = get_stock_daily_task_list("AAPL", session_pdf, staging_str, NOW_TS)
    assert len(task_list) == 1 and len(task_list[0]["wanted_date_list"]) == 13
    fast_dict = {"min_interval_seconds_in": 0, "same_request_wait_seconds_in": 0, "timeout_seconds_in": 5, "pacing_backoff_seconds_in": 0.05, "alert_in": False}
    summary_pdf = download_stock_daily_pdf(app, "AAPL", task_list, log_file_path_str_in=log_str, **fast_dict)
    app.disconnect_app()
    assert summary_pdf.loc[0, ["symbol", "bar_kind", "status_str", "saved_bool", "attempt_int", "bar_count_int", "missing_count_int"]].tolist() == ["AAPL", "daily", "partial", True, 3, 12, 1]
    assert set(app.request_log) == {("AAPL", "STK", "SMART", task_list[0]["end_datetime_str"], "2 Y", "1 day", 1, "TRADES", 2)}
    # THE STAGED YEAR FILE: THE SPY COLUMNS, A DATE LABEL AT MIDNIGHT NEW YORK, THE VOLUME KEPT, NO BAR ON LABOR DAY OR ON THE MISSING DATE
    text_pdf = pd.read_csv(f"{staging_str}ohlcv_data_2026.csv", dtype=str, keep_default_na=False)
    assert list(text_pdf.columns) == config.OHLCV_COL_STR_LIST and len(text_pdf) == 12
    assert text_pdf["timestamp"].iloc[0] == "2026-09-01 00:00:00-04:00" and set(text_pdf["volume"]) == {"12345678"}
    assert "2026-09-10" not in set(text_pdf["date"]) and "2026-09-07" not in set(text_pdf["date"])
    log_pdf = pd.read_csv(log_str, dtype=str, keep_default_na=False)
    assert list(log_pdf.columns) == INDEX_DOWNLOAD_LOG_COL_STR_LIST and set(log_pdf["symbol"]) == {"AAPL"} and set(log_pdf["missing_str"]) == {"2026-09-10"}
    # A LATER RUN FOR THE MISSING DATE ADDS IT TO THE STAGED FILE AND KEEPS THE 12 ROWS
    app = get_app(FakeStockDailyApp)
    session_pdf = get_stock_session_pdf("AAPL", "daily", date_list_in=["2026-09-10"], raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)
    assert download_stock_daily_pdf(app, "AAPL", get_stock_daily_task_list("AAPL", session_pdf, staging_str, NOW_TS), log_file_path_str_in=log_str, **fast_dict).loc[0, "status_str"] == "complete"
    app.disconnect_app()
    text_pdf = pd.read_csv(f"{staging_str}ohlcv_data_2026.csv", dtype=str, keep_default_na=False)
    assert len(text_pdf) == 13 and text_pdf["date"].is_monotonic_increasing and "2026-09-10" in set(text_pdf["date"])
    assert get_stock_session_pdf("AAPL", "daily", date1_in="2026-09-01", date2_in="2026-09-18", raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS,
                                 alert_in=False).empty
    passed("daily download: AAPL contract, partial year chunk, volume kept, holiday bar dropped, second run adds the missing date")


# TEST: THE MERGE
def test_merge():
    raw_str, staging_str = get_folder_tuple("merge")
    # 1-MINUTE STAGING: GOOD, ALREADY IN RAW, PRICE ISSUE (HIGH BELOW LOW), PARTIAL; THE RAW FOLDER DOES NOT EXIST YET EXCEPT FOR ONE FILE ADDED BELOW
    write_session_file(staging_str, "2026-09-14")
    write_session_file(staging_str, "2026-09-15")
    write_session_file(staging_str, "2026-09-16", price_list_in=(100.0, 98.0, 99.0, 100.5))
    write_session_file(staging_str, "2026-09-17", skip_list_in=("11:00",))
    other_raw_str = f"{raw_str}other/"
    write_session_file(other_raw_str, "2026-09-15", price_list_in=(1.0, 2.0, 0.5, 1.5))
    raw_before_bytes = open(f"{other_raw_str}ohlcv_data_20260915.csv", "rb").read()
    # DRY RUN: NOTHING CHANGES AND NO RAW FOLDER IS CREATED
    result_pdf = merge_stock_staging_into_raw_pdf("AAPL", "1min", False, False, staging_str, f"{raw_str}missing/", NOW_TS, False).set_index("file_name_str")
    assert result_pdf["decision_str"].str.split(":").str[0].to_dict() == {"ohlcv_data_20260914.csv": "add", "ohlcv_data_20260915.csv": "add", "ohlcv_data_20260916.csv": "skip",
                                                                         "ohlcv_data_20260917.csv": "skip"}
    assert not os.path.exists(f"{raw_str}missing/") and not os.path.exists(f"{staging_str}merge_log.csv") and not os.path.exists(f"{staging_str}merged/")
    # APPLY WITH A RAW FOLDER THAT DOES NOT EXIST YET (THE FIRST MERGE): CREATED, THE GOOD FILES ADDED, THE ADDED STAGING FILES MOVED TO merged/
    result_pdf = merge_stock_staging_into_raw_pdf("AAPL", "1min", True, False, staging_str, f"{raw_str}new/", NOW_TS, False).set_index("file_name_str")
    assert result_pdf["decision_str"].str.split(":").str[0].to_dict() == {"ohlcv_data_20260914.csv": "added", "ohlcv_data_20260915.csv": "added", "ohlcv_data_20260916.csv": "skip",
                                                                         "ohlcv_data_20260917.csv": "skip"}, result_pdf["decision_str"].to_dict()
    assert result_pdf.loc["ohlcv_data_20260916.csv", "decision_str"].startswith("skip: candlestick sanity") and result_pdf.loc["ohlcv_data_20260917.csv", "missing_count_int"] == 1
    assert sorted(os.listdir(f"{raw_str}new/")) == ["ohlcv_data_20260914.csv", "ohlcv_data_20260915.csv"]
    assert sorted(os.listdir(f"{staging_str}merged/")) == ["ohlcv_data_20260914.csv", "ohlcv_data_20260915.csv"]
    log_pdf = pd.read_csv(f"{staging_str}merge_log.csv")
    assert list(log_pdf.columns) == MERGE_LOG_COL_STR_LIST and len(log_pdf) == 2
    # A DATE ALREADY IN RAW IS SKIPPED AND THE EXISTING RAW FILE IS UNTOUCHED BYTE FOR BYTE
    write_session_file(staging_str, "2026-09-15")
    result_pdf = merge_stock_staging_into_raw_pdf("AAPL", "1min", True, False, staging_str, other_raw_str, NOW_TS, False).set_index("file_name_str")
    assert result_pdf.loc["ohlcv_data_20260915.csv", "decision_str"].startswith("skip: date already in raw")
    assert open(f"{other_raw_str}ohlcv_data_20260915.csv", "rb").read() == raw_before_bytes
    # --include-partial ADDS THE PARTIAL SESSION BUT NEVER THE ONE WITH A PRICE ISSUE
    result_pdf = merge_stock_staging_into_raw_pdf("AAPL", "1min", True, True, staging_str, f"{raw_str}new/", NOW_TS, False).set_index("file_name_str")
    assert result_pdf.loc["ohlcv_data_20260917.csv", "decision_str"] == "added" and result_pdf.loc["ohlcv_data_20260916.csv", "decision_str"].startswith("skip")
    # NOTHING STAGED: AN EMPTY RESULT AND NO FOLDER IS CREATED
    empty_raw_str, empty_staging_str = get_folder_tuple("merge_empty")
    assert merge_stock_staging_into_raw_pdf("AAPL", "1min", True, False, empty_staging_str, empty_raw_str, NOW_TS, False).empty and not os.path.exists(empty_raw_str)
    # DAILY: THE STAGED YEAR FILE IS MERGED INTO THE RAW YEAR FILE; THE ROW ALREADY IN RAW WINS AND THE CONFLICT IS COUNTED; THE BACKUP IS A SIBLING OF THE RAW FOLDER
    daily_raw_str, daily_staging_str = f"{raw_str}aapl_daily/", f"{staging_str}aapl_daily_staging/"
    os.makedirs(daily_staging_str)
    pd.DataFrame({"timestamp": ["2026-09-14 00:00:00-04:00", "2026-09-15 00:00:00-04:00"], "open": ["100.0", "101.0"], "high": ["101.0", "102.0"], "low": ["99.0", "100.0"],
                  "close": ["100.5", "101.5"], "volume": ["5000000", "6000000"], "created_ts": ["", ""], "date": ["2026-09-14", "2026-09-15"]}).to_csv(f"{daily_staging_str}ohlcv_data_2026.csv", index=False)
    os.makedirs(daily_raw_str)
    pd.DataFrame({"timestamp": ["2026-09-14 00:00:00-04:00"], "open": ["100.0"], "high": ["101.0"], "low": ["99.0"], "close": ["999.0"], "volume": ["5000000"], "created_ts": [""],
                  "date": ["2026-09-14"]}).to_csv(f"{daily_raw_str}ohlcv_data_2026.csv", index=False)
    result_pdf = merge_stock_staging_into_raw_pdf("AAPL", "daily", False, False, daily_staging_str, daily_raw_str, NOW_TS, False)
    assert result_pdf["decision_str"].tolist() == ["merge"] and len(pd.read_csv(f"{daily_raw_str}ohlcv_data_2026.csv")) == 1
    result_pdf = merge_stock_staging_into_raw_pdf("AAPL", "daily", True, False, daily_staging_str, daily_raw_str, NOW_TS, False)
    assert result_pdf["decision_str"].tolist() == ["merged"]
    merged_pdf = pd.read_csv(f"{daily_raw_str}ohlcv_data_2026.csv", dtype=str, keep_default_na=False)
    assert merged_pdf["date"].tolist() == ["2026-09-14", "2026-09-15"] and merged_pdf["close"].tolist() == ["999.0", "101.5"] and merged_pdf["volume"].tolist() == ["5000000", "6000000"]
    log_pdf = pd.read_csv(f"{daily_staging_str}merge_log.csv")
    assert list(log_pdf.columns) == INDEX_MERGE_LOG_COL_STR_LIST and log_pdf["conflict_minute_count_int"].tolist() == [1] and log_pdf["symbol"].tolist() == ["AAPL"]
    backup_name_list = [name_str for name_str in os.listdir(raw_str) if name_str.startswith("aapl_daily_backup_")]
    assert len(backup_name_list) == 1 and os.path.isfile(f"{raw_str}{backup_name_list[0]}/ohlcv_data_2026.csv") and not os.path.exists(f"{daily_staging_str}ohlcv_data_2026.csv")
    assert not any(name_str.startswith("aapl_daily_backup_") for name_str in os.listdir(daily_raw_str))
    # DAILY INTO A RAW FOLDER THAT DOES NOT EXIST YET: CREATED AND THE FILE ADDED
    first_raw_str, first_staging_str = get_folder_tuple("merge_first_daily")
    os.makedirs(first_staging_str)
    pd.DataFrame({"timestamp": ["2026-09-14 00:00:00-04:00"], "open": ["100.0"], "high": ["101.0"], "low": ["99.0"], "close": ["100.5"], "volume": ["5000000"], "created_ts": [""],
                  "date": ["2026-09-14"]}).to_csv(f"{first_staging_str}ohlcv_data_2026.csv", index=False)
    result_pdf = merge_stock_staging_into_raw_pdf("AAPL", "daily", True, False, first_staging_str, first_raw_str, NOW_TS, False)
    assert result_pdf["decision_str"].tolist() == ["merged"] and os.listdir(first_raw_str) == ["ohlcv_data_2026.csv"], result_pdf.to_string()
    passed("merge: dry run creates nothing, 1-minute add-only into a new raw folder, checks, include-partial, daily merge (existing rows win, sibling backup), logs")


# TEST: THE STATUS
def test_status():
    # BUILD THE DEFAULT FOLDERS OF A CLEAN STATE: TWO STAGED 1-MINUTE DAYS, ONE DAILY YEAR FILE IN RAW, A LOG WITH A NO-DATA REQUEST AND A PARTIAL ONE
    minute_staging_str = stock_config.get_stock_folder_path_str("AAPL", "1min", True)
    for file_name_str in os.listdir(minute_staging_str):
        os.remove(f"{minute_staging_str}{file_name_str}") if os.path.isfile(f"{minute_staging_str}{file_name_str}") else None
    write_session_file(minute_staging_str, "2026-09-14")
    write_session_file(minute_staging_str, "2026-09-15")
    pd.DataFrame({"date": ["2026-09-16", "2026-09-16", "2026-09-17"], "status_str": ["no_data", "no_data", "partial"], "saved_bool": ["False", "False", "True"],
                  "error_code_int": ["162", "162", ""], "error_str": ["HMDS query returned no data", "HMDS query returned no data", ""]}).to_csv(
        stock_config.get_stock_log_path_str("AAPL", "1min", "download"), index=False)
    daily_raw_str = stock_config.get_stock_folder_path_str("AAPL", "daily", False)
    os.makedirs(daily_raw_str, exist_ok=True)
    pd.DataFrame({"timestamp": ["2026-09-14 00:00:00-04:00"], "open": ["1"], "high": ["2"], "low": ["1"], "close": ["2"], "volume": ["3"], "created_ts": [""],
                  "date": ["2026-09-14"]}).to_csv(f"{daily_raw_str}ohlcv_data_2026.csv", index=False)
    # THE FOLDER TABLE: ONE ROW PER BAR KIND AND PLACE
    status_pdf = get_stock_status_pdf().set_index(["bar_kind", "place"])
    assert len(status_pdf) == 6 and set(status_pdf["symbol"]) == {"AAPL"}
    assert status_pdf.loc[("1min", "staging"), ["file_count_int", "row_count_int", "date_count_int", "first_date_str", "last_date_str"]].tolist() == [2, 780, 2, "2026-09-14", "2026-09-15"]
    assert status_pdf.loc[("daily", "raw"), ["file_count_int", "row_count_int", "date_count_int"]].tolist() == [1, 1, 1] and not status_pdf.loc[("1min", "raw"), "exists_bool"]
    # THE MISSING SESSIONS: SINCE THE START DATE, IN NEITHER RAW NOR STAGING
    expected_int, missing_date_list = get_stock_missing_date_list("AAPL", "1min", NOW_TS)
    assert expected_int > 4900 and len(missing_date_list) == expected_int - 2 and str(missing_date_list[0]) == "2007-01-03" and "2026-09-14" not in [str(d) for d in missing_date_list]
    assert str(missing_date_list[-1]) == "2026-10-06"
    # THE LOG PROBLEMS: THE LAST ATTEMPT OF EACH REQUEST THAT IS NOT COMPLETE; THE DAILY LOG IS ABSENT
    problem_pdf = get_stock_log_problem_pdf("AAPL", "1min")
    assert problem_pdf[["label", "status_str", "saved_bool", "error_code_int"]].values.tolist() == [["2026-09-16", "no_data", "False", "162"], ["2026-09-17", "partial", "True", ""]]
    assert get_stock_log_problem_pdf("AAPL", "daily").empty
    # THE STOCK FOLDERS ARE NOT "OTHER FOLDERS" (A BACKUP OF A STOCK RAW FOLDER IS)
    scan_root_str = tempfile.mkdtemp(prefix="so_stock_names_").replace("\\", "/") + "/"
    for name_str in ["ibkr_aapl_1min", "ibkr_aapl_1min_staging", "ibkr_aapl_daily", "ibkr_aapl_daily_staging", "ibkr_aapl_daily_backup_20261008_120000", "unrelated"]:
        os.makedirs(f"{scan_root_str}{name_str}")
    assert set(get_other_folder_pdf(scan_root_str)["name"]) == {"ibkr_aapl_daily_backup_20261008_120000"}
    passed("status: folder table, missing sessions, log problems, stock folders not reported as other folders")


# TEST: THE SCRIPTS
def test_scripts():
    root_str = os.path.dirname(TESTS_PATH_STR)
    env_dict = {**os.environ, "PYTHONPATH": root_str, "PYTHONIOENCODING": "utf-8"}

    def run(script_str_in, *args_in):
        return subprocess.run([sys.executable, os.path.join(root_str, "scripts", script_str_in), *args_in], capture_output=True, text=True, env=env_dict, encoding="utf-8")

    # --list PLANS WITHOUT A CONNECTION: 2 SESSIONS FOR THE 1-MINUTE BARS, 1 YEAR REQUEST FOR THE DAILY BARS (2025-12-30 / 31 ARE ONE YEAR)
    listed = run("download_ibkr_stock.py", "--list", "--dates", "2025-12-30", "2025-12-31", "--symbols", "AAPL")
    assert listed.returncode == 0 and "AAPL" in listed.stdout and "1min" in listed.stdout and "2 session(s)" in listed.stdout and "1 year request(s)" in listed.stdout, listed.stdout + listed.stderr
    assert "ibkr_aapl_1min_staging/" in listed.stdout and "ibkr_aapl_daily_staging/" in listed.stdout and "untouched window" in listed.stdout
    only_daily = run("download_ibkr_stock.py", "--list", "--dates", "2025-12-30", "--bars", "daily")
    assert only_daily.returncode == 0 and "1min" not in only_daily.stdout and "1 year request(s)" in only_daily.stdout, only_daily.stdout + only_daily.stderr
    # THE MERGE DRY RUN AND THE STATUS RUN ON THE (TEMPORARY) DATA ROOT
    merged = run("merge_stock_staging_into_raw.py", "--bars", "daily")
    assert merged.returncode == 0 and "no staging files" in merged.stdout, merged.stdout + merged.stderr
    status = run("stock_data_status.py")
    assert status.returncode == 0 and "Missing sessions" in status.stdout and "AAPL" in status.stdout and "ibkr_aapl" not in status.stderr, status.stdout + status.stderr
    passed("scripts: --list plans both bar kinds without a connection, the merge dry run and the status run")


# RUN THE TESTS
if __name__ == "__main__":
    test_folders_and_contract()
    test_planning()
    test_minute_download()
    test_daily_download()
    test_merge()
    test_status()
    test_scripts()
    print("\nAll stock pipeline tests passed ✅")
