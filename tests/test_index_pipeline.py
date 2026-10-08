"""
Index Pipeline Tests (simulated IBKR server, temporary folders, no connection, no real data folder)

Run from the workspace root:   python tests/test_index_pipeline.py

What is verified:
    1. Folders: every index / bar size has its own raw and staging folder, none inside the SPY raw folder (the research pipeline reads every CSV there).
    2. Planning: the UTC end formats, one request per 1-minute session and per year of daily bars, the "finished session" rule (today only from 17:00),
       the default start (first date IBKR has), skipping present dates, --from-start, dates that are not sessions.
    3. The 1-minute download: a complete session, a session with a gap (retried, then saved and flagged partial), an empty session (retried, not saved),
       an error, a pacing violation, bars of another date (dropped and counted), the 2016-style 03:15 start with the 09:15-09:30 gap (not a gap),
       the unset volume written as an empty cell, the file format of the SPY raw files, the download log.
    4. The daily download: a year chunk with a missing date (partial), the second run adding the missing date to the staged file without losing rows.
    5. Resuming: a second run requests nothing.
    6. The merge: dry run changes nothing; 1-minute files are added (add-only, a date already in raw is skipped, price issues / partial / other dates are
       refused); daily files are merged into the raw file of the year with existing rows winning and conflicts counted; the merge log.
    7. The scripts: --list needs no connection; the folders of the SPY pipeline are untouched.
"""

import os
import sys
import subprocess

# ADD THE TESTS FOLDER TO THE PATH AND IMPORT THE SIMULATED PROBE SERVER (THIS ALSO POINTS THE DATA ROOT TO A TEMPORARY FOLDER)
TESTS_PATH_STR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_PATH_STR)
from test_vix_probe import FakeProbeApp, passed

import tempfile
from decimal import Decimal
import pandas as pd
# IMPORT THE INDEX PIPELINE
from ingest import config
from ingest import index_config
from ingest.index_status import get_other_folder_pdf
from ingest.raw_files import get_backup_path_str
from so import paths as so_paths
from ingest.index_download import (IndexRequestMixin, get_index_end_str, get_index_volume_str, get_index_task_list, get_index_closed_session_pdf,
                                   download_index_task_pdf, INDEX_DOWNLOAD_LOG_COL_STR_LIST)
from ingest.index_merge import merge_index_staging_into_raw_pdf, INDEX_MERGE_LOG_COL_STR_LIST
from ingest.sessions import get_session_pdf

# DEFINE THE CLOCK OF THE TESTS (A TUESDAY EVENING, AFTER THE INDEX SESSION)
NOW_TS = pd.Timestamp("2026-10-06 18:00", tz=config.NY_TZ_STR)

# CLASS: SIMULATED APPLICATION WITH SCRIPTED BEHAVIOURS PER SESSION
class FakeIndexApp(IndexRequestMixin, FakeProbeApp):
    """
    Index server that accepts only UTC end dates (like the real one). behaviour_dict maps (symbol, "YYYY-MM-DD") to a list of behaviours, one per attempt (the last
    repeats): "ok" (09:31 - 16:14), "gap" (10:00 and 10:01 missing), "era2016" (03:15 start, 09:15 - 09:30 missing), "empty", "no_data", "error", "pacing",
    "otherday" (an extra bar on the previous day), "unset_volume". Daily bars: all weekdays of the 2 years before the end date, except daily_missing_set.
    """

    # METHOD: INITIALIZE
    def __init__(self, behaviour_dict_in=None, daily_missing_set_in=None):
        FakeProbeApp.__init__(self, {}, "10.45")
        self.behaviour_dict, self.daily_missing_set, self.attempt_dict, self.request_log = behaviour_dict_in or {}, daily_missing_set_in or set(), {}, []

    # METHOD: SIMULATE A HISTORICAL REQUEST
    def reqHistoricalData(self, reqId, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions):
        self.request_log.append((contract.symbol, contract.secType, contract.exchange, endDateTime, durationStr, barSizeSetting, useRTH, whatToShow, formatDate))
        self.reply(self.answer_index_bars, reqId, contract.symbol, endDateTime, barSizeSetting)

    # METHOD: ANSWER A HISTORICAL REQUEST
    def answer_index_bars(self, req_id_int_in, symbol_str_in, end_str_in, bar_size_str_in):
        # THE INDICES ACCEPT ONLY THE UTC END FORMAT OR NO END
        if end_str_in != "" and "-" not in end_str_in:
            self.send_error(req_id_int_in, 10314, "End Date/Time: The date, time, or time-zone entered is invalid.")
            return
        # PARSE THE END
        end_ts = pd.Timestamp("2026-10-06 18:00", tz=config.NY_TZ_STR) if end_str_in == "" else \
            pd.to_datetime(end_str_in, format="%Y%m%d-%H:%M:%S").tz_localize("UTC").tz_convert(config.NY_TZ_STR)
        # IF THE BARS ARE DAILY
        if bar_size_str_in == "1 day":
            for day_ts in pd.bdate_range(end=end_ts.tz_localize(None).normalize(), periods=520):
                if str(day_ts.date()) not in self.daily_missing_set:
                    self.historicalData(req_id_int_in, self.get_bar(day_ts.strftime("%Y%m%d"), Decimal(2 ** 127 - 1)))
            self.historicalDataEnd(req_id_int_in, "", "")
            return
        # COLLECT THE BEHAVIOUR OF THIS ATTEMPT
        date_str = str(end_ts.date())
        key_tuple = (symbol_str_in, date_str)
        self.attempt_dict[key_tuple] = self.attempt_dict.get(key_tuple, 0) + 1
        behaviour_list = self.behaviour_dict.get(key_tuple, ["ok"])
        behaviour_str = behaviour_list[min(self.attempt_dict[key_tuple], len(behaviour_list)) - 1]
        # ANSWER ERRORS
        if behaviour_str == "no_data":
            self.send_error(req_id_int_in, 162, "Historical Market Data Service error message:HMDS query returned no data")
            return
        if behaviour_str == "pacing":
            self.send_error(req_id_int_in, 162, "Historical Market Data Service error message:API historical data query cancelled: Pacing violation")
            return
        if behaviour_str == "error":
            self.send_error(req_id_int_in, 200, "No security definition has been found for the request")
            return
        # SEND THE MINUTES
        day_ts = end_ts.normalize()
        start_ts, last_ts = (day_ts + pd.Timedelta(hours=3, minutes=15)) if behaviour_str == "era2016" else (day_ts + pd.Timedelta(hours=9, minutes=31)), day_ts + pd.Timedelta(hours=16, minutes=14)
        if behaviour_str != "empty":
            for minute_ts in pd.date_range(start_ts, last_ts, freq="1min"):
                hhmm_str = minute_ts.strftime("%H:%M")
                if behaviour_str == "gap" and hhmm_str in ["10:00", "10:01"]:
                    continue
                if behaviour_str == "era2016" and "09:15" <= hhmm_str <= "09:30":
                    continue
                self.historicalData(req_id_int_in, self.get_bar(str(int(minute_ts.timestamp())), Decimal(2 ** 127 - 1) if behaviour_str == "unset_volume" else Decimal(0)))
            if behaviour_str == "otherday":
                self.historicalData(req_id_int_in, self.get_bar(str(int((day_ts - pd.Timedelta(hours=9)).timestamp())), Decimal(0)))
        # SEND THE END
        self.historicalDataEnd(req_id_int_in, "", "")

# FUNCTION: BUILD A CONNECTED APPLICATION
def get_app(behaviour_dict_in=None, daily_missing_set_in=None):
    app = FakeIndexApp(behaviour_dict_in, daily_missing_set_in)
    assert app.connect_app(), "handshake failed"
    return app

# FUNCTION: DEFINE FOLDERS OF A TEST
def get_folder_tuple(name_str_in):
    base_str = tempfile.mkdtemp(prefix=f"so_index_{name_str_in}_").replace("\\", "/")
    return f"{base_str}/raw/", f"{base_str}/staging/", f"{base_str}/staging/download_log.csv", f"{base_str}/staging/merge_log.csv"

# FUNCTION: DOWNLOAD WITH FAST PACING
def download(app_in, task_list_in, log_str_in):
    return download_index_task_pdf(app_in, task_list_in, log_file_path_str_in=log_str_in, min_interval_seconds_in=0, same_request_wait_seconds_in=0,
                                   timeout_seconds_in=5, pacing_backoff_seconds_in=0.05, alert_in=False)

# TEST: FOLDERS
def test_folders():
    raw_spy_str = config.RAW_OHLCV_PATH_STR
    folder_str_list = [index_config.get_index_folder_path_str(symbol_str, bar_kind_str, staging_bool)
                       for symbol_str in ["VIX", "VIX3M"] for bar_kind_str in ["1min", "daily"] for staging_bool in [False, True]]
    assert len(set(folder_str_list)) == 8
    assert all(not folder_str.startswith(raw_spy_str) and folder_str.endswith("/") for folder_str in folder_str_list)
    assert os.path.basename(raw_spy_str.rstrip("/")) == os.path.basename(so_paths.LOCAL_OHLCV_DATA_FILE_PATH_STR.rstrip("/"))
    assert index_config.get_index_folder_path_str("VIX3M", "1min", True).endswith("/store01_rawzone/ibkr_vix_family_staging/vix3m_1min/")
    assert index_config.get_index_folder_path_str("VIX", "daily").endswith("/store01_rawzone/ibkr_vix_family/vix_daily/")
    passed("folders: 8 separate folders, none inside the SPY raw folder")

# TEST: THE FOUR RESOLVED FOLDER PATHS
def test_renamed_folder_paths():
    rawzone_str = os.path.dirname(config.RAW_OHLCV_PATH_STR.rstrip("/")) + "/"
    spy_raw_str = config.RAW_OHLCV_PATH_STR
    spy_staging_str = config.STAGING_OHLCV_PATH_STR
    index_raw_str = index_config.INDEX_RAW_ROOT_PATH_STR
    index_staging_str = index_config.INDEX_STAGING_ROOT_PATH_STR
    assert spy_raw_str == config.DATA_ROOT_PATH_STR + so_paths.LOCAL_OHLCV_DATA_FILE_PATH_STR[len(so_paths.LOCAL_PATH_STR):]
    assert config.SPY_RAW_FOLDER_NAME_STR == "ibkr_spy_1min"
    assert spy_staging_str == f"{rawzone_str}ibkr_spy_1min_staging/"
    assert index_raw_str == f"{rawzone_str}ibkr_vix_family/"
    assert index_staging_str == f"{rawzone_str}ibkr_vix_family_staging/"
    index_folder_str_list = [index_raw_str, index_staging_str] + [
        index_config.get_index_folder_path_str(symbol_str, bar_kind_str, staging_bool)
        for symbol_str in ["VIX", "VIX3M"] for bar_kind_str in ["1min", "daily"] for staging_bool in [False, True]]
    assert all(not folder_str.startswith(spy_raw_str) for folder_str in index_folder_str_list)
    assert os.path.basename(get_backup_path_str(index_raw_str).rstrip("/")).startswith("ibkr_vix_family_backup_")
    assert os.path.basename(get_backup_path_str(spy_raw_str).rstrip("/")).startswith(os.path.basename(spy_raw_str.rstrip("/")) + "_backup_")
    scan_root_str = tempfile.mkdtemp(prefix="so_index_names_").replace("\\", "/") + "/"
    for name_str in ["ibkr_vix_family", "ibkr_vix_family_staging", "ibkr_SPY_ohlcv_data", "ibkr_SPY_ohlcv_data_incoming",
                     "ibkr_VIX_ohlcv_data", "ibkr_VIX_ohlcv_data_incoming", "ibkr_VIX_ohlcv_data_backup_20261008_120000",
                     "ibkr_spy_1min_backup_20261008_120000", "unrelated"]:
        os.makedirs(f"{scan_root_str}{name_str}")
    other_name_set = set(get_other_folder_pdf(scan_root_str)["name"])
    assert "ibkr_vix_family" not in other_name_set and "ibkr_vix_family_staging" not in other_name_set
    assert {"ibkr_SPY_ohlcv_data", "ibkr_SPY_ohlcv_data_incoming", "ibkr_VIX_ohlcv_data", "ibkr_VIX_ohlcv_data_incoming",
            "ibkr_VIX_ohlcv_data_backup_20261008_120000", "ibkr_spy_1min_backup_20261008_120000"} <= other_name_set
    assert "unrelated" not in other_name_set
    passed("renamed folders: four resolved paths, index folders outside the SPY raw folder, old names still found")

# TEST: HELPERS
def test_helpers():
    assert get_index_end_str(pd.Timestamp("2026-07-17 17:00", tz=config.NY_TZ_STR)) == "20260717-21:00:00"
    assert get_index_end_str(pd.Timestamp("2026-01-16 17:00", tz=config.NY_TZ_STR)) == "20260116-22:00:00"
    assert get_index_end_str(None) == ""
    assert get_index_volume_str(2 ** 127 - 1) == "" and get_index_volume_str(0) == "0" and get_index_volume_str(-1) == "-1" and get_index_volume_str(12) == "12"
    session_pdf = get_session_pdf("2026-10-05", "2026-10-06")
    assert list(get_index_closed_session_pdf(session_pdf, pd.Timestamp("2026-10-06 16:30", tz=config.NY_TZ_STR))["date"].astype(str)) == ["2026-10-05"]
    assert list(get_index_closed_session_pdf(session_pdf, pd.Timestamp("2026-10-06 17:00", tz=config.NY_TZ_STR))["date"].astype(str)) == ["2026-10-05", "2026-10-06"]
    passed("helpers: UTC end (EDT and EST), unset volume, finished-session rule")

# TEST: PLANNING
def test_planning():
    raw_str, staging_str, _, _ = get_folder_tuple("plan")
    # NOTHING PRESENT: START AT THE FIRST DATE IBKR HAS (VIX 2005-10-03: A MONDAY)
    task_list = get_index_task_list("VIX", "1min", date2_in="2005-10-07", raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)
    assert [task["label_str"] for task in task_list] == ["2005-10-03", "2005-10-04", "2005-10-05", "2005-10-06", "2005-10-07"]
    task_dict = task_list[0]
    assert (task_dict["end_datetime_str"], task_dict["duration_str"], task_dict["bar_size_str"]) == ("20051003-21:00:00", "1 D", "1 min")
    assert (task_dict["contract"].symbol, task_dict["contract"].secType, task_dict["contract"].exchange, task_dict["contract"].primaryExchange) == ("VIX", "IND", "CBOE", "")
    assert task_dict["file_path_str"] == f"{staging_str}ohlcv_data_20051003.csv"
    # VIX3M NEVER STARTS BEFORE ITS FIRST DATE (2009-08-12), WHATEVER DATE IS ASKED
    task_list = get_index_task_list("VIX3M", "1min", date1_in="2009-08-10", date2_in="2009-08-13", raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)
    assert [task["label_str"] for task in task_list] == ["2009-08-12", "2009-08-13"]
    # DAILY: ONE TASK PER YEAR OF THE WANTED DATES, END = LAST WANTED DATE + 3 DAYS (UTC), NOW WHEN THAT IS IN THE FUTURE
    task_list = get_index_task_list("VIX", "daily", date1_in="2025-12-29", date2_in="2026-01-05", raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)
    assert [(task["label_str"], len(task["wanted_date_list"]), task["end_datetime_str"], task["duration_str"]) for task in task_list] == \
           [("2025", 3, "20260103-05:00:00", "2 Y"), ("2026", 2, "20260108-05:00:00", "2 Y")], [(t["label_str"], t["end_datetime_str"]) for t in task_list]
    task_list = get_index_task_list("VIX", "daily", date1_in="2026-10-01", raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)
    assert len(task_list) == 1 and task_list[0]["end_datetime_str"] == "" and len(task_list[0]["wanted_date_list"]) == 4
    # A DAY THAT IS NOT A SESSION IS SKIPPED (2026-09-07 IS LABOR DAY), A SESSION OF TODAY BEFORE 17:00 IS NOT YET DOWNLOADABLE
    assert [t["label_str"] for t in get_index_task_list("VIX", "1min", date_list_in=["2026-09-07", "2026-09-08"], raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)] == ["2026-09-08"]
    assert get_index_task_list("VIX", "1min", date_list_in=["2026-10-06"], raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=pd.Timestamp("2026-10-06 16:30", tz=config.NY_TZ_STR), alert_in=False) == []
    passed("planning: UTC ends, one request per session / year, start dates, closed rule")

# TEST: THE 1-MINUTE DOWNLOAD
def test_minute_download():
    raw_str, staging_str, log_str, _ = get_folder_tuple("minute")
    date_list = ["2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23"]
    behaviour_dict = {("VIX", "2026-09-15"): ["gap"], ("VIX", "2026-09-16"): ["empty", "ok"], ("VIX", "2026-09-17"): ["empty"], ("VIX", "2026-09-18"): ["error", "ok"],
                      ("VIX", "2026-09-21"): ["pacing", "ok"], ("VIX", "2026-09-22"): ["otherday"], ("VIX", "2026-09-23"): ["era2016"]}
    app = get_app(behaviour_dict)
    task_list = get_index_task_list("VIX", "1min", date_list_in=date_list, raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)
    summary_pdf = download(app, task_list, log_str).set_index("label")
    app.disconnect_app()
    # EVERY REQUEST WAS AN INDEX REQUEST: IND / CBOE, TRADES, 1 min, RTH, EPOCH DATES, 1 D, UTC END
    assert all(req[1:3] == ("IND", "CBOE") and req[5] == "1 min" and req[6] == 1 and req[7] == "TRADES" and req[8] == 2 and req[4] == "1 D" and req[3].endswith("-21:00:00") for req in app.request_log)
    # THE OUTCOMES: THE GAP SESSION IS SAVED AS PARTIAL AFTER 3 ATTEMPTS; EMPTY THEN OK; STILL EMPTY AFTER 3 ATTEMPTS: NOT SAVED; ERROR THEN OK
    assert summary_pdf.loc["2026-09-14", ["status_str", "saved_bool", "attempt_int"]].tolist() == ["complete", True, 1]
    assert summary_pdf.loc["2026-09-15", ["status_str", "saved_bool", "attempt_int", "missing_count_int"]].tolist() == ["partial", True, 3, 2]
    assert summary_pdf.loc["2026-09-16", ["status_str", "saved_bool", "attempt_int"]].tolist() == ["complete", True, 2]
    assert summary_pdf.loc["2026-09-17", ["status_str", "saved_bool", "attempt_int"]].tolist() == ["empty", False, 3]
    assert summary_pdf.loc["2026-09-18", ["status_str", "saved_bool", "attempt_int"]].tolist() == ["complete", True, 2]
    # A PACING VIOLATION DOES NOT USE AN ATTEMPT
    assert summary_pdf.loc["2026-09-21", ["status_str", "saved_bool", "attempt_int"]].tolist() == ["complete", True, 1]
    # A BAR OF ANOTHER DATE IS DROPPED AND COUNTED, THE SESSION IS STILL COMPLETE
    assert summary_pdf.loc["2026-09-22", ["status_str", "bar_count_int"]].tolist() == ["complete", 404]
    # THE 2016-STYLE SESSION (03:15 START, 09:15 - 09:30 GAP) IS NOT PARTIAL: ONLY THE CORE HOURS ARE CHECKED
    assert summary_pdf.loc["2026-09-23", ["status_str", "missing_count_int"]].tolist() == ["complete", 0]
    # THE FILES: THE SPY RAW FORMAT, ONE PER SAVED SESSION
    assert sorted(os.listdir(staging_str)) == sorted([f"ohlcv_data_{d.replace('-', '')}.csv" for d in date_list if d != "2026-09-17"] + ["download_log.csv"])
    text_pdf = pd.read_csv(f"{staging_str}ohlcv_data_20260914.csv", dtype=str, keep_default_na=False)
    assert list(text_pdf.columns) == config.OHLCV_COL_STR_LIST and len(text_pdf) == 404
    assert text_pdf["timestamp"].iloc[0] == "2026-09-14 09:31:00-04:00" and text_pdf["timestamp"].iloc[-1] == "2026-09-14 16:14:00-04:00"
    assert set(text_pdf["date"]) == {"2026-09-14"} and set(text_pdf["created_ts"]) == {""} and set(text_pdf["volume"]) == {"0"}
    assert text_pdf[["open", "high", "low", "close"]].iloc[0].tolist() == ["20.0", "21.0", "19.5", "20.5"]
    era_pdf = pd.read_csv(f"{staging_str}ohlcv_data_20260923.csv", dtype=str, keep_default_na=False)
    assert era_pdf["timestamp"].iloc[0] == "2026-09-23 03:15:00-04:00" and "2026-09-23 09:20:00-04:00" not in set(era_pdf["timestamp"])
    # THE UNSET VOLUME IS AN EMPTY CELL (NOT 2**127 - 1): SIMULATE IT ON ONE MORE SESSION
    app = get_app({("VIX", "2026-09-24"): ["unset_volume"]})
    task_list = get_index_task_list("VIX", "1min", date_list_in=["2026-09-24"], raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)
    download(app, task_list, log_str)
    app.disconnect_app()
    assert set(pd.read_csv(f"{staging_str}ohlcv_data_20260924.csv", dtype=str, keep_default_na=False)["volume"]) == {""}
    # THE LOG: EVERY ATTEMPT, WITH THE VERSIONS
    log_pdf = pd.read_csv(log_str, dtype=str, keep_default_na=False)
    assert list(log_pdf.columns) == INDEX_DOWNLOAD_LOG_COL_STR_LIST
    gap_pdf = log_pdf[log_pdf["label"] == "2026-09-15"]
    assert len(gap_pdf) == 3 and set(gap_pdf["missing_str"]) == {"10:00 10:01"} and set(gap_pdf["first_bar_str"]) == {"09:31"} and set(gap_pdf["last_bar_str"]) == {"16:14"}
    assert (log_pdf["server_version_int"] == "999").all() and (log_pdf["ibapi_version_str"] != "").all()
    assert set(log_pdf[log_pdf["label"] == "2026-09-22"]["extra_str"]) == {"1 bar(s) of another date"}
    assert log_pdf[log_pdf["label"] == "2026-09-18"]["status_str"].tolist() == ["error", "complete"] and log_pdf[log_pdf["label"] == "2026-09-18"]["error_code_int"].iloc[0] == "200"
    # A SECOND RUN REQUESTS ONLY WHAT IS MISSING: THE UNSAVED SESSION 2026-09-17 (DEFAULT RANGE STARTS AFTER THE LAST DATE PRESENT, SO USE --from-start)
    assert [t["label_str"] for t in get_index_task_list("VIX", "1min", date1_in="2026-09-14", date2_in="2026-09-24", raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)] == ["2026-09-17"]
    assert [t["label_str"] for t in get_index_task_list("VIX", "1min", raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)][0] == "2026-09-25"
    passed("1-minute download: complete / partial / empty / error / pacing / other-date bar / 2016 window / unset volume, files, log, resume")

# TEST: THE DAILY DOWNLOAD
def test_daily_download():
    raw_str, staging_str, log_str, _ = get_folder_tuple("daily")
    # 2026-09-01 .. 2026-09-18 HAS 13 SESSIONS (LABOR DAY 09-07); THE SERVER LACKS 09-10 AND HAS A BAR ON LABOR DAY (NOT A SESSION)
    app = get_app(daily_missing_set_in={"2026-09-10"})
    task_list = get_index_task_list("VIX", "daily", date1_in="2026-09-01", date2_in="2026-09-18", raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)
    assert len(task_list) == 1 and len(task_list[0]["wanted_date_list"]) == 13
    summary_pdf = download(app, task_list, log_str)
    assert summary_pdf.loc[0, ["status_str", "saved_bool", "attempt_int", "bar_count_int", "missing_count_int"]].tolist() == ["partial", True, 3, 12, 1]
    assert all(req[5] == "1 day" and req[4] == "2 Y" and req[1:3] == ("IND", "CBOE") for req in app.request_log)
    text_pdf = pd.read_csv(f"{staging_str}ohlcv_data_2026.csv", dtype=str, keep_default_na=False)
    assert list(text_pdf.columns) == config.OHLCV_COL_STR_LIST and len(text_pdf) == 12
    assert text_pdf["timestamp"].iloc[0] == "2026-09-01 00:00:00-04:00" and text_pdf["date"].iloc[0] == "2026-09-01" and set(text_pdf["volume"]) == {""}
    assert "2026-09-10" not in set(text_pdf["date"]) and "2026-09-07" not in set(text_pdf["date"])
    log_pdf = pd.read_csv(log_str, dtype=str, keep_default_na=False)
    assert set(log_pdf["missing_str"]) == {"2026-09-10"} and set(log_pdf["extra_str"]) == {"2026-09-07"} and set(log_pdf["expected_count_int"]) == {"13"}
    app.disconnect_app()
    # A LATER RUN FOR THE MISSING DATE (NOW AVAILABLE) ADDS IT TO THE STAGED FILE OF THE YEAR AND KEEPS THE 12 ROWS
    app = get_app()
    task_list = get_index_task_list("VIX", "daily", date_list_in=["2026-09-10"], raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False)
    assert len(task_list) == 1
    assert download(app, task_list, log_str).loc[0, "status_str"] == "complete"
    app.disconnect_app()
    text_pdf = pd.read_csv(f"{staging_str}ohlcv_data_2026.csv", dtype=str, keep_default_na=False)
    assert len(text_pdf) == 13 and text_pdf["date"].is_monotonic_increasing and "2026-09-10" in set(text_pdf["date"])
    # NOTHING LEFT TO DOWNLOAD IN THE RANGE
    assert get_index_task_list("VIX", "daily", date1_in="2026-09-01", date2_in="2026-09-18", raw_path_str_in=raw_str, staging_path_str_in=staging_str, now_ny_ts_in=NOW_TS, alert_in=False) == []
    passed("daily download: partial year chunk, holiday bar dropped, second run adds the missing date")

# FUNCTION: WRITE A SESSION FILE
def write_session_file(folder_str_in, date_str_in, hhmm_start_str_in="09:31", hhmm_end_str_in="16:14", price_list_in=(20.0, 21.0, 19.5, 20.5), skip_list_in=()):
    os.makedirs(folder_str_in, exist_ok=True)
    minute_index = pd.date_range(f"{date_str_in} {hhmm_start_str_in}", f"{date_str_in} {hhmm_end_str_in}", freq="1min", tz=config.NY_TZ_STR)
    minute_index = minute_index[[ts.strftime("%H:%M") not in skip_list_in for ts in minute_index]]
    pd.DataFrame({"timestamp": [ts.isoformat(sep=" ") for ts in minute_index], "open": str(price_list_in[0]), "high": str(price_list_in[1]), "low": str(price_list_in[2]),
                  "close": str(price_list_in[3]), "volume": "", "created_ts": "", "date": date_str_in}).to_csv(f"{folder_str_in}ohlcv_data_{date_str_in.replace('-', '')}.csv", index=False)

# TEST: THE MERGE
def test_merge():
    raw_str, staging_str, _, merge_log_str = get_folder_tuple("merge")
    # 1-MINUTE STAGING: GOOD, ALREADY IN RAW, PRICE ISSUE (HIGH BELOW LOW), PARTIAL, EMPTY OF THE DATE (A BAR OF ANOTHER DATE ONLY IS NOT TESTED HERE)
    write_session_file(staging_str, "2026-09-14")
    write_session_file(staging_str, "2026-09-15")
    write_session_file(staging_str, "2026-09-16", price_list_in=(20.0, 19.0, 19.5, 20.5))
    write_session_file(staging_str, "2026-09-17", skip_list_in=("11:00",))
    write_session_file(raw_str, "2026-09-15", price_list_in=(1.0, 2.0, 0.5, 1.5))
    raw_before_dict = {name_str: open(f"{raw_str}{name_str}", "rb").read() for name_str in os.listdir(raw_str)}
    # DRY RUN: NOTHING CHANGES
    result_pdf = merge_index_staging_into_raw_pdf("VIX", "1min", False, False, staging_str, raw_str, merge_log_str, alert_in=False).set_index("file_name_str")
    assert result_pdf["decision_str"].str.split(":").str[0].to_dict() == {"ohlcv_data_20260914.csv": "add", "ohlcv_data_20260915.csv": "skip", "ohlcv_data_20260916.csv": "skip",
                                                                         "ohlcv_data_20260917.csv": "skip"}
    assert result_pdf.loc["ohlcv_data_20260917.csv", "missing_count_int"] == 1 and result_pdf.loc["ohlcv_data_20260916.csv", "price_issue_count_int"] == 404
    assert sorted(os.listdir(staging_str)) == [f"ohlcv_data_2026091{d}.csv" for d in "4567"] and os.listdir(raw_str) == ["ohlcv_data_20260915.csv"] and not os.path.exists(merge_log_str)
    # APPLY: ONE FILE ADDED, THE EXISTING RAW FILE UNTOUCHED BYTE FOR BYTE, THE ADDED STAGING FILE MOVED TO merged/
    result_pdf = merge_index_staging_into_raw_pdf("VIX", "1min", True, False, staging_str, raw_str, merge_log_str, alert_in=False).set_index("file_name_str")
    assert result_pdf.loc["ohlcv_data_20260914.csv", "decision_str"] == "added"
    assert sorted(os.listdir(raw_str)) == ["ohlcv_data_20260914.csv", "ohlcv_data_20260915.csv"] and open(f"{raw_str}ohlcv_data_20260915.csv", "rb").read() == raw_before_dict["ohlcv_data_20260915.csv"]
    assert os.listdir(f"{staging_str}merged/") == ["ohlcv_data_20260914.csv"] and not os.path.exists(f"{staging_str}ohlcv_data_20260914.csv")
    log_pdf = pd.read_csv(merge_log_str)
    assert list(log_pdf.columns) == INDEX_MERGE_LOG_COL_STR_LIST and len(log_pdf) == 1 and log_pdf.loc[0, "decision_str"] == "added"
    # --include-partial ADDS THE PARTIAL SESSION
    result_pdf = merge_index_staging_into_raw_pdf("VIX", "1min", True, True, staging_str, raw_str, merge_log_str, alert_in=False).set_index("file_name_str")
    assert result_pdf.loc["ohlcv_data_20260917.csv", "decision_str"] == "added" and result_pdf.loc["ohlcv_data_20260916.csv", "decision_str"].startswith("skip")
    # DAILY: THE STAGED YEAR FILE IS MERGED INTO THE RAW YEAR FILE; THE ROW ALREADY IN RAW WINS AND THE CONFLICT IS COUNTED
    daily_staging_str, daily_raw_str = f"{staging_str}daily/", f"{raw_str}daily/"
    os.makedirs(daily_staging_str)
    pd.DataFrame({"timestamp": ["2026-09-14 00:00:00-04:00", "2026-09-15 00:00:00-04:00"], "open": ["20.0", "21.0"], "high": ["21.0", "22.0"], "low": ["19.0", "20.0"],
                  "close": ["20.5", "21.5"], "volume": ["", ""], "created_ts": ["", ""], "date": ["2026-09-14", "2026-09-15"]}).to_csv(f"{daily_staging_str}ohlcv_data_2026.csv", index=False)
    os.makedirs(daily_raw_str)
    pd.DataFrame({"timestamp": ["2026-09-14 00:00:00-04:00"], "open": ["20.0"], "high": ["21.0"], "low": ["19.0"], "close": ["99.0"], "volume": [""], "created_ts": [""],
                  "date": ["2026-09-14"]}).to_csv(f"{daily_raw_str}ohlcv_data_2026.csv", index=False)
    result_pdf = merge_index_staging_into_raw_pdf("VIX", "daily", False, False, daily_staging_str, daily_raw_str, merge_log_str, alert_in=False)
    assert result_pdf["decision_str"].tolist() == ["merge"] and len(pd.read_csv(f"{daily_raw_str}ohlcv_data_2026.csv")) == 1
    result_pdf = merge_index_staging_into_raw_pdf("VIX", "daily", True, False, daily_staging_str, daily_raw_str, merge_log_str, alert_in=False)
    assert result_pdf["decision_str"].tolist() == ["merged"]
    merged_pdf = pd.read_csv(f"{daily_raw_str}ohlcv_data_2026.csv", dtype=str, keep_default_na=False)
    assert merged_pdf["date"].tolist() == ["2026-09-14", "2026-09-15"] and merged_pdf["close"].tolist() == ["99.0", "21.5"]
    log_pdf = pd.read_csv(merge_log_str)
    assert log_pdf[log_pdf["bar_kind"] == "daily"]["conflict_minute_count_int"].tolist() == [1]
    raw_parent_str = os.path.dirname(raw_str.rstrip("/"))
    backup_name_list = [name_str for name_str in os.listdir(raw_parent_str) if name_str.startswith("raw_backup_")]
    assert not os.path.exists(f"{daily_staging_str}ohlcv_data_2026.csv") and len(backup_name_list) == 1
    assert os.path.isfile(f"{raw_parent_str}/{backup_name_list[0]}/daily/ohlcv_data_2026.csv")
    assert not any(name_str.startswith("daily_backup_") for name_str in os.listdir(raw_str))
    passed("merge: dry run, add-only 1-minute files, checks, include-partial, daily merge with existing rows winning, log")

# TEST: THE FOLDER RENAME ON A TEMPORARY DATA ROOT
def test_rename_data_folders():
    import importlib.util
    root_str = os.path.dirname(TESTS_PATH_STR)
    script_str = os.path.join(root_str, "scripts", "rename_data_folders.py")
    spec = importlib.util.spec_from_file_location("rename_data_folders", script_str)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.get_manifest_difference_str_list(
        [{"relative_path": "ibkr_SPY_ohlcv_data/a.csv", "size_bytes": "1", "row_count": "0", "sha256": "aa"}],
        [{"relative_path": "ibkr_spy_1min/a.csv", "size_bytes": "1", "row_count": "0", "sha256": "bb"}],
        [("ibkr_SPY_ohlcv_data", "ibkr_spy_1min")])
    assert module.get_manifest_difference_str_list(
        [{"relative_path": "ibkr_SPY_ohlcv_data/a.csv", "size_bytes": "1", "row_count": "0", "sha256": "aa"}],
        [{"relative_path": "ibkr_spy_1min/a.csv", "size_bytes": "1", "row_count": "0", "sha256": "aa"}],
        [("ibkr_SPY_ohlcv_data", "ibkr_spy_1min")]) == []

    def build_tree(base_str):
        rawzone_str = module.get_rawzone_path_str(base_str)
        file_dict = {"ibkr_SPY_ohlcv_data/ohlcv_data_2024.csv": b"timestamp,open\r\n2024-01-02,100.00\r\n",
                     "ibkr_SPY_ohlcv_data/nested/note.txt": b"keep\r\n",
                     "ibkr_SPY_ohlcv_data_incoming/download_log.csv": b"status\r\nok\r\n",
                     "ibkr_SPY_ohlcv_data_incoming/live/ohlcv_live_20261008.csv": b"timestamp\r\n",
                     "ibkr_SPY_ohlcv_data_incoming/merged/old.csv": b"timestamp,open\r\n",
                     "ibkr_SPY_ohlcv_data_backup_20261001_010101/ohlcv_data_2024.csv": b"timestamp,open\r\n2024-01-02,100.00\r\n",
                     "ibkr_VIX_ohlcv_data/vix_1min/ohlcv_data_20261001.csv": b"timestamp,open\r\n2026-10-01,1\r\n",
                     "ibkr_VIX_ohlcv_data/vix_1min_backup_20261001_010101/ohlcv_data_20261001.csv": b"timestamp,open\r\n2026-10-01,1\r\n",
                     "ibkr_VIX_ohlcv_data_incoming/vix_daily/ohlcv_data_2026.csv": b"timestamp,open\r\n2026-10-01,2\r\n",
                     "ibkr_VIX_ohlcv_data_backup_20261002_020202/vix_1min/a.csv": b"timestamp,open\r\n",
                     "ibkr_ohlcv_data_backup_20261004_185149/keep.csv": b"do-not-rename\r\n",
                     "unrelated/x.csv": b"x\r\n"}
        for relative_str, blob_bytes in file_dict.items():
            path_str = f"{rawzone_str}{relative_str}"
            os.makedirs(os.path.dirname(path_str), exist_ok=True)
            with open(path_str, "wb") as file_object:
                file_object.write(blob_bytes)
        return rawzone_str, file_dict

    import contextlib
    import io
    module.get_running_job_str_list = lambda: []
    original_raw_str = config.RAW_OHLCV_PATH_STR
    config.RAW_OHLCV_PATH_STR = os.path.dirname(original_raw_str.rstrip("/")) + "/ibkr_SPY_ohlcv_data/"

    def run_main(arg_str_list):
        stdout_file = io.StringIO()
        old_argv = sys.argv
        sys.argv = [script_str] + arg_str_list
        try:
            with contextlib.redirect_stdout(stdout_file):
                try:
                    code_int = module.main()
                except SystemExit as error:
                    code_int = error.code
        finally:
            sys.argv = old_argv
        return code_int, stdout_file.getvalue()

    base_str = tempfile.mkdtemp(prefix="so_rename_").replace("\\", "/")
    rawzone_str, file_dict = build_tree(base_str)
    dry_manifest_str = f"{base_str}/manifest_dry.csv"
    dry_code_int, dry_stdout_str = run_main(["--data-root", base_str, "--manifest", dry_manifest_str])
    assert dry_code_int == 0 and "Dry run: nothing was renamed." in dry_stdout_str and "ibkr_SPY_ohlcv_data -> ibkr_spy_1min" in dry_stdout_str \
        and "Rename-Item -LiteralPath" in dry_stdout_str and "Warning:" in dry_stdout_str and "ibkr_spy_1min" in dry_stdout_str, dry_stdout_str
    assert os.path.isdir(f"{rawzone_str}ibkr_SPY_ohlcv_data") and not os.path.exists(f"{rawzone_str}ibkr_spy_1min")
    dry_manifest_text = open(dry_manifest_str, encoding="utf-8").read()
    assert "ibkr_SPY_ohlcv_data/ohlcv_data_2024.csv" in dry_manifest_text and "ibkr_ohlcv_data_backup_20261004_185149" not in dry_manifest_text
    blocked_str = tempfile.mkdtemp(prefix="so_rename_block_").replace("\\", "/")
    blocked_zone_str, _ = build_tree(blocked_str)
    os.makedirs(f"{blocked_zone_str}ibkr_spy_1min")
    with open(f"{blocked_zone_str}ibkr_spy_1min/keep.txt", "wb") as file_object:
        file_object.write(b"keep")
    blocked_manifest_str = f"{blocked_str}/manifest_block.csv"
    blocked_code_int, blocked_stdout_str = run_main(["--apply", "--ignore-research-paths", "--data-root", blocked_str, "--manifest", blocked_manifest_str])
    assert blocked_code_int != 0 and "already exists" in blocked_stdout_str and not os.path.exists(blocked_manifest_str), blocked_stdout_str
    assert open(f"{blocked_zone_str}ibkr_spy_1min/keep.txt", "rb").read() == b"keep" and os.path.isdir(f"{blocked_zone_str}ibkr_SPY_ohlcv_data")
    refused_code_int, refused_stdout_str = run_main(["--apply", "--data-root", base_str, "--manifest", f"{base_str}/manifest_refused.csv"])
    assert refused_code_int == 1 and "ibkr_SPY_ohlcv_data" in refused_stdout_str and "ibkr_spy_1min" in refused_stdout_str \
        and not os.path.exists(f"{base_str}/manifest_refused.csv") and os.path.isdir(f"{rawzone_str}ibkr_SPY_ohlcv_data"), refused_stdout_str
    applied_code_int, applied_stdout_str = run_main(["--apply", "--ignore-research-paths", "--data-root", base_str, "--manifest", f"{base_str}/manifest_apply.csv"])
    assert applied_code_int == 0 and "identical" in applied_stdout_str, applied_stdout_str
    renamed_dict = {"ibkr_spy_1min/ohlcv_data_2024.csv": "ibkr_SPY_ohlcv_data/ohlcv_data_2024.csv",
                    "ibkr_spy_1min/nested/note.txt": "ibkr_SPY_ohlcv_data/nested/note.txt",
                    "ibkr_spy_1min_staging/download_log.csv": "ibkr_SPY_ohlcv_data_incoming/download_log.csv",
                    "ibkr_spy_1min_staging/live/ohlcv_live_20261008.csv": "ibkr_SPY_ohlcv_data_incoming/live/ohlcv_live_20261008.csv",
                    "ibkr_spy_1min_staging/merged/old.csv": "ibkr_SPY_ohlcv_data_incoming/merged/old.csv",
                    "ibkr_spy_1min_backup_20261001_010101/ohlcv_data_2024.csv": "ibkr_SPY_ohlcv_data_backup_20261001_010101/ohlcv_data_2024.csv",
                    "ibkr_vix_family/vix_1min/ohlcv_data_20261001.csv": "ibkr_VIX_ohlcv_data/vix_1min/ohlcv_data_20261001.csv",
                    "ibkr_vix_family/vix_1min_backup_20261001_010101/ohlcv_data_20261001.csv": "ibkr_VIX_ohlcv_data/vix_1min_backup_20261001_010101/ohlcv_data_20261001.csv",
                    "ibkr_vix_family_staging/vix_daily/ohlcv_data_2026.csv": "ibkr_VIX_ohlcv_data_incoming/vix_daily/ohlcv_data_2026.csv",
                    "ibkr_vix_family_backup_20261002_020202/vix_1min/a.csv": "ibkr_VIX_ohlcv_data_backup_20261002_020202/vix_1min/a.csv"}
    for new_str, old_str in renamed_dict.items():
        assert open(f"{rawzone_str}{new_str}", "rb").read() == file_dict[old_str], new_str
    for old_name_str in ["ibkr_SPY_ohlcv_data", "ibkr_SPY_ohlcv_data_incoming", "ibkr_VIX_ohlcv_data", "ibkr_VIX_ohlcv_data_incoming",
                         "ibkr_SPY_ohlcv_data_backup_20261001_010101", "ibkr_VIX_ohlcv_data_backup_20261002_020202"]:
        assert not os.path.exists(f"{rawzone_str}{old_name_str}"), old_name_str
    assert open(f"{rawzone_str}ibkr_ohlcv_data_backup_20261004_185149/keep.csv", "rb").read() == b"do-not-rename\r\n"
    assert open(f"{rawzone_str}unrelated/x.csv", "rb").read() == b"x\r\n"
    config.RAW_OHLCV_PATH_STR = original_raw_str
    passed("rename script: dry run, refuse when the new folder exists, move on a temporary root, bytes identical")

# TEST: THE PROCESS LIST WHEN POWERSHELL OR ps IS MISSING
def test_running_job_list_without_powershell():
    import importlib.util
    root_str = os.path.dirname(TESTS_PATH_STR)
    script_str = os.path.join(root_str, "scripts", "rename_data_folders.py")
    spec = importlib.util.spec_from_file_location("rename_data_folders_missing_ps", script_str)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    original_run = module.subprocess.run

    def raise_missing(*args, **kwargs):
        raise FileNotFoundError("powershell")

    module.subprocess.run = raise_missing
    try:
        result_list = module.get_running_job_str_list()
    finally:
        module.subprocess.run = original_run
    assert result_list and result_list[0].startswith("Could not list processes"), result_list
    passed("process list: a missing powershell or ps returns Could not list processes")

# TEST: THE SCRIPTS
def test_scripts():
    root_str = os.path.dirname(TESTS_PATH_STR)
    env_dict = {**os.environ, "PYTHONPATH": root_str, "PYTHONIOENCODING": "utf-8"}
    listed = subprocess.run([sys.executable, os.path.join(root_str, "scripts", "download_ibkr_index.py"), "--skip-name-check", "--list", "--symbols", "VIX3M", "--bars", "1min", "--date2", "2009-08-14"],
                            capture_output=True, text=True, env=env_dict, encoding="utf-8")
    assert listed.returncode == 0 and "Plan: 3 request(s)" in listed.stdout and "vix3m_1min" in listed.stdout, listed.stdout + listed.stderr
    merged = subprocess.run([sys.executable, os.path.join(root_str, "scripts", "merge_index_staging_into_raw.py"), "--skip-name-check"], capture_output=True, text=True, env=env_dict, encoding="utf-8")
    assert merged.returncode == 0 and "no staging files" in merged.stdout, merged.stdout + merged.stderr
    passed("scripts: --list plans without a connection, the merge dry run runs on empty folders")

# TEST: THE SPY RAW FOLDER NAME CHECK
def test_folder_name_check():
    root_str = os.path.dirname(TESTS_PATH_STR)
    script_str = os.path.join(root_str, "scripts", "download_ibkr_index.py")
    parent_str = os.path.dirname(config.RAW_OHLCV_PATH_STR.rstrip("/"))
    original_str = config.RAW_OHLCV_PATH_STR
    try:
        config.RAW_OHLCV_PATH_STR = f"{parent_str}/ibkr_spy_1min/"
        assert config.get_folder_name_problem_str() == ""
        config.RAW_OHLCV_PATH_STR = f"{parent_str}/ibkr_SPY_ohlcv_data/"
        message_str = config.get_folder_name_problem_str()
        assert "ibkr_spy_1min" in message_str and "ibkr_SPY_ohlcv_data" in message_str
    finally:
        config.RAW_OHLCV_PATH_STR = original_str

    def run_list(leaf_str, extra_str_list):
        raw_str = f"{parent_str}/{leaf_str}/"
        arg_text = ", ".join([repr(script_str), "'--list'", "'--symbols'", "'VIX3M'", "'--bars'", "'1min'", "'--date2'", "'2009-08-14'"] + [repr(arg_str) for arg_str in extra_str_list])
        helper_str = (
            "import sys\n"
            f"sys.path.insert(0, {root_str!r})\n"
            "import ingest.config as config\n"
            f"config.RAW_OHLCV_PATH_STR = {raw_str!r}\n"
            f"sys.argv = [{arg_text}]\n"
            "import runpy\n"
            f"runpy.run_path({script_str!r}, run_name='__main__')\n"
        )
        return subprocess.run([sys.executable, "-c", helper_str], capture_output=True, text=True, encoding="utf-8",
                              env={**os.environ, "PYTHONPATH": root_str, "PYTHONIOENCODING": "utf-8"})

    disagree = run_list("ibkr_SPY_ohlcv_data", [])
    assert disagree.returncode == 1 and "ibkr_spy_1min" in disagree.stdout and "ibkr_SPY_ohlcv_data" in disagree.stdout and "Plan:" not in disagree.stdout, disagree.stdout + disagree.stderr
    skipped = run_list("ibkr_SPY_ohlcv_data", ["--skip-name-check"])
    assert skipped.returncode == 0 and "Plan: 3 request(s)" in skipped.stdout, skipped.stdout + skipped.stderr
    agree = run_list("ibkr_spy_1min", [])
    assert agree.returncode == 0 and "Plan: 3 request(s)" in agree.stdout, agree.stdout + agree.stderr
    passed("folder name check: empty when the leaf is ibkr_spy_1min, message and exit 1 otherwise, --skip-name-check exits 0")

# RUN THE TESTS
if __name__ == "__main__":
    test_folders()
    test_renamed_folder_paths()
    test_helpers()
    test_planning()
    test_minute_download()
    test_daily_download()
    test_merge()
    test_rename_data_folders()
    test_running_job_list_without_powershell()
    test_scripts()
    test_folder_name_check()
    print("\nAll index pipeline tests passed ✅")
