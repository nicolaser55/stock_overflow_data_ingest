"""
Ingest Tests (simulated IBKR server, temporary data folder)

Run from the workspace root:   python tests/test_ingest.py   (or python tests/run_all_tests.py)
(Plain asserts, no test framework required. Every test prints ✅ or raises. No connection to IBKR and no access to the
real data folder: SO_INGEST_DATA_PATH points to a temporary folder before ingest is imported.)

What is verified:
    1. IBKR client: error callbacks of every ibapi layout (9.81, 10.19, 10.45), bar timestamps and Decimal volumes,
       the end datetime string, late bars of a timed-out request ignored, informational messages not ending a request.
    2. Sessions: full and half days, the closed-session filter, the real New York clock.
    3. Download: complete / partial / extra / duplicate / empty / no data / pacing / error / timeout behaviours, retries,
       what is saved and what is not, the file format, the download log, the in-flight limit and the submission interval,
       the pacing pause, skipping of sessions already present.
    4. Raw files: text kept exactly, existing rows win, conflicts counted, all-or-nothing merge (move failure, write
       failure), organize / orphan rules.
    5. Maintenance: staging -> raw merge (add-only, dry run, partial, non-session, sanity), clean, organize, rebuild
       (separate folder and in place), the issue report.
    6. Live stream on a simulated clock (mid-session start, half day, before the open).
    7. Compatibility: the research workspace's reader (so.core.raw_data.read_raw_ohlcv_pdf) reads the organized folder
       with every minute exactly once.
"""

import os
import sys
import shutil
import tempfile
import warnings
from decimal import Decimal

# POINT THE INGEST DATA ROOT TO A TEMPORARY FOLDER (BEFORE ingest IS IMPORTED)
TEST_ROOT_PATH_STR = tempfile.mkdtemp(prefix="so_ingest_test_").replace("\\", "/") + "/"
os.environ["SO_INGEST_DATA_PATH"] = TEST_ROOT_PATH_STR
# ADD THE TESTS FOLDER TO THE PATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# IGNORE WARNINGS FROM LIBRARIES
warnings.filterwarnings("ignore")

import pandas as pd
from ibapi.common import BarData
# IMPORT INGEST MODULES
from ingest import config, raw_files
from ingest.ibkr_client import get_error_dict, get_error_status_str, get_bar_ny_ts, get_volume_number, get_ibkr_end_datetime_str
from ingest.sessions import get_ny_now_ts, get_session_pdf, get_closed_session_pdf
from ingest.ibkr_download import get_download_session_pdf, download_session_pdf, DOWNLOAD_LOG_COL_STR_LIST
from ingest.raw_files import (get_ohlcv_file_pdf, read_ohlcv_text_pdf, get_merged_text_pdf_tup, merge_files_into_target_dict,
                              get_organized_period_str, get_organized_file_period_str, get_orphan_file_period_str, TS_KEY_COL_STR)
from ingest.raw_maintenance import merge_staging_into_raw_pdf, organize_raw_pdf, clean_raw_orphans_pdf, rebuild_raw_pdf, get_raw_issue_pdf
from ingest.ibkr_stream import LiveBarStream, get_market_status_dict
from fake_ibkr import FakeIbkrApp

# DEFINE A FIXED "NOW" (TUESDAY 2026-10-06 13:00 NEW YORK) FOR REPRODUCIBLE ROLL-UP RULES
NOW_NY_TS = pd.Timestamp("2026-10-06 13:00", tz=config.NY_TZ_STR)

# FUNCTION: PRINT A PASSED TEST
def passed(name_str_in):
    print(f"✅ {name_str_in}")

# FUNCTION: GET A FRESH TEMPORARY FOLDER
def get_folder_str(name_str_in):
    # CREATE AN EMPTY FOLDER UNDER THE TEST ROOT
    path_str = f"{TEST_ROOT_PATH_STR}{name_str_in}/"
    shutil.rmtree(path_str, ignore_errors=True)
    os.makedirs(path_str)
    return path_str

# FUNCTION: BUILD SYNTHETIC RAW TEXT FOR A DATE RANGE
def get_raw_text_pdf(date1_str_in, date2_str_in, utc_bool_in=False, base_float_in=100.0):
    # LIST TO HOLD THE ROWS
    row_list = []
    # ITERATE OVER THE SESSIONS AND THEIR MINUTES
    for session_row in get_session_pdf(date1_str_in, date2_str_in).itertuples():
        for idx, ts in enumerate(pd.date_range(session_row.first_bar_ts, session_row.last_bar_ts, freq="1min")):
            ts_text = (ts.tz_convert("UTC") if utc_bool_in else ts).isoformat(sep=" ")
            price_float = base_float_in + idx * 0.01
            row_list.append((ts_text, f"{price_float:.2f}", f"{price_float + 0.05:.2f}", f"{price_float - 0.05:.2f}", f"{price_float + 0.01:.2f}",
                             str(1000 + idx), "", str(session_row.date)))
    # RETURN THE TEXT DATAFRAME
    return pd.DataFrame(row_list, columns=config.OHLCV_COL_STR_LIST)

"""
1. IBKR Client
"""

# FUNCTION: TEST THE ERROR LAYOUTS AND CLASSIFICATION
def test_error_layouts():
    # IBAPI 9.81: (code, text)
    assert get_error_dict((162, "x")) == {"error_time_int": None, "error_code_int": 162, "error_str": "x"}
    # IBAPI 10.19: (code, text, json)
    assert get_error_dict((200, "no sec def", "")) == {"error_time_int": None, "error_code_int": 200, "error_str": "no sec def"}
    # IBAPI 10.45: (time, code, text, json)
    assert get_error_dict((1786702266819, 2106, "HMDS ok", "")) == {"error_time_int": 1786702266819, "error_code_int": 2106, "error_str": "HMDS ok"}
    # CLASSIFICATION
    assert get_error_status_str(2106, "HMDS data farm connection is OK") == "info"
    assert get_error_status_str(162, "Historical Market Data Service error message:API historical data query cancelled: Pacing violation") == "pacing"
    assert get_error_status_str(162, "Historical Market Data Service error message:HMDS query returned no data: SPY") == "no_data"
    assert get_error_status_str(200, "No security definition") == "error"
    passed("error callback layouts of ibapi 9.81 / 10.19 / 10.45 and their classification")

# FUNCTION: TEST THE BAR PARSING
def test_bar_parsing():
    # EPOCH SECONDS (formatDate=2) -> NEW YORK TIME, WINTER AND SUMMER
    assert get_bar_ny_ts("1104762600") == pd.Timestamp("2005-01-03 09:30", tz=config.NY_TZ_STR)
    assert str(get_bar_ny_ts("1784295000")) == "2026-07-17 09:30:00-04:00"
    # STRING DATES (formatDate=1)
    assert get_bar_ny_ts("20260717 09:30:00 US/Eastern") == pd.Timestamp("2026-07-17 09:30", tz=config.NY_TZ_STR)
    assert get_bar_ny_ts("20260717 09:30:00") == pd.Timestamp("2026-07-17 09:30", tz=config.NY_TZ_STR)
    # VOLUMES: DECIMAL (IBAPI 10), INT (9.81), WHOLE FLOAT, FRACTIONAL
    assert get_volume_number(Decimal("453200")) == 453200 and isinstance(get_volume_number(Decimal("453200")), int)
    assert get_volume_number(453200) == 453200 and get_volume_number(453200.0) == 453200 and isinstance(get_volume_number(453200.0), int)
    assert get_volume_number(Decimal("12.5")) == 12.5
    # END DATETIME STRING (THE END IS EXCLUSIVE: LAST BAR 15:59 -> 16:00:00)
    assert get_ibkr_end_datetime_str(pd.Timestamp("2005-07-01 16:00", tz=config.NY_TZ_STR)) == "20050701 16:00:00 US/Eastern"
    assert get_ibkr_end_datetime_str(pd.Timestamp("2005-07-01 20:00", tz="UTC")) == "20050701 16:00:00 US/Eastern"
    passed("bar timestamps (epoch and text), Decimal / int volumes, end datetime string")

# FUNCTION: TEST THE REQUEST LIFECYCLE
def test_request_lifecycle():
    # CONNECT A SIMULATED APP
    app = FakeIbkrApp({"2025-12-02": ["timeout"]})
    assert app.connect_app() and app.next_req_id_int == 100
    session_row = next(get_session_pdf("2025-12-02", "2025-12-02").itertuples())
    # A TIMED-OUT REQUEST IS CANCELLED AND ITS LATE BARS ARE IGNORED
    result_dict = app.request_historical_bars_dict(None, session_row.last_bar_ts + pd.Timedelta(minutes=1), 390 * 60, 0.2)
    assert result_dict["status_str"] == "timeout" and result_dict["req_id_int"] in app.cancel_list
    late_bar = BarData()
    late_bar.date, late_bar.open, late_bar.high, late_bar.low, late_bar.close, late_bar.volume = "1764685800", 1, 1, 1, 1, Decimal(1)
    app.historicalData(result_dict["req_id_int"], late_bar)
    app.historicalDataEnd(result_dict["req_id_int"], "", "")
    assert result_dict["req_id_int"] not in app.request_dict
    # A COMPLETE REQUEST: INFORMATIONAL MESSAGE (2176) DOES NOT END IT, NO CANCEL AFTER THE END (NO ERROR 366)
    app.scenario_dict = {}
    cancel_count_int = len(app.cancel_list)
    result_dict = app.request_historical_bars_dict(None, session_row.last_bar_ts + pd.Timedelta(minutes=1), 390 * 60, 5)
    assert result_dict["status_str"] == "complete" and len(result_dict["bar_tuple_list"]) == 390 and len(app.cancel_list) == cancel_count_int
    assert result_dict["bar_tuple_list"][0][0] == session_row.first_bar_ts and isinstance(result_dict["bar_tuple_list"][0][5], int)
    # DISCONNECT RELEASES NOTHING PENDING AND MARKS THE APP DISCONNECTED
    app.disconnect_app()
    assert not app.isConnected()
    passed("timed-out request cancelled and its late bars ignored; info messages ignored; no cancel after the end")

"""
2. Sessions
"""

# FUNCTION: TEST THE SESSIONS
def test_sessions():
    # FULL AND HALF DAYS
    session_pdf = get_session_pdf("2025-11-26", "2025-12-01")
    assert session_pdf["date"].astype(str).tolist() == ["2025-11-26", "2025-11-28", "2025-12-01"]
    assert session_pdf["bar_count_int"].tolist() == [390, 210, 390]
    # CLOSED FILTER: AT 16:04 THE SESSION OF THE DAY IS NOT CLOSED YET (5-MINUTE BUFFER), AT 16:05 IT IS
    one_day_pdf = get_session_pdf("2025-12-01", "2025-12-01")
    assert get_closed_session_pdf(one_day_pdf, pd.Timestamp("2025-12-01 16:04", tz=config.NY_TZ_STR)).empty
    assert len(get_closed_session_pdf(one_day_pdf, pd.Timestamp("2025-12-01 16:05", tz=config.NY_TZ_STR))) == 1
    # THE CLOCK IS REAL NEW YORK TIME (NOT THE LOCAL CLOCK LABELLED AS NEW YORK)
    now_ts = get_ny_now_ts()
    assert str(now_ts.tz) == config.NY_TZ_STR and abs((now_ts - pd.Timestamp.now(tz="UTC")).total_seconds()) < 5
    passed("sessions (full / half days), closed-session filter with buffer, real New York clock")

"""
3. Download
"""

# FUNCTION: TEST THE DOWNLOAD BEHAVIOURS
def test_download_behaviours():
    # DEFINE THE FOLDERS
    raw_path_str, staging_path_str = get_folder_str("dl_raw"), get_folder_str("dl_staging")
    log_path_str = f"{staging_path_str}download_log.csv"
    # DEFINE ONE BEHAVIOUR PER SESSION
    scenario_dict = {"2025-11-24": ["partial", "ok"], "2025-11-25": ["pacing", "ok"], "2025-11-26": ["timeout"], "2025-12-01": ["no_data"],
                     "2025-12-02": ["extra"], "2025-12-03": ["dup"], "2025-12-04": ["error", "ok"], "2025-12-05": ["partial"], "2025-12-08": ["empty"]}
    app = FakeIbkrApp(scenario_dict)
    app.connect_app()
    # SELECT AND DOWNLOAD
    session_pdf = get_download_session_pdf("2025-11-24", "2025-12-09", raw_path_str_in=raw_path_str, staging_path_str_in=staging_path_str, now_ny_ts_in=NOW_NY_TS, alert_in=False)
    assert len(session_pdf) == 11
    summary_pdf = download_session_pdf(app, session_pdf, staging_path_str_in=staging_path_str, log_file_path_str_in=log_path_str, timeout_seconds_in=0.3,
                                       pacing_backoff_seconds_in=0.2, same_request_wait_seconds_in=0.05, alert_in=False)
    status_dict = dict(zip(summary_pdf["date"].astype(str), summary_pdf["status_str"]))
    saved_dict = dict(zip(summary_pdf["date"].astype(str), summary_pdf["saved_bool"]))
    # FINAL STATUSES
    assert status_dict == {"2025-11-24": "complete", "2025-11-25": "complete", "2025-11-26": "timeout", "2025-11-28": "complete", "2025-12-01": "no_data",
                           "2025-12-02": "complete", "2025-12-03": "complete", "2025-12-04": "complete", "2025-12-05": "partial", "2025-12-08": "empty",
                           "2025-12-09": "complete"}, status_dict
    # SAVED: COMPLETE ONES, AND THE PARTIAL ONE AFTER ITS LAST ATTEMPT; NEVER A TIMEOUT / ERROR / EMPTY
    assert [d for d, s in saved_dict.items() if s] == ["2025-11-24", "2025-11-25", "2025-11-28", "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05", "2025-12-09"]
    assert app.attempt_dict["2025-12-05"] == config.REQUEST_ATTEMPT_COUNT and app.attempt_dict["2025-11-26"] == config.REQUEST_ATTEMPT_COUNT
    # FILE CONTENTS: SESSION MINUTES ONLY, ONE ROW PER MINUTE
    count_dict = {f[-12:-4]: len(pd.read_csv(f"{staging_path_str}{f}")) for f in os.listdir(staging_path_str) if f.startswith("ohlcv_data_")}
    assert count_dict == {"20251124": 390, "20251125": 390, "20251128": 210, "20251202": 390, "20251203": 390, "20251204": 390, "20251205": 360, "20251209": 390}
    # FILE FORMAT (SAME COLUMNS AND TEXT AS THE EXISTING RAW DAY FILES)
    with open(f"{staging_path_str}ohlcv_data_20251128.csv") as file:
        line_list = file.read().splitlines()
    assert line_list[0] == "timestamp,open,high,low,close,volume,created_ts,date"
    assert line_list[1].startswith("2025-11-28 09:30:00-05:00,100.0,") and line_list[1].endswith(",1030,,2025-11-28")
    assert line_list[-1].startswith("2025-11-28 12:59:00-05:00,")
    # THE LOG HAS ONE ROW PER ATTEMPT, WITH THE IBAPI VERSION
    log_pdf = pd.read_csv(log_path_str)
    assert log_pdf.columns.tolist() == DOWNLOAD_LOG_COL_STR_LIST and len(log_pdf) == sum(app.attempt_dict.values())
    assert log_pdf["ibapi_version_str"].notna().all() and (log_pdf.loc[log_pdf["date"] == "2025-12-02", "extra_count_int"] == 1).all()
    # NOT A SINGLE TEMPORARY FILE LEFT
    assert not [f for f in os.listdir(staging_path_str) if f.endswith(".tmp")]
    # SESSIONS ALREADY IN STAGING ARE SKIPPED NEXT TIME (UNLESS REDOWNLOAD)
    again_pdf = get_download_session_pdf("2025-11-24", "2025-12-09", raw_path_str_in=raw_path_str, staging_path_str_in=staging_path_str, now_ny_ts_in=NOW_NY_TS, alert_in=False)
    assert again_pdf["date"].astype(str).tolist() == ["2025-11-26", "2025-12-01", "2025-12-08"]
    assert len(get_download_session_pdf("2025-11-24", "2025-12-09", redownload_bool_in=True, raw_path_str_in=raw_path_str, staging_path_str_in=staging_path_str,
                                        now_ny_ts_in=NOW_NY_TS, alert_in=False)) == 11
    # DEFAULT START = THE DAY AFTER THE LAST PRESENT DATE
    assert str(get_download_session_pdf(None, "2025-12-12", raw_path_str_in=raw_path_str, staging_path_str_in=staging_path_str,
                                        now_ny_ts_in=pd.Timestamp("2025-12-13 12:00", tz=config.NY_TZ_STR), alert_in=False)["date"].min()) == "2025-12-10"
    passed("download: retries, saved / not saved per behaviour, file format, log, skip of present sessions, default start")

# FUNCTION: TEST THE PACING AND CONCURRENCY OF THE DOWNLOAD
def test_download_pacing():
    # DEFINE THE FOLDERS
    staging_path_str = get_folder_str("pace_staging")
    # SIMULATE SLOW ANSWERS (0.3 S) WITH THE IBAPI 9.81 LAYOUT AND INTEGER VOLUMES
    app = FakeIbkrApp({"2025-12-04": ["pacing", "ok"]}, error_layout_str_in="9.81", decimal_volume_bool_in=False, delay_seconds_in=0.3)
    app.connect_app()
    session_pdf = get_session_pdf("2025-12-01", "2025-12-12")
    summary_pdf = download_session_pdf(app, session_pdf, staging_path_str_in=staging_path_str, log_file_path_str_in=f"{staging_path_str}log.csv",
                                       max_in_flight_int_in=3, min_interval_seconds_in=0.05, timeout_seconds_in=5, pacing_backoff_seconds_in=0.5,
                                       same_request_wait_seconds_in=0.05, alert_in=False)
    assert (summary_pdf["status_str"] == "complete").all() and len(summary_pdf) == 10
    # AT MOST 3 IN FLIGHT, AND MORE THAN 1 (CONCURRENCY IS USED)
    assert 1 < app.max_open_int <= 3, app.max_open_int
    # SUBMISSIONS ARE AT LEAST THE MINIMUM INTERVAL APART
    submit_time_list = sorted(request_tup[4] for request_tup in app.sent_request_list)
    assert min(b - a for a, b in zip(submit_time_list, submit_time_list[1:])) >= 0.045
    # AFTER THE PACING VIOLATION (ANSWERED AT ~0.3 S) NOTHING IS SUBMITTED FOR THE BACK-OFF
    pacing_req_time = next(t for _, d, _, _, t in app.sent_request_list if d == "2025-12-04")
    later_time_list = [t for t in submit_time_list if t > pacing_req_time + 0.3]
    assert not later_time_list or later_time_list[0] >= pacing_req_time + 0.3 + 0.45
    passed("download: at most N requests in flight, minimum interval between submissions, pause after a pacing violation (ibapi 9.81 layout)")

"""
4. Raw Files
"""

# FUNCTION: TEST THE TEXT-PRESERVING MERGE
def test_text_merge():
    # DEFINE A TARGET WITH UTC TEXT AND AN EXTRA COLUMN, AND A SOURCE WITH NEW YORK TEXT, A CONFLICT AND A NEW DAY
    path_str = get_folder_str("merge_text")
    target_pdf = get_raw_text_pdf("2025-12-01", "2025-12-01", utc_bool_in=True).assign(highest_val="")
    target_pdf.loc[5, "high"] = "100.50"
    target_pdf.to_csv(f"{path_str}ohlcv_data_202512.csv", index=False)
    source_pdf = pd.concat([get_raw_text_pdf("2025-12-01", "2025-12-01").iloc[:10], get_raw_text_pdf("2025-12-02", "2025-12-02")])
    source_pdf.to_csv(f"{path_str}ohlcv_data_20251202.csv", index=False)
    target_bytes = open(f"{path_str}ohlcv_data_202512.csv", "rb").read()
    # MERGE IN MEMORY
    merged_pdf, info_dict = get_merged_text_pdf_tup([read_ohlcv_text_pdf(f"{path_str}ohlcv_data_202512.csv"), read_ohlcv_text_pdf(f"{path_str}ohlcv_data_20251202.csv")])
    assert info_dict["output_row_count"] == 780 and info_dict["duplicate_row_count"] == 10 and info_dict["conflict_minute_count"] == 1
    # THE EXISTING ROW WINS AND KEEPS ITS TEXT; NEW ROWS KEEP THEIRS
    assert merged_pdf.loc[5, "high"] == "100.50" and merged_pdf.loc[5, "timestamp"] == "2025-12-01 14:35:00+00:00"
    assert merged_pdf.loc[390, "timestamp"] == "2025-12-02 09:30:00-05:00" and merged_pdf.loc[0, "open"] == "100.00"
    # DRY RUN CHANGES NOTHING
    merge_files_into_target_dict(f"{path_str}ohlcv_data_202512.csv", [f"{path_str}ohlcv_data_20251202.csv"], False, f"{path_str[:-1]}_backup/", False)
    assert open(f"{path_str}ohlcv_data_202512.csv", "rb").read() == target_bytes and os.path.isfile(f"{path_str}ohlcv_data_20251202.csv")
    # APPLY: TARGET REWRITTEN (TEXT KEPT), SOURCE MOVED TO THE BACKUP, TARGET BACKED UP
    result_dict = merge_files_into_target_dict(f"{path_str}ohlcv_data_202512.csv", [f"{path_str}ohlcv_data_20251202.csv"], True, f"{path_str[:-1]}_backup/", False)
    assert result_dict["status_str"] == "merged" and not os.path.isfile(f"{path_str}ohlcv_data_20251202.csv")
    assert open(f"{path_str[:-1]}_backup/ohlcv_data_202512.csv", "rb").read() == target_bytes
    with open(f"{path_str}ohlcv_data_202512.csv") as file:
        text_str = file.read()
    assert text_str.splitlines()[0] == "timestamp,open,high,low,close,volume,created_ts,date,highest_val"
    assert text_str.splitlines()[6] == "2025-12-01 14:35:00+00:00,100.05,100.50,100.00,100.06,1005,,2025-12-01,"
    assert text_str.splitlines()[391].startswith("2025-12-02 09:30:00-05:00,100.00,100.05,99.95,100.01,1000,,2025-12-02")
    passed("text-preserving merge: UTC and New York text kept, '100.00' kept, existing row wins, conflict counted, backup made")

# FUNCTION: TEST THE ALL-OR-NOTHING MERGE
def test_merge_rollback():
    # DEFINE A TARGET AND TWO SOURCES
    path_str = get_folder_str("merge_rollback")
    get_raw_text_pdf("2025-12-01", "2025-12-01").to_csv(f"{path_str}ohlcv_data_202512.csv", index=False)
    get_raw_text_pdf("2025-12-02", "2025-12-02").to_csv(f"{path_str}ohlcv_data_20251202.csv", index=False)
    get_raw_text_pdf("2025-12-03", "2025-12-03").to_csv(f"{path_str}ohlcv_data_20251203.csv", index=False)
    before_dict = {f: open(f"{path_str}{f}", "rb").read() for f in os.listdir(path_str)}
    # CASE 1: THE SECOND SOURCE CANNOT BE MOVED (DELETES REFUSED) -> NOTHING CHANGES
    original_move = raw_files.shutil.move
    call_list = []
    def failing_move(src, dst):
        call_list.append(src)
        if len(call_list) == 2:
            raise PermissionError("access denied")
        return original_move(src, dst)
    raw_files.shutil.move = failing_move
    try:
        result_dict = merge_files_into_target_dict(f"{path_str}ohlcv_data_202512.csv", [f"{path_str}ohlcv_data_20251202.csv", f"{path_str}ohlcv_data_20251203.csv"],
                                                   True, f"{path_str[:-1]}_backup1/", False)
    finally:
        raw_files.shutil.move = original_move
    assert result_dict["status_str"] == "rolled_back"
    assert {f: open(f"{path_str}{f}", "rb").read() for f in os.listdir(path_str)} == before_dict
    # CASE 2: THE TARGET WRITE FAILS -> TARGET AND SOURCES RESTORED
    original_write = raw_files.write_text_pdf_atomic
    def failing_write(pdf_in, path_in):
        raise OSError("disk full")
    raw_files.write_text_pdf_atomic = failing_write
    try:
        result_dict = merge_files_into_target_dict(f"{path_str}ohlcv_data_202512.csv", [f"{path_str}ohlcv_data_20251202.csv", f"{path_str}ohlcv_data_20251203.csv"],
                                                   True, f"{path_str[:-1]}_backup2/", False)
    finally:
        raw_files.write_text_pdf_atomic = original_write
    assert result_dict["status_str"] == "failed"
    assert {f: open(f"{path_str}{f}", "rb").read() for f in os.listdir(path_str)} == before_dict
    # CASE 3: NEW TARGET, WRITE FAILS -> NO TARGET LEFT, SOURCES RESTORED
    raw_files.write_text_pdf_atomic = failing_write
    try:
        merge_files_into_target_dict(f"{path_str}ohlcv_data_2025.csv", [f"{path_str}ohlcv_data_20251202.csv"], True, f"{path_str[:-1]}_backup3/", False)
    finally:
        raw_files.write_text_pdf_atomic = original_write
    assert {f: open(f"{path_str}{f}", "rb").read() for f in os.listdir(path_str)} == before_dict
    passed("all-or-nothing merge: refused delete, failed write, failed write of a new target -> folder unchanged")

# FUNCTION: TEST THE ROLL-UP RULES
def test_rollup_rules():
    # ORGANIZE RULE AT 2026-10-06
    assert get_organized_period_str(pd.Timestamp("2026-10-05").date(), NOW_NY_TS) == "20261005"
    assert get_organized_period_str(pd.Timestamp("2026-09-30").date(), NOW_NY_TS) == "202609"
    assert get_organized_period_str(pd.Timestamp("2025-12-31").date(), NOW_NY_TS) == "2025"
    assert get_organized_file_period_str("20260814", NOW_NY_TS) == "202608" and get_organized_file_period_str("202512", NOW_NY_TS) == "2025"
    assert get_organized_file_period_str("202609", NOW_NY_TS) == "202609" and get_organized_file_period_str("2024", NOW_NY_TS) == "2024"
    assert get_organized_file_period_str("20251231", NOW_NY_TS) == "2025"
    # NEAR MIDNIGHT IN NEW YORK THE MONTH HAS NOT CHANGED YET (THE OLD SCRIPTS USED THE LAPTOP CLOCK)
    assert get_organized_period_str(pd.Timestamp("2026-09-30").date(), pd.Timestamp("2026-09-30 23:30", tz=config.NY_TZ_STR)) == "20260930"
    # ORPHAN RULE
    period_set = {"2025", "202609", "20250102", "20260903", "20261001"}
    assert get_orphan_file_period_str("20250102", period_set) == "2025" and get_orphan_file_period_str("20260903", period_set) == "202609"
    assert get_orphan_file_period_str("20261001", period_set) == "20261001" and get_orphan_file_period_str("2025", period_set) == "2025"
    passed("organize and orphan roll-up rules (New York month boundary)")

"""
5. Maintenance
"""

# FUNCTION: BUILD A RAW AND A STAGING FOLDER
def get_raw_and_staging_tup(name_str_in):
    # DEFINE THE FOLDERS
    raw_path_str, staging_path_str = get_folder_str(f"{name_str_in}_raw"), get_folder_str(f"{name_str_in}_staging")
    # RAW: A YEAR FILE (UTC TEXT), A MONTH FILE, AN ORPHAN DAY FILE OF 2024, A STRAY FILE
    get_raw_text_pdf("2024-12-23", "2024-12-31", utc_bool_in=True).to_csv(f"{raw_path_str}ohlcv_data_2024.csv", index=False)
    get_raw_text_pdf("2025-12-01", "2025-12-05").to_csv(f"{raw_path_str}ohlcv_data_202512.csv", index=False)
    get_raw_text_pdf("2024-12-31", "2024-12-31").to_csv(f"{raw_path_str}ohlcv_data_20241231.csv", index=False)
    open(f"{raw_path_str}notes.txt", "w").write("not data")
    # STAGING: COMPLETE DAYS, A DAY ALREADY IN RAW, A PARTIAL DAY, A NON-SESSION DAY, A DAY WITH A BROKEN CANDLE
    for date_str in ["2025-12-08", "2025-12-09", "2025-12-03"]:
        get_raw_text_pdf(date_str, date_str).to_csv(f"{staging_path_str}ohlcv_data_{date_str.replace('-', '')}.csv", index=False)
    get_raw_text_pdf("2025-12-10", "2025-12-10").iloc[30:].to_csv(f"{staging_path_str}ohlcv_data_20251210.csv", index=False)
    get_raw_text_pdf("2025-12-12", "2025-12-12").assign(date="2025-12-13", timestamp=lambda p: p["timestamp"].str.replace("2025-12-12", "2025-12-13")) \
        .to_csv(f"{staging_path_str}ohlcv_data_20251213.csv", index=False)
    broken_pdf = get_raw_text_pdf("2025-12-11", "2025-12-11")
    broken_pdf.loc[10, "high"] = "50.00"
    broken_pdf.to_csv(f"{staging_path_str}ohlcv_data_20251211.csv", index=False)
    # RETURN THE FOLDERS
    return raw_path_str, staging_path_str

# FUNCTION: TEST THE STAGING -> RAW MERGE
def test_merge_staging():
    # BUILD THE FOLDERS
    raw_path_str, staging_path_str = get_raw_and_staging_tup("stage")
    raw_before_dict = {f: open(f"{raw_path_str}{f}", "rb").read() for f in os.listdir(raw_path_str)}
    merge_kwargs = dict(staging_path_str_in=staging_path_str, raw_path_str_in=raw_path_str, merged_path_str_in=f"{staging_path_str}merged/",
                        log_file_path_str_in=f"{staging_path_str}merge_log.csv", now_ny_ts_in=NOW_NY_TS, alert_in=False)
    # DRY RUN: DECISIONS, NOTHING WRITTEN
    result_pdf = merge_staging_into_raw_pdf(False, False, **merge_kwargs)
    decision_dict = dict(zip(result_pdf["date"].astype(str), result_pdf["decision_str"]))
    assert decision_dict["2025-12-08"] == "add" and decision_dict["2025-12-09"] == "add"
    assert decision_dict["2025-12-03"].startswith("skip: date already in raw") and decision_dict["2025-12-10"].startswith("skip: partial")
    assert decision_dict["2025-12-13"].startswith("skip: not a closed") and decision_dict["2025-12-11"].startswith("skip: candlestick")
    assert {f: open(f"{raw_path_str}{f}", "rb").read() for f in os.listdir(raw_path_str)} == raw_before_dict
    # APPLY WITH PARTIAL: ADD-ONLY (EXISTING FILES BYTE FOR BYTE UNCHANGED), FILES MOVED TO merged/, LOG WRITTEN
    result_pdf = merge_staging_into_raw_pdf(True, True, **merge_kwargs)
    assert sorted(result_pdf.loc[result_pdf["decision_str"] == "added", "date"].astype(str)) == ["2025-12-08", "2025-12-09", "2025-12-10"]
    assert all(open(f"{raw_path_str}{f}", "rb").read() == b for f, b in raw_before_dict.items())
    assert open(f"{raw_path_str}ohlcv_data_20251208.csv", "rb").read() == open(f"{staging_path_str}merged/ohlcv_data_20251208.csv", "rb").read()
    assert not os.path.isfile(f"{staging_path_str}ohlcv_data_20251208.csv") and os.path.isfile(f"{staging_path_str}ohlcv_data_20251203.csv")
    assert len(pd.read_csv(f"{staging_path_str}merge_log.csv")) == 3
    # A SECOND APPLY ADDS NOTHING
    assert not (merge_staging_into_raw_pdf(True, True, **merge_kwargs)["decision_str"] == "added").any()
    passed("staging -> raw: add-only, existing date / partial / non-session / broken candle skipped, dry run, merged/ and log")

# FUNCTION: TEST CLEAN, ORGANIZE, ISSUES AND THE REPO READER
def test_clean_organize_issues():
    # BUILD THE FOLDERS AND ADD THE STAGED DAYS
    raw_path_str, staging_path_str = get_raw_and_staging_tup("org")
    merge_staging_into_raw_pdf(True, True, staging_path_str, raw_path_str, f"{staging_path_str}merged/", f"{staging_path_str}merge_log.csv", NOW_NY_TS, False)
    # ISSUES BEFORE: THE ORPHAN DAY FILE DUPLICATES 2024-12-31, 2025-12-10 IS PARTIAL
    issue_pdf = get_raw_issue_pdf([raw_path_str], "2024-12-23", NOW_NY_TS, alert_in=False)
    issue_dict = dict(zip(issue_pdf["date"].astype(str), issue_pdf["issue_str"]))
    assert issue_dict["2024-12-31"] == "duplicate_rows" and issue_dict["2025-12-10"] == "missing_minutes" and issue_dict["2025-01-02"] == "missing_session"
    # CLEAN: THE ORPHAN IS FOLDED INTO 2024, THE DECEMBER DAY FILES INTO 202512
    clean_pdf = clean_raw_orphans_pdf(True, raw_path_str, alert_in=False)
    assert sorted(clean_pdf["target_str"]) == ["ohlcv_data_2024.csv", "ohlcv_data_202512.csv"] and (clean_pdf["status_str"] == "merged").all()
    # ORGANIZE AT 2026-10-06: 202512 GOES INTO 2025
    organize_pdf = organize_raw_pdf(True, raw_path_str, NOW_NY_TS, alert_in=False)
    assert organize_pdf["target_str"].tolist() == ["ohlcv_data_2025.csv"]
    assert sorted(get_ohlcv_file_pdf(raw_path_str, alert_in=False)["file_name_str"]) == ["ohlcv_data_2024.csv", "ohlcv_data_2025.csv"]
    # A SECOND ORGANIZE DOES NOTHING
    assert organize_raw_pdf(True, raw_path_str, NOW_NY_TS, alert_in=False).empty
    # ISSUES AFTER: NO DUPLICATE ANY MORE; THE PARTIAL DAY IS STILL REPORTED
    issue_pdf = get_raw_issue_pdf([raw_path_str], "2024-12-23", NOW_NY_TS, alert_in=False)
    assert not issue_pdf["issue_str"].str.contains("duplicate").any() and "2025-12-10" in set(issue_pdf["date"].astype(str))
    # THE RESEARCH WORKSPACE'S READER SEES EVERY MINUTE EXACTLY ONCE
    from so.core.raw_data import read_raw_ohlcv_pdf
    repo_pdf = read_raw_ohlcv_pdf(raw_path_str)
    expected_count_int = get_session_pdf("2024-12-23", "2024-12-31")["bar_count_int"].sum() + get_session_pdf("2025-12-01", "2025-12-10")["bar_count_int"].sum() - 30
    assert len(repo_pdf) == repo_pdf["timestamp"].nunique() == expected_count_int, (len(repo_pdf), expected_count_int)
    assert str(repo_pdf["timestamp"].dt.tz) == config.NY_TZ_STR
    passed("issue report (duplicates, partial, missing sessions), clean, organize (idempotent), repo reader sees every minute once")

# FUNCTION: TEST THE REBUILD
def test_rebuild():
    # BUILD THE FOLDERS (ORPHAN INCLUDED) AND A CONFLICT: THE ORPHAN DAY FILE HAS A DIFFERENT HIGH THAN THE YEAR FILE
    raw_path_str, _ = get_raw_and_staging_tup("rebuild")
    orphan_pdf = pd.read_csv(f"{raw_path_str}ohlcv_data_20241231.csv", dtype=str, keep_default_na=False)
    orphan_pdf.loc[0, "high"] = "999.99"
    orphan_pdf.to_csv(f"{raw_path_str}ohlcv_data_20241231.csv", index=False)
    # INTO A SEPARATE FOLDER: SOURCE UNTOUCHED, ORGANIZED LAYOUT, YEAR FILE ROW WINS
    source_before_dict = {f: open(f"{raw_path_str}{f}", "rb").read() for f in os.listdir(raw_path_str)}
    target_path_str = f"{raw_path_str[:-1]}_rebuilt/"
    rebuild_raw_pdf(raw_path_str, target_path_str, False, now_ny_ts_in=NOW_NY_TS, alert_in=False)
    assert not os.path.isdir(target_path_str)
    result_pdf = rebuild_raw_pdf(raw_path_str, target_path_str, True, now_ny_ts_in=NOW_NY_TS, alert_in=False)
    assert sorted(result_pdf["file_name_str"]) == ["ohlcv_data_2024.csv", "ohlcv_data_2025.csv"] and result_pdf["row_count_int"].sum() == get_session_pdf("2024-12-23", "2024-12-31")["bar_count_int"].sum() + 5 * 390
    assert {f: open(f"{raw_path_str}{f}", "rb").read() for f in os.listdir(raw_path_str)} == source_before_dict
    assert "999.99" not in open(f"{target_path_str}ohlcv_data_2024.csv").read()
    # IN PLACE: STALE FILES MOVED TO THE BACKUP, SAME MINUTES
    rebuild_raw_pdf(raw_path_str, raw_path_str, True, now_ny_ts_in=NOW_NY_TS, alert_in=False)
    assert sorted(get_ohlcv_file_pdf(raw_path_str, alert_in=False)["file_name_str"]) == ["ohlcv_data_2024.csv", "ohlcv_data_2025.csv"]
    assert os.path.isfile(f"{raw_path_str}notes.txt")
    assert read_ohlcv_text_pdf(f"{raw_path_str}ohlcv_data_2025.csv")[TS_KEY_COL_STR].is_unique
    passed("rebuild into a separate folder (source untouched, year rows win) and in place (stale files backed up)")

"""
6. Live Stream
"""

# CLASS: SIMULATED CLOCK
class Clock:
    def __init__(self, ts_str_in):
        self.ts = pd.Timestamp(ts_str_in, tz=config.NY_TZ_STR)
    def now(self):
        return self.ts
    def wait(self, seconds_float_in):
        self.ts += pd.Timedelta(seconds=seconds_float_in)
        return False

# FUNCTION: TEST THE LIVE STREAM
def test_live_stream():
    # CONNECT A SIMULATED APP
    app = FakeIbkrApp()
    app.connect_app()
    live_path_str = get_folder_str("live")
    # MID-SESSION START ON A HALF DAY: BACK-FILL FROM THE OPEN, THEN ONE POLL PER MINUTE UNTIL THE LAST BAR
    clock = Clock("2025-11-28 11:47:10")
    stream = LiveBarStream(app, live_path_str_in=live_path_str, now_func_in=clock.now, wait_func_in=clock.wait, alert_in=False)
    assert stream.start()
    stream.thread.join(30)
    live_pdf = stream.get_live_pdf()
    assert len(live_pdf) == 210 and live_pdf["timestamp"].is_unique and stream.poll_dict_list[0]["window_str"] == "09:30-11:45"
    assert all(p["missing_count_int"] == 0 for p in stream.poll_dict_list)
    # EVERY POLL HAPPENS AT HH:MM:30 (MINUTE + DELAY) AND NEVER ASKS FOR A BAR THAT IS NOT COMPLETE
    assert all(p["poll_ts"].second == 30 for p in stream.poll_dict_list[1:]) and clock.ts == pd.Timestamp("2025-11-28 13:00:30", tz=config.NY_TZ_STR)
    assert len(pd.read_csv(f"{live_path_str}ohlcv_live_20251128.csv")) == 210
    # BEFORE THE OPEN: WAITS, THEN FIRST POLL AT 09:31:30
    clock = Clock("2025-12-02 08:00:00")
    stream = LiveBarStream(app, live_path_str_in=live_path_str, now_func_in=clock.now, wait_func_in=clock.wait, alert_in=False)
    stream.start()
    stream.thread.join(30)
    assert stream.poll_dict_list[0]["status_str"] == "waiting" and stream.poll_dict_list[1]["poll_ts"] == pd.Timestamp("2025-12-02 09:31:30", tz=config.NY_TZ_STR)
    assert len(stream.get_live_pdf()) == 390
    # NO SESSION (SATURDAY): DOES NOT START; MARKET STATUS GIVES THE NEXT OPEN
    clock = Clock("2025-11-29 10:00")
    assert not LiveBarStream(app, now_func_in=clock.now, wait_func_in=clock.wait, alert_in=False).start()
    status_dict = get_market_status_dict(clock.now())
    assert not status_dict["market_open_bool"] and status_dict["next_first_bar_ts"] == pd.Timestamp("2025-12-01 09:30", tz=config.NY_TZ_STR)
    passed("live stream: back-fill, aligned polls, stops after the last bar, waits before the open, no session on a Saturday")

# RUN THE TESTS
if __name__ == "__main__":
    # TRY TO RUN EVERY TEST, THEN REMOVE THE TEMPORARY FOLDER
    try:
        test_error_layouts()
        test_bar_parsing()
        test_request_lifecycle()
        test_sessions()
        test_download_behaviours()
        test_download_pacing()
        test_text_merge()
        test_merge_rollback()
        test_rollup_rules()
        test_merge_staging()
        test_clean_organize_issues()
        test_rebuild()
        test_live_stream()
        print("\nAll ingest tests passed ✅")
    finally:
        shutil.rmtree(TEST_ROOT_PATH_STR, ignore_errors=True)
