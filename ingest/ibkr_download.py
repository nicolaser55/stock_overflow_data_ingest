import time
from collections import deque
from datetime import datetime, timezone
import pandas as pd
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE IBKR CLIENT FUNCTIONS
from ingest.ibkr_client import get_contract, get_ibapi_version_str
# IMPORT THE SESSION FUNCTIONS
from ingest.sessions import get_ny_now_ts, get_session_pdf, get_closed_session_pdf, get_session_check_dict, get_session_minute_index
# IMPORT THE RAW FILE FUNCTIONS
from ingest.raw_files import get_file_name_str, get_present_date_set, write_text_pdf_atomic, append_log_rows

"""
IBKR Session Download: one request per NYSE session, several sessions in flight, every session checked minute by minute

What changed compared with get_date_ohlcv_pdf + generate_func_data_date_range of ibkr_data_stream.ipynb, and why:

    1. A session is saved only when IBKR said the request is finished (historicalDataEnd). A timeout or an error is
       retried and never written (before, the bars received before a 10 s timeout were saved as the day's file, and
       access mode "I" then skipped that date forever).
    2. Every session is checked against the NYSE schedule minute by minute before it is saved. A session with missing
       minutes is requested again (up to REQUEST_ATTEMPT_COUNT times, at least 16 s apart: IBKR rejects identical
       requests within 15 s); if it is still partial, it is saved and flagged "partial" in the download log (IBKR does
       not have every minute of every day: e.g. 2009-07-27 and 2013-12-23 are partial in the existing raw data).
    3. Bars outside the session (pre / post market) are dropped and counted. Duplicate minutes are dropped.
    4. Several sessions are in flight at once (MAX_IN_FLIGHT_REQUEST_COUNT) with a minimum interval between submissions
       (REQUEST_MIN_INTERVAL_SECONDS) that stays inside IBKR's pacing rules; a pacing violation pauses submissions with
       an increasing back-off and re-queues the session.
    5. Which sessions to download is decided once, from the file names and timestamps already present in the raw and
       staging folders, instead of reading every day file again before each request.
    6. Files go to the STAGING folder, written atomically. Every attempt is appended to the download log (staging folder),
       with the ibapi version, so the provenance of each staged day is recorded.
    7. Only sessions that have closed (in real New York time, plus a buffer) are requested.

File format (identical to the existing raw day files): timestamp (New York time with offset, e.g.
"2026-07-17 09:30:00-04:00"), open, high, low, close, volume, created_ts (empty), date ("YYYY-MM-DD").
"""

# DEFINE THE DOWNLOAD LOG COLUMNS
DOWNLOAD_LOG_COL_STR_LIST = ["logged_at_utc", "date", "attempt_int", "status_str", "saved_bool", "expected_count_int", "bar_count_int",
                             "missing_count_int", "extra_count_int", "duplicate_count_int", "missing_str", "extra_str", "error_code_int",
                             "error_str", "request_seconds_float", "end_datetime_str", "duration_str", "ibapi_version_str"]
# DEFINE THE MINIMUM WAIT BEFORE THE SAME SESSION IS REQUESTED AGAIN (IBKR REJECTS IDENTICAL REQUESTS WITHIN 15 SECONDS)
SAME_REQUEST_WAIT_SECONDS = 16
# DEFINE THE MAXIMUM NUMBER OF PACING RE-QUEUES OF ONE SESSION
MAX_PACING_REQUEUE_INT = 10

"""
Session Selection
"""

# FUNCTION: GET THE SESSIONS TO DOWNLOAD
def get_download_session_pdf(date1_in=None, date2_in=None, date_list_in=None, redownload_bool_in=False,
                             raw_path_str_in=config.RAW_OHLCV_PATH_STR, staging_path_str_in=config.STAGING_OHLCV_PATH_STR,
                             now_ny_ts_in=None, alert_in=True):
    """
    Selects the closed NYSE sessions to download: every session of [date1, date2] (or of date_list) that is not already
    in the raw or staging folder (unless redownload_bool_in).

    Args:
        date1_in (str | date | None): First date (None = the day after the last date present in raw + staging)
        date2_in (str | date | None): Last date (None = today)
        date_list_in (list | None): Explicit dates (replaces the range)
        redownload_bool_in (bool): Download even the sessions already present (the staging file is replaced)
        raw_path_str_in (str): Raw folder
        staging_path_str_in (str): Staging folder
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)
        alert_in (bool): Display information

    Returns:
        pd.DataFrame: Sessions (get_session_pdf columns) to download, oldest first
    """
    # COLLECT THE CURRENT NEW YORK TIME
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    # IF EXPLICIT DATES ARE GIVEN
    if date_list_in:
        # COLLECT THE DATES
        date_list = sorted({pd.Timestamp(date_in).date() for date_in in date_list_in})
        # COLLECT THEIR SESSIONS
        session_pdf = get_session_pdf(date_list[0], date_list[-1])
        session_pdf = session_pdf[session_pdf["date"].isin(date_list)]
        # REPORT THE DATES WITHOUT A SESSION
        no_session_list = sorted(set(date_list) - set(session_pdf["date"]))
        print(f"⚠️ Not NYSE sessions (skipped): {[str(d) for d in no_session_list]}") if alert_in and no_session_list else None
    # IF A RANGE IS GIVEN
    else:
        # IF THERE IS NO FIRST DATE
        if date1_in is None:
            # COLLECT THE LAST PRESENT DATE
            present_date_set = get_present_date_set([raw_path_str_in, staging_path_str_in], now_ny_ts.date() - pd.Timedelta(days=400), now_ny_ts.date())
            # IF NOTHING IS PRESENT IN THE LAST 400 DAYS
            if not present_date_set:
                # RAISE AN ERROR (A FULL HISTORY DOWNLOAD MUST BE ASKED FOR EXPLICITLY)
                raise ValueError("No data in the last 400 days of the raw and staging folders: give the first date explicitly.")
            # START THE DAY AFTER THE LAST PRESENT DATE
            date1_in = max(present_date_set) + pd.Timedelta(days=1)
            print(f"ℹ️ Last date present in raw + staging: {max(present_date_set)}; starting at {date1_in}") if alert_in else None
        # COLLECT THE SESSIONS OF THE RANGE
        session_pdf = get_session_pdf(date1_in, date2_in if date2_in is not None else now_ny_ts.date())
    # IF THERE ARE NO SESSIONS
    if session_pdf.empty:
        # RETURN THE EMPTY DATAFRAME
        return session_pdf
    # KEEP THE CLOSED SESSIONS
    closed_session_pdf = get_closed_session_pdf(session_pdf, now_ny_ts)
    # DISPLAY THE SESSIONS NOT CLOSED YET
    open_date_list = sorted(set(session_pdf["date"]) - set(closed_session_pdf["date"]))
    print(f"ℹ️ Not closed yet (skipped): {[str(d) for d in open_date_list]}") if alert_in and open_date_list else None
    # IF PRESENT SESSIONS MUST BE SKIPPED
    if not redownload_bool_in and not closed_session_pdf.empty:
        # COLLECT THE PRESENT DATES OF THE RANGE
        present_date_set = get_present_date_set([raw_path_str_in, staging_path_str_in], closed_session_pdf["date"].min(), closed_session_pdf["date"].max())
        # DISPLAY INFORMATION
        print(f"ℹ️ Already present in raw or staging (skipped): {len(present_date_set & set(closed_session_pdf['date'])):,} session(s)") if alert_in else None
        # DROP THE PRESENT DATES
        closed_session_pdf = closed_session_pdf[~closed_session_pdf["date"].isin(present_date_set)]
    # RETURN THE SESSIONS
    return closed_session_pdf.reset_index(drop=True)

"""
Session Bars
"""

# FUNCTION: GET THE REQUEST WINDOW OF A SESSION
def get_session_request_tup(session_row_in):
    """
    Args:
        session_row_in (pd.Series | namedtuple): Row of get_session_pdf

    Returns:
        tuple: (exclusive end pd.Timestamp = last bar + 1 minute, duration in seconds = expected bars x 60)
    """
    # RETURN THE END AND THE DURATION
    return session_row_in.last_bar_ts + pd.Timedelta(minutes=1), int(session_row_in.bar_count_int) * 60

# FUNCTION: GET THE TEXT DATAFRAME OF A SESSION
def get_session_text_pdf(bar_tuple_list_in, session_row_in):
    """
    Builds the session file from the received bars: keeps the session minutes, drops duplicate minutes, sorts by time
    and formats the columns as in the existing raw files.

    Args:
        bar_tuple_list_in (list[tuple]): Bars (timestamp, open, high, low, close, volume) from ingest.ibkr_client
        session_row_in (pd.Series | namedtuple): Row of get_session_pdf

    Returns:
        pd.DataFrame: config.OHLCV_COL_STR_LIST columns
    """
    # CREATE THE BAR DATAFRAME
    bar_pdf = pd.DataFrame(bar_tuple_list_in, columns=["timestamp", "open", "high", "low", "close", "volume"])
    # IF THERE ARE NO BARS
    if bar_pdf.empty:
        # RETURN AN EMPTY DATAFRAME
        return pd.DataFrame(columns=config.OHLCV_COL_STR_LIST)
    # KEEP THE SESSION MINUTES, ONE ROW PER MINUTE, IN TIME ORDER
    bar_pdf["timestamp"] = pd.DatetimeIndex(bar_pdf["timestamp"]).tz_convert(config.NY_TZ_STR)
    bar_pdf = bar_pdf[bar_pdf["timestamp"].isin(get_session_minute_index(session_row_in))]
    bar_pdf = bar_pdf.drop_duplicates("timestamp", keep="first").sort_values("timestamp").reset_index(drop=True)
    # FORMAT THE TIMESTAMP ("2026-07-17 09:30:00-04:00") AND ADD THE EMPTY created_ts AND THE DATE
    bar_pdf["timestamp"] = bar_pdf["timestamp"].map(lambda ts: ts.isoformat(sep=" "))
    bar_pdf["created_ts"] = ""
    bar_pdf["date"] = str(session_row_in.date)
    # RETURN THE DATAFRAME
    return bar_pdf[config.OHLCV_COL_STR_LIST]

"""
Download Loop
"""

# FUNCTION: DOWNLOAD SESSIONS INTO THE STAGING FOLDER
def download_session_pdf(app_in, session_pdf_in, contract_in=None, staging_path_str_in=config.STAGING_OHLCV_PATH_STR,
                         log_file_path_str_in=config.DOWNLOAD_LOG_FILE_PATH_STR, max_in_flight_int_in=config.MAX_IN_FLIGHT_REQUEST_COUNT,
                         min_interval_seconds_in=config.REQUEST_MIN_INTERVAL_SECONDS, timeout_seconds_in=config.REQUEST_TIMEOUT_SECONDS,
                         attempt_count_int_in=config.REQUEST_ATTEMPT_COUNT, pacing_backoff_seconds_in=config.PACING_BACKOFF_SECONDS,
                         same_request_wait_seconds_in=SAME_REQUEST_WAIT_SECONDS, alert_in=True):
    """
    Downloads every session of session_pdf_in into the staging folder (one file per session) and logs every attempt.

    Args:
        app_in (IbkrApp): Connected application
        session_pdf_in (pd.DataFrame): Sessions to download (get_download_session_pdf)
        contract_in (Contract | None): Contract (None = the configured SPY contract)
        staging_path_str_in (str): Staging folder
        log_file_path_str_in (str): Download log path
        max_in_flight_int_in (int): Maximum requests in flight
        min_interval_seconds_in (float): Minimum interval between submissions
        timeout_seconds_in (float): Maximum wait per request
        attempt_count_int_in (int): Attempts per session (pacing re-queues are not counted)
        pacing_backoff_seconds_in (float): First pause after a pacing violation (doubled each time, capped)
        same_request_wait_seconds_in (float): Minimum wait before the same session is requested again
        alert_in (bool): Display progress

    Returns:
        pd.DataFrame: One row per session: date, status_str (final), saved_bool, attempt_int, bar_count_int,
                      missing_count_int, file_path_str
    """
    # DEFINE THE CONTRACT
    contract = contract_in if contract_in is not None else get_contract()
    # COLLECT THE IBAPI VERSION
    ibapi_version_str = get_ibapi_version_str()
    # DEFINE THE QUEUE OF (SESSION ROW, ATTEMPT, PACING COUNT, EARLIEST SUBMIT TIME)
    queue_deque = deque((session_row, 1, 0, 0.0) for session_row in session_pdf_in.itertuples(index=False))
    # DEFINE THE REQUESTS IN FLIGHT (reqId -> (SESSION ROW, ATTEMPT, PACING COUNT, SUBMIT TIME))
    in_flight_dict = {}
    # DEFINE THE FINAL RESULTS AND THE SCHEDULING STATE
    final_dict_list, session_count_int = [], len(session_pdf_in)
    next_submit_time_float, backoff_seconds_float = 0.0, float(pacing_backoff_seconds_in)
    start_time_float = time.time()
    # DISPLAY INFORMATION
    print(f"⏳ Downloading {session_count_int:,} session(s) into {staging_path_str_in} "
          f"({max_in_flight_int_in} in flight, ≥ {min_interval_seconds_in} s between requests)") if alert_in else None
    # LOOP UNTIL EVERY SESSION IS FINISHED
    while queue_deque or in_flight_dict:
        # IF THE CONNECTION WAS LOST
        if not app_in.isConnected():
            # DISPLAY INFORMATION
            print("❌ Connection to IBKR lost: stopping (re-run to continue; finished sessions are saved).") if alert_in else None
            # RELEASE THE REQUESTS IN FLIGHT
            for req_id_int in list(in_flight_dict):
                app_in.timeout_request(req_id_int)
                app_in.forget_request(req_id_int)
            # STOP THE LOOP
            break
        # COLLECT THE CURRENT TIME
        now_float = time.time()
        # SUBMIT REQUESTS WHILE THERE IS ROOM AND THE PACING ALLOWS IT
        while queue_deque and len(in_flight_dict) < max_in_flight_int_in and now_float >= next_submit_time_float:
            # FIND THE FIRST QUEUED SESSION THAT MAY BE SUBMITTED NOW
            ready_idx = next((idx for idx, queue_tup in enumerate(queue_deque) if queue_tup[3] <= now_float), None)
            # IF NO SESSION IS READY
            if ready_idx is None:
                # STOP SUBMITTING
                break
            # TAKE THE SESSION OUT OF THE QUEUE
            session_row, attempt_int, pacing_int, _ = queue_deque[ready_idx]
            del queue_deque[ready_idx]
            # SUBMIT THE REQUEST
            end_ts, duration_seconds_int = get_session_request_tup(session_row)
            req_id_int = app_in.submit_historical_request_int(contract, end_ts, duration_seconds_int, str(session_row.date))
            in_flight_dict[req_id_int] = (session_row, attempt_int, pacing_int, now_float)
            # SET THE EARLIEST NEXT SUBMISSION
            next_submit_time_float = now_float + min_interval_seconds_in
        # WAIT A LITTLE FOR ANY REQUEST TO FINISH
        app_in.any_done_event.wait(timeout=0.05)
        app_in.any_done_event.clear()
        # ITERATE OVER THE REQUESTS IN FLIGHT
        for req_id_int in list(in_flight_dict):
            # COLLECT THE REQUEST
            session_row, attempt_int, pacing_int, submit_time_float = in_flight_dict[req_id_int]
            # IF THE REQUEST IS STILL PENDING
            if app_in.get_request_status_str(req_id_int) == "pending":
                # IF IT HAS NOT TIMED OUT YET
                if time.time() - submit_time_float < timeout_seconds_in:
                    # KEEP WAITING
                    continue
                # MARK IT AS TIMED OUT
                app_in.timeout_request(req_id_int)
            # COLLECT THE RESULT AND FORGET THE REQUEST
            result_dict = app_in.get_request_result_dict(req_id_int)
            # COLLECT THE REQUEST STRINGS (END DATETIME, DURATION, LABEL) FOR THE LOG
            with app_in.lock:
                request_dict = {k: v for k, v in app_in.request_dict.get(req_id_int, {}).items() if k.endswith("_str")}
            app_in.forget_request(req_id_int)
            del in_flight_dict[req_id_int]
            # BUILD THE SESSION FILE AND CHECK IT (ONLY A FINISHED REQUEST IS EVER SAVED)
            text_pdf = get_session_text_pdf(result_dict["bar_tuple_list"], session_row) if result_dict["status_str"] == "complete" else None
            raw_check_dict = get_session_check_dict(pd.Series([bar_tup[0] for bar_tup in result_dict["bar_tuple_list"]], dtype="datetime64[ns, America/New_York]"), session_row)
            # DEFINE THE OUTCOME STATUS
            status_str = raw_check_dict["status_str"] if result_dict["status_str"] == "complete" else result_dict["status_str"]
            # DECIDE WHETHER TO RETRY
            retry_bool = (status_str == "pacing" and pacing_int < MAX_PACING_REQUEUE_INT) or \
                         (status_str in ["partial", "empty", "no_data", "error", "timeout"] and attempt_int < attempt_count_int_in)
            # DECIDE WHETHER TO SAVE (COMPLETE, OR STILL PARTIAL AFTER THE LAST ATTEMPT)
            save_bool = status_str == "complete" or (status_str == "partial" and not retry_bool)
            # DEFINE THE FILE PATH
            file_path_str = f"{staging_path_str_in}{get_file_name_str(pd.Timestamp(session_row.date).strftime('%Y%m%d'))}"
            # IF THE SESSION MUST BE SAVED
            if save_bool:
                # WRITE THE FILE
                write_text_pdf_atomic(text_pdf, file_path_str)
            # LOG THE ATTEMPT
            append_log_rows([{"logged_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "date": str(session_row.date),
                              "attempt_int": attempt_int, "status_str": status_str, "saved_bool": save_bool,
                              "expected_count_int": raw_check_dict["expected_count_int"], "bar_count_int": len(text_pdf) if text_pdf is not None else raw_check_dict["bar_count_int"],
                              "missing_count_int": raw_check_dict["missing_count_int"], "extra_count_int": raw_check_dict["extra_count_int"],
                              "duplicate_count_int": raw_check_dict["duplicate_count_int"], "missing_str": raw_check_dict["missing_str"],
                              "extra_str": raw_check_dict["extra_str"], "error_code_int": result_dict["error_code_int"], "error_str": result_dict["error_str"],
                              "request_seconds_float": result_dict["request_seconds_float"], "end_datetime_str": request_dict.get("end_datetime_str", ""),
                              "duration_str": request_dict.get("duration_str", ""), "ibapi_version_str": ibapi_version_str}],
                            log_file_path_str_in, DOWNLOAD_LOG_COL_STR_LIST)
            # IF THE ANSWER WAS A PACING VIOLATION
            if status_str == "pacing":
                # PAUSE EVERY SUBMISSION AND INCREASE THE NEXT PAUSE
                next_submit_time_float = time.time() + backoff_seconds_float
                print(f"⚠️ Pacing violation: pausing submissions for {backoff_seconds_float:.0f} s") if alert_in else None
                backoff_seconds_float = min(backoff_seconds_float * 2, config.PACING_BACKOFF_MAX_SECONDS)
            # IF THE SESSION MUST BE RETRIED
            if retry_bool:
                # PUT IT BACK IN THE QUEUE (PACING DOES NOT USE AN ATTEMPT)
                queue_deque.append((session_row, attempt_int + (status_str != "pacing"), pacing_int + (status_str == "pacing"),
                                    time.time() + same_request_wait_seconds_in))
            # IF THE SESSION IS FINISHED
            else:
                # STORE THE FINAL RESULT
                final_dict_list.append({"date": session_row.date, "status_str": status_str, "saved_bool": save_bool, "attempt_int": attempt_int,
                                        "bar_count_int": len(text_pdf) if text_pdf is not None else 0, "missing_count_int": raw_check_dict["missing_count_int"],
                                        "file_path_str": file_path_str if save_bool else ""})
            # DISPLAY PROGRESS
            if alert_in:
                # DEFINE THE ACTION TEXT
                action_str = "saved" if save_bool else ("retry" if retry_bool else "gave up")
                icon_str = "✅" if status_str == "complete" else ("⚠️" if save_bool or retry_bool else "❌")
                print(f"{icon_str} [{len(final_dict_list):,}/{session_count_int:,}] {session_row.date} {status_str} "
                      f"({raw_check_dict['bar_count_int']}/{raw_check_dict['expected_count_int']} bars, attempt {attempt_int}, "
                      f"{result_dict['request_seconds_float']:.1f} s) -> {action_str}"
                      + (f" [{result_dict['error_code_int']}] {result_dict['error_str']}" if result_dict["error_str"] else ""))
    # CREATE THE SUMMARY
    summary_pdf = pd.DataFrame(final_dict_list, columns=["date", "status_str", "saved_bool", "attempt_int", "bar_count_int", "missing_count_int", "file_path_str"])
    # DISPLAY THE SUMMARY
    if alert_in:
        print(f"\n🏁 Finished in {time.time() - start_time_float:.1f} s: " + ", ".join(f"{k} {v}" for k, v in summary_pdf["status_str"].value_counts().items())
              + f"; {int(summary_pdf['saved_bool'].sum()) if not summary_pdf.empty else 0} file(s) saved. Log: {log_file_path_str_in}")
    # RETURN THE SUMMARY
    return summary_pdf.sort_values("date").reset_index(drop=True)
