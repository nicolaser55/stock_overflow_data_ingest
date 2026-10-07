import os
import time
import threading
from collections import deque
from datetime import datetime, timezone
import pandas as pd
# IMPORT THE INGEST CONFIGURATION
from ingest import config
from ingest import index_config
# IMPORT THE IBKR CLIENT FUNCTIONS
from ingest.ibkr_client import IbkrApp, get_contract, get_ibapi_version_str, get_volume_number
# IMPORT THE SESSION FUNCTIONS
from ingest.sessions import get_ny_now_ts, get_session_pdf
# IMPORT THE RAW FILE FUNCTIONS
from ingest.raw_files import (TS_KEY_COL_STR, get_file_name_str, get_present_date_set, get_ts_utc_series, read_ohlcv_text_pdf, write_text_pdf_atomic,
                              append_log_rows, get_merged_text_pdf_tup)

"""
Index Download: VIX / VIX3M, daily plus 1-minute bars, into their own staging folders

How this differs from the SPY downloader (ingest.ibkr_download), and why:

    1. The request format. A dated request to an index is accepted only with a UTC end time ("YYYYMMDD-HH:MM:SS"). A 1-minute request is one session
       (end 17:00 New York on the session date, duration "1 D", regular trading hours); a daily request covers one calendar year of wanted dates
       (end = last wanted date + a few days, duration "2 Y"; the bars outside the wanted dates are dropped).
    2. No fixed number of bars. The session window of the indices changed over the years, so a 1-minute session is checked only for missing
       minutes inside the core hours (09:31 - 15:59); the first bar, the last bar and the bars of another date are logged, not judged.
    3. A session is "downloadable" once the indices have stopped calculating: every session before today, and today only from 17:00 New York.
    4. Everything else is the SPY downloader's behaviour: a session is saved only when IBKR said the request is finished; a partial session is requested
       again (up to REQUEST_ATTEMPT_COUNT attempts, 16 s apart) and, if still partial, saved and flagged; pacing violations pause the submissions with a growing
       back-off; files are written atomically into the staging folder; every attempt is appended to the download log with the ibapi and server versions.

File format: the SPY raw format. 1-minute files: timestamp = bar start in New York time with offset ("2016-03-01 09:31:00-05:00"), one file per session
(ohlcv_data_YYYYMMDD.csv). Daily files: timestamp = midnight New York of the session date ("2026-07-17 00:00:00-04:00": a date label, not a time), one file per
year (ohlcv_data_YYYY.csv; rows are added to the staged file of the year when it already exists). Volume: IBKR's unset value is written as an empty cell.
"""

# DEFINE THE DOWNLOAD LOG COLUMNS
INDEX_DOWNLOAD_LOG_COL_STR_LIST = ["logged_at_utc", "symbol", "bar_kind", "label", "attempt_int", "status_str", "saved_bool", "bar_count_int", "expected_count_int",
                                   "missing_count_int", "extra_count_int", "duplicate_count_int", "first_bar_str", "last_bar_str", "missing_str", "extra_str",
                                   "error_code_int", "error_str", "request_seconds_float", "end_datetime_str", "duration_str", "ibapi_version_str", "server_version_int"]
# DEFINE THE MINIMUM WAIT BEFORE THE SAME REQUEST IS SENT AGAIN (IBKR REJECTS IDENTICAL REQUESTS WITHIN 15 SECONDS)
SAME_REQUEST_WAIT_SECONDS = 16
# DEFINE THE MAXIMUM NUMBER OF PACING RE-QUEUES OF ONE TASK
MAX_PACING_REQUEUE_INT = 10
# DEFINE THE NUMBER OF MISSING MINUTES / DATES LISTED IN THE LOG
MAX_LISTED_INT = 10

"""
Application
"""

# CLASS: REQUEST SUPPORT FOR ANY BAR SIZE AND END FORMAT (MIXED INTO IbkrApp, AND INTO THE SIMULATED APPLICATION OF THE TESTS)
class IndexRequestMixin:
    """
    Sends historical requests with a free bar size and end string, and stores the bars RAW (the date field as text: a daily bar has
    "YYYYMMDD", an intraday bar epoch seconds; the base class would read a daily date as epoch seconds).
    """

    # METHOD: SUBMIT A HISTORICAL BAR REQUEST
    def submit_index_request_int(self, contract_in, end_datetime_str_in, duration_str_in, bar_size_str_in, label_str_in=""):
        """
        Args:
            contract_in (Contract): Contract
            end_datetime_str_in (str): End datetime ("" = now, or the UTC format "YYYYMMDD-HH:MM:SS")
            duration_str_in (str): Duration, e.g. "1 D"
            bar_size_str_in (str): Bar size, "1 min" or "1 day"
            label_str_in (str): Free label

        Returns:
            int: Request id
        """
        # COLLECT A REQUEST ID
        req_id_int = self.get_next_req_id_int()
        # REGISTER THE REQUEST BEFORE SENDING IT (THE ANSWER CAN ARRIVE BEFORE reqHistoricalData RETURNS)
        with self.lock:
            # STORE THE REQUEST STATE
            self.request_dict[req_id_int] = {"label_str": label_str_in, "end_datetime_str": end_datetime_str_in, "duration_str": duration_str_in,
                                             "bar_tuple_list": [], "status_str": "pending", "error_code_int": None, "error_str": "",
                                             "submit_time_float": time.time(), "end_time_float": None, "done_event": threading.Event()}
        # SEND THE REQUEST (POSITIONAL ARGUMENTS: THE KEYWORD NAMES DIFFER BETWEEN IBAPI VERSIONS)
        self.reqHistoricalData(req_id_int, contract_in, end_datetime_str_in, duration_str_in, bar_size_str_in, config.WHAT_TO_SHOW_STR,
                               config.USE_RTH_INT, config.FORMAT_DATE_INT, False, [])
        # RETURN THE REQUEST ID
        return req_id_int

    # CALLBACK: RECEIVE ONE HISTORICAL BAR (STORED RAW)
    def historicalData(self, reqId, bar):
        """
        Args:
            reqId (int): Request id
            bar (BarData): Bar
        """
        # LOCK THE REQUEST DICTIONARY
        with self.lock:
            # COLLECT THE REQUEST STATE
            request_state_dict = self.request_dict.get(reqId)
            # IF THE REQUEST IS UNKNOWN OR FINISHED (A LATE ANSWER TO A TIMED-OUT REQUEST)
            if request_state_dict is None or request_state_dict["status_str"] != "pending":
                # IGNORE THE BAR
                return
            # STORE THE RAW BAR
            request_state_dict["bar_tuple_list"].append((str(bar.date), float(bar.open), float(bar.high), float(bar.low), float(bar.close),
                                                         get_volume_number(bar.volume)))

# CLASS: IBKR APPLICATION FOR THE INDEX DOWNLOAD
class IndexApp(IndexRequestMixin, IbkrApp):
    """
    IbkrApp with the index request methods (connection, request registry, error routing, waiting and timeouts are inherited).
    """

"""
Request Strings And Bars
"""

# FUNCTION: GET THE UTC END STRING OF A REQUEST
def get_index_end_str(end_ts_in):
    """
    Args:
        end_ts_in (pd.Timestamp | None): End of the window (any timezone); None = now

    Returns:
        str: "" (now) or the UTC format "YYYYMMDD-HH:MM:SS" that the indices accept
    """
    # IF THE REQUEST ENDS NOW
    if end_ts_in is None:
        # RETURN THE EMPTY STRING
        return ""
    # RETURN THE UTC FORMAT
    return end_ts_in.tz_convert("UTC").strftime("%Y%m%d-%H:%M:%S")

# FUNCTION: GET THE TEXT OF A VOLUME
def get_index_volume_str(volume_in):
    """
    Args:
        volume_in (int | float): Volume received (indices have none: 0, negative, or the unset value of ibapi 10)

    Returns:
        str: "" for the unset value, otherwise the number as received
    """
    # IF THE VOLUME IS THE UNSET VALUE
    if volume_in >= index_config.INDEX_UNSET_VOLUME_MIN_FLOAT:
        # RETURN AN EMPTY CELL
        return ""
    # RETURN THE NUMBER AS TEXT
    return str(volume_in)

# FUNCTION: GET THE NEW YORK TIMESTAMP OF A BAR DATE FIELD
def get_index_bar_ts(bar_date_str_in):
    """
    Args:
        bar_date_str_in (str): Date field of a bar: "YYYYMMDD" (daily bar) or epoch seconds (intraday bar)

    Returns:
        pd.Timestamp: New York time (a daily bar: midnight of its date)
    """
    # IF THE FIELD IS A DATE
    if len(bar_date_str_in) == 8 and bar_date_str_in.isdigit():
        # RETURN MIDNIGHT NEW YORK OF THAT DATE
        return pd.Timestamp(f"{bar_date_str_in[:4]}-{bar_date_str_in[4:6]}-{bar_date_str_in[6:]}").tz_localize(config.NY_TZ_STR)
    # RETURN THE EPOCH IN NEW YORK TIME
    return pd.Timestamp(int(bar_date_str_in), unit="s", tz="UTC").tz_convert(config.NY_TZ_STR)

# FUNCTION: GET THE RECEIVED BARS AS A DATAFRAME
def get_received_pdf(bar_tuple_list_in):
    """
    Args:
        bar_tuple_list_in (list[tuple]): Raw bars (date field, open, high, low, close, volume)

    Returns:
        pd.DataFrame: timestamp (New York), open, high, low, close, volume, in the order received
    """
    # CREATE THE DATAFRAME
    bar_pdf = pd.DataFrame(bar_tuple_list_in, columns=["timestamp", "open", "high", "low", "close", "volume"])
    # IF THERE ARE NO BARS
    if bar_pdf.empty:
        # RETURN THE EMPTY DATAFRAME WITH TYPED TIMESTAMPS
        return bar_pdf.assign(timestamp=pd.to_datetime(bar_pdf["timestamp"], utc=True).dt.tz_convert(config.NY_TZ_STR))
    # CONVERT THE DATE FIELDS
    bar_pdf["timestamp"] = pd.DatetimeIndex([get_index_bar_ts(date_str) for date_str in bar_pdf["timestamp"]])
    # RETURN THE DATAFRAME
    return bar_pdf

"""
Checks
"""

# FUNCTION: CHECK THE 1-MINUTE BARS OF A SESSION
def get_minute_check_dict(received_pdf_in, session_date_in):
    """
    Keeps the bars of the session date (one row per minute) and counts what is wrong. Only the core hours (09:31 - 15:59) are checked for missing
    minutes; the edges of the session differ by era and are only reported (first and last bar).

    Args:
        received_pdf_in (pd.DataFrame): get_received_pdf
        session_date_in (datetime.date): Session date

    Returns:
        dict: status_str ("complete", "partial", "empty"), kept_pdf (bars of the date, one per minute, sorted), bar_count_int, expected_count_int
              (minutes of the core hours between the first and the last bar), missing_count_int, extra_count_int (bars of another date),
              duplicate_count_int, first_bar_str, last_bar_str, missing_str, extra_str
    """
    # SPLIT THE BARS OF THE DATE AND THE OTHERS
    date_mask = received_pdf_in["timestamp"].dt.date == session_date_in
    day_pdf = received_pdf_in[date_mask]
    extra_count_int = int((~date_mask).sum())
    # REMOVE DUPLICATE MINUTES AND SORT
    kept_pdf = day_pdf.drop_duplicates("timestamp", keep="first").sort_values("timestamp").reset_index(drop=True)
    duplicate_count_int = len(day_pdf) - len(kept_pdf)
    # DEFINE THE RESULT FOR AN EMPTY SESSION
    result_dict = {"status_str": "empty", "kept_pdf": kept_pdf, "bar_count_int": len(kept_pdf), "expected_count_int": 0, "missing_count_int": 0,
                   "extra_count_int": extra_count_int, "duplicate_count_int": duplicate_count_int, "first_bar_str": "", "last_bar_str": "",
                   "missing_str": "", "extra_str": ""}
    # IF THERE ARE NO BARS OF THE DATE
    if kept_pdf.empty:
        # RETURN THE RESULT
        return result_dict
    # DEFINE THE CORE MINUTES BETWEEN THE FIRST AND THE LAST BAR
    first_ts, last_ts = kept_pdf["timestamp"].iloc[0], kept_pdf["timestamp"].iloc[-1]
    core_first_ts = pd.Timestamp(f"{session_date_in} {index_config.INDEX_CORE_FIRST_TIME_STR}", tz=config.NY_TZ_STR)
    core_last_ts = pd.Timestamp(f"{session_date_in} {index_config.INDEX_CORE_LAST_TIME_STR}", tz=config.NY_TZ_STR)
    low_ts, high_ts = max(first_ts, core_first_ts), min(last_ts, core_last_ts)
    expected_index = pd.date_range(low_ts, high_ts, freq="1min") if low_ts <= high_ts else pd.DatetimeIndex([], tz=config.NY_TZ_STR)
    # FIND THE MISSING MINUTES
    missing_index = expected_index.difference(pd.DatetimeIndex(kept_pdf["timestamp"]))
    # COMPLETE THE RESULT
    result_dict.update(status_str="partial" if len(missing_index) else "complete", expected_count_int=len(expected_index), missing_count_int=len(missing_index),
                       first_bar_str=first_ts.strftime("%H:%M"), last_bar_str=last_ts.strftime("%H:%M"),
                       missing_str=" ".join(ts.strftime("%H:%M") for ts in missing_index[:MAX_LISTED_INT]) + (" ..." if len(missing_index) > MAX_LISTED_INT else ""),
                       extra_str=f"{extra_count_int} bar(s) of another date" if extra_count_int else "")
    # RETURN THE RESULT
    return result_dict

# FUNCTION: CHECK THE DAILY BARS OF A CHUNK
def get_daily_check_dict(received_pdf_in, wanted_date_list_in, range_session_date_list_in):
    """
    Keeps the daily bars of the wanted dates and counts what is wrong.

    Args:
        received_pdf_in (pd.DataFrame): get_received_pdf
        wanted_date_list_in (list[datetime.date]): Session dates to keep
        range_session_date_list_in (list[datetime.date]): Every NYSE session between the first and the last wanted date (a received date in that range that
                                                          is not a session is reported as extra)

    Returns:
        dict: status_str ("complete", "partial", "empty"), kept_pdf (bars of the wanted dates, one per date, sorted), bar_count_int,
              expected_count_int (wanted dates), missing_count_int, extra_count_int, duplicate_count_int, first_bar_str, last_bar_str, missing_str, extra_str
    """
    # COLLECT THE DATES AND REMOVE DUPLICATE DATES
    received_pdf = received_pdf_in.assign(_date=received_pdf_in["timestamp"].dt.date)
    unique_pdf = received_pdf.drop_duplicates("_date", keep="first")
    duplicate_count_int = len(received_pdf) - len(unique_pdf)
    # KEEP THE WANTED DATES
    wanted_date_set = set(wanted_date_list_in)
    kept_pdf = unique_pdf[unique_pdf["_date"].isin(wanted_date_set)].sort_values("timestamp").drop(columns="_date").reset_index(drop=True)
    # FIND THE MISSING DATES AND THE EXTRA DATES (IN THE RANGE OF THE WANTED DATES, NOT SESSIONS)
    missing_date_list = sorted(wanted_date_set - set(unique_pdf["_date"]))
    in_range_date_set = {date_object for date_object in unique_pdf["_date"] if min(wanted_date_set) <= date_object <= max(wanted_date_set)}
    extra_date_list = sorted(in_range_date_set - set(range_session_date_list_in))
    # RETURN THE RESULT
    return {"status_str": "empty" if kept_pdf.empty else ("partial" if missing_date_list else "complete"), "kept_pdf": kept_pdf, "bar_count_int": len(kept_pdf),
            "expected_count_int": len(wanted_date_set), "missing_count_int": len(missing_date_list), "extra_count_int": len(extra_date_list),
            "duplicate_count_int": duplicate_count_int, "first_bar_str": str(kept_pdf["timestamp"].iloc[0].date()) if len(kept_pdf) else "",
            "last_bar_str": str(kept_pdf["timestamp"].iloc[-1].date()) if len(kept_pdf) else "",
            "missing_str": " ".join(str(d) for d in missing_date_list[:MAX_LISTED_INT]) + (" ..." if len(missing_date_list) > MAX_LISTED_INT else ""),
            "extra_str": " ".join(str(d) for d in extra_date_list[:MAX_LISTED_INT]) + (" ..." if len(extra_date_list) > MAX_LISTED_INT else "")}

# FUNCTION: GET THE TEXT DATAFRAME OF CHECKED BARS
def get_index_text_pdf(kept_pdf_in):
    """
    Args:
        kept_pdf_in (pd.DataFrame): kept_pdf of a check dictionary

    Returns:
        pd.DataFrame: config.OHLCV_COL_STR_LIST columns, every cell text, as in the SPY raw files
    """
    # IF THERE ARE NO BARS
    if kept_pdf_in.empty:
        # RETURN AN EMPTY DATAFRAME
        return pd.DataFrame(columns=config.OHLCV_COL_STR_LIST)
    # BUILD THE TEXT COLUMNS
    text_pdf = pd.DataFrame({"timestamp": kept_pdf_in["timestamp"].map(lambda ts: ts.isoformat(sep=" ")),
                             "open": kept_pdf_in["open"].map(str), "high": kept_pdf_in["high"].map(str), "low": kept_pdf_in["low"].map(str),
                             "close": kept_pdf_in["close"].map(str), "volume": kept_pdf_in["volume"].map(get_index_volume_str), "created_ts": "",
                             "date": kept_pdf_in["timestamp"].dt.strftime("%Y-%m-%d")})
    # RETURN THE DATAFRAME
    return text_pdf[config.OHLCV_COL_STR_LIST].reset_index(drop=True)

"""
Task Selection
"""

# FUNCTION: GET THE SESSIONS THE INDICES HAVE FINISHED
def get_index_closed_session_pdf(session_pdf_in, now_ny_ts_in):
    """
    Args:
        session_pdf_in (pd.DataFrame): get_session_pdf
        now_ny_ts_in (pd.Timestamp): Current New York time

    Returns:
        pd.DataFrame: Sessions before today, and today's session from INDEX_SESSION_DONE_TIME_STR on
    """
    # DEFINE THE TIME OF TODAY FROM WHICH THE SESSION IS DONE
    done_ts = pd.Timestamp(f"{now_ny_ts_in.date()} {index_config.INDEX_SESSION_DONE_TIME_STR}", tz=config.NY_TZ_STR)
    # RETURN THE FINISHED SESSIONS
    return session_pdf_in[(session_pdf_in["date"] < now_ny_ts_in.date()) | ((session_pdf_in["date"] == now_ny_ts_in.date()) & (now_ny_ts_in >= done_ts))].reset_index(drop=True)

# FUNCTION: GET THE REQUESTS (TASKS) TO DOWNLOAD
def get_index_task_list(symbol_str_in, bar_kind_str_in, date1_in=None, date2_in=None, date_list_in=None, from_start_bool_in=False, redownload_bool_in=False,
                        raw_path_str_in=None, staging_path_str_in=None, now_ny_ts_in=None, alert_in=True):
    """
    Selects the finished sessions of an index that are not in its raw or staging folder, and turns them into requests: one per session for the 1-minute
    bars, one per calendar year (of the missing sessions) for the daily bars.

    Args:
        symbol_str_in (str): "VIX" or "VIX3M"
        bar_kind_str_in (str): "1min" or "daily"
        date1_in (str | date | None): First date (None = the day after the last date present, or the first date IBKR has when nothing is present)
        date2_in (str | date | None): Last date (None = today)
        date_list_in (list | None): Explicit dates (replaces the range)
        from_start_bool_in (bool): Start at the first date IBKR has, so that gaps anywhere in the history are filled (not only after the last date)
        redownload_bool_in (bool): Request sessions already present too (a staging file is replaced for 1-minute bars; rows already staged win for daily bars)
        raw_path_str_in (str | None): Raw folder (None = the configured one)
        staging_path_str_in (str | None): Staging folder (None = the configured one)
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)
        alert_in (bool): Display information

    Returns:
        list[dict]: Tasks, oldest first (see get_task_dict)
    """
    # DEFINE THE FOLDERS AND THE CLOCK
    raw_path_str = raw_path_str_in or index_config.get_index_folder_path_str(symbol_str_in, bar_kind_str_in, False)
    staging_path_str = staging_path_str_in or index_config.get_index_folder_path_str(symbol_str_in, bar_kind_str_in, True)
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    start_date = pd.Timestamp(index_config.INDEX_SPEC_DICT[symbol_str_in]["start_date_str"]).date()
    # COLLECT THE DATES ALREADY PRESENT
    present_date_set = get_present_date_set([raw_path_str, staging_path_str])
    # IF EXPLICIT DATES ARE GIVEN
    if date_list_in:
        # COLLECT THEIR SESSIONS
        date_list = sorted({pd.Timestamp(date_in).date() for date_in in date_list_in})
        session_pdf = get_session_pdf(date_list[0], date_list[-1])
        session_pdf = session_pdf[session_pdf["date"].isin(date_list)]
        # REPORT THE DATES WITHOUT A SESSION
        no_session_list = sorted(set(date_list) - set(session_pdf["date"]))
        print(f"⚠️ {symbol_str_in}: not NYSE sessions (skipped): {[str(d) for d in no_session_list]}") if alert_in and no_session_list else None
    # IF A RANGE IS GIVEN OR DEFAULT
    else:
        # DEFINE THE FIRST DATE (EXPLICIT, THE FIRST DATE IBKR HAS, OR THE DAY AFTER THE LAST DATE PRESENT)
        if date1_in is not None:
            first_date = pd.Timestamp(date1_in).date()
        elif from_start_bool_in or not present_date_set:
            first_date = start_date
        else:
            first_date = max(present_date_set) + pd.Timedelta(days=1)
            print(f"ℹ️ {symbol_str_in} {bar_kind_str_in}: last date present in raw + staging: {max(present_date_set)}; starting at {first_date}") if alert_in else None
        # NEVER BEFORE THE FIRST DATE IBKR HAS
        first_date = max(first_date, start_date)
        # COLLECT THE SESSIONS OF THE RANGE
        session_pdf = get_session_pdf(first_date, date2_in if date2_in is not None else now_ny_ts.date())
    # IF THERE ARE NO SESSIONS
    if session_pdf.empty:
        # RETURN NO TASKS
        return []
    # KEEP THE FINISHED SESSIONS AND REPORT THE OTHERS
    closed_session_pdf = get_index_closed_session_pdf(session_pdf, now_ny_ts)
    open_date_list = sorted(set(session_pdf["date"]) - set(closed_session_pdf["date"]))
    print(f"ℹ️ {symbol_str_in}: not finished yet (skipped): {[str(d) for d in open_date_list]}") if alert_in and open_date_list else None
    # IF PRESENT SESSIONS MUST BE SKIPPED
    if not redownload_bool_in and not closed_session_pdf.empty:
        # DROP THE PRESENT DATES
        print(f"ℹ️ {symbol_str_in} {bar_kind_str_in}: already present in raw or staging (skipped): "
              f"{len(present_date_set & set(closed_session_pdf['date'])):,} session(s)") if alert_in else None
        closed_session_pdf = closed_session_pdf[~closed_session_pdf["date"].isin(present_date_set)]
    # IF NOTHING IS LEFT
    if closed_session_pdf.empty:
        # RETURN NO TASKS
        return []
    # COLLECT THE SESSION DATES AND THE CONTRACT
    wanted_date_list = sorted(closed_session_pdf["date"])
    contract = get_contract(symbol_str_in, index_config.INDEX_SEC_TYPE_STR, index_config.INDEX_CURRENCY_STR, index_config.INDEX_EXCHANGE_STR,
                            index_config.INDEX_PRIMARY_EXCHANGE_STR)
    # IF THE BARS ARE 1-MINUTE BARS
    if bar_kind_str_in == "1min":
        # RETURN ONE TASK PER SESSION
        return [get_task_dict(symbol_str_in, bar_kind_str_in, contract, [date_object], [date_object], staging_path_str, now_ny_ts) for date_object in wanted_date_list]
    # GROUP THE WANTED DATES BY YEAR AND RETURN ONE TASK PER YEAR
    task_dict_list = []
    for year_int in sorted({date_object.year for date_object in wanted_date_list}):
        # COLLECT THE WANTED DATES AND THE SESSIONS OF THE YEAR RANGE
        year_date_list = [date_object for date_object in wanted_date_list if date_object.year == year_int]
        range_session_pdf = get_session_pdf(year_date_list[0], year_date_list[-1])
        task_dict_list.append(get_task_dict(symbol_str_in, bar_kind_str_in, contract, year_date_list, list(range_session_pdf["date"]), staging_path_str, now_ny_ts))
    # RETURN THE TASKS
    return task_dict_list

# FUNCTION: GET THE REQUEST OF A TASK
def get_task_dict(symbol_str_in, bar_kind_str_in, contract_in, wanted_date_list_in, range_session_date_list_in, staging_path_str_in, now_ny_ts_in):
    """
    Args:
        symbol_str_in (str): "VIX" or "VIX3M"
        bar_kind_str_in (str): "1min" or "daily"
        contract_in (Contract): Contract
        wanted_date_list_in (list[datetime.date]): Session dates the task must bring (one for a 1-minute task)
        range_session_date_list_in (list[datetime.date]): NYSE sessions between the first and the last wanted date
        staging_path_str_in (str): Staging folder
        now_ny_ts_in (pd.Timestamp): Current New York time

    Returns:
        dict: symbol_str, bar_kind_str, label_str ("2016-03-01" or "2016"), contract, bar_size_str, end_datetime_str, duration_str, wanted_date_list,
              range_session_date_list, staging_path_str, file_path_str
    """
    # IF THE TASK IS ONE 1-MINUTE SESSION
    if bar_kind_str_in == "1min":
        # DEFINE THE LABEL, THE END (17:00 NEW YORK ON THE SESSION DATE, UTC FORMAT) AND THE DURATION
        label_str = str(wanted_date_list_in[0])
        end_datetime_str = get_index_end_str(pd.Timestamp(f"{label_str} {index_config.INDEX_MINUTE_END_TIME_STR}", tz=config.NY_TZ_STR))
        duration_str, period_str = index_config.INDEX_MINUTE_DURATION_STR, pd.Timestamp(label_str).strftime("%Y%m%d")
    # IF THE TASK IS A YEAR OF DAILY BARS
    else:
        # DEFINE THE LABEL, THE END (A FEW DAYS AFTER THE LAST WANTED DATE; NOW WHEN THAT IS IN THE FUTURE) AND THE DURATION
        label_str = str(wanted_date_list_in[0].year)
        end_ts = pd.Timestamp(wanted_date_list_in[-1], tz=config.NY_TZ_STR) + pd.Timedelta(days=index_config.INDEX_DAILY_END_MARGIN_DAYS)
        end_datetime_str = get_index_end_str(end_ts) if end_ts < now_ny_ts_in else ""
        duration_str, period_str = index_config.INDEX_DAILY_DURATION_STR, label_str
    # RETURN THE TASK
    return {"symbol_str": symbol_str_in, "bar_kind_str": bar_kind_str_in, "label_str": label_str, "contract": contract_in,
            "bar_size_str": index_config.INDEX_BAR_KIND_DICT[bar_kind_str_in]["bar_size_str"], "end_datetime_str": end_datetime_str, "duration_str": duration_str,
            "wanted_date_list": list(wanted_date_list_in), "range_session_date_list": list(range_session_date_list_in), "staging_path_str": staging_path_str_in,
            "file_path_str": f"{staging_path_str_in}{get_file_name_str(period_str)}"}

"""
Download Loop
"""

# FUNCTION: GET THE CHECK OF A TASK RESULT
def get_task_check_dict(task_dict_in, result_dict_in):
    """
    Args:
        task_dict_in (dict): get_task_dict
        result_dict_in (dict): IbkrApp.get_request_result_dict

    Returns:
        dict: get_minute_check_dict or get_daily_check_dict of the bars received
    """
    # CONVERT THE BARS
    received_pdf = get_received_pdf(result_dict_in["bar_tuple_list"])
    # RETURN THE CHECK OF THE BAR KIND
    if task_dict_in["bar_kind_str"] == "1min":
        return get_minute_check_dict(received_pdf, task_dict_in["wanted_date_list"][0])
    return get_daily_check_dict(received_pdf, task_dict_in["wanted_date_list"], task_dict_in["range_session_date_list"])

# FUNCTION: SAVE THE BARS OF A TASK INTO THE STAGING FOLDER
def save_task_text_pdf(task_dict_in, text_pdf_in):
    """
    1-minute task: writes the session file (replacing a staged file of the same session). Daily task: adds the rows to the staged file of the year when it
    exists (rows already staged win), so a second run never loses what an earlier run staged.

    Args:
        task_dict_in (dict): get_task_dict
        text_pdf_in (pd.DataFrame): get_index_text_pdf
    """
    # DEFINE THE FILE PATH
    file_path_str = task_dict_in["file_path_str"]
    # IF THE TASK IS A YEAR OF DAILY BARS AND THE STAGED FILE EXISTS
    if task_dict_in["bar_kind_str"] == "daily" and os.path.isfile(file_path_str):
        # MERGE THE EXISTING ROWS (FIRST, SO THEY WIN) WITH THE NEW ROWS
        new_pdf = text_pdf_in.assign(**{TS_KEY_COL_STR: get_ts_utc_series(text_pdf_in["timestamp"])})
        text_pdf_in = get_merged_text_pdf_tup([read_ohlcv_text_pdf(file_path_str), new_pdf])[0]
    # WRITE THE FILE
    write_text_pdf_atomic(text_pdf_in, file_path_str)

# FUNCTION: DOWNLOAD TASKS INTO THE STAGING FOLDERS
def download_index_task_pdf(app_in, task_list_in, log_file_path_str_in=index_config.INDEX_DOWNLOAD_LOG_FILE_PATH_STR,
                            max_in_flight_int_in=config.MAX_IN_FLIGHT_REQUEST_COUNT, min_interval_seconds_in=config.REQUEST_MIN_INTERVAL_SECONDS,
                            timeout_seconds_in=config.REQUEST_TIMEOUT_SECONDS, attempt_count_int_in=config.REQUEST_ATTEMPT_COUNT,
                            pacing_backoff_seconds_in=config.PACING_BACKOFF_SECONDS, same_request_wait_seconds_in=SAME_REQUEST_WAIT_SECONDS, alert_in=True):
    """
    Downloads every task (several in flight, paced) into its staging folder and logs every attempt.

    Args:
        app_in (IndexApp): Connected application
        task_list_in (list[dict]): Tasks (get_index_task_list)
        log_file_path_str_in (str): Download log path
        max_in_flight_int_in (int): Maximum requests in flight
        min_interval_seconds_in (float): Minimum interval between submissions
        timeout_seconds_in (float): Maximum wait per request
        attempt_count_int_in (int): Attempts per task (pacing re-queues are not counted)
        pacing_backoff_seconds_in (float): First pause after a pacing violation (doubled each time, capped)
        same_request_wait_seconds_in (float): Minimum wait before the same task is requested again
        alert_in (bool): Display progress

    Returns:
        pd.DataFrame: One row per task: symbol, bar_kind, label, status_str (final), saved_bool, attempt_int, bar_count_int, missing_count_int, file_path_str
    """
    # COLLECT THE VERSIONS FOR THE LOG
    ibapi_version_str = get_ibapi_version_str()
    server_version_int = app_in.serverVersion() if app_in.isConnected() else 0
    # DEFINE THE QUEUE OF (TASK, ATTEMPT, PACING COUNT, EARLIEST SUBMIT TIME) AND THE REQUESTS IN FLIGHT (reqId -> (TASK, ATTEMPT, PACING COUNT, SUBMIT TIME))
    queue_deque = deque((task_dict, 1, 0, 0.0) for task_dict in task_list_in)
    in_flight_dict = {}
    # DEFINE THE FINAL RESULTS AND THE SCHEDULING STATE
    final_dict_list, task_count_int = [], len(task_list_in)
    next_submit_time_float, backoff_seconds_float = 0.0, float(pacing_backoff_seconds_in)
    start_time_float = time.time()
    # DISPLAY INFORMATION
    print(f"⏳ Downloading {task_count_int:,} request(s) ({max_in_flight_int_in} in flight, ≥ {min_interval_seconds_in} s between requests)") if alert_in else None
    # LOOP UNTIL EVERY TASK IS FINISHED
    while queue_deque or in_flight_dict:
        # IF THE CONNECTION WAS LOST
        if not app_in.isConnected():
            # DISPLAY INFORMATION
            print("❌ Connection to IBKR lost: stopping (re-run to continue; finished requests are saved).") if alert_in else None
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
            # FIND THE FIRST QUEUED TASK THAT MAY BE SUBMITTED NOW
            ready_idx = next((idx for idx, queue_tup in enumerate(queue_deque) if queue_tup[3] <= now_float), None)
            # IF NO TASK IS READY
            if ready_idx is None:
                # STOP SUBMITTING
                break
            # TAKE THE TASK OUT OF THE QUEUE AND SUBMIT IT
            task_dict, attempt_int, pacing_int, _ = queue_deque[ready_idx]
            del queue_deque[ready_idx]
            req_id_int = app_in.submit_index_request_int(task_dict["contract"], task_dict["end_datetime_str"], task_dict["duration_str"], task_dict["bar_size_str"],
                                                         f"{task_dict['symbol_str']} {task_dict['bar_kind_str']} {task_dict['label_str']}")
            in_flight_dict[req_id_int] = (task_dict, attempt_int, pacing_int, now_float)
            # SET THE EARLIEST NEXT SUBMISSION
            next_submit_time_float = now_float + min_interval_seconds_in
        # WAIT A LITTLE FOR ANY REQUEST TO FINISH
        app_in.any_done_event.wait(timeout=0.05)
        app_in.any_done_event.clear()
        # ITERATE OVER THE REQUESTS IN FLIGHT
        for req_id_int in list(in_flight_dict):
            # COLLECT THE REQUEST
            task_dict, attempt_int, pacing_int, submit_time_float = in_flight_dict[req_id_int]
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
            app_in.forget_request(req_id_int)
            del in_flight_dict[req_id_int]
            # CHECK THE BARS (ONLY A FINISHED REQUEST IS EVER SAVED)
            check_dict = get_task_check_dict(task_dict, result_dict) if result_dict["status_str"] == "complete" else None
            # DEFINE THE OUTCOME STATUS
            status_str = check_dict["status_str"] if check_dict is not None else result_dict["status_str"]
            # DECIDE WHETHER TO RETRY
            retry_bool = (status_str == "pacing" and pacing_int < MAX_PACING_REQUEUE_INT) or \
                         (status_str in ["partial", "empty", "no_data", "error", "timeout"] and attempt_int < attempt_count_int_in)
            # DECIDE WHETHER TO SAVE (COMPLETE, OR STILL PARTIAL AFTER THE LAST ATTEMPT)
            save_bool = status_str == "complete" or (status_str == "partial" and not retry_bool)
            # IF THE TASK MUST BE SAVED
            if save_bool:
                # WRITE THE STAGING FILE
                save_task_text_pdf(task_dict, get_index_text_pdf(check_dict["kept_pdf"]))
            # LOG THE ATTEMPT
            check_value_dict = check_dict if check_dict is not None else {}
            append_log_rows([{"logged_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "symbol": task_dict["symbol_str"],
                              "bar_kind": task_dict["bar_kind_str"], "label": task_dict["label_str"], "attempt_int": attempt_int, "status_str": status_str,
                              "saved_bool": save_bool, "bar_count_int": check_value_dict.get("bar_count_int", 0),
                              "expected_count_int": check_value_dict.get("expected_count_int", 0), "missing_count_int": check_value_dict.get("missing_count_int", 0),
                              "extra_count_int": check_value_dict.get("extra_count_int", 0), "duplicate_count_int": check_value_dict.get("duplicate_count_int", 0),
                              "first_bar_str": check_value_dict.get("first_bar_str", ""), "last_bar_str": check_value_dict.get("last_bar_str", ""),
                              "missing_str": check_value_dict.get("missing_str", ""), "extra_str": check_value_dict.get("extra_str", ""),
                              "error_code_int": result_dict["error_code_int"], "error_str": result_dict["error_str"],
                              "request_seconds_float": result_dict["request_seconds_float"], "end_datetime_str": task_dict["end_datetime_str"],
                              "duration_str": task_dict["duration_str"], "ibapi_version_str": ibapi_version_str, "server_version_int": server_version_int}],
                            log_file_path_str_in, INDEX_DOWNLOAD_LOG_COL_STR_LIST)
            # IF THE ANSWER WAS A PACING VIOLATION
            if status_str == "pacing":
                # PAUSE EVERY SUBMISSION AND INCREASE THE NEXT PAUSE
                next_submit_time_float = time.time() + backoff_seconds_float
                print(f"⚠️ Pacing violation: pausing submissions for {backoff_seconds_float:.0f} s") if alert_in else None
                backoff_seconds_float = min(backoff_seconds_float * 2, config.PACING_BACKOFF_MAX_SECONDS)
            # IF THE TASK MUST BE RETRIED
            if retry_bool:
                # PUT IT BACK IN THE QUEUE (PACING DOES NOT USE AN ATTEMPT)
                queue_deque.append((task_dict, attempt_int + (status_str != "pacing"), pacing_int + (status_str == "pacing"), time.time() + same_request_wait_seconds_in))
            # IF THE TASK IS FINISHED
            else:
                # STORE THE FINAL RESULT
                final_dict_list.append({"symbol": task_dict["symbol_str"], "bar_kind": task_dict["bar_kind_str"], "label": task_dict["label_str"],
                                        "status_str": status_str, "saved_bool": save_bool, "attempt_int": attempt_int,
                                        "bar_count_int": check_value_dict.get("bar_count_int", 0) if save_bool else 0,
                                        "missing_count_int": check_value_dict.get("missing_count_int", 0),
                                        "file_path_str": task_dict["file_path_str"] if save_bool else ""})
            # DISPLAY PROGRESS
            if alert_in:
                # DEFINE THE ACTION TEXT
                action_str = "saved" if save_bool else ("retry" if retry_bool else "gave up")
                icon_str = "✅" if status_str == "complete" else ("⚠️" if save_bool or retry_bool else "❌")
                print(f"{icon_str} [{len(final_dict_list):,}/{task_count_int:,}] {task_dict['symbol_str']} {task_dict['bar_kind_str']} {task_dict['label_str']} {status_str} "
                      f"({check_value_dict.get('bar_count_int', 0)} bars{' ' + check_value_dict['first_bar_str'] + '-' + check_value_dict['last_bar_str'] if check_value_dict.get('first_bar_str') else ''}, "
                      f"{check_value_dict.get('missing_count_int', 0)} missing, attempt {attempt_int}, {result_dict['request_seconds_float']:.1f} s) -> {action_str}"
                      + (f" [{result_dict['error_code_int']}] {result_dict['error_str']}" if result_dict["error_str"] else ""))
    # CREATE THE SUMMARY
    summary_pdf = pd.DataFrame(final_dict_list, columns=["symbol", "bar_kind", "label", "status_str", "saved_bool", "attempt_int", "bar_count_int", "missing_count_int", "file_path_str"])
    # DISPLAY THE SUMMARY
    if alert_in:
        print(f"\n🏁 Finished in {time.time() - start_time_float:.1f} s: " + ", ".join(f"{k} {v}" for k, v in summary_pdf["status_str"].value_counts().items())
              + f"; {int(summary_pdf['saved_bool'].sum()) if not summary_pdf.empty else 0} file(s) saved. Log: {log_file_path_str_in}")
    # RETURN THE SUMMARY
    return summary_pdf.sort_values(["symbol", "bar_kind", "label"]).reset_index(drop=True)
