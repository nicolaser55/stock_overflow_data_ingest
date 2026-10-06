import numpy as np
import pandas as pd
# IMPORT MARKET DATETIME FUNCTIONS OF THE RESEARCH WORKSPACE
from so.core.datetime_utils import get_date_range_market_schedule_pdf
# IMPORT THE INGEST CONFIGURATION
from ingest import config

"""
Sessions: the current New York time, the NYSE schedule, and the minute-by-minute check of a session's bars

Current time:
    The old organize / rebuild / find-issues scripts used convert_to_ny_ts(datetime.now()). datetime.now() is the
    laptop's local clock WITHOUT a timezone, and convert_to_ny_ts LABELS a naive time as New York time instead of
    converting it. On a laptop in Riyadh (UTC+3) that "New York time" was 7 hours ahead of the real one, so the current
    month / year cutoff could be wrong around midnight and a session still trading could be treated as closed.
    get_ny_now_ts() reads the real clock in New York time.

Session bars:
    The schedule comes from so.core.datetime_utils (pandas_market_calendars, NYSE). A session's expected bars are every
    minute from the open (09:30) to the last bar (market_close_ts, one minute before the close: 15:59, or 12:59 on half
    days), so half days are handled by the schedule, not by a fixed bar count.
"""

# FUNCTION: GET THE CURRENT NEW YORK TIME
def get_ny_now_ts():
    """
    Returns:
        pd.Timestamp: Current time in New York (timezone-aware, whole seconds)
    """
    # RETURN THE CURRENT NEW YORK TIME
    return pd.Timestamp.now(tz=config.NY_TZ_STR).floor("s")

# FUNCTION: GET THE SESSIONS OF A DATE RANGE
def get_session_pdf(date1_in, date2_in):
    """
    Returns the NYSE sessions of an inclusive date range.

    Args:
        date1_in (str | date | pd.Timestamp): First date
        date2_in (str | date | pd.Timestamp): Last date

    Returns:
        pd.DataFrame: date (datetime.date), first_bar_ts, last_bar_ts (New York), bar_count_int (expected 1-minute bars)
    """
    # DEFINE THE COLUMNS
    col_str_list = ["date", "first_bar_ts", "last_bar_ts", "bar_count_int"]
    # COLLECT THE SCHEDULE (DATES AS STRINGS: THE SCHEDULE FUNCTION EXPECTS DATES, NOT TIMESTAMPS)
    schedule_pdf = get_date_range_market_schedule_pdf(str(pd.Timestamp(date1_in).date()), str(pd.Timestamp(date2_in).date()))
    # IF THERE ARE NO SESSIONS
    if schedule_pdf.empty:
        # RETURN AN EMPTY DATAFRAME
        return pd.DataFrame(columns=col_str_list)
    # CREATE THE SESSION DATAFRAME
    session_pdf = pd.DataFrame({"date": pd.to_datetime(schedule_pdf["date"]).dt.date, "first_bar_ts": schedule_pdf["market_open_ts"],
                                "last_bar_ts": schedule_pdf["market_close_ts"]})
    # COUNT THE EXPECTED BARS
    session_pdf["bar_count_int"] = ((session_pdf["last_bar_ts"] - session_pdf["first_bar_ts"]).dt.total_seconds() // 60 + 1).astype(int)
    # RETURN THE DATAFRAME
    return session_pdf.reset_index(drop=True)

# FUNCTION: KEEP THE SESSIONS THAT HAVE CLOSED
def get_closed_session_pdf(session_pdf_in, now_ny_ts_in, buffer_minutes_in=config.SESSION_CLOSE_BUFFER_MINUTES):
    """
    Keeps the sessions whose close (last bar + 1 minute) plus a buffer is at or before now.

    Args:
        session_pdf_in (pd.DataFrame): Sessions from get_session_pdf
        now_ny_ts_in (pd.Timestamp): Current New York time
        buffer_minutes_in (int): Minutes after the close before a session counts as closed

    Returns:
        pd.DataFrame: Closed sessions
    """
    # DEFINE THE CLOSED MASK
    closed_mask = session_pdf_in["last_bar_ts"] + pd.Timedelta(minutes=1 + buffer_minutes_in) <= now_ny_ts_in
    # RETURN THE CLOSED SESSIONS
    return session_pdf_in[closed_mask].reset_index(drop=True)

# FUNCTION: GET THE LAST CLOSED SESSION DATE
def get_last_closed_session_date(now_ny_ts_in):
    """
    Args:
        now_ny_ts_in (pd.Timestamp): Current New York time

    Returns:
        datetime.date: Date of the most recent closed session
    """
    # COLLECT THE SESSIONS OF THE LAST TWO WEEKS
    session_pdf = get_closed_session_pdf(get_session_pdf(now_ny_ts_in - pd.Timedelta(days=14), now_ny_ts_in), now_ny_ts_in)
    # RETURN THE LAST DATE
    return session_pdf["date"].iloc[-1]

# FUNCTION: GET THE EXPECTED MINUTES OF A SESSION
def get_session_minute_index(session_row_in):
    """
    Args:
        session_row_in (pd.Series | namedtuple): Row of get_session_pdf

    Returns:
        pd.DatetimeIndex: Every expected bar start (New York)
    """
    # RETURN THE MINUTES FROM THE FIRST TO THE LAST BAR
    return pd.date_range(session_row_in.first_bar_ts, session_row_in.last_bar_ts, freq="1min")

# FUNCTION: CHECK THE BARS OF ONE SESSION
def get_session_check_dict(ts_series_in, session_row_in, max_listed_int_in=10):
    """
    Compares a session's bar timestamps with the expected minutes.

    Args:
        ts_series_in (pd.Series): Bar timestamps of the session (timezone-aware)
        session_row_in (pd.Series | namedtuple): Row of get_session_pdf
        max_listed_int_in (int): Number of missing / extra minutes listed

    Returns:
        dict: date, status_str ("complete", "partial", "empty"), expected_count_int, bar_count_int, missing_count_int,
              extra_count_int, duplicate_count_int, missing_str, extra_str (first minutes, "HH:MM")
    """
    # COLLECT THE EXPECTED AND ACTUAL MINUTES (AS NEW YORK TIME)
    expected_index = get_session_minute_index(session_row_in)
    actual_index = pd.DatetimeIndex(ts_series_in).tz_convert(config.NY_TZ_STR)
    # COMPARE THEM
    missing_index = expected_index.difference(actual_index)
    extra_index = actual_index.unique().difference(expected_index)
    # DEFINE THE STATUS
    status_str = "empty" if len(actual_index) == 0 else ("complete" if len(missing_index) == 0 else "partial")
    # RETURN THE CHECK
    return {"date": session_row_in.date, "status_str": status_str, "expected_count_int": len(expected_index),
            "bar_count_int": len(actual_index), "missing_count_int": len(missing_index), "extra_count_int": len(extra_index),
            "duplicate_count_int": int(actual_index.duplicated().sum()),
            "missing_str": get_minute_list_str(missing_index, max_listed_int_in), "extra_str": get_minute_list_str(extra_index, max_listed_int_in)}

# FUNCTION: FORMAT A FEW MINUTES AS TEXT
def get_minute_list_str(ts_index_in, max_listed_int_in):
    """
    Args:
        ts_index_in (pd.DatetimeIndex): Minutes
        max_listed_int_in (int): Number of minutes listed

    Returns:
        str: "09:30, 09:31 (+5 more)" or ""
    """
    # FORMAT THE FIRST MINUTES
    minute_str = ", ".join(ts.strftime("%H:%M") for ts in ts_index_in[:max_listed_int_in])
    # RETURN THE TEXT WITH THE REMAINDER
    return minute_str + (f" (+{len(ts_index_in) - max_listed_int_in} more)" if len(ts_index_in) > max_listed_int_in else "")

# FUNCTION: CHECK EVERY SESSION OF A DATASET (VECTORIZED)
def get_ohlcv_issue_pdf(ts_series_in, session_pdf_in, max_listed_int_in=10):
    """
    Compares the bars of a whole dataset with the expected minutes of every session. Reports, per date: fully missing
    sessions, missing minutes, extra minutes (outside the session, e.g. pre/post market), duplicated rows, and dates
    with bars but no session (weekends, holidays).

    Args:
        ts_series_in (pd.Series): Bar timestamps (timezone-aware)
        session_pdf_in (pd.DataFrame): Expected sessions (get_session_pdf, already limited to closed sessions)
        max_listed_int_in (int): Number of missing / extra minutes listed per date

    Returns:
        pd.DataFrame: One row per problem date: date, issue_str, expected_count_int, bar_count_int, unique_count_int,
                      missing_count_int, missing_str, extra_count_int, extra_str, duplicate_count_int
    """
    # BUILD THE EXPECTED MINUTES OF EVERY SESSION (ONE ROW PER MINUTE)
    count_arr = session_pdf_in["bar_count_int"].to_numpy()
    expected_ts_arr = np.repeat(session_pdf_in["first_bar_ts"].dt.tz_convert("UTC").dt.tz_localize(None).to_numpy(), count_arr) + \
                      (np.arange(count_arr.sum()) - np.repeat(np.cumsum(count_arr) - count_arr, count_arr)) * np.timedelta64(1, "m")
    expected_pdf = pd.DataFrame({"ts": pd.to_datetime(expected_ts_arr, utc=True), "date": np.repeat(session_pdf_in["date"].to_numpy(), count_arr)})
    # COLLECT THE ACTUAL MINUTES WITH THEIR NEW YORK DATE
    actual_ts_series = pd.Series(pd.DatetimeIndex(ts_series_in).tz_convert("UTC"))
    actual_pdf = pd.DataFrame({"ts": actual_ts_series, "date": actual_ts_series.dt.tz_convert(config.NY_TZ_STR).dt.date})
    # COUNT THE ROWS AND THE DUPLICATES PER DATE
    row_count_series = actual_pdf.groupby("date").size()
    unique_actual_pdf = actual_pdf.drop_duplicates("ts")
    unique_count_series = unique_actual_pdf.groupby("date").size()
    # COMPARE THE EXPECTED AND THE ACTUAL MINUTES
    compare_pdf = expected_pdf.merge(unique_actual_pdf, on="ts", how="outer", suffixes=("_expected", "_actual"), indicator=True)
    compare_pdf["date"] = compare_pdf["date_expected"].where(compare_pdf["_merge"] != "right_only", compare_pdf["date_actual"])
    # COLLECT THE MISSING AND EXTRA MINUTES PER DATE
    missing_pdf = compare_pdf[compare_pdf["_merge"] == "left_only"]
    extra_pdf = compare_pdf[compare_pdf["_merge"] == "right_only"]
    # DEFINE THE DATES TO REPORT
    session_date_set = set(session_pdf_in["date"])
    duplicate_date_set = set((row_count_series - unique_count_series.reindex(row_count_series.index, fill_value=0)).loc[lambda s: s > 0].index)
    report_date_list = sorted(set(missing_pdf["date"]) | set(extra_pdf["date"]) | duplicate_date_set)
    # GROUP THE MINUTES BY DATE ONCE
    missing_group_dict = {date_object: group_pdf["ts"] for date_object, group_pdf in missing_pdf.groupby("date")}
    extra_group_dict = {date_object: group_pdf["ts"] for date_object, group_pdf in extra_pdf.groupby("date")}
    expected_count_dict = dict(zip(session_pdf_in["date"], session_pdf_in["bar_count_int"]))
    # LIST TO HOLD THE PROBLEM ROWS
    row_dict_list = []
    # ITERATE OVER THE PROBLEM DATES
    for date_object in report_date_list:
        # COLLECT THE MISSING AND EXTRA MINUTES (NEW YORK TIME)
        missing_index = pd.DatetimeIndex(missing_group_dict.get(date_object, pd.Series(dtype="datetime64[ns, UTC]"))).tz_convert(config.NY_TZ_STR).sort_values()
        extra_index = pd.DatetimeIndex(extra_group_dict.get(date_object, pd.Series(dtype="datetime64[ns, UTC]"))).tz_convert(config.NY_TZ_STR).sort_values()
        bar_count_int = int(row_count_series.get(date_object, 0))
        # NAME THE ISSUES
        issue_str_list = (["no_session"] if date_object not in session_date_set else []) + \
                         (["missing_session"] if date_object in session_date_set and bar_count_int == 0 else []) + \
                         (["missing_minutes"] if len(missing_index) and bar_count_int > 0 else []) + \
                         (["extra_minutes"] if len(extra_index) and date_object in session_date_set else []) + \
                         (["duplicate_rows"] if date_object in duplicate_date_set else [])
        # STORE THE ROW
        row_dict_list.append({"date": date_object, "issue_str": ",".join(issue_str_list), "expected_count_int": int(expected_count_dict.get(date_object, 0)),
                              "bar_count_int": bar_count_int, "unique_count_int": int(unique_count_series.get(date_object, 0)),
                              "missing_count_int": len(missing_index), "missing_str": get_minute_list_str(missing_index, max_listed_int_in),
                              "extra_count_int": len(extra_index), "extra_str": get_minute_list_str(extra_index, max_listed_int_in),
                              "duplicate_count_int": bar_count_int - int(unique_count_series.get(date_object, 0))})
    # RETURN THE PROBLEM DATES
    return pd.DataFrame(row_dict_list, columns=["date", "issue_str", "expected_count_int", "bar_count_int", "unique_count_int", "missing_count_int",
                                                "missing_str", "extra_count_int", "extra_str", "duplicate_count_int"])
