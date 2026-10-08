import os
import pandas as pd
# IMPORT THE STOCK CONFIGURATION
from ingest import stock_config
# IMPORT THE SESSION FUNCTIONS
from ingest.sessions import get_ny_now_ts, get_session_pdf, get_closed_session_pdf
# IMPORT THE FOLDER SUMMARY OF THE INDEX STATUS AND THE RAW FILE FUNCTIONS
from ingest.index_status import get_folder_summary_dict
from ingest.raw_files import get_present_date_set, get_ohlcv_file_pdf

"""
Stock Status (read-only): where the stock data lives, how much there is, and what is missing

Nothing is written. The functions list the folders, count files / rows / dates, compare the dates present (raw + staging) with the closed NYSE sessions since
the start date of the stock, and read the download log to tell "never requested" from "requested, IBKR returned nothing".
"""


# FUNCTION: GET THE FOLDER TABLE OF ALL STOCKS
def get_stock_status_pdf():
    """
    Returns:
        pd.DataFrame: One row per stock / bar kind / place (raw, staging, staging/merged): symbol, bar_kind, place, path, and the get_folder_summary_dict fields
    """
    # LIST TO HOLD THE ROWS
    row_dict_list = []
    # ITERATE OVER THE STOCKS, THE BAR KINDS AND THE PLACES
    for symbol_str in stock_config.STOCK_SPEC_DICT:
        for bar_kind_str in stock_config.STOCK_BAR_KIND_LIST:
            raw_path_str = stock_config.get_stock_folder_path_str(symbol_str, bar_kind_str, False)
            staging_path_str = stock_config.get_stock_folder_path_str(symbol_str, bar_kind_str, True)
            for place_str, path_str in [("raw", raw_path_str), ("staging", staging_path_str), ("staging/merged", f"{staging_path_str}merged/")]:
                row_dict_list.append({"symbol": symbol_str, "bar_kind": bar_kind_str, "place": place_str, "path": path_str, **get_folder_summary_dict(path_str)})
    # RETURN THE TABLE
    return pd.DataFrame(row_dict_list)


# FUNCTION: FIND THE MISSING DATES OF ONE STOCK AND BAR KIND
def get_stock_missing_date_list(symbol_str_in, bar_kind_str_in, now_ny_ts_in=None):
    """
    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        bar_kind_str_in (str): "1min" or "daily"
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)

    Returns:
        tuple: (expected session count, list of missing session dates: closed NYSE sessions since the start date of the stock that are in neither the raw nor
               the staging folder)
    """
    # DEFINE THE CLOCK AND THE START
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    start_date = pd.Timestamp(stock_config.STOCK_SPEC_DICT[symbol_str_in]["start_date_str"]).date()
    # COLLECT THE CLOSED SESSIONS AND THE DATES PRESENT
    session_pdf = get_closed_session_pdf(get_session_pdf(start_date, now_ny_ts.date()), now_ny_ts)
    present_date_set = get_present_date_set([stock_config.get_stock_folder_path_str(symbol_str_in, bar_kind_str_in, False),
                                             stock_config.get_stock_folder_path_str(symbol_str_in, bar_kind_str_in, True)])
    # RETURN THE COUNT AND THE MISSING DATES
    return len(session_pdf), [date_object for date_object in session_pdf["date"] if date_object not in present_date_set]


# FUNCTION: SUMMARIZE THE DOWNLOAD LOG OF ONE STOCK AND BAR KIND
def get_stock_log_problem_pdf(symbol_str_in, bar_kind_str_in):
    """
    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        bar_kind_str_in (str): "1min" or "daily"

    Returns:
        pd.DataFrame: The last attempt of every request that did not end saved and complete: symbol, bar_kind, label (the session date; for daily bars the year),
                      status_str, saved_bool, error_code_int, error_str (empty when the log does not exist)
    """
    # DEFINE THE COLUMNS AND THE LOG
    col_str_list = ["symbol", "bar_kind", "label", "status_str", "saved_bool", "error_code_int", "error_str"]
    log_file_path_str = stock_config.get_stock_log_path_str(symbol_str_in, bar_kind_str_in, "download")
    # IF THERE IS NO LOG
    if not os.path.isfile(log_file_path_str):
        # RETURN AN EMPTY TABLE
        return pd.DataFrame(columns=col_str_list)
    # READ THE LOG (THE 1-MINUTE LOG IDENTIFIES A REQUEST BY "date", THE DAILY LOG BY "label")
    log_pdf = pd.read_csv(log_file_path_str, dtype=str, keep_default_na=False)
    key_col_str = "date" if bar_kind_str_in == "1min" else "label"
    # KEEP THE LAST ATTEMPT OF EVERY REQUEST AND THE ONES THAT ARE NOT COMPLETE
    log_pdf = log_pdf.drop_duplicates([key_col_str], keep="last")
    log_pdf = log_pdf[log_pdf["status_str"] != "complete"]
    # RETURN THE PROBLEMS
    return pd.DataFrame({"symbol": symbol_str_in, "bar_kind": bar_kind_str_in, "label": log_pdf[key_col_str], "status_str": log_pdf["status_str"],
                         "saved_bool": log_pdf["saved_bool"], "error_code_int": log_pdf["error_code_int"], "error_str": log_pdf["error_str"]},
                        columns=col_str_list).reset_index(drop=True)


# FUNCTION: GET THE ROWS OF ONE SESSION DATE FROM THE OHLCV FILES OF FOLDERS (DAY, MONTH OR YEAR FILES)
def get_date_row_pdf(path_str_list_in, date_in):
    """
    Args:
        path_str_list_in (list[str]): Folders ending with "/"
        date_in (datetime.date): Session date

    Returns:
        pd.DataFrame: The rows of the date (text columns of the files), in file order (empty when there are none)
    """
    # LIST TO HOLD THE ROWS OF EVERY FILE THAT MAY CONTAIN THE DATE
    pdf_list = []
    # ITERATE OVER THE FOLDERS AND THE FILES COVERING THE DATE
    for path_str in path_str_list_in:
        file_pdf = get_ohlcv_file_pdf(path_str, alert_in=False)
        for file_row in file_pdf[(file_pdf["first_date"] <= date_in) & (file_pdf["last_date"] >= date_in)].itertuples(index=False):
            # READ THE FILE AS TEXT AND KEEP THE DATE
            text_pdf = pd.read_csv(file_row.file_path_str, dtype=str, keep_default_na=False)
            pdf_list.append(text_pdf[text_pdf["date"] == str(date_in)])
    # RETURN THE ROWS
    return pd.concat(pdf_list, ignore_index=True) if pdf_list else pd.DataFrame()


# FUNCTION: COMPARE THE DAILY BAR OF A STOCK WITH ITS 1-MINUTE BARS OF THE SAME DATES
def get_stock_daily_vs_minute_pdf(symbol_str_in, date_list_in=None, minute_path_str_list_in=None, daily_path_str_list_in=None):
    """
    Aggregates the 1-minute bars of a date (open of the first bar, highest high, lowest low, close of the last bar, sum of the volumes) and puts them next to the
    daily bar of the same date. Prices that agree and a volume ratio of about 1 mean the two downloads are on the same price basis and the same volume unit;
    a ratio of about 100 or 0.01 means a different unit, and different prices mean a different adjustment (for example after a split).

    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        date_list_in (list | None): Session dates (None = the last 10 dates present in both the 1-minute and the daily folders)
        minute_path_str_list_in (list[str] | None): 1-minute folders (None = the raw and staging folders of the stock)
        daily_path_str_list_in (list[str] | None): Daily folders (None = the raw and staging folders of the stock)

    Returns:
        pd.DataFrame: One row per date present in both: date, then daily_ and minute_ open, high, low, close, volume, and volume_ratio_float (daily / minute)
    """
    # DEFINE THE FOLDERS
    minute_path_str_list = minute_path_str_list_in or [stock_config.get_stock_folder_path_str(symbol_str_in, "1min", staging_bool) for staging_bool in [False, True]]
    daily_path_str_list = daily_path_str_list_in or [stock_config.get_stock_folder_path_str(symbol_str_in, "daily", staging_bool) for staging_bool in [False, True]]
    # DEFINE THE DATES (EXPLICIT, OR THE LAST 10 PRESENT IN BOTH)
    date_list = sorted({pd.Timestamp(date_in).date() for date_in in date_list_in}) if date_list_in else \
        sorted(get_present_date_set(minute_path_str_list) & get_present_date_set(daily_path_str_list))[-10:]
    # LIST TO HOLD THE ROWS
    row_dict_list = []
    # ITERATE OVER THE DATES
    for date_object in date_list:
        # COLLECT THE BARS OF THE DATE
        minute_pdf = get_date_row_pdf(minute_path_str_list, date_object)
        daily_pdf = get_date_row_pdf(daily_path_str_list, date_object)
        # SKIP A DATE THAT IS NOT IN BOTH
        if minute_pdf.empty or daily_pdf.empty:
            continue
        # PARSE THE NUMBERS AND AGGREGATE THE MINUTES
        minute_pdf = minute_pdf.sort_values("timestamp").reset_index(drop=True)
        for col_str in ["open", "high", "low", "close", "volume"]:
            minute_pdf[col_str] = pd.to_numeric(minute_pdf[col_str], errors="coerce")
        daily_row = daily_pdf.iloc[0]
        minute_volume = float(minute_pdf["volume"].sum())
        daily_volume = float(pd.to_numeric(daily_row["volume"], errors="coerce"))
        # STORE THE ROW
        row_dict_list.append({"date": date_object, "daily_open": float(daily_row["open"]), "minute_open": float(minute_pdf["open"].iloc[0]),
                              "daily_high": float(daily_row["high"]), "minute_high": float(minute_pdf["high"].max()),
                              "daily_low": float(daily_row["low"]), "minute_low": float(minute_pdf["low"].min()),
                              "daily_close": float(daily_row["close"]), "minute_close": float(minute_pdf["close"].iloc[-1]),
                              "daily_volume": daily_volume, "minute_volume": minute_volume,
                              "volume_ratio_float": round(daily_volume / minute_volume, 4) if minute_volume else float("nan")})
    # RETURN THE TABLE
    return pd.DataFrame(row_dict_list, columns=["date", "daily_open", "minute_open", "daily_high", "minute_high", "daily_low", "minute_low", "daily_close", "minute_close",
                                                "daily_volume", "minute_volume", "volume_ratio_float"])
