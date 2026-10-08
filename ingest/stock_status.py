import os
import pandas as pd
# IMPORT THE STOCK CONFIGURATION
from ingest import stock_config
# IMPORT THE SESSION FUNCTIONS
from ingest.sessions import get_ny_now_ts, get_session_pdf, get_closed_session_pdf
# IMPORT THE FOLDER SUMMARY OF THE INDEX STATUS AND THE RAW FILE FUNCTIONS
from ingest.index_status import get_folder_summary_dict
from ingest.raw_files import get_present_date_set

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
