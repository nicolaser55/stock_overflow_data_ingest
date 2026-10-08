import os
import pandas as pd
# IMPORT THE INGEST CONFIGURATION
from ingest import index_config
from ingest import stock_config
# IMPORT THE SESSION AND DOWNLOAD FUNCTIONS
from ingest.sessions import get_ny_now_ts, get_session_pdf
from ingest.index_download import get_index_closed_session_pdf
# IMPORT THE RAW FILE FUNCTIONS
from ingest.raw_files import get_ohlcv_file_pdf, get_present_date_set

"""
Index Status (read-only): where the VIX / VIX3M data lives, how much there is, and what is missing

Nothing is written. The functions list the folders, count files / rows / dates, compare the dates present (raw + staging) with the NYSE sessions
since the first date IBKR has, and read the download log to tell "never requested" from "requested, IBKR returned nothing".
"""

# FUNCTION: SUMMARIZE ONE FOLDER
def get_folder_summary_dict(path_str_in):
    """
    Args:
        path_str_in (str): Folder ending with "/"

    Returns:
        dict: exists_bool, file_count_int, row_count_int, size_mb_float, date_count_int, first_date_str, last_date_str
    """
    # DEFINE THE EMPTY SUMMARY
    summary_dict = {"exists_bool": os.path.isdir(path_str_in), "file_count_int": 0, "row_count_int": 0, "size_mb_float": 0.0, "date_count_int": 0,
                    "first_date_str": "", "last_date_str": ""}
    # IF THE FOLDER DOES NOT EXIST
    if not summary_dict["exists_bool"]:
        # RETURN THE EMPTY SUMMARY
        return summary_dict
    # COLLECT THE DATA FILES
    file_pdf = get_ohlcv_file_pdf(path_str_in, alert_in=False)
    # COUNT THE ROWS AND THE BYTES (A FILE'S ROWS = ITS LINES - THE HEADER)
    for file_path_str in file_pdf["file_path_str"]:
        summary_dict["size_mb_float"] += os.path.getsize(file_path_str) / 1e6
        with open(file_path_str, "rb") as file_object:
            summary_dict["row_count_int"] += max(sum(1 for _ in file_object) - 1, 0)
    # COLLECT THE DATES
    present_date_set = get_present_date_set([path_str_in]) if len(file_pdf) else set()
    summary_dict.update(file_count_int=len(file_pdf), size_mb_float=round(summary_dict["size_mb_float"], 1), date_count_int=len(present_date_set),
                        first_date_str=str(min(present_date_set)) if present_date_set else "", last_date_str=str(max(present_date_set)) if present_date_set else "")
    # RETURN THE SUMMARY
    return summary_dict

# FUNCTION: GET THE FOLDER TABLE OF ALL INDICES
def get_index_status_pdf():
    """
    Returns:
        pd.DataFrame: One row per index / bar kind / place (raw, staging, merged): symbol, bar_kind, place, path, and the get_folder_summary_dict fields
    """
    # LIST TO HOLD THE ROWS
    row_dict_list = []
    # ITERATE OVER THE INDICES AND BAR KINDS
    for symbol_str in index_config.INDEX_SPEC_DICT:
        for bar_kind_str in index_config.INDEX_BAR_KIND_DICT:
            # ITERATE OVER THE PLACES
            raw_path_str = index_config.get_index_folder_path_str(symbol_str, bar_kind_str, False)
            staging_path_str = index_config.get_index_folder_path_str(symbol_str, bar_kind_str, True)
            for place_str, path_str in [("raw", raw_path_str), ("staging", staging_path_str), ("staging/merged", f"{staging_path_str}merged/")]:
                row_dict_list.append({"symbol": symbol_str, "bar_kind": bar_kind_str, "place": place_str, "path": path_str, **get_folder_summary_dict(path_str)})
    # RETURN THE TABLE
    return pd.DataFrame(row_dict_list)

# FUNCTION: FIND OTHER FOLDERS THAT MAY HOLD INDEX DATA
def get_other_folder_pdf(rawzone_path_str_in=None):
    """
    Lists the folders of the raw zone whose name looks related (ibkr, index, vix) and that are not one of the configured index or stock folders, so that
    data left in a folder under another name (including <raw folder>_backup_...) is found.

    Args:
        rawzone_path_str_in (str | None): Raw zone folder (None = the parent of the configured index raw root)

    Returns:
        pd.DataFrame: name, path, subfolder_count_int, csv_file_count_int (all levels)
    """
    # DEFINE THE RAW ZONE
    rawzone_path_str = rawzone_path_str_in or os.path.dirname(index_config.INDEX_RAW_ROOT_PATH_STR.rstrip("/")) + "/"
    # IF THE RAW ZONE DOES NOT EXIST
    if not os.path.isdir(rawzone_path_str):
        # RETURN AN EMPTY TABLE
        return pd.DataFrame(columns=["name", "path", "subfolder_count_int", "csv_file_count_int"])
    # DEFINE THE CONFIGURED FOLDERS (THE CURRENT INDEX ROOTS AND THE STOCK FOLDERS)
    configured_name_set = {os.path.basename(root_str.rstrip("/")) for root_str in [index_config.INDEX_RAW_ROOT_PATH_STR, index_config.INDEX_STAGING_ROOT_PATH_STR]}
    configured_name_set |= stock_config.get_stock_folder_name_set()
    # LIST TO HOLD THE ROWS
    row_dict_list = []
    # ITERATE OVER THE FOLDERS
    for name_str in sorted(os.listdir(rawzone_path_str)):
        # SKIP THE CONFIGURED FOLDERS AND UNRELATED NAMES
        if name_str in configured_name_set or not os.path.isdir(f"{rawzone_path_str}{name_str}") or not any(key in name_str.lower() for key in ["ibkr", "index", "vix"]):
            continue
        # COUNT THE SUBFOLDERS AND CSV FILES
        walk_list = list(os.walk(f"{rawzone_path_str}{name_str}"))
        row_dict_list.append({"name": name_str, "path": f"{rawzone_path_str}{name_str}/", "subfolder_count_int": sum(len(dir_list) for _, dir_list, _ in walk_list),
                              "csv_file_count_int": sum(1 for _, _, file_list in walk_list for file_str in file_list if file_str.endswith(".csv"))})
    # RETURN THE TABLE
    return pd.DataFrame(row_dict_list, columns=["name", "path", "subfolder_count_int", "csv_file_count_int"])

# FUNCTION: FIND THE MISSING DATES OF ONE INDEX AND BAR KIND
def get_missing_date_list(symbol_str_in, bar_kind_str_in, now_ny_ts_in=None):
    """
    Args:
        symbol_str_in (str): "VIX" or "VIX3M"
        bar_kind_str_in (str): "1min" or "daily"
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)

    Returns:
        tuple: (expected session count, list of missing session dates: finished NYSE sessions since the first date IBKR has that are in neither the raw nor the staging folder)
    """
    # DEFINE THE CLOCK AND THE START
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    start_date = pd.Timestamp(index_config.INDEX_SPEC_DICT[symbol_str_in]["start_date_str"]).date()
    # COLLECT THE FINISHED SESSIONS AND THE DATES PRESENT
    session_pdf = get_index_closed_session_pdf(get_session_pdf(start_date, now_ny_ts.date()), now_ny_ts)
    present_date_set = get_present_date_set([index_config.get_index_folder_path_str(symbol_str_in, bar_kind_str_in, False),
                                             index_config.get_index_folder_path_str(symbol_str_in, bar_kind_str_in, True)])
    # RETURN THE COUNT AND THE MISSING DATES
    return len(session_pdf), [date_object for date_object in session_pdf["date"] if date_object not in present_date_set]

# FUNCTION: SUMMARIZE THE DOWNLOAD LOG
def get_log_problem_pdf(log_file_path_str_in=index_config.INDEX_DOWNLOAD_LOG_FILE_PATH_STR):
    """
    Args:
        log_file_path_str_in (str): Download log

    Returns:
        pd.DataFrame: The last attempt of every request (symbol, bar_kind, label) that did not end saved and complete: symbol, bar_kind, label, status_str,
                      saved_bool, error_code_int, error_str (empty when the log does not exist)
    """
    # DEFINE THE COLUMNS
    col_str_list = ["symbol", "bar_kind", "label", "status_str", "saved_bool", "error_code_int", "error_str"]
    # IF THERE IS NO LOG
    if not os.path.isfile(log_file_path_str_in):
        # RETURN AN EMPTY TABLE
        return pd.DataFrame(columns=col_str_list)
    # KEEP THE LAST ATTEMPT OF EVERY REQUEST AND THE ONES THAT ARE NOT COMPLETE
    log_pdf = pd.read_csv(log_file_path_str_in, dtype=str, keep_default_na=False).drop_duplicates(["symbol", "bar_kind", "label"], keep="last")
    # RETURN THE PROBLEMS
    return log_pdf[log_pdf["status_str"] != "complete"][col_str_list].reset_index(drop=True)
