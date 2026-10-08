import os
import shutil
from datetime import datetime, timezone
import numpy as np
import pandas as pd
# IMPORT THE INGEST CONFIGURATION
from ingest import config
from ingest import index_config
# IMPORT THE INDEX DOWNLOAD CHECKS
from ingest.index_download import get_minute_check_dict
# IMPORT THE RAW FILE FUNCTIONS
from ingest.raw_files import (TS_KEY_COL_STR, get_ohlcv_file_pdf, read_ohlcv_text_pdf, append_log_rows, get_present_date_set, get_backup_path_str,
                              merge_files_into_target_dict)

"""
Index Merge: add the checked staging files of an index to its raw folder (ADD-ONLY)

    1-minute files (one per session): copied into the raw folder as new day files after the checks below. A date the raw folder already has is never
        replaced, and an existing raw file is never changed. Added files are moved to <staging>/merged/.
    Daily files (one per year): merged into the raw file of the same period with the engine of ingest.raw_files (text kept, rows already in raw win,
        conflicting rows are counted and reported, backup first, atomic write, verified, all or nothing). The backup folder is a sibling of the index
        raw root, <raw root>_backup_YYYYMMDD_HHMMSS/<leaf>/ (for example ibkr_vix_family_backup_<time>/vix_daily/). The staged file is moved there.

Checks before a file is added: every price is a positive number, high >= max(open, close, low) and low <= min(open, close), no minute / date twice, every
timestamp is a bar of the date of the file (1-minute files), and no missing minute inside the core hours (09:31 - 15:59) unless include_partial_bool_in.
The bad tick rule and the volume checks of the SPY pipeline are not applied: an index has no volume and its jumps are not trades.

Every run is a DRY RUN unless apply_bool_in is True. Nothing here touches the SPY folders.

Research note: the data after 2026-05-14 belongs to the untouched window of the research. Adding it to the raw folder is allowed; evaluating on it is not.
"""

# DEFINE THE MERGE LOG COLUMNS
INDEX_MERGE_LOG_COL_STR_LIST = ["merged_at_utc", "symbol", "bar_kind", "file_str", "decision_str", "status_str", "row_count_int", "missing_count_int",
                                "conflict_minute_count_int", "staging_file_str", "raw_file_str"]

# FUNCTION: COUNT THE PRICE PROBLEMS OF A TEXT DATAFRAME
def get_price_issue_count_int(text_pdf_in):
    """
    Args:
        text_pdf_in (pd.DataFrame): read_ohlcv_text_pdf

    Returns:
        int: Rows with a price that is not a positive number, or with high < max(open, close, low), or low > min(open, close)
    """
    # PARSE THE PRICES
    number_pdf = text_pdf_in[["open", "high", "low", "close"]].apply(pd.to_numeric, errors="coerce")
    # DEFINE THE ROWS WITH A BAD PRICE
    bad_mask = number_pdf.isna().any(axis=1) | (number_pdf <= 0).any(axis=1) | ~np.isfinite(number_pdf).all(axis=1)
    bad_mask |= (number_pdf["high"] < number_pdf[["open", "close", "low"]].max(axis=1)) | (number_pdf["low"] > number_pdf[["open", "close", "high"]].min(axis=1))
    # RETURN THE COUNT
    return int(bad_mask.sum())

# FUNCTION: CHECK THE MINUTES OF A 1-MINUTE STAGING FILE
def get_file_minute_check_dict(text_pdf_in, session_date_in):
    """
    Args:
        text_pdf_in (pd.DataFrame): read_ohlcv_text_pdf
        session_date_in (datetime.date): Date of the file name

    Returns:
        dict: get_minute_check_dict of the bars of the file (extra_count_int > 0: bars of another date)
    """
    # BUILD THE RECEIVED FORMAT (TIMESTAMPS IN NEW YORK TIME)
    received_pdf = pd.DataFrame({"timestamp": pd.DatetimeIndex(text_pdf_in[TS_KEY_COL_STR]).tz_convert(config.NY_TZ_STR), "open": 0.0, "high": 0.0, "low": 0.0,
                                 "close": 0.0, "volume": 0})
    # RETURN THE CHECK
    return get_minute_check_dict(received_pdf, session_date_in)

# FUNCTION: MERGE THE STAGING FILES OF ONE INDEX AND BAR KIND
def merge_index_staging_into_raw_pdf(symbol_str_in, bar_kind_str_in, apply_bool_in=False, include_partial_bool_in=False, staging_path_str_in=None,
                                     raw_path_str_in=None, log_file_path_str_in=index_config.INDEX_MERGE_LOG_FILE_PATH_STR, alert_in=True):
    """
    Args:
        symbol_str_in (str): "VIX" or "VIX3M"
        bar_kind_str_in (str): "1min" or "daily"
        apply_bool_in (bool): False = dry run
        include_partial_bool_in (bool): Also add 1-minute sessions with missing minutes
        staging_path_str_in (str | None): Staging folder (None = the configured one)
        raw_path_str_in (str | None): Raw folder (None = the configured one)
        log_file_path_str_in (str): Merge log
        alert_in (bool): Display information

    Returns:
        pd.DataFrame: One row per staging file: symbol, bar_kind, file_name_str, row_count_int, missing_count_int, price_issue_count_int, in_raw_bool,
                      decision_str ("add", "added", "merge", "merged", "skip: ...")
    """
    # DEFINE THE FOLDERS
    staging_path_str = staging_path_str_in or index_config.get_index_folder_path_str(symbol_str_in, bar_kind_str_in, True)
    raw_path_str = raw_path_str_in or index_config.get_index_folder_path_str(symbol_str_in, bar_kind_str_in, False)
    # COLLECT THE STAGING FILES (1-MINUTE: DAY FILES; DAILY: ANY PERIOD)
    file_pdf = get_ohlcv_file_pdf(staging_path_str, alert_in=False)
    file_pdf = file_pdf[file_pdf["period_kind_str"] == "day"].reset_index(drop=True) if bar_kind_str_in == "1min" else file_pdf
    # IF THERE ARE NO FILES
    if file_pdf.empty:
        # DISPLAY INFORMATION AND RETURN
        print(f"ℹ️ {symbol_str_in} {bar_kind_str_in}: no staging files in {staging_path_str}") if alert_in else None
        return pd.DataFrame()
    # COLLECT THE DATES ALREADY IN RAW
    raw_date_set = get_present_date_set([raw_path_str])
    # DEFINE THE BACKUP AS A SIBLING OF THE INDEX RAW ROOT (<root>_backup_<time>/<leaf>/), NOT A FOLDER INSIDE IT
    raw_root_path_str = os.path.dirname(raw_path_str.rstrip("/")) + "/"
    # DEFINE THE LEAF (vix_daily, vix_1min, ...)
    leaf_name_str = os.path.basename(raw_path_str.rstrip("/"))
    # DEFINE THE BACKUP FOLDER OF THIS RUN (CREATED ONLY WHEN USED)
    backup_path_str = f"{get_backup_path_str(raw_root_path_str)}{leaf_name_str}/"
    # LIST TO HOLD THE RESULTS AND THE LOG ROWS
    row_dict_list, log_dict_list = [], []
    # ITERATE OVER THE STAGING FILES
    for file_row in file_pdf.itertuples(index=False):
        # READ THE FILE AS TEXT AND CHECK IT
        text_pdf = read_ohlcv_text_pdf(file_row.file_path_str)
        price_issue_count_int = get_price_issue_count_int(text_pdf)
        duplicate_count_int = int(text_pdf[TS_KEY_COL_STR].duplicated().sum())
        raw_file_path_str = f"{raw_path_str}{file_row.file_name_str}"
        missing_count_int, status_str, conflict_count_int = 0, "complete", 0
        # IF THE FILE IS A 1-MINUTE SESSION
        if bar_kind_str_in == "1min":
            # CHECK THE MINUTES
            check_dict = get_file_minute_check_dict(text_pdf, file_row.first_date)
            missing_count_int, status_str = check_dict["missing_count_int"], check_dict["status_str"]
            # DECIDE
            in_raw_bool = file_row.first_date in raw_date_set or os.path.isfile(raw_file_path_str)
            if in_raw_bool:
                decision_str = "skip: date already in raw (raw data is never replaced)"
            elif check_dict["extra_count_int"]:
                decision_str = f"skip: {check_dict['extra_count_int']} bar(s) of another date"
            elif price_issue_count_int or duplicate_count_int:
                decision_str = f"skip: {price_issue_count_int} price issue(s), {duplicate_count_int} duplicate minute(s)"
            elif status_str == "empty":
                decision_str = "skip: no bars"
            elif status_str == "partial" and not include_partial_bool_in:
                decision_str = f"skip: partial ({missing_count_int} missing minutes; use --include-partial)"
            else:
                decision_str = "add"
            # IF THE FILE MUST BE ADDED FOR REAL
            if decision_str == "add" and apply_bool_in:
                # COPY IT INTO RAW (TEMPORARY NAME, THEN RENAME: THE FILE APPEARS COMPLETE OR NOT AT ALL)
                os.makedirs(raw_path_str, exist_ok=True)
                shutil.copy2(file_row.file_path_str, f"{raw_file_path_str}.tmp")
                os.replace(f"{raw_file_path_str}.tmp", raw_file_path_str)
                # VERIFY THE COPY
                if not read_ohlcv_text_pdf(raw_file_path_str)[TS_KEY_COL_STR].equals(text_pdf[TS_KEY_COL_STR]):
                    raise OSError(f"verification of {raw_file_path_str} failed")
                # MOVE THE STAGING FILE TO THE MERGED FOLDER
                merged_path_str = f"{staging_path_str}merged/"
                os.makedirs(merged_path_str, exist_ok=True)
                shutil.move(file_row.file_path_str, f"{merged_path_str}{file_row.file_name_str}")
                decision_str = "added"
        # IF THE FILE IS A DAILY FILE
        else:
            # DEFINE WHETHER THE RAW FILE OF THE PERIOD EXISTS
            in_raw_bool = os.path.isfile(raw_file_path_str)
            # DECIDE (THE ENGINE KEEPS EVERY ROW ALREADY IN RAW)
            if price_issue_count_int or duplicate_count_int:
                decision_str = f"skip: {price_issue_count_int} price issue(s), {duplicate_count_int} duplicate date(s)"
            else:
                decision_str = "merge"
            # IF THE FILE MUST BE MERGED FOR REAL OR AS A DRY RUN
            if decision_str == "merge":
                # RUN THE ENGINE (DRY RUN UNLESS APPLY)
                merge_dict = merge_files_into_target_dict(raw_file_path_str, [file_row.file_path_str], apply_bool_in, backup_path_str, alert_in)
                conflict_count_int = merge_dict["conflict_minute_count"]
                decision_str = {"merged": "merged", "dry_run": "merge"}.get(merge_dict["status_str"], f"skip: {merge_dict['status_str']} ({merge_dict['message_str']})")
        # IF THE FILE WAS ADDED OR MERGED
        if decision_str in ["added", "merged"]:
            # STORE THE LOG ROW
            log_dict_list.append({"merged_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "symbol": symbol_str_in, "bar_kind": bar_kind_str_in,
                                  "file_str": file_row.file_name_str, "decision_str": decision_str, "status_str": status_str, "row_count_int": len(text_pdf),
                                  "missing_count_int": missing_count_int, "conflict_minute_count_int": conflict_count_int,
                                  "staging_file_str": file_row.file_name_str, "raw_file_str": file_row.file_name_str})
        # STORE THE RESULT
        row_dict_list.append({"symbol": symbol_str_in, "bar_kind": bar_kind_str_in, "file_name_str": file_row.file_name_str, "row_count_int": len(text_pdf),
                              "missing_count_int": missing_count_int, "price_issue_count_int": price_issue_count_int, "in_raw_bool": in_raw_bool,
                              "decision_str": decision_str})
    # WRITE THE MERGE LOG
    append_log_rows(log_dict_list, log_file_path_str_in, INDEX_MERGE_LOG_COL_STR_LIST)
    # CREATE THE RESULT DATAFRAME
    result_pdf = pd.DataFrame(row_dict_list)
    # DISPLAY THE SUMMARY
    if alert_in:
        print(("" if apply_bool_in else "🔎 [DRY RUN] ") + f"{symbol_str_in} {bar_kind_str_in} staging -> raw: "
              + ", ".join(f"{k}: {v}" for k, v in result_pdf["decision_str"].str.split(":").str[0].value_counts().items()))
    # RETURN THE RESULT
    return result_pdf
