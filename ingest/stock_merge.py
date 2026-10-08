import os
import pandas as pd
# IMPORT THE STOCK CONFIGURATION
from ingest import stock_config
# IMPORT THE SPY 1-MINUTE MERGE AND THE INDEX DAILY MERGE
from ingest.raw_maintenance import merge_staging_into_raw_pdf
from ingest.index_merge import merge_index_staging_into_raw_pdf
# IMPORT THE RAW FILE FUNCTIONS
from ingest.raw_files import get_ohlcv_file_pdf, get_backup_path_str

"""
Stock Merge: add the checked staging files of a stock to its raw folder (ADD-ONLY)

Nothing here is a new merge engine:

    1-minute files  ingest.raw_maintenance.merge_staging_into_raw_pdf (the SPY merge): a staged day is added when it is a closed NYSE session, every minute
                    is present (or include_partial), the candlesticks pass the repo's sanity check and the raw folder does not have the date yet. The bad ticks
                    are counted and reported, as for SPY. Added files are moved to <staging>/merged/. A date already in raw, or an existing raw file, is never
                    replaced.
    daily files     ingest.index_merge.merge_index_staging_into_raw_pdf: the staged year file is merged into the raw file of the year with the text-preserving
                    engine (rows already in raw win, conflicting rows are counted and reported, backup first, atomic write, verified, all or nothing).
                    The backup folder is a sibling of the raw folder, <raw folder>_backup_YYYYMMDD_HHMMSS/.

Every run is a DRY RUN unless apply_bool_in is True. The raw folder of the stock is created when the first file is added. Nothing here touches the SPY or
index folders.

Research note: the data after 2026-05-14 belongs to the untouched window of the research. Adding it to the raw folder is allowed; evaluating on it is not.
Split warning: a stock split changes the prices IBKR returns for the whole history. Raw files are add-only, so files added before a split keep the old basis;
see STOCK_PIPELINE.md.
"""


# FUNCTION: MERGE THE STAGING FILES OF ONE STOCK AND BAR KIND
def merge_stock_staging_into_raw_pdf(symbol_str_in, bar_kind_str_in, apply_bool_in=False, include_partial_bool_in=False, staging_path_str_in=None,
                                     raw_path_str_in=None, now_ny_ts_in=None, alert_in=True):
    """
    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        bar_kind_str_in (str): "1min" or "daily"
        apply_bool_in (bool): False = dry run
        include_partial_bool_in (bool): Also add 1-minute sessions with missing minutes (daily files: ignored)
        staging_path_str_in (str | None): Staging folder (None = the configured one)
        raw_path_str_in (str | None): Raw folder (None = the configured one)
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock; 1-minute files only)
        alert_in (bool): Display information

    Returns:
        pd.DataFrame: One row per staging file (the result of the merge function used for the bar kind; empty when there are no staging files)
    """
    # DEFINE THE FOLDERS AND THE LOG
    staging_path_str = staging_path_str_in or stock_config.get_stock_folder_path_str(symbol_str_in, bar_kind_str_in, True)
    raw_path_str = raw_path_str_in or stock_config.get_stock_folder_path_str(symbol_str_in, bar_kind_str_in, False)
    log_file_path_str = f"{staging_path_str}merge_log.csv"
    # IF THERE IS NOTHING STAGED
    if get_ohlcv_file_pdf(staging_path_str, alert_in=False).empty:
        # DISPLAY INFORMATION AND RETURN
        print(f"ℹ️ {symbol_str_in} {bar_kind_str_in}: no staging files in {staging_path_str}") if alert_in else None
        return pd.DataFrame()
    # CREATE THE RAW FOLDER OF THE STOCK WHEN FILES ARE ADDED FOR REAL (THE FIRST MERGE)
    os.makedirs(raw_path_str, exist_ok=True) if apply_bool_in else None
    # IF THE BARS ARE 1-MINUTE BARS
    if bar_kind_str_in == "1min":
        # RUN THE SPY MERGE
        result_pdf = merge_staging_into_raw_pdf(apply_bool_in, include_partial_bool_in, staging_path_str, raw_path_str, f"{staging_path_str}merged/", log_file_path_str,
                                                now_ny_ts_in, alert_in)
        # RETURN THE RESULT
        return result_pdf
    # RUN THE INDEX DAILY MERGE (THE BACKUP IS A SIBLING OF THE RAW FOLDER OF THE STOCK)
    return merge_index_staging_into_raw_pdf(symbol_str_in, bar_kind_str_in, apply_bool_in, False, staging_path_str, raw_path_str, log_file_path_str, alert_in,
                                            get_backup_path_str(raw_path_str))
