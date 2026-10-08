import os
# IMPORT THE INGEST CONFIGURATION
from ingest import config

"""
Index Pipeline Configuration (VIX and VIX3M from IBKR, daily and 1-minute bars)

The SPY pipeline (ingest.config and the modules around it) is not touched. The index pipeline has its own folders, one raw
folder and one staging folder per index and bar size, so that the research pipeline (which reads every CSV of the SPY raw
folder) can never read an index file by accident:

    <data root>/store01_rawzone/ibkr_vix_family/vix_1min/          raw (add-only), ohlcv_data_YYYY[MM[DD]].csv
    <data root>/store01_rawzone/ibkr_vix_family/vix_daily/
    <data root>/store01_rawzone/ibkr_vix_family/vix3m_1min/
    <data root>/store01_rawzone/ibkr_vix_family/vix3m_daily/
    <data root>/store01_rawzone/ibkr_vix_family_staging/vix_1min/ staging (same four names), download_log.csv, merge_log.csv

The file names, the columns and the timestamp text format are the ones of the SPY raw files (timestamp, open, high, low, close,
volume, created_ts, date), so every helper of ingest.raw_files (naming, text-preserving merge, roll-up) works on these folders.

Facts these settings come from (project doc claude/VIX_PROBE_RESULTS_2026-10-06.md; observed on 2026-10-06, not guaranteed):
    - IBKR serves VIX and VIX3M as IND contracts on CBOE, TRADES, without an extra subscription for history.
    - A dated request is accepted only with a UTC end time "YYYYMMDD-HH:MM:SS" (the SPY format "... US/Eastern" is refused, error 10314).
    - The earliest data IBKR returned: VIX 2005-10-03, VIX3M 2009-08-12.
    - The session window of the 1-minute bars changes with the era (the first bar is 09:31, or 03:15 from 2016; the last bar is 15:59, 16:14 or
      16:29 or later), so no fixed number of bars is expected: the first bar, the last bar and the missing minutes are logged instead.
    - Index volume is not meaningful (0, negative or the unset value of ibapi 10, 2**127 - 1). The unset value is written as an empty cell.
"""

"""
Contract
"""

# DEFINE THE CONTRACT FIELDS OF THE INDICES (NO PRIMARY EXCHANGE)
INDEX_SEC_TYPE_STR = "IND"
INDEX_EXCHANGE_STR = "CBOE"
INDEX_CURRENCY_STR = "USD"
INDEX_PRIMARY_EXCHANGE_STR = ""
# DEFINE THE INDICES: FOLDER NAME AND FIRST DATE IBKR HAS (START DATES OF THE PROBE; USED WHEN NOTHING IS DOWNLOADED YET)
INDEX_SPEC_DICT = {"VIX": {"folder_str": "vix", "start_date_str": "2005-10-03"},
                   "VIX3M": {"folder_str": "vix3m", "start_date_str": "2009-08-12"}}
# DEFINE THE BAR KINDS: FOLDER SUFFIX AND IBKR BAR SIZE
INDEX_BAR_KIND_DICT = {"1min": {"bar_size_str": "1 min"}, "daily": {"bar_size_str": "1 day"}}

"""
Folders
"""

# DEFINE THE RAW AND STAGING FOLDER NAMES (THE ONLY PLACE THESE NAMES ARE WRITTEN)
INDEX_RAW_FOLDER_NAME_STR = "ibkr_vix_family"
INDEX_STAGING_FOLDER_NAME_STR = "ibkr_vix_family_staging"
# DEFINE THE PREVIOUS INDEX RAW AND STAGING FOLDER NAMES (THE STATUS SCAN STILL REPORTS A FOLDER LEFT UNDER ONE OF THESE)
INDEX_LEGACY_RAW_FOLDER_NAME_STR = "ibkr_VIX_ohlcv_data"
# DEFINE THE PREVIOUS INDEX STAGING FOLDER NAME
INDEX_LEGACY_STAGING_FOLDER_NAME_STR = "ibkr_VIX_ohlcv_data_incoming"
# DEFINE THE LIST THE STATUS SCAN READS
INDEX_LEGACY_FOLDER_NAME_LIST = [INDEX_LEGACY_RAW_FOLDER_NAME_STR, INDEX_LEGACY_STAGING_FOLDER_NAME_STR]
# DEFINE THE RAW ZONE (PARENT OF THE SPY RAW FOLDER)
INDEX_RAWZONE_PATH_STR = os.path.dirname(config.RAW_OHLCV_PATH_STR.rstrip("/")) + "/"
# DEFINE THE RAW AND STAGING ROOTS (SIBLINGS OF THE SPY FOLDERS, UNDER THE SAME DATA ROOT)
INDEX_RAW_ROOT_PATH_STR = f"{INDEX_RAWZONE_PATH_STR}{INDEX_RAW_FOLDER_NAME_STR}/"
# DEFINE THE STAGING ROOT
INDEX_STAGING_ROOT_PATH_STR = f"{INDEX_RAWZONE_PATH_STR}{INDEX_STAGING_FOLDER_NAME_STR}/"
# DEFINE THE LOGS (ONLY EVER APPENDED TO)
INDEX_DOWNLOAD_LOG_FILE_PATH_STR = f"{INDEX_STAGING_ROOT_PATH_STR}download_log.csv"
INDEX_MERGE_LOG_FILE_PATH_STR = f"{INDEX_STAGING_ROOT_PATH_STR}merge_log.csv"

"""
Request Settings
"""

# DEFINE THE END OF A 1-MINUTE REQUEST (NEW YORK TIME ON THE SESSION DATE; AFTER THE LATEST INDEX BAR SEEN, 16:29 FROM 2022)
INDEX_MINUTE_END_TIME_STR = "17:00:00"
# DEFINE THE WINDOW OF A 1-MINUTE REQUEST (ONE SESSION: WITH useRTH = 1 THE DURATION IS COUNTED IN TRADING TIME)
INDEX_MINUTE_DURATION_STR = "1 D"
# DEFINE THE WINDOW OF A DAILY REQUEST (ONE CALENDAR YEAR OF DATES IS REQUESTED PER CHUNK; THE EXTRA YEAR IS FILTERED OUT)
INDEX_DAILY_DURATION_STR = "2 Y"
# DEFINE THE DAYS ADDED AFTER THE LAST WANTED DAILY DATE IN THE END OF A DAILY REQUEST (THE BAR OF THE END DATE ITSELF IS NOT ASSUMED INCLUDED)
INDEX_DAILY_END_MARGIN_DAYS = 3
# DEFINE THE TIME OF DAY (NEW YORK) FROM WHICH A SESSION OF TODAY IS DOWNLOADABLE (THE INDICES CALCULATE UNTIL AFTER 16:15; EARLIER SESSIONS ALWAYS ARE)
INDEX_SESSION_DONE_TIME_STR = "17:00:00"
# DEFINE THE CORE HOURS CHECKED FOR MISSING MINUTES (THE EDGES OF THE SESSION DIFFER BY ERA AND ARE ONLY LOGGED)
INDEX_CORE_FIRST_TIME_STR = "09:31"
INDEX_CORE_LAST_TIME_STR = "15:59"
# DEFINE THE FIRST DATE OF THE UNTOUCHED WINDOW OF THE RESEARCH (DATA IS STAGED NORMALLY; THE RESEARCH MUST NOT EVALUATE ON IT)
INDEX_UNTOUCHED_WINDOW_START_STR = "2026-05-14"
# DEFINE THE VALUE FROM WHICH A VOLUME IS THE UNSET VALUE OF IBAPI 10 (2**127 - 1)
INDEX_UNSET_VOLUME_MIN_FLOAT = float(2 ** 126)


# FUNCTION: GET THE FOLDER OF AN INDEX AND A BAR KIND
def get_index_folder_path_str(symbol_str_in, bar_kind_str_in, staging_bool_in=False):
    """
    Args:
        symbol_str_in (str): "VIX" or "VIX3M"
        bar_kind_str_in (str): "1min" or "daily"
        staging_bool_in (bool): True = the staging folder, False = the raw folder

    Returns:
        str: Folder ending with "/", e.g. ".../ibkr_vix_family/vix3m_1min/"
    """
    # RETURN THE FOLDER
    return f"{INDEX_STAGING_ROOT_PATH_STR if staging_bool_in else INDEX_RAW_ROOT_PATH_STR}{INDEX_SPEC_DICT[symbol_str_in]['folder_str']}_{bar_kind_str_in}/"
