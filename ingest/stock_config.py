import os
# IMPORT THE INGEST CONFIGURATION
from ingest import config
from ingest import index_config
# IMPORT THE IBKR CLIENT FUNCTION THAT BUILDS A CONTRACT
from ingest.ibkr_client import get_contract

"""
Stock Pipeline Configuration (US stocks from IBKR, 1-minute and daily bars, one raw and one staging folder per stock and bar size)

The SPY pipeline (ingest.config and the modules around it) is not touched. A stock has its own folders, so that the research pipeline (which reads every
CSV of the SPY raw folder) can never read a stock file by accident:

    <data root>/store01_rawzone/ibkr_aapl_1min/             raw (add-only), ohlcv_data_YYYY[MM[DD]].csv
    <data root>/store01_rawzone/ibkr_aapl_1min_staging/     staging, download_log.csv, merge_log.csv, merged/
    <data root>/store01_rawzone/ibkr_aapl_daily/
    <data root>/store01_rawzone/ibkr_aapl_daily_staging/

The file names, the columns and the timestamp text format are the ones of the SPY raw files (timestamp, open, high, low, close, volume, created_ts, date).

Reuse (nothing is copied):
    1-minute bars   the SPY engine: ingest.ibkr_download (session selection, minute-by-minute check against the NYSE schedule, retries, pacing, log) and
                    ingest.raw_maintenance.merge_staging_into_raw_pdf (session check, candlestick sanity, bad tick count, add-only), with another contract
                    and other folders.
    daily bars      the index engine: ingest.index_download (one request per calendar year, UTC end string, log) and ingest.index_merge (daily merge,
                    existing rows win), with a stock contract and the volume kept.

Add a stock: one entry in STOCK_SPEC_DICT (symbol, folder name, primary exchange, first date to request).
"""

"""
Contract
"""

# DEFINE THE CONTRACT FIELDS OF THE STOCKS (SMART ROUTING; THE PRIMARY EXCHANGE IS PER STOCK)
STOCK_SEC_TYPE_STR = "STK"
STOCK_CURRENCY_STR = "USD"
STOCK_EXCHANGE_STR = "SMART"
# DEFINE THE STOCKS: FOLDER NAME PART, PRIMARY EXCHANGE AND THE FIRST DATE TO REQUEST WHEN NOTHING IS DOWNLOADED YET
# (start_date_str IS AN ASSUMPTION, NOT A FACT OF IBKR: IT IS NOT VERIFIED WHAT IS THE EARLIEST AAPL 1-MINUTE BAR IBKR HOLDS; A SESSION BEFORE IT COMES BACK AS "no_data")
STOCK_SPEC_DICT = {"AAPL": {"folder_str": "aapl", "primary_exchange_str": "NASDAQ", "start_date_str": "2007-01-03"}}
# DEFINE THE BAR KINDS (FOLDER SUFFIX AND IBKR BAR SIZE; THE SAME NAMES AS THE INDEX PIPELINE)
STOCK_BAR_KIND_LIST = list(index_config.INDEX_BAR_KIND_DICT)
# DEFINE THE FIRST DATE OF THE UNTOUCHED WINDOW OF THE RESEARCH (DATA IS STAGED NORMALLY; THE RESEARCH MUST NOT EVALUATE ON IT)
STOCK_UNTOUCHED_WINDOW_START_STR = index_config.INDEX_UNTOUCHED_WINDOW_START_STR

"""
Folders
"""

# DEFINE THE RAW ZONE (PARENT OF THE SPY RAW FOLDER)
STOCK_RAWZONE_PATH_STR = os.path.dirname(config.RAW_OHLCV_PATH_STR.rstrip("/")) + "/"


# FUNCTION: GET THE NAME OF A STOCK FOLDER
def get_stock_folder_name_str(symbol_str_in, bar_kind_str_in, staging_bool_in=False):
    """
    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT, e.g. "AAPL"
        bar_kind_str_in (str): "1min" or "daily"
        staging_bool_in (bool): True = the staging folder, False = the raw folder

    Returns:
        str: Folder name, e.g. "ibkr_aapl_1min" or "ibkr_aapl_1min_staging"
    """
    # RETURN THE NAME
    return f"ibkr_{STOCK_SPEC_DICT[symbol_str_in]['folder_str']}_{bar_kind_str_in}" + ("_staging" if staging_bool_in else "")


# FUNCTION: GET THE FOLDER OF A STOCK AND A BAR KIND
def get_stock_folder_path_str(symbol_str_in, bar_kind_str_in, staging_bool_in=False):
    """
    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        bar_kind_str_in (str): "1min" or "daily"
        staging_bool_in (bool): True = the staging folder, False = the raw folder

    Returns:
        str: Folder ending with "/", e.g. ".../store01_rawzone/ibkr_aapl_1min/"
    """
    # RETURN THE FOLDER
    return f"{STOCK_RAWZONE_PATH_STR}{get_stock_folder_name_str(symbol_str_in, bar_kind_str_in, staging_bool_in)}/"


# FUNCTION: GET THE LOG PATH OF A STOCK AND A BAR KIND
def get_stock_log_path_str(symbol_str_in, bar_kind_str_in, log_kind_str_in):
    """
    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        bar_kind_str_in (str): "1min" or "daily"
        log_kind_str_in (str): "download" or "merge"

    Returns:
        str: Path of download_log.csv or merge_log.csv in the staging folder (only ever appended to)
    """
    # RETURN THE PATH
    return f"{get_stock_folder_path_str(symbol_str_in, bar_kind_str_in, True)}{log_kind_str_in}_log.csv"


# FUNCTION: GET THE NAMES OF EVERY STOCK FOLDER
def get_stock_folder_name_set():
    """
    Returns:
        set[str]: Raw and staging folder names of every configured stock and bar kind
    """
    # RETURN THE NAMES
    return {get_stock_folder_name_str(symbol_str, bar_kind_str, staging_bool) for symbol_str in STOCK_SPEC_DICT for bar_kind_str in STOCK_BAR_KIND_LIST
            for staging_bool in [False, True]}


# FUNCTION: GET THE CONTRACT OF A STOCK
def get_stock_contract(symbol_str_in):
    """
    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT

    Returns:
        Contract: STK, SMART routing, USD, with the primary exchange of the stock
    """
    # RETURN THE CONTRACT
    return get_contract(symbol_str_in, STOCK_SEC_TYPE_STR, STOCK_CURRENCY_STR, STOCK_EXCHANGE_STR, STOCK_SPEC_DICT[symbol_str_in]["primary_exchange_str"])
