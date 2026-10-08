import os
# IMPORT THE SHARED DATA PATHS OF THE RESEARCH WORKSPACE (THE SPY RAW FOLDER IS RE-ROOTED FROM so.paths, NOT COPIED HERE)
from so import paths as so_paths

"""
Stock Overflow Data Ingest Configuration

Every constant of the ingest workspace lives here: where the data folder is, the IBKR connection, the contract, the
historical request settings, the pacing rules, and the staging / merge conventions.

Data root:
    The laptop reaches the data folder of nicodesktop through the network share //100.123.162.2/stock_overflow_data/.
    The root can be overridden with the environment variable SO_INGEST_DATA_PATH (for example to run the tests on a
    temporary folder). The SPY raw folder is the research workspace path (so.paths.LOCAL_OHLCV_DATA_FILE_PATH_STR)
    re-rooted under this data root. The staging folder is the sibling store01_rawzone/ibkr_spy_1min_staging/
    (SPY_STAGING_FOLDER_NAME_STR).

Staging:
    Downloads never go straight into the raw folder that the research pipeline reads. They are written to the staging
    folder (store01_rawzone/ibkr_spy_1min_staging/), checked, and only then added to the raw folder by
    scripts/merge_staging_into_raw.py (dry run by default).
"""

"""
Data Folders
"""

# DEFINE THE DATA ROOT (ENVIRONMENT OVERRIDE OR THE NETWORK SHARE OF NICODESKTOP)
DATA_ROOT_PATH_STR = os.environ.get("SO_INGEST_DATA_PATH", "//100.123.162.2/stock_overflow_data/").replace("\\", "/").rstrip("/") + "/"
# DEFINE THE RAW IBKR MINUTE BAR FOLDER (SAME RELATIVE PATH AS so.paths.LOCAL_OHLCV_DATA_FILE_PATH_STR)
RAW_OHLCV_PATH_STR = DATA_ROOT_PATH_STR + so_paths.LOCAL_OHLCV_DATA_FILE_PATH_STR[len(so_paths.LOCAL_PATH_STR):]
# DEFINE THE SPY RAW FOLDER NAME USED WHEN THE SHARE IS RENAMED (THE LIVE RAW PATH STILL COMES FROM so.paths)
SPY_RAW_FOLDER_NAME_STR = "ibkr_spy_1min"
# DEFINE THE STAGING FOLDER NAME (SIBLING OF THE RAW FOLDER UNDER store01_rawzone/, NOT A SUFFIX OF THE RAW FOLDER)
SPY_STAGING_FOLDER_NAME_STR = "ibkr_spy_1min_staging"
# DEFINE THE STAGING FOLDER (NEW DOWNLOADS WAIT HERE UNTIL THEY ARE MERGED INTO THE RAW FOLDER)
STAGING_OHLCV_PATH_STR = os.path.dirname(RAW_OHLCV_PATH_STR.rstrip("/")) + f"/{SPY_STAGING_FOLDER_NAME_STR}/"
# DEFINE THE FOLDER WHERE MERGED STAGING FILES ARE MOVED (KEPT AS A RECORD OF WHAT WAS ADDED AND WHEN)
STAGING_MERGED_PATH_STR = f"{STAGING_OHLCV_PATH_STR}merged/"
# DEFINE THE FOLDER WHERE THE LIVE STREAM WRITES ITS INTRADAY BARS (NEVER MERGED; THE AFTER-CLOSE DOWNLOAD IS THE RECORD)
LIVE_OHLCV_PATH_STR = f"{STAGING_OHLCV_PATH_STR}live/"
# DEFINE THE DOWNLOAD LOG (ONE ROW PER DOWNLOAD ATTEMPT OF A SESSION; ONLY EVER APPENDED TO)
DOWNLOAD_LOG_FILE_PATH_STR = f"{STAGING_OHLCV_PATH_STR}download_log.csv"
# DEFINE THE MERGE LOG (ONE ROW PER STAGING FILE ADDED TO THE RAW FOLDER; ONLY EVER APPENDED TO)
MERGE_LOG_FILE_PATH_STR = f"{STAGING_OHLCV_PATH_STR}merge_log.csv"

# FUNCTION: REPORT WHEN THE SPY RAW FOLDER NAME AND so.paths DISAGREE
def get_folder_name_problem_str():
    """
    Returns:
        str: "" when the raw folder name matches this repo, otherwise one paragraph (this never raises)
    """
    # COMPARE THE LEAF FROM so.paths WITH THE NAME THIS REPO EXPECTS
    resolved_name_str = os.path.basename(RAW_OHLCV_PATH_STR.rstrip("/"))
    # MATCHING NAMES NEED NO MESSAGE
    if resolved_name_str == SPY_RAW_FOLDER_NAME_STR:
        return ""
    # STATE THE RESOLVED FOLDER, THE EXPECTED NAME, AND THE SAFE ORDER
    return (
        f"The SPY raw folder the code resolves from so.paths is {RAW_OHLCV_PATH_STR} ({resolved_name_str}), "
        f"but this repo expects {SPY_RAW_FOLDER_NAME_STR}. "
        "Switch so.paths.LOCAL_OHLCV_DATA_FILE_PATH_STR in the research repo to that folder name (and rename the folder on the share, with every download, merge "
        "and notebook stopped), then reinstall the research repo in the venv."
    )

"""
File Conventions (identical to the existing raw files)
"""

# DEFINE THE FILE NAME PREFIX (ohlcv_data_YYYY.csv, ohlcv_data_YYYYMM.csv, ohlcv_data_YYYYMMDD.csv)
OHLCV_FILE_PREFIX_STR = "ohlcv_data"
# DEFINE THE COLUMNS OF A DOWNLOADED SESSION FILE (created_ts IS LEFT EMPTY, AS IN EVERY EXISTING RAW FILE)
OHLCV_COL_STR_LIST = ["timestamp", "open", "high", "low", "close", "volume", "created_ts", "date"]
# DEFINE THE TIMESTAMP TEXT FORMAT (NEW YORK TIME WITH OFFSET, E.G. 2026-07-17 09:30:00-04:00)
TIMESTAMP_FORMAT_STR = "%Y-%m-%d %H:%M:%S%z"
# DEFINE THE NEW YORK TIMEZONE NAME
NY_TZ_STR = "America/New_York"

"""
IBKR Connection (IB Gateway live: 4001, IB Gateway paper: 4002, TWS live: 7496, TWS paper: 7497)
"""

# DEFINE THE HOST AND PORT
IBKR_HOST_STR = "127.0.0.1"
IBKR_PORT_INT = 4001
# DEFINE THE CLIENT IDS (DIFFERENT IDS LET THE DOWNLOADER, THE LIVE STREAM AND A NOTEBOOK BE CONNECTED AT THE SAME TIME)
IBKR_DOWNLOAD_CLIENT_ID_INT = 1
IBKR_STREAM_CLIENT_ID_INT = 2
IBKR_NOTEBOOK_CLIENT_ID_INT = 3
# DEFINE THE MAXIMUM WAIT FOR THE CONNECTION HANDSHAKE (nextValidId) (seconds)
IBKR_CONNECT_TIMEOUT_SECONDS = 15

"""
Contract (SPY on SMART routing, primary exchange ARCA: the contract of every existing raw file)
"""

# DEFINE THE CONTRACT FIELDS
CONTRACT_SYMBOL_STR = "SPY"
CONTRACT_SEC_TYPE_STR = "STK"
CONTRACT_CURRENCY_STR = "USD"
CONTRACT_EXCHANGE_STR = "SMART"
CONTRACT_PRIMARY_EXCHANGE_STR = "ARCA"

"""
Historical Request Settings (one request per session: 1-minute TRADES bars, regular trading hours)
"""

# DEFINE THE BAR SIZE, DATA TYPE, REGULAR TRADING HOURS FLAG AND DATE FORMAT (2 = EPOCH SECONDS)
BAR_SIZE_STR = "1 min"
WHAT_TO_SHOW_STR = "TRADES"
USE_RTH_INT = 1
FORMAT_DATE_INT = 2
# DEFINE THE TIMEZONE SUFFIX OF THE END DATETIME ("US/Eastern" IS THE NAME THE EXISTING NOTEBOOK USED SUCCESSFULLY)
IBKR_END_DATETIME_TZ_STR = "US/Eastern"
# DEFINE THE MAXIMUM WAIT FOR ONE HISTORICAL REQUEST (seconds; the old notebook used 10, too short for old sessions)
REQUEST_TIMEOUT_SECONDS = 60
# DEFINE THE NUMBER OF ATTEMPTS PER SESSION (A PARTIAL OR FAILED SESSION IS REQUESTED AGAIN UP TO THIS COUNT)
REQUEST_ATTEMPT_COUNT = 3
# DEFINE THE DELAY AFTER THE SESSION CLOSE BEFORE A SESSION IS CONSIDERED DOWNLOADABLE (minutes)
SESSION_CLOSE_BUFFER_MINUTES = 5

"""
Pacing (IBKR historical data limits)

    - at most 50 simultaneous open historical requests;
    - no identical request within 15 seconds;
    - no 6 or more requests for the same contract, exchange and data type within 2 seconds;
    - (60 requests per 10 minutes applies to bars of 30 seconds or less, not to 1-minute bars).
The defaults stay well inside these limits: 4 requests in flight and at least 0.5 s between two submissions
(at most 4 requests per 2 seconds).
"""

# DEFINE THE MAXIMUM NUMBER OF REQUESTS IN FLIGHT
MAX_IN_FLIGHT_REQUEST_COUNT = 4
# DEFINE THE MINIMUM INTERVAL BETWEEN TWO REQUEST SUBMISSIONS (seconds)
REQUEST_MIN_INTERVAL_SECONDS = 0.5
# DEFINE THE WAIT AFTER A PACING VIOLATION BEFORE THE NEXT SUBMISSION (seconds; doubled after each violation, capped)
PACING_BACKOFF_SECONDS = 15
PACING_BACKOFF_MAX_SECONDS = 300

"""
IBKR Message Codes
"""

# DEFINE THE INFORMATIONAL CODE RANGE (DATA FARM STATUS MESSAGES, NOT ERRORS)
IBKR_INFO_CODE_MIN_INT = 2100
IBKR_INFO_CODE_MAX_INT = 2199
# DEFINE THE CONNECTIVITY CODES (1100 LOST, 1101 RESTORED DATA LOST, 1102 RESTORED DATA KEPT)
IBKR_CONNECTIVITY_CODE_LIST = [1100, 1101, 1102]
# DEFINE THE HISTORICAL DATA SERVICE ERROR CODE (PACING VIOLATIONS AND "NO DATA" BOTH ARRIVE WITH THIS CODE)
IBKR_HMDS_ERROR_CODE_INT = 162
# DEFINE THE TEXT THAT IDENTIFIES A PACING VIOLATION AND AN EMPTY ANSWER INSIDE A 162 MESSAGE (lower case)
IBKR_PACING_TEXT_STR = "pacing violation"
IBKR_NO_DATA_TEXT_STR = "returned no data"

"""
Live Stream (ingest.ibkr_stream)
"""

# DEFINE THE DELAY AFTER A MINUTE ENDS BEFORE ITS BAR IS REQUESTED (seconds; the old notebook found 30 s reliable)
STREAM_BAR_DELAY_SECONDS = 30
# DEFINE THE LOOKBACK OF EVERY POLL (seconds; covers the last few minutes so a missed poll heals itself)
STREAM_LOOKBACK_SECONDS = 300
