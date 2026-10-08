import pandas as pd
# IMPORT THE STOCK CONFIGURATION
from ingest import stock_config
# IMPORT THE SPY 1-MINUTE ENGINE AND THE INDEX DAILY ENGINE
from ingest.ibkr_download import download_session_pdf
from ingest.index_download import get_task_dict, download_index_task_pdf
# IMPORT THE SESSION FUNCTIONS
from ingest.sessions import get_ny_now_ts, get_session_pdf, get_closed_session_pdf
# IMPORT THE RAW FILE FUNCTIONS
from ingest.raw_files import get_present_date_set

"""
Stock Download: 1-minute and daily bars of a stock (AAPL) into its own staging folders

Nothing here is a new download engine. The requests, the pacing, the retries, the checks and the log are those of the engines that already exist:

    1-minute bars   ingest.ibkr_download.download_session_pdf (the SPY engine): one request per NYSE session, the session checked minute by minute against
                    the NYSE schedule (390 bars, fewer on early closes), a partial session retried and then saved and flagged, errors never saved. Only the
                    contract (the stock) and the folders differ. Use ingest.ibkr_client.IbkrApp.
    daily bars      ingest.index_download.download_index_task_pdf (the index engine): one request per calendar year of missing sessions (UTC end string,
                    duration "2 Y", bars outside the wanted dates dropped), a missing date makes the year "partial". Use ingest.index_download.IndexApp
                    (it stores the daily date field raw). The volume of a stock is kept (the index engine blanks only IBKR's unset value).

What is new here: which sessions to request (a stock has its own start date, the SPY "session closed" rule applies, a session already in the raw or staging
folder is skipped) and where the files and logs go (ingest.stock_config). The two bar kinds need two different applications (IbkrApp for 1-minute bars,
IndexApp for daily bars): connect them one after the other.

Not verified on the real server: how far back IBKR holds 1-minute bars of the stock, and whether the bars are adjusted for splits (see STOCK_PIPELINE.md).
"""


# FUNCTION: GET THE SESSIONS TO DOWNLOAD
def get_stock_session_pdf(symbol_str_in, bar_kind_str_in, date1_in=None, date2_in=None, date_list_in=None, from_start_bool_in=False, redownload_bool_in=False,
                          raw_path_str_in=None, staging_path_str_in=None, now_ny_ts_in=None, alert_in=True):
    """
    Selects the closed NYSE sessions of a stock that are not already in its raw or staging folder (unless redownload_bool_in).

    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        bar_kind_str_in (str): "1min" or "daily"
        date1_in (str | date | None): First date (None = the day after the last date present, or the start date of the stock when nothing is present)
        date2_in (str | date | None): Last date (None = today)
        date_list_in (list | None): Explicit dates (replaces the range)
        from_start_bool_in (bool): Start at the start date of the stock, so that gaps anywhere in the history are filled (not only after the last date)
        redownload_bool_in (bool): Request the sessions already present too (a staged 1-minute file is replaced; rows already staged win for daily bars)
        raw_path_str_in (str | None): Raw folder (None = the configured one)
        staging_path_str_in (str | None): Staging folder (None = the configured one)
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)
        alert_in (bool): Display information

    Returns:
        pd.DataFrame: Sessions (get_session_pdf columns) to download, oldest first. An explicit date1_in or date_list_in is never moved to the start date.
    """
    # DEFINE THE FOLDERS, THE CLOCK AND THE START DATE
    raw_path_str = raw_path_str_in or stock_config.get_stock_folder_path_str(symbol_str_in, bar_kind_str_in, False)
    staging_path_str = staging_path_str_in or stock_config.get_stock_folder_path_str(symbol_str_in, bar_kind_str_in, True)
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    start_date = pd.Timestamp(stock_config.STOCK_SPEC_DICT[symbol_str_in]["start_date_str"]).date()
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
        # DEFINE THE FIRST DATE (EXPLICIT, THE START DATE OF THE STOCK, OR THE DAY AFTER THE LAST DATE PRESENT)
        if date1_in is not None:
            first_date = pd.Timestamp(date1_in).date()
        elif from_start_bool_in or not present_date_set:
            first_date = start_date
            print(f"ℹ️ {symbol_str_in} {bar_kind_str_in}: starting at {first_date}, the start date set in stock_config (not verified against IBKR)") if alert_in else None
        else:
            first_date = max(present_date_set) + pd.Timedelta(days=1)
            print(f"ℹ️ {symbol_str_in} {bar_kind_str_in}: last date present in raw + staging: {max(present_date_set)}; starting at {first_date}") if alert_in else None
        # COLLECT THE SESSIONS OF THE RANGE
        session_pdf = get_session_pdf(first_date, date2_in if date2_in is not None else now_ny_ts.date())
    # IF THERE ARE NO SESSIONS
    if session_pdf.empty:
        # RETURN THE EMPTY DATAFRAME
        return session_pdf.reset_index(drop=True)
    # KEEP THE CLOSED SESSIONS AND REPORT THE OTHERS
    closed_session_pdf = get_closed_session_pdf(session_pdf, now_ny_ts)
    open_date_list = sorted(set(session_pdf["date"]) - set(closed_session_pdf["date"]))
    print(f"ℹ️ {symbol_str_in}: not closed yet (skipped): {[str(d) for d in open_date_list]}") if alert_in and open_date_list else None
    # IF PRESENT SESSIONS MUST BE SKIPPED
    if not redownload_bool_in and not closed_session_pdf.empty:
        # REPORT AND DROP THE PRESENT DATES
        print(f"ℹ️ {symbol_str_in} {bar_kind_str_in}: already present in raw or staging (skipped): "
              f"{len(present_date_set & set(closed_session_pdf['date'])):,} session(s)") if alert_in else None
        closed_session_pdf = closed_session_pdf[~closed_session_pdf["date"].isin(present_date_set)]
    # RETURN THE SESSIONS
    return closed_session_pdf.reset_index(drop=True)


# FUNCTION: GET THE DAILY REQUESTS (ONE PER CALENDAR YEAR)
def get_stock_daily_task_list(symbol_str_in, session_pdf_in, staging_path_str_in=None, now_ny_ts_in=None):
    """
    Args:
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        session_pdf_in (pd.DataFrame): get_stock_session_pdf of the daily bars
        staging_path_str_in (str | None): Staging folder (None = the configured one)
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)

    Returns:
        list[dict]: One task per calendar year of the sessions (ingest.index_download.get_task_dict), oldest first
    """
    # IF THERE ARE NO SESSIONS
    if session_pdf_in.empty:
        # RETURN NO TASKS
        return []
    # DEFINE THE FOLDER, THE CLOCK AND THE CONTRACT
    staging_path_str = staging_path_str_in or stock_config.get_stock_folder_path_str(symbol_str_in, "daily", True)
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    contract = stock_config.get_stock_contract(symbol_str_in)
    # COLLECT THE WANTED DATES
    wanted_date_list = sorted(session_pdf_in["date"])
    # LIST TO HOLD THE TASKS
    task_dict_list = []
    # ITERATE OVER THE YEARS
    for year_int in sorted({date_object.year for date_object in wanted_date_list}):
        # COLLECT THE WANTED DATES AND THE SESSIONS BETWEEN THE FIRST AND THE LAST OF THE YEAR
        year_date_list = [date_object for date_object in wanted_date_list if date_object.year == year_int]
        range_session_pdf = get_session_pdf(year_date_list[0], year_date_list[-1])
        # STORE THE TASK
        task_dict_list.append(get_task_dict(symbol_str_in, "daily", contract, year_date_list, list(range_session_pdf["date"]), staging_path_str, now_ny_ts))
    # RETURN THE TASKS
    return task_dict_list


# FUNCTION: DOWNLOAD 1-MINUTE SESSIONS OF A STOCK INTO ITS STAGING FOLDER
def download_stock_minute_pdf(app_in, symbol_str_in, session_pdf_in, staging_path_str_in=None, log_file_path_str_in=None, **download_kwargs_in):
    """
    Args:
        app_in (IbkrApp): Connected application
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        session_pdf_in (pd.DataFrame): get_stock_session_pdf of the 1-minute bars
        staging_path_str_in (str | None): Staging folder (None = the configured one)
        log_file_path_str_in (str | None): Download log (None = download_log.csv in the staging folder)
        **download_kwargs_in: Pacing and retry settings of ingest.ibkr_download.download_session_pdf

    Returns:
        pd.DataFrame: One row per session (download_session_pdf)
    """
    # RUN THE SPY ENGINE WITH THE CONTRACT AND THE FOLDERS OF THE STOCK
    return download_session_pdf(app_in, session_pdf_in, contract_in=stock_config.get_stock_contract(symbol_str_in),
                                staging_path_str_in=staging_path_str_in or stock_config.get_stock_folder_path_str(symbol_str_in, "1min", True),
                                log_file_path_str_in=log_file_path_str_in or stock_config.get_stock_log_path_str(symbol_str_in, "1min", "download"),
                                **download_kwargs_in)


# FUNCTION: DOWNLOAD DAILY BARS OF A STOCK INTO ITS STAGING FOLDER
def download_stock_daily_pdf(app_in, symbol_str_in, task_list_in, log_file_path_str_in=None, **download_kwargs_in):
    """
    Args:
        app_in (IndexApp): Connected application
        symbol_str_in (str): Stock symbol of STOCK_SPEC_DICT
        task_list_in (list[dict]): get_stock_daily_task_list
        log_file_path_str_in (str | None): Download log (None = download_log.csv in the staging folder)
        **download_kwargs_in: Pacing and retry settings of ingest.index_download.download_index_task_pdf

    Returns:
        pd.DataFrame: One row per task (download_index_task_pdf)
    """
    # RUN THE INDEX ENGINE WITH THE LOG OF THE STOCK
    return download_index_task_pdf(app_in, task_list_in, log_file_path_str_in=log_file_path_str_in or stock_config.get_stock_log_path_str(symbol_str_in, "daily", "download"),
                                   **download_kwargs_in)
