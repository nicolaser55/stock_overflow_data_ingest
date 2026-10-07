import time
import threading
import pandas as pd
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE IBKR CLIENT AND ITS HELPERS
from ingest.ibkr_client import IbkrApp, get_contract, get_ibkr_end_datetime_str, get_bar_ny_ts, get_volume_number
# IMPORT THE SESSION FUNCTIONS
from ingest.sessions import get_session_pdf

"""
Index Probe: find out what IBKR offers for an index (VIX, VIX3M) BEFORE a download pipeline is built for it

Read-only. Nothing is written to the raw or staging folders; the probe only asks IBKR questions and reports the answers:

    1. Does the contract resolve (IND, CBOE, USD)? Which trading hours, time zone and exchanges does IBKR report?
    2. What is the earliest date IBKR has data for (reqHeadTimeStamp)?
    3. For a few sample sessions (2005, 2007-12 = start of VIX3M, 2008, 2015, 2020, a recent one): are DAILY bars and
       1-MINUTE bars returned, how many, from which first to which last bar, and is volume present?
    4. Optionally, 1-minute bars including the extended (global trading hours) window, to see whether VIX is also
       calculated before 09:30 / after 16:15.
    5. A CONTROL: SPY (the contract of the existing pipeline) on the last sample date, so that "no data for VIX" can be told
       apart from "the connection or the request is wrong".

Every answer is classified in plain words (get_diagnosis_str): data returned / market data permission missing / no data for
the window / contract not found / pacing / timeout.

Why a subclass of IbkrApp: the existing client is built for one request type (1-minute TRADES bars of a stock session).
ProbeApp reuses its connection, request registry, error routing and ibapi-version handling, and adds contract details,
the head timestamp, and bar requests with a free bar size / data type / trading-hours flag. Daily bars come back as
"YYYYMMDD" text (not epoch seconds), so bars are stored raw here and parsed by get_probe_bar_ts.
"""

"""
Probe Settings
"""

# DEFINE THE INDEX CONTRACT FIELDS (VIX IS A CBOE INDEX: NO PRIMARY EXCHANGE, NO SMART ROUTING)
PROBE_SEC_TYPE_STR = "IND"
PROBE_EXCHANGE_STR = "CBOE"
PROBE_CURRENCY_STR = "USD"
# DEFINE THE DEFAULT INDEX SYMBOLS
PROBE_SYMBOL_STR_LIST = ["VIX", "VIX3M"]
# DEFINE THE DEFAULT SAMPLE DATES (MAPPED TO THE NEXT NYSE SESSION): FIRST RAW SPY SESSION, FIRST VIX3M DAY, 2008 CRASH, 2015 FLASH
# CRASH, 2020 CRASH, AND THE LAST SESSION BEFORE THE UNTOUCHED WINDOW
PROBE_SAMPLE_DATE_STR_LIST = ["2005-01-04", "2007-12-04", "2008-10-10", "2015-08-24", "2020-03-16", "2026-05-13"]
# DEFINE THE FIRST DAY OF THE UNTOUCHED WINDOW OF THE RESEARCH (docs/RESEARCH_STATE_*: never evaluated; the probe refuses it by default)
UNTOUCHED_WINDOW_START_STR = "2026-05-14"
# DEFINE THE SESSION WINDOW OF A 1-MINUTE PROBE: THE END OF THE REQUEST (NEW YORK, A LITTLE AFTER THE 16:15 INDEX CLOSE) AND ITS LENGTH
# ("1 D" WITH REGULAR TRADING HOURS = ONE SESSION; A DURATION IN SECONDS IS COUNTED IN TRADING TIME, SO "25200 S" REACHED BACK INTO THE
# PREVIOUS SESSION: THE FIRST REAL RUN RETURNED 420 SPY BARS INSTEAD OF 390. THE EXTENDED WINDOW IS 03:00 -> 16:30 = 13.5 HOURS)
PROBE_WINDOW_END_TIME_STR = "16:30:00"
PROBE_REGULAR_DURATION_STR = "1 D"
PROBE_EXTENDED_DURATION_STR = "48600 S"
# DEFINE THE DURATION OF A DAILY PROBE (ONE MONTH OF DAILY BARS ENDING ON THE SAMPLE DATE)
PROBE_DAILY_DURATION_STR = "1 M"
# DEFINE THE BAR SIZES
PROBE_MINUTE_BAR_SIZE_STR = "1 min"
PROBE_DAILY_BAR_SIZE_STR = "1 day"
# DEFINE THE PAUSE BETWEEN TWO REQUESTS, THE MAXIMUM WAIT FOR ONE REQUEST AND THE WAIT AFTER A PACING VIOLATION (seconds)
PROBE_PAUSE_SECONDS = 1.0
PROBE_TIMEOUT_SECONDS = 30
PROBE_PACING_WAIT_SECONDS = 15
# DEFINE THE VOLUME ABOVE WHICH IBAPI 10 HAS SENT ITS "UNSET" VALUE (2**127 - 1)
PROBE_UNSET_VOLUME_FLOAT = 1e15
# DEFINE THE IBKR ERROR CODES AND TEXTS THAT MEAN "NOT SUBSCRIBED / NO PERMISSION" (lower case texts)
PROBE_PERMISSION_CODE_INT_LIST = [354, 10089, 10090, 10167, 10168]
PROBE_PERMISSION_TEXT_STR_LIST = ["no market data permissions", "not subscribed", "market data is not subscribed"]
# DEFINE THE IBKR ERROR CODE FOR AN INVALID END DATE / TIME (THE FIRST REAL RUN: EVERY DATED INDEX REQUEST FAILED WITH IT, WHILE THE SAME
# STRING WORKED FOR SPY) AND THE END DATE FORMATS TRIED IN ORDER WHEN IT HAPPENS
PROBE_INVALID_END_CODE_INT = 10314
PROBE_END_VARIANT_STR_LIST = ["us_eastern", "america_new_york", "utc", "no_timezone"]
# DEFINE THE COLUMNS OF THE RESULT TABLE
PROBE_COL_STR_LIST = ["symbol", "test", "target_date", "status", "diagnosis", "bars", "first_bar", "last_bar", "missing_minutes",
                      "volume_info", "first_bar_ohlc", "end_format", "error_code", "error_text", "seconds"]

"""
Bar Parsing
"""

# FUNCTION: CONVERT A PROBE BAR DATE FIELD TO A TIMESTAMP
def get_probe_bar_ts(bar_date_in):
    """
    Converts the date field of a bar. Daily bars come as "YYYYMMDD" (a date, returned as a naive midnight timestamp);
    intraday bars with formatDate=2 as epoch seconds (returned in New York time).

    Args:
        bar_date_in (str | int): The bar's date field

    Returns:
        pd.Timestamp: Naive date (daily bar) or New York timestamp (intraday bar)
    """
    # CONVERT THE FIELD TO A STRING
    bar_date_str = str(bar_date_in).strip()
    # IF THE FIELD IS A DAILY DATE (EIGHT DIGITS: AN EPOCH IN SECONDS HAS TEN)
    if bar_date_str.isdigit() and len(bar_date_str) == 8:
        # RETURN THE NAIVE DATE
        return pd.Timestamp(f"{bar_date_str[:4]}-{bar_date_str[4:6]}-{bar_date_str[6:]}")
    # RETURN THE NEW YORK TIMESTAMP (EPOCH OR "YYYYMMDD HH:MM:SS [TZ]")
    return get_bar_ny_ts(bar_date_str)

# FUNCTION: FORMAT A PROBE TIMESTAMP AS TEXT
def get_probe_ts_str(ts_in):
    """
    Args:
        ts_in (pd.Timestamp): Naive date or New York timestamp

    Returns:
        str: "YYYY-MM-DD" for a naive date, "YYYY-MM-DD HH:MM" for a New York timestamp
    """
    # RETURN THE TEXT
    return ts_in.strftime("%Y-%m-%d") if ts_in.tzinfo is None else ts_in.strftime("%Y-%m-%d %H:%M")

# FUNCTION: SUMMARIZE THE BARS OF A PROBE REQUEST
def get_bar_summary_dict(bar_tuple_list_in, intraday_bool_in):
    """
    Args:
        bar_tuple_list_in (list): Raw bars (date field, open, high, low, close, volume)
        intraday_bool_in (bool): True for 1-minute bars (counts the missing minutes between the first and last bar)

    Returns:
        dict: bars, first_bar, last_bar, missing_minutes, volume_info, first_bar_ohlc
    """
    # IF THERE ARE NO BARS
    if not bar_tuple_list_in:
        # RETURN AN EMPTY SUMMARY
        return {"bars": 0, "first_bar": "", "last_bar": "", "missing_minutes": "", "volume_info": "", "first_bar_ohlc": ""}
    # PARSE THE TIMESTAMPS
    ts_list = [get_probe_bar_ts(bar_tuple[0]) for bar_tuple in bar_tuple_list_in]
    # COUNT THE MINUTES BETWEEN THE FIRST AND LAST BAR THAT HAVE NO BAR (1-MINUTE BARS ONLY)
    missing_minutes = int((max(ts_list) - min(ts_list)).total_seconds() // 60 + 1 - len(set(ts_list))) if intraday_bool_in else ""
    # CLASSIFY THE VOLUMES
    max_volume_float = max(float(bar_tuple[5]) for bar_tuple in bar_tuple_list_in)
    volume_info_str = "unset" if max_volume_float >= PROBE_UNSET_VOLUME_FLOAT else ("zero_or_negative" if max_volume_float <= 0 else "present")
    # COLLECT THE FIRST BAR (BY TIME)
    first_bar_tuple = bar_tuple_list_in[ts_list.index(min(ts_list))]
    # RETURN THE SUMMARY
    return {"bars": len(bar_tuple_list_in), "first_bar": get_probe_ts_str(min(ts_list)), "last_bar": get_probe_ts_str(max(ts_list)),
            "missing_minutes": missing_minutes, "volume_info": volume_info_str,
            "first_bar_ohlc": f"{first_bar_tuple[1]}/{first_bar_tuple[2]}/{first_bar_tuple[3]}/{first_bar_tuple[4]}"}

"""
Diagnosis
"""

# FUNCTION: EXPLAIN AN ANSWER IN PLAIN WORDS
def get_diagnosis_str(status_str_in, error_code_int_in, error_str_in, count_int_in):
    """
    Args:
        status_str_in (str): "complete", "no_data", "pacing", "error" or "timeout" (see IbkrApp)
        error_code_int_in (int | None): IBKR error code
        error_str_in (str): IBKR error text
        count_int_in (int): Number of bars / contracts / timestamps returned

    Returns:
        str: Diagnosis
    """
    # LOWER THE ERROR TEXT
    error_str = (error_str_in or "").lower()
    # IF THE REQUEST COMPLETED
    if status_str_in == "complete":
        # RETURN WHETHER ANYTHING CAME BACK
        return "data returned" if count_int_in > 0 else "completed with nothing returned"
    # IF IBKR REPORTS A MISSING SUBSCRIPTION OR PERMISSION
    if error_code_int_in in PROBE_PERMISSION_CODE_INT_LIST or any(text_str in error_str for text_str in PROBE_PERMISSION_TEXT_STR_LIST):
        # RETURN THE PERMISSION DIAGNOSIS
        return "market data permission missing"
    # IF IBKR HAS NO DATA FOR THE WINDOW
    if status_str_in == "no_data":
        # RETURN THE NO DATA DIAGNOSIS
        return "no data for this window (before the history starts, or not offered for this data type)"
    # IF THE REQUEST BROKE THE PACING RULES
    if status_str_in == "pacing":
        # RETURN THE PACING DIAGNOSIS
        return "pacing violation (run the probe again later)"
    # IF THERE WAS NO ANSWER
    if status_str_in == "timeout":
        # RETURN THE TIMEOUT DIAGNOSIS
        return "no answer before the timeout"
    # IF THE CONTRACT DOES NOT EXIST
    if error_code_int_in == 200:
        # RETURN THE CONTRACT DIAGNOSIS
        return "contract not found (symbol, security type or exchange)"
    # RETURN THE GENERIC DIAGNOSIS
    return "IBKR error (see error_text)"

"""
End Date Formats
"""

# FUNCTION: GET THE END DATETIME STRING OF A REQUEST IN ONE OF THE FORMATS
def get_probe_end_str(end_ts_in, variant_str_in):
    """
    Args:
        end_ts_in (pd.Timestamp | None): Exclusive end of the window (New York time); None = now
        variant_str_in: "now" (empty string: IBKR uses the current time), "us_eastern" ("YYYYMMDD HH:MM:SS US/Eastern", the format of
            the SPY downloader), "america_new_york" (same with the IANA name), "utc" ("YYYYMMDD-HH:MM:SS", UTC) or "no_timezone"
            ("YYYYMMDD HH:MM:SS", read in the time zone of IB Gateway)

    Returns:
        str: IBKR end datetime string
    """
    # IF THE REQUEST ENDS NOW
    if variant_str_in == "now":
        # RETURN THE EMPTY STRING
        return ""
    # DEFINE THE NEW YORK END
    end_ts = end_ts_in.tz_convert(config.NY_TZ_STR)
    # RETURN THE FORMAT
    if variant_str_in == "us_eastern":
        return get_ibkr_end_datetime_str(end_ts)
    if variant_str_in == "america_new_york":
        return f"{end_ts.strftime('%Y%m%d %H:%M:%S')} America/New_York"
    if variant_str_in == "utc":
        return end_ts.tz_convert("UTC").strftime("%Y%m%d-%H:%M:%S")
    return end_ts.strftime("%Y%m%d %H:%M:%S")

"""
Probe Application
"""

# CLASS: IBKR APPLICATION WITH PROBE REQUESTS
class ProbeApp(IbkrApp):
    """
    IbkrApp plus contract details, head timestamp and free-form bar requests. Connection, request registry, error routing and
    waiting are inherited: every probe request is a normal entry of request_dict and finishes with one status.
    """

    # METHOD: REGISTER A PROBE REQUEST
    def register_probe_request_int(self, kind_str_in, label_str_in, end_datetime_str_in="", duration_str_in=""):
        """
        Args:
            kind_str_in (str): "details", "head" or "bars"
            label_str_in (str): Free label
            end_datetime_str_in (str): IBKR end datetime string (bars)
            duration_str_in (str): IBKR duration string (bars)

        Returns:
            int: Request id
        """
        # COLLECT A REQUEST ID
        req_id_int = self.get_next_req_id_int()
        # STORE THE REQUEST STATE BEFORE SENDING (THE ANSWER CAN ARRIVE BEFORE THE SEND RETURNS)
        with self.lock:
            self.request_dict[req_id_int] = {"label_str": label_str_in, "end_datetime_str": end_datetime_str_in, "duration_str": duration_str_in,
                                             "bar_tuple_list": [], "status_str": "pending", "error_code_int": None, "error_str": "",
                                             "submit_time_float": time.time(), "end_time_float": None, "done_event": threading.Event(),
                                             "kind_str": kind_str_in, "detail_dict_list": [], "head_str": ""}
        # RETURN THE REQUEST ID
        return req_id_int

    # METHOD: REQUEST THE CONTRACT DETAILS AND WAIT
    def request_contract_details_dict(self, contract_in, timeout_seconds_in=PROBE_TIMEOUT_SECONDS, label_str_in=""):
        """
        Args:
            contract_in (Contract): Contract
            timeout_seconds_in (float): Maximum wait
            label_str_in (str): Free label

        Returns:
            dict: Request result (see get_request_result_dict) with detail_dict_list
        """
        # REGISTER AND SEND THE REQUEST
        req_id_int = self.register_probe_request_int("details", label_str_in)
        self.reqContractDetails(req_id_int, contract_in)
        # WAIT AND RETURN THE RESULT
        return self.wait_request_dict(req_id_int, timeout_seconds_in)

    # METHOD: REQUEST THE HEAD TIMESTAMP AND WAIT
    def request_head_timestamp_dict(self, contract_in, what_to_show_str_in=config.WHAT_TO_SHOW_STR, timeout_seconds_in=PROBE_TIMEOUT_SECONDS,
                                    label_str_in=""):
        """
        Args:
            contract_in (Contract): Contract
            what_to_show_str_in (str): Data type
            timeout_seconds_in (float): Maximum wait
            label_str_in (str): Free label

        Returns:
            dict: Request result with head_str (epoch seconds as text)
        """
        # REGISTER AND SEND THE REQUEST (REGULAR TRADING HOURS, EPOCH DATES)
        req_id_int = self.register_probe_request_int("head", label_str_in)
        self.reqHeadTimeStamp(req_id_int, contract_in, what_to_show_str_in, config.USE_RTH_INT, config.FORMAT_DATE_INT)
        # WAIT AND RETURN THE RESULT
        return self.wait_request_dict(req_id_int, timeout_seconds_in)

    # METHOD: REQUEST BARS AND WAIT
    def request_probe_bars_dict(self, contract_in, end_ts_in, duration_str_in, bar_size_str_in, use_rth_int_in,
                                what_to_show_str_in=config.WHAT_TO_SHOW_STR, timeout_seconds_in=PROBE_TIMEOUT_SECONDS, label_str_in="",
                                end_variant_list_in=None):
        """
        Args:
            contract_in (Contract): Contract
            end_ts_in (pd.Timestamp | None): Exclusive end of the window (New York time); None = the latest bars (no end date)
            duration_str_in (str): IBKR duration string, e.g. "25200 S" or "1 M"
            bar_size_str_in (str): IBKR bar size, e.g. "1 min" or "1 day"
            use_rth_int_in (int): 1 = regular trading hours only, 0 = everything
            what_to_show_str_in (str): Data type
            timeout_seconds_in (float): Maximum wait
            label_str_in (str): Free label
            end_variant_list_in (list | None): End date formats to try in order (default PROBE_END_VARIANT_STR_LIST; ["utc"] for the indices
                once the first probe has shown that it is the one that works)

        Returns:
            dict: Request result with raw bars (date field, open, high, low, close, volume)
        """
        # DEFINE THE END FORMATS TO TRY (NO END = NOW; A DATED END IS RETRIED IN THE NEXT FORMAT WHILE IBKR ANSWERS "INVALID END DATE")
        variant_str_list = ["now"] if end_ts_in is None else (end_variant_list_in or PROBE_END_VARIANT_STR_LIST)
        # ITERATE OVER THE FORMATS
        for variant_str in variant_str_list:
            # DEFINE THE END STRING
            end_datetime_str = get_probe_end_str(end_ts_in, variant_str)
            # REGISTER AND SEND THE REQUEST (POSITIONAL ARGUMENTS: THE KEYWORD NAMES DIFFER BETWEEN IBAPI VERSIONS)
            req_id_int = self.register_probe_request_int("bars", label_str_in, end_datetime_str, duration_str_in)
            self.reqHistoricalData(req_id_int, contract_in, end_datetime_str, duration_str_in, bar_size_str_in, what_to_show_str_in,
                                   use_rth_int_in, config.FORMAT_DATE_INT, False, [])
            # WAIT FOR THE RESULT AND NOTE THE FORMAT
            result_dict = self.wait_request_dict(req_id_int, timeout_seconds_in)
            result_dict["end_variant"] = variant_str
            # IF THE END DATE WAS ACCEPTED (OR THE ERROR IS ANOTHER ONE)
            if result_dict["error_code_int"] != PROBE_INVALID_END_CODE_INT:
                # STOP TRYING
                break
        # RETURN THE LAST RESULT
        return result_dict

    # METHOD: GET THE RESULT OF A REQUEST (ADDS THE DETAILS AND THE HEAD TIMESTAMP)
    def get_request_result_dict(self, req_id_int_in):
        """
        Args:
            req_id_int_in (int): Request id

        Returns:
            dict: See IbkrApp.get_request_result_dict, plus detail_dict_list and head_str
        """
        # COLLECT THE BASE RESULT
        result_dict = IbkrApp.get_request_result_dict(self, req_id_int_in)
        # ADD THE PROBE FIELDS
        with self.lock:
            request_state_dict = self.request_dict[req_id_int_in]
            result_dict["detail_dict_list"] = list(request_state_dict.get("detail_dict_list", []))
            result_dict["head_str"] = request_state_dict.get("head_str", "")
        # RETURN THE RESULT
        return result_dict

    # METHOD: TIME OUT A REQUEST
    def timeout_request(self, req_id_int_in):
        """
        Args:
            req_id_int_in (int): Request id
        """
        # COLLECT THE KIND OF THE REQUEST
        with self.lock:
            kind_str = self.request_dict.get(req_id_int_in, {}).get("kind_str", "bars")
        # IF THE REQUEST IS A BAR REQUEST
        if kind_str == "bars":
            # USE THE BASE METHOD (MARKS THE TIMEOUT AND CANCELS THE HISTORICAL REQUEST)
            IbkrApp.timeout_request(self, req_id_int_in)
            # EXIT FUNCTION
            return
        # MARK THE REQUEST AS TIMED OUT
        self.finish_request(req_id_int_in, "timeout", None, "no answer before the timeout")
        # IF THE REQUEST IS A HEAD TIMESTAMP REQUEST AND THE APPLICATION IS CONNECTED
        if kind_str == "head" and self.isConnected():
            # CANCEL IT
            self.cancelHeadTimeStamp(req_id_int_in)

    """
    EWrapper Callbacks (called from the API thread)
    """

    # CALLBACK: RECEIVE ONE HISTORICAL BAR (STORED RAW: DAILY BARS HAVE A DIFFERENT DATE FORMAT)
    def historicalData(self, reqId, bar):
        """
        Args:
            reqId (int): Request id
            bar (BarData): Bar
        """
        # LOCK THE REQUEST DICTIONARY
        with self.lock:
            # COLLECT THE REQUEST STATE
            request_state_dict = self.request_dict.get(reqId)
            # IF THE REQUEST IS UNKNOWN OR FINISHED
            if request_state_dict is None or request_state_dict["status_str"] != "pending":
                # IGNORE THE BAR
                return
            # STORE THE RAW BAR
            request_state_dict["bar_tuple_list"].append((str(bar.date), float(bar.open), float(bar.high), float(bar.low), float(bar.close),
                                                         get_volume_number(bar.volume)))

    # CALLBACK: RECEIVE ONE CONTRACT DETAILS ENTRY
    def contractDetails(self, reqId, contractDetails):
        """
        Args:
            reqId (int): Request id
            contractDetails (ContractDetails): Contract details
        """
        # DEFINE THE DETAILS
        contract = contractDetails.contract
        detail_dict = {"con_id": contract.conId, "symbol": contract.symbol, "sec_type": contract.secType, "currency": contract.currency,
                       "exchange": contract.exchange, "primary_exchange": contract.primaryExchange, "local_symbol": contract.localSymbol,
                       "long_name": contractDetails.longName, "time_zone": contractDetails.timeZoneId, "min_tick": contractDetails.minTick,
                       "valid_exchanges": contractDetails.validExchanges, "trading_hours": contractDetails.tradingHours,
                       "liquid_hours": contractDetails.liquidHours}
        # STORE THE DETAILS
        with self.lock:
            # IF THE REQUEST IS KNOWN AND PENDING
            if self.request_dict.get(reqId, {}).get("status_str") == "pending":
                # STORE THEM
                self.request_dict[reqId]["detail_dict_list"].append(detail_dict)

    # CALLBACK: THE CONTRACT DETAILS ARE COMPLETE
    def contractDetailsEnd(self, reqId):
        """
        Args:
            reqId (int): Request id
        """
        # FINISH THE REQUEST
        self.finish_request(reqId, "complete")

    # CALLBACK: RECEIVE THE HEAD TIMESTAMP
    def headTimestamp(self, reqId, headTimestamp):
        """
        Args:
            reqId (int): Request id
            headTimestamp (str): Earliest data point (epoch seconds as text with formatDate=2)
        """
        # STORE THE TIMESTAMP
        with self.lock:
            # IF THE REQUEST IS KNOWN AND PENDING
            if self.request_dict.get(reqId, {}).get("status_str") == "pending":
                # STORE IT
                self.request_dict[reqId]["head_str"] = str(headTimestamp)
        # FINISH THE REQUEST
        self.finish_request(reqId, "complete")

"""
Plan
"""

# FUNCTION: MAP DATES TO SESSION DATES
def get_sample_session_date_list(date_str_list_in=None):
    """
    Args:
        date_str_list_in (list | None): Dates "YYYY-MM-DD" (default PROBE_SAMPLE_DATE_STR_LIST)

    Returns:
        list: The first NYSE session on or after each date, without duplicates, in the given order
    """
    # DEFINE THE RESULT
    session_date_str_list = []
    # ITERATE OVER THE DATES
    for date_str in (date_str_list_in or PROBE_SAMPLE_DATE_STR_LIST):
        # COLLECT THE FIRST SESSION ON OR AFTER THE DATE
        session_pdf = get_session_pdf(date_str, pd.Timestamp(date_str) + pd.Timedelta(days=10))
        # STORE THE SESSION DATE (ONCE)
        session_date_str = str(session_pdf["date"].iloc[0])
        session_date_str_list.append(session_date_str) if session_date_str not in session_date_str_list else None
    # RETURN THE DATES
    return session_date_str_list

# FUNCTION: GET THE STEPS OF THE PROBE
def get_probe_step_list(symbol_str_list_in, session_date_str_list_in, exchange_str_in=PROBE_EXCHANGE_STR, control_bool_in=True, extended_bool_in=True):
    """
    Args:
        symbol_str_list_in (list): Index symbols
        session_date_str_list_in (list): Sample session dates (from get_sample_session_date_list)
        exchange_str_in (str): Index exchange
        control_bool_in (bool): Add the SPY control (daily and 1-minute bars on the last sample date)
        extended_bool_in (bool): Add a 1-minute request including extended hours on the last sample date

    Returns:
        list: One dict per request: symbol, test, target_date, kind_str, contract, end_ts, duration_str, bar_size_str, use_rth_int
    """
    # DEFINE THE STEPS
    step_dict_list = []
    # ITERATE OVER THE INDEX SYMBOLS
    for symbol_str in symbol_str_list_in:
        # DEFINE THE CONTRACT
        contract = get_contract(symbol_str, PROBE_SEC_TYPE_STR, PROBE_CURRENCY_STR, exchange_str_in, "")
        # ADD THE CONTRACT DETAILS AND THE EARLIEST DATA POINT
        step_dict_list.append({"symbol": symbol_str, "test": "contract_details", "target_date": "", "kind_str": "details", "contract": contract})
        step_dict_list.append({"symbol": symbol_str, "test": "earliest_data", "target_date": "", "kind_str": "head", "contract": contract})
        # ADD THE LATEST DAILY AND 1-MINUTE BARS WITHOUT AN END DATE (TELLS A SUBSCRIPTION PROBLEM FROM AN END DATE PROBLEM; ONLY COUNTS AND
        # TIMES ARE REPORTED, NEVER PRICES: THE LATEST BARS LIE INSIDE THE UNTOUCHED WINDOW OF THE RESEARCH)
        step_dict_list.append({"symbol": symbol_str, "test": "daily_bars_latest", "target_date": "", "kind_str": "bars", "contract": contract,
                               "end_ts": None, "duration_str": PROBE_DAILY_DURATION_STR, "bar_size_str": PROBE_DAILY_BAR_SIZE_STR, "use_rth_int": 1})
        step_dict_list.append({"symbol": symbol_str, "test": "minute_bars_latest", "target_date": "", "kind_str": "bars", "contract": contract,
                               "end_ts": None, "duration_str": PROBE_REGULAR_DURATION_STR, "bar_size_str": PROBE_MINUTE_BAR_SIZE_STR, "use_rth_int": 1})
        # ITERATE OVER THE SAMPLE DATES
        for date_str in session_date_str_list_in:
            # DEFINE THE END OF THE WINDOW
            end_ts = pd.Timestamp(f"{date_str} {PROBE_WINDOW_END_TIME_STR}", tz=config.NY_TZ_STR)
            # ADD THE DAILY AND THE 1-MINUTE REQUESTS
            step_dict_list.append({"symbol": symbol_str, "test": "daily_bars", "target_date": date_str, "kind_str": "bars", "contract": contract,
                                   "end_ts": end_ts, "duration_str": PROBE_DAILY_DURATION_STR, "bar_size_str": PROBE_DAILY_BAR_SIZE_STR, "use_rth_int": 1})
            step_dict_list.append({"symbol": symbol_str, "test": "minute_bars", "target_date": date_str, "kind_str": "bars", "contract": contract,
                                   "end_ts": end_ts, "duration_str": PROBE_REGULAR_DURATION_STR, "bar_size_str": PROBE_MINUTE_BAR_SIZE_STR, "use_rth_int": 1})
        # IF THE EXTENDED REQUEST IS WANTED
        if extended_bool_in and session_date_str_list_in:
            # ADD THE EXTENDED-HOURS 1-MINUTE REQUEST ON THE LAST SAMPLE DATE
            step_dict_list.append({"symbol": symbol_str, "test": "minute_bars_extended", "target_date": session_date_str_list_in[-1], "kind_str": "bars",
                                   "contract": contract, "end_ts": pd.Timestamp(f"{session_date_str_list_in[-1]} {PROBE_WINDOW_END_TIME_STR}", tz=config.NY_TZ_STR),
                                   "duration_str": PROBE_EXTENDED_DURATION_STR, "bar_size_str": PROBE_MINUTE_BAR_SIZE_STR, "use_rth_int": 0})
    # IF THE CONTROL IS WANTED
    if control_bool_in and session_date_str_list_in:
        # DEFINE THE SPY CONTRACT, THE LAST SAMPLE DATE AND ITS WINDOW (SPY: 09:30 -> 16:00)
        contract, date_str = get_contract(), session_date_str_list_in[-1]
        end_ts = pd.Timestamp(f"{date_str} {PROBE_WINDOW_END_TIME_STR}", tz=config.NY_TZ_STR)
        # ADD THE CONTROL REQUESTS
        step_dict_list.append({"symbol": f"{config.CONTRACT_SYMBOL_STR} (control)", "test": "daily_bars", "target_date": date_str, "kind_str": "bars",
                               "contract": contract, "end_ts": end_ts, "duration_str": PROBE_DAILY_DURATION_STR, "bar_size_str": PROBE_DAILY_BAR_SIZE_STR, "use_rth_int": 1})
        step_dict_list.append({"symbol": f"{config.CONTRACT_SYMBOL_STR} (control)", "test": "minute_bars", "target_date": date_str, "kind_str": "bars",
                               "contract": contract, "end_ts": end_ts, "duration_str": PROBE_REGULAR_DURATION_STR, "bar_size_str": PROBE_MINUTE_BAR_SIZE_STR, "use_rth_int": 1})
    # RETURN THE STEPS
    return step_dict_list

"""
Run
"""

# FUNCTION: BUILD THE RESULT ROW OF A STEP
def get_probe_row_dict(step_dict_in, result_dict_in):
    """
    Args:
        step_dict_in (dict): Step from get_probe_step_list
        result_dict_in (dict): Result of the request

    Returns:
        dict: Row with the PROBE_COL_STR_LIST columns
    """
    # DEFINE THE KIND AND THE STATUS
    kind_str, status_str = step_dict_in["kind_str"], result_dict_in["status_str"]
    # SUMMARIZE THE ANSWER
    if kind_str == "details":
        # THE CONTRACTS FOUND
        detail_dict_list = result_dict_in["detail_dict_list"]
        summary_dict = {"bars": len(detail_dict_list), "first_bar": "", "last_bar": "", "missing_minutes": "", "volume_info": "",
                        "first_bar_ohlc": "; ".join(f"conId {d['con_id']} {d['long_name']} {d['exchange']} tz={d['time_zone']}" for d in detail_dict_list)[:200]}
    elif kind_str == "head":
        # THE EARLIEST DATA POINT
        head_str = result_dict_in["head_str"]
        summary_dict = {"bars": 1 if head_str else 0, "first_bar": get_probe_ts_str(get_probe_bar_ts(head_str)) if head_str else "", "last_bar": "",
                        "missing_minutes": "", "volume_info": "", "first_bar_ohlc": ""}
    else:
        # THE BARS
        summary_dict = get_bar_summary_dict(result_dict_in["bar_tuple_list"], step_dict_in["bar_size_str"] == PROBE_MINUTE_BAR_SIZE_STR)
    # NEVER KEEP PRICES OF THE LATEST BARS (UNTOUCHED WINDOW)
    summary_dict["first_bar_ohlc"] = "" if step_dict_in["test"].endswith("_latest") else summary_dict["first_bar_ohlc"]
    # RETURN THE ROW
    return {"symbol": step_dict_in["symbol"], "test": step_dict_in["test"], "target_date": step_dict_in["target_date"], "status": status_str,
            "diagnosis": get_diagnosis_str(status_str, result_dict_in["error_code_int"], result_dict_in["error_str"], summary_dict["bars"]),
            **summary_dict, "end_format": result_dict_in.get("end_variant", ""), "error_code": result_dict_in["error_code_int"] if result_dict_in["error_code_int"] is not None else "",
            "error_text": result_dict_in["error_str"][:200], "seconds": result_dict_in["request_seconds_float"]}

# FUNCTION: SEND ONE STEP AND WAIT FOR ITS ANSWER
def request_probe_step_dict(app_in, step_dict_in, timeout_seconds_in=PROBE_TIMEOUT_SECONDS):
    """
    Args:
        app_in (ProbeApp): Connected application
        step_dict_in (dict): Step from get_probe_step_list
        timeout_seconds_in (float): Maximum wait

    Returns:
        dict: Request result
    """
    # DEFINE THE LABEL
    label_str = f"{step_dict_in['symbol']} {step_dict_in['test']} {step_dict_in['target_date']}".strip()
    # IF THE STEP IS A CONTRACT DETAILS REQUEST
    if step_dict_in["kind_str"] == "details":
        # RETURN THE RESULT
        return app_in.request_contract_details_dict(step_dict_in["contract"], timeout_seconds_in, label_str)
    # IF THE STEP IS A HEAD TIMESTAMP REQUEST
    if step_dict_in["kind_str"] == "head":
        # RETURN THE RESULT
        return app_in.request_head_timestamp_dict(step_dict_in["contract"], timeout_seconds_in=timeout_seconds_in, label_str_in=label_str)
    # RETURN THE BAR RESULT
    return app_in.request_probe_bars_dict(step_dict_in["contract"], step_dict_in["end_ts"], step_dict_in["duration_str"], step_dict_in["bar_size_str"],
                                          step_dict_in["use_rth_int"], timeout_seconds_in=timeout_seconds_in, label_str_in=label_str)

# FUNCTION: RUN THE PROBE
def run_probe_tuple(app_in, step_dict_list_in, timeout_seconds_in=PROBE_TIMEOUT_SECONDS, pause_seconds_in=PROBE_PAUSE_SECONDS,
                    pacing_wait_seconds_in=PROBE_PACING_WAIT_SECONDS, sleep_func_in=time.sleep, alert_in=True):
    """
    Runs the steps one after the other (a pause between requests keeps the probe far inside the pacing limits; a pacing violation
    is retried once after a wait).

    Args:
        app_in (ProbeApp): Connected application
        step_dict_list_in (list): Steps from get_probe_step_list
        timeout_seconds_in (float): Maximum wait per request
        pause_seconds_in (float): Pause between two requests
        pacing_wait_seconds_in (float): Wait before the retry after a pacing violation
        sleep_func_in (callable): Sleep function (replaced in tests)
        alert_in (bool): Display one line per step

    Returns:
        tuple: (result table DataFrame, contract details DataFrame)
    """
    # DEFINE THE ROWS AND THE CONTRACT DETAILS
    row_dict_list, detail_dict_list = [], []
    # ITERATE OVER THE STEPS
    for step_idx, step_dict in enumerate(step_dict_list_in, start=1):
        # SEND THE REQUEST
        result_dict = request_probe_step_dict(app_in, step_dict, timeout_seconds_in)
        # IF IBKR REPORTS A PACING VIOLATION
        if result_dict["status_str"] == "pacing":
            # WAIT AND SEND IT AGAIN ONCE
            sleep_func_in(pacing_wait_seconds_in)
            result_dict = request_probe_step_dict(app_in, step_dict, timeout_seconds_in)
        # STORE THE ROW AND THE CONTRACT DETAILS
        row_dict = get_probe_row_dict(step_dict, result_dict)
        row_dict_list.append(row_dict)
        detail_dict_list += [{"requested_symbol": step_dict["symbol"], **detail_dict} for detail_dict in result_dict["detail_dict_list"]]
        # DISPLAY INFORMATION
        print(f"[{step_idx}/{len(step_dict_list_in)}] {row_dict['symbol']:<14} {row_dict['test']:<21} {row_dict['target_date']:<10} "
              f"{row_dict['diagnosis']} (bars {row_dict['bars']})" + (f" [IBKR {row_dict['error_code']}: {row_dict['error_text'][:110]}]" if row_dict["error_text"] else "")) if alert_in else None
        # PAUSE BEFORE THE NEXT REQUEST
        sleep_func_in(pause_seconds_in)
    # RETURN THE TABLES
    return pd.DataFrame(row_dict_list, columns=PROBE_COL_STR_LIST), pd.DataFrame(detail_dict_list)

"""
Verdict
"""

# FUNCTION: SUMMARIZE THE RESULTS IN PLAIN WORDS
def get_verdict_str_list(row_pdf_in):
    """
    Args:
        row_pdf_in (pd.DataFrame): Result table of run_probe_tuple

    Returns:
        list: One line per symbol, plus the overall conclusion
    """
    # DEFINE THE LINES
    line_str_list = []
    # DEFINE WHETHER THE CONTROL WORKED (NO CONTROL: UNKNOWN)
    control_pdf = row_pdf_in[row_pdf_in["symbol"].str.contains("control")]
    control_ok_bool = None if control_pdf.empty else bool((control_pdf["diagnosis"] == "data returned").all())
    # ITERATE OVER THE SYMBOLS (THE CONTROL EXCLUDED)
    for symbol_str in [s for s in row_pdf_in["symbol"].unique() if "control" not in s]:
        # COLLECT THE ROWS OF THE SYMBOL
        symbol_pdf = row_pdf_in[row_pdf_in["symbol"] == symbol_str]
        # COUNT THE ANSWERS
        contract_ok_bool = bool((symbol_pdf[symbol_pdf["test"] == "contract_details"]["diagnosis"] == "data returned").any())
        head_pdf = symbol_pdf[symbol_pdf["test"] == "earliest_data"]
        daily_pdf, minute_pdf = symbol_pdf[symbol_pdf["test"] == "daily_bars"], symbol_pdf[symbol_pdf["test"] == "minute_bars"]
        latest_pdf = symbol_pdf[symbol_pdf["test"].str.endswith("_latest")]
        permission_count_int = int((symbol_pdf["diagnosis"] == "market data permission missing").sum())
        # STORE THE LINE
        line_str_list.append(f"{symbol_str}: contract {'found' if contract_ok_bool else 'NOT found'}; "
                             f"earliest data {head_pdf['first_bar'].iloc[0] if len(head_pdf) and head_pdf['first_bar'].iloc[0] else 'unknown'}; "
                             f"daily bars on {int((daily_pdf['diagnosis'] == 'data returned').sum())}/{len(daily_pdf)} sample dates; "
                             f"1-minute bars on {int((minute_pdf['diagnosis'] == 'data returned').sum())}/{len(minute_pdf)} sample dates; "
                             f"latest bars without an end date: {int((latest_pdf['diagnosis'] == 'data returned').sum())}/{len(latest_pdf)}; "
                             f"permission errors: {permission_count_int}")
    # LIST THE DISTINCT IBKR ERRORS (CODE AND TEXT) BEHIND THE FAILED REQUESTS
    failed_pdf = row_pdf_in[(row_pdf_in["error_text"] != "") & (row_pdf_in["diagnosis"] != "data returned")]
    for (error_code, error_text_str), count_int in failed_pdf.groupby(["error_code", "error_text"]).size().items():
        # STORE THE LINE
        line_str_list.append(f"IBKR error {error_code} x{count_int}: {error_text_str[:160]}")
    # COLLECT THE DATED REQUESTS THAT WORKED AND THE LATEST-BARS REQUESTS THAT WORKED (INDEX SYMBOLS ONLY)
    index_pdf = row_pdf_in[~row_pdf_in["symbol"].str.contains("control")]
    dated_ok_pdf = index_pdf[index_pdf["test"].isin(["daily_bars", "minute_bars"]) & (index_pdf["diagnosis"] == "data returned")]
    latest_ok_pdf = index_pdf[index_pdf["test"].str.endswith("_latest") & (index_pdf["diagnosis"] == "data returned")]
    # IF A DATED REQUEST NEEDED ANOTHER END FORMAT THAN THE SPY DOWNLOADER'S
    if (dated_ok_pdf["end_format"] != "us_eastern").any():
        # STORE THE FINDING
        line_str_list.append(f"END FORMAT: dated index requests worked only with {sorted(set(dated_ok_pdf['end_format']) - {'us_eastern'})} (not the SPY format): "
                             f"the VIX download must use it.")
    # IF NO DATED REQUEST WORKED BUT THE LATEST BARS CAME BACK
    if dated_ok_pdf.empty and not latest_ok_pdf.empty:
        # STORE THE FINDING
        line_str_list.append("Latest bars (no end date) work but every dated request fails: see the IBKR errors above.")
    # IF THE CONTROL FAILED
    if control_ok_bool is False:
        # STORE THE CONCLUSION
        line_str_list.append("CONTROL FAILED: SPY did not return data either, so the problem is the connection or the request, not VIX. Fix that first.")
    # IF ANY PERMISSION IS MISSING
    elif (row_pdf_in["diagnosis"] == "market data permission missing").any():
        # STORE THE CONCLUSION
        line_str_list.append("IBKR reports missing market data permissions: look up the exact subscription for the CBOE index in Client Portal "
                             "(Settings > Market Data Subscriptions) before building a download pipeline.")
    # IF NOTHING CAME BACK FOR ANY INDEX
    elif not (row_pdf_in[~row_pdf_in["symbol"].str.contains("control")]["diagnosis"] == "data returned").any():
        # STORE THE CONCLUSION
        line_str_list.append("No index request returned data: read the error_text column of the CSV.")
    # RETURN THE LINES
    return line_str_list
