import time
import threading
from decimal import Decimal
from datetime import datetime, timezone
import pandas as pd
# IMPORT THE IBKR API
import ibapi
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
# IMPORT THE INGEST CONFIGURATION
from ingest import config

"""
IBKR Client: one connection to IB Gateway / TWS that runs any number of historical bar requests at the same time

What changed compared with the TestApp class of ibkr_data_stream.ipynb, and why:

    1. Results are kept PER REQUEST (request_dict[reqId]) instead of in one shared historical_data_list. With one shared
       list, a late answer to a timed-out request was appended to the next request's bars. Several requests can now be
       in flight together (ingest.ibkr_download uses this to download several sessions at once).
    2. A request ends in exactly one status: "complete" (historicalDataEnd received), "no_data", "pacing", "error"
       (an IBKR error for this reqId) or "timeout". Before, an IBKR error never released the wait, so every failed
       request waited for the full timeout, and the bars received before a timeout were saved as if the day was complete.
    3. historicalDataEnd no longer calls cancelHistoricalData: the request is already finished, and the cancel produced
       the "366 No historical data query found" error seen after every request in the old notebook.
    4. error() accepts the signatures of every ibapi version (9.81: reqId, code, text; 10.19: + advancedOrderRejectJson;
       10.3x / 10.45: reqId, errorTime, code, text, advancedOrderRejectJson), so the same code runs with the repo pin
       (9.81.1.post1) and with the TWS API source install (10.45.1).
    5. Bar timestamps are converted from epoch seconds directly to New York time (no detour through the local clock),
       and the ibapi 10 Decimal volume is converted to an integer.
    6. The connection is confirmed by nextValidId (the handshake) instead of assuming that connect() succeeded.

Threading:
    EClient.run() reads the socket in a background thread and calls the callbacks below from that thread. Every access to
    request_dict is protected by a lock. The caller waits on each request's threading.Event.
"""

"""
Contract And Request Strings
"""

# FUNCTION: GET THE CONTRACT OF THE CONFIGURATION (SPY, SMART, ARCA)
def get_contract(symbol_str_in=config.CONTRACT_SYMBOL_STR, sec_type_str_in=config.CONTRACT_SEC_TYPE_STR,
                 currency_str_in=config.CONTRACT_CURRENCY_STR, exchange_str_in=config.CONTRACT_EXCHANGE_STR,
                 primary_exchange_str_in=config.CONTRACT_PRIMARY_EXCHANGE_STR):
    """
    Creates an IBKR contract.

    Args:
        symbol_str_in (str): Symbol, e.g. "SPY"
        sec_type_str_in (str): Security type, e.g. "STK"
        currency_str_in (str): Currency, e.g. "USD"
        exchange_str_in (str): Routing exchange, e.g. "SMART"
        primary_exchange_str_in (str): Primary listing exchange, e.g. "ARCA"

    Returns:
        Contract: The IBKR contract
    """
    # INSTANTIATE THE CONTRACT
    contract = Contract()
    # DEFINE THE CONTRACT ATTRIBUTES
    contract.symbol = symbol_str_in
    contract.secType = sec_type_str_in
    contract.currency = currency_str_in
    contract.exchange = exchange_str_in
    contract.primaryExchange = primary_exchange_str_in
    # RETURN THE CONTRACT
    return contract

# FUNCTION: GET THE IBKR END DATETIME STRING
def get_ibkr_end_datetime_str(end_ts_in):
    """
    Formats the END of a request window as IBKR expects it ("YYYYMMDD HH:MM:SS US/Eastern"). The end is exclusive:
    to receive the bar that starts at 15:59, the end must be 16:00:00.

    Args:
        end_ts_in (pd.Timestamp | str): End of the window (timezone-aware, or naive New York time)

    Returns:
        str: IBKR end datetime string
    """
    # CONVERT THE END TO A NEW YORK TIMESTAMP
    end_ts = pd.Timestamp(end_ts_in)
    end_ts = end_ts.tz_localize(config.NY_TZ_STR) if end_ts.tzinfo is None else end_ts.tz_convert(config.NY_TZ_STR)
    # RETURN THE FORMATTED STRING
    return f"{end_ts.strftime('%Y%m%d %H:%M:%S')} {config.IBKR_END_DATETIME_TZ_STR}"

# FUNCTION: GET THE IBAPI VERSION STRING
def get_ibapi_version_str():
    """
    Returns the installed ibapi version (recorded in the download log).

    Returns:
        str: Version string, e.g. "10.45.1" ("unknown" if it cannot be read)
    """
    # TRY THE VERSION FUNCTION OF THE PACKAGE, THEN THE PACKAGE METADATA
    try:
        # RETURN THE VERSION OF THE PACKAGE FUNCTION
        return ibapi.get_version_string()
    # IF THE FUNCTION DOES NOT EXIST
    except AttributeError:
        # IMPORT THE METADATA MODULE
        from importlib.metadata import version, PackageNotFoundError
        # TRY THE PACKAGE METADATA
        try:
            # RETURN THE METADATA VERSION
            return version("ibapi")
        # IF THE PACKAGE HAS NO METADATA
        except PackageNotFoundError:
            # RETURN UNKNOWN
            return "unknown"

"""
Bar Parsing
"""

# FUNCTION: CONVERT AN IBKR BAR DATE TO A NEW YORK TIMESTAMP
def get_bar_ny_ts(bar_date_in):
    """
    Converts the date field of an IBKR bar to a New York timestamp. With formatDate=2 the field holds epoch seconds;
    a "YYYYMMDD HH:MM:SS [timezone]" string (formatDate=1) is also accepted.

    Args:
        bar_date_in (str | int): The bar's date field

    Returns:
        pd.Timestamp: Bar start in New York time
    """
    # CONVERT THE FIELD TO A STRING
    bar_date_str = str(bar_date_in).strip()
    # IF THE FIELD IS EPOCH SECONDS
    if bar_date_str.isdigit():
        # RETURN THE NEW YORK TIMESTAMP
        return pd.Timestamp(int(bar_date_str), unit="s", tz="UTC").tz_convert(config.NY_TZ_STR)
    # SPLIT THE DATE, TIME AND OPTIONAL TIMEZONE
    part_str_list = bar_date_str.split()
    # PARSE THE DATE AND TIME (NEW YORK TIME UNLESS A TIMEZONE IS GIVEN)
    local_ts = pd.Timestamp(datetime.strptime(" ".join(part_str_list[:2]), "%Y%m%d %H:%M:%S"))
    tz_str = part_str_list[2] if len(part_str_list) > 2 else config.NY_TZ_STR
    # RETURN THE NEW YORK TIMESTAMP
    return local_ts.tz_localize(tz_str).tz_convert(config.NY_TZ_STR)

# FUNCTION: CONVERT AN IBKR VOLUME TO A NUMBER
def get_volume_number(volume_in):
    """
    Converts a bar volume (int in ibapi 9.81, Decimal in ibapi 10) to an int when it is whole, a float otherwise.

    Args:
        volume_in (int | float | Decimal): The bar's volume

    Returns:
        int | float: Volume
    """
    # CONVERT THE VOLUME TO A DECIMAL (str() AVOIDS BINARY FLOAT NOISE)
    volume_decimal = Decimal(str(volume_in))
    # IF THE VOLUME IS A WHOLE NUMBER
    if volume_decimal == volume_decimal.to_integral_value():
        # RETURN AN INTEGER
        return int(volume_decimal)
    # RETURN A FLOAT
    return float(volume_decimal)

# FUNCTION: CONVERT AN IBKR BAR TO A TUPLE
def get_bar_tuple(bar_in):
    """
    Converts an IBKR BarData object to (timestamp, open, high, low, close, volume).

    Args:
        bar_in (BarData): Bar received in historicalData

    Returns:
        tuple: (pd.Timestamp New York, float, float, float, float, int | float)
    """
    # RETURN THE BAR TUPLE
    return (get_bar_ny_ts(bar_in.date), float(bar_in.open), float(bar_in.high), float(bar_in.low), float(bar_in.close),
            get_volume_number(bar_in.volume))

"""
Error Parsing
"""

# FUNCTION: PARSE THE ARGUMENTS OF AN IBKR ERROR CALLBACK
def get_error_dict(arg_tup_in):
    """
    Normalizes the arguments that follow reqId in EWrapper.error across ibapi versions.

    Args:
        arg_tup_in (tuple): Arguments after reqId:
            (code, text)                                     ibapi 9.81
            (code, text, advancedOrderRejectJson)            ibapi 10.19
            (errorTime, code, text, advancedOrderRejectJson) ibapi 10.3x / 10.45

    Returns:
        dict: error_time_int (None if absent), error_code_int, error_str
    """
    # IF THE SECOND ARGUMENT IS THE CODE (A TIME WAS SENT FIRST)
    if len(arg_tup_in) >= 3 and isinstance(arg_tup_in[1], int) and isinstance(arg_tup_in[2], str):
        # RETURN THE 10.3X+ LAYOUT
        return {"error_time_int": arg_tup_in[0], "error_code_int": int(arg_tup_in[1]), "error_str": arg_tup_in[2]}
    # RETURN THE OLDER LAYOUT
    return {"error_time_int": None, "error_code_int": int(arg_tup_in[0]), "error_str": str(arg_tup_in[1]) if len(arg_tup_in) > 1 else ""}

# FUNCTION: CLASSIFY AN IBKR ERROR FOR A HISTORICAL REQUEST
def get_error_status_str(error_code_int_in, error_str_in):
    """
    Classifies an error received for a historical request.

    Args:
        error_code_int_in (int): IBKR error code
        error_str_in (str): IBKR error text

    Returns:
        str: "info" (not an error: data farm messages), "pacing", "no_data" or "error"
    """
    # IF THE CODE IS INFORMATIONAL
    if config.IBKR_INFO_CODE_MIN_INT <= error_code_int_in <= config.IBKR_INFO_CODE_MAX_INT:
        # RETURN INFO
        return "info"
    # IF THE CODE IS THE HISTORICAL DATA SERVICE ERROR
    if error_code_int_in == config.IBKR_HMDS_ERROR_CODE_INT:
        # IF THE TEXT REPORTS A PACING VIOLATION
        if config.IBKR_PACING_TEXT_STR in error_str_in.lower():
            # RETURN PACING
            return "pacing"
        # IF THE TEXT REPORTS AN EMPTY ANSWER
        if config.IBKR_NO_DATA_TEXT_STR in error_str_in.lower():
            # RETURN NO DATA
            return "no_data"
    # RETURN ERROR
    return "error"

"""
IBKR Application
"""

# CLASS: IBKR APPLICATION (EWRAPPER CALLBACKS + ECLIENT REQUESTS)
class IbkrApp(EWrapper, EClient):
    """
    One IBKR connection. Use connect_app() / disconnect_app(), then submit_historical_request_int() and
    wait_request_dict() (or request_historical_bars_dict() for one blocking request).
    """

    # METHOD: INITIALIZE THE APPLICATION
    def __init__(self, alert_in=True):
        """
        Args:
            alert_in (bool): Display connection and error messages
        """
        # INITIALIZE THE ECLIENT (THE APPLICATION IS ITS OWN WRAPPER)
        EClient.__init__(self, self)
        # DEFINE THE DISPLAY FLAG
        self.alert_bool = alert_in
        # DEFINE THE LOCK THAT PROTECTS THE REQUEST DICTIONARY AND THE REQUEST ID COUNTER
        self.lock = threading.Lock()
        # DEFINE THE REQUEST DICTIONARY (reqId -> request state)
        self.request_dict = {}
        # DEFINE THE NEXT REQUEST ID (SET BY nextValidId)
        self.next_req_id_int = None
        # DEFINE THE HANDSHAKE EVENT
        self.connected_event = threading.Event()
        # DEFINE THE EVENT SET WHENEVER ANY REQUEST FINISHES (LETS A CALLER WAIT FOR "SOMETHING HAPPENED")
        self.any_done_event = threading.Event()
        # DEFINE THE LIST OF MESSAGES NOT TIED TO A REQUEST (CONNECTION STATUS, ...)
        self.message_dict_list = []
        # DEFINE THE API THREAD
        self.api_thread = None

    """
    Connection
    """

    # METHOD: CONNECT AND WAIT FOR THE HANDSHAKE
    def connect_app(self, host_str_in=config.IBKR_HOST_STR, port_int_in=config.IBKR_PORT_INT,
                    client_id_int_in=config.IBKR_DOWNLOAD_CLIENT_ID_INT, timeout_seconds_in=config.IBKR_CONNECT_TIMEOUT_SECONDS):
        """
        Connects to IB Gateway / TWS, starts the API thread and waits for nextValidId.

        Args:
            host_str_in (str): Host
            port_int_in (int): Port (4001 Gateway live, 4002 Gateway paper, 7496 TWS live, 7497 TWS paper)
            client_id_int_in (int): Client id (must differ from every other connected client)
            timeout_seconds_in (float): Maximum wait for the handshake

        Returns:
            bool: True when connected
        """
        # IF THE APPLICATION IS ALREADY CONNECTED
        if self.isConnected() and self.connected_event.is_set():
            # DISPLAY INFORMATION
            print("✅ Already connected to IBKR") if self.alert_bool else None
            # RETURN TRUE
            return True
        # RESET THE HANDSHAKE EVENT
        self.connected_event.clear()
        # DISPLAY INFORMATION
        print(f"⏳ Connecting to IBKR at {host_str_in}:{port_int_in} (client id {client_id_int_in}) ...") if self.alert_bool else None
        # OPEN THE SOCKET
        self.connect(host_str_in, port_int_in, client_id_int_in)
        # START THE API THREAD
        self.api_thread = threading.Thread(target=self.run, name=f"ibkr_api_{client_id_int_in}", daemon=True)
        self.api_thread.start()
        # WAIT FOR THE HANDSHAKE
        connected_bool = self.connected_event.wait(timeout=timeout_seconds_in)
        # IF THE HANDSHAKE DID NOT ARRIVE
        if not connected_bool:
            # DISPLAY INFORMATION
            print(f"❌ No handshake from IBKR after {timeout_seconds_in} s. Is IB Gateway / TWS running with the API enabled "
                  f"on port {port_int_in}, and is client id {client_id_int_in} free?") if self.alert_bool else None
            # CLOSE THE SOCKET
            self.disconnect()
            # RETURN FALSE
            return False
        # DISPLAY INFORMATION
        print(f"✅ Connected to IBKR (ibapi {get_ibapi_version_str()}, server version {self.serverVersion()})") if self.alert_bool else None
        # RETURN TRUE
        return True

    # METHOD: DISCONNECT
    def disconnect_app(self):
        """
        Disconnects (pending requests are released with the status "error").
        """
        # IF THE APPLICATION IS CONNECTED
        if self.isConnected():
            # DISPLAY INFORMATION
            print("✅ Disconnecting from IBKR ...") if self.alert_bool else None
            # DISCONNECT
            self.disconnect()
        # RELEASE EVERY PENDING REQUEST
        with self.lock:
            # ITERATE OVER THE PENDING REQUESTS
            for request_state_dict in self.request_dict.values():
                # IF THE REQUEST IS STILL PENDING
                if request_state_dict["status_str"] == "pending":
                    # MARK IT AS AN ERROR
                    request_state_dict.update(status_str="error", error_str="disconnected")
                    request_state_dict["done_event"].set()
        # RESET THE HANDSHAKE EVENT
        self.connected_event.clear()

    """
    Requests
    """

    # METHOD: GET THE NEXT REQUEST ID
    def get_next_req_id_int(self):
        """
        Returns a new request id (unique for this connection).

        Returns:
            int: Request id
        """
        # LOCK THE COUNTER
        with self.lock:
            # COLLECT THE ID AND INCREASE THE COUNTER
            req_id_int = self.next_req_id_int
            self.next_req_id_int += 1
        # RETURN THE ID
        return req_id_int

    # METHOD: SUBMIT A HISTORICAL BAR REQUEST
    def submit_historical_request_int(self, contract_in, end_ts_in, duration_seconds_int_in, label_str_in=""):
        """
        Registers and sends a historical bar request (1-minute TRADES bars, regular trading hours, epoch dates).

        Args:
            contract_in (Contract): Contract
            end_ts_in (pd.Timestamp): Exclusive end of the window (New York time)
            duration_seconds_int_in (int): Window length in seconds
            label_str_in (str): Free label kept with the request (e.g. the session date)

        Returns:
            int: Request id
        """
        # COLLECT A REQUEST ID
        req_id_int = self.get_next_req_id_int()
        # DEFINE THE REQUEST STRINGS
        end_datetime_str = get_ibkr_end_datetime_str(end_ts_in)
        duration_str = f"{int(duration_seconds_int_in)} S"
        # REGISTER THE REQUEST BEFORE SENDING IT (THE ANSWER CAN ARRIVE BEFORE reqHistoricalData RETURNS)
        with self.lock:
            # STORE THE REQUEST STATE
            self.request_dict[req_id_int] = {"label_str": label_str_in, "end_datetime_str": end_datetime_str,
                                             "duration_str": duration_str, "bar_tuple_list": [], "status_str": "pending",
                                             "error_code_int": None, "error_str": "", "submit_time_float": time.time(),
                                             "end_time_float": None, "done_event": threading.Event()}
        # SEND THE REQUEST (POSITIONAL ARGUMENTS: THE KEYWORD NAMES DIFFER BETWEEN IBAPI VERSIONS)
        self.reqHistoricalData(req_id_int, contract_in, end_datetime_str, duration_str, config.BAR_SIZE_STR,
                               config.WHAT_TO_SHOW_STR, config.USE_RTH_INT, config.FORMAT_DATE_INT, False, [])
        # RETURN THE REQUEST ID
        return req_id_int

    # METHOD: MARK A REQUEST AS FINISHED
    def finish_request(self, req_id_int_in, status_str_in, error_code_int_in=None, error_str_in=""):
        """
        Sets the final status of a pending request and releases its waiter (no effect on a finished request).

        Args:
            req_id_int_in (int): Request id
            status_str_in (str): "complete", "no_data", "pacing", "error" or "timeout"
            error_code_int_in (int | None): IBKR error code
            error_str_in (str): IBKR error text
        """
        # LOCK THE REQUEST DICTIONARY
        with self.lock:
            # COLLECT THE REQUEST STATE
            request_state_dict = self.request_dict.get(req_id_int_in)
            # IF THE REQUEST IS UNKNOWN OR ALREADY FINISHED
            if request_state_dict is None or request_state_dict["status_str"] != "pending":
                # EXIT FUNCTION
                return
            # STORE THE FINAL STATUS
            request_state_dict.update(status_str=status_str_in, error_code_int=error_code_int_in, error_str=error_str_in,
                                      end_time_float=time.time())
            # RELEASE THE WAITERS
            request_state_dict["done_event"].set()
            self.any_done_event.set()

    # METHOD: GET THE RESULT OF A REQUEST (NON-BLOCKING)
    def get_request_result_dict(self, req_id_int_in):
        """
        Returns a snapshot of a request's state.

        Args:
            req_id_int_in (int): Request id

        Returns:
            dict: req_id_int, label_str, status_str ("pending", "complete", "no_data", "pacing", "error", "timeout"),
                  bar_tuple_list, error_code_int, error_str, request_seconds_float
        """
        # LOCK THE REQUEST DICTIONARY
        with self.lock:
            # COLLECT THE REQUEST STATE
            request_state_dict = self.request_dict[req_id_int_in]
            # DEFINE THE ELAPSED TIME
            end_time_float = request_state_dict["end_time_float"] or time.time()
            # RETURN A COPY OF THE STATE
            return {"req_id_int": req_id_int_in, "label_str": request_state_dict["label_str"],
                    "status_str": request_state_dict["status_str"], "bar_tuple_list": list(request_state_dict["bar_tuple_list"]),
                    "error_code_int": request_state_dict["error_code_int"], "error_str": request_state_dict["error_str"],
                    "request_seconds_float": round(end_time_float - request_state_dict["submit_time_float"], 3)}

    # METHOD: GET THE STATUS OF A REQUEST (NON-BLOCKING, NO COPY OF THE BARS)
    def get_request_status_str(self, req_id_int_in):
        """
        Args:
            req_id_int_in (int): Request id

        Returns:
            str: "pending", "complete", "no_data", "pacing", "error" or "timeout" ("unknown" if forgotten)
        """
        # LOCK THE REQUEST DICTIONARY
        with self.lock:
            # RETURN THE STATUS
            return self.request_dict.get(req_id_int_in, {}).get("status_str", "unknown")

    # METHOD: CANCEL A PENDING REQUEST AFTER A TIMEOUT
    def timeout_request(self, req_id_int_in):
        """
        Marks a pending request as timed out and cancels it at IBKR (bars that still arrive are ignored).

        Args:
            req_id_int_in (int): Request id
        """
        # COLLECT THE STATUS
        with self.lock:
            # DEFINE WHETHER THE REQUEST IS STILL PENDING
            pending_bool = self.request_dict.get(req_id_int_in, {}).get("status_str") == "pending"
        # IF THE REQUEST IS STILL PENDING
        if pending_bool:
            # MARK IT AS TIMED OUT
            self.finish_request(req_id_int_in, "timeout", None, "no historicalDataEnd before the timeout")
            # IF THE APPLICATION IS CONNECTED
            if self.isConnected():
                # CANCEL THE REQUEST AT IBKR
                self.cancelHistoricalData(req_id_int_in)

    # METHOD: WAIT FOR A REQUEST
    def wait_request_dict(self, req_id_int_in, timeout_seconds_in=config.REQUEST_TIMEOUT_SECONDS):
        """
        Waits until a request finishes or times out, then returns its result and forgets it.

        Args:
            req_id_int_in (int): Request id
            timeout_seconds_in (float): Maximum wait

        Returns:
            dict: See get_request_result_dict
        """
        # WAIT FOR THE REQUEST
        if not self.request_dict[req_id_int_in]["done_event"].wait(timeout=timeout_seconds_in):
            # MARK IT AS TIMED OUT
            self.timeout_request(req_id_int_in)
        # COLLECT THE RESULT
        result_dict = self.get_request_result_dict(req_id_int_in)
        # FORGET THE REQUEST
        self.forget_request(req_id_int_in)
        # RETURN THE RESULT
        return result_dict

    # METHOD: FORGET A FINISHED REQUEST
    def forget_request(self, req_id_int_in):
        """
        Removes a request from the request dictionary (bars that still arrive for it are ignored).

        Args:
            req_id_int_in (int): Request id
        """
        # LOCK THE REQUEST DICTIONARY
        with self.lock:
            # REMOVE THE REQUEST
            self.request_dict.pop(req_id_int_in, None)

    # METHOD: REQUEST HISTORICAL BARS AND WAIT (ONE BLOCKING REQUEST)
    def request_historical_bars_dict(self, contract_in, end_ts_in, duration_seconds_int_in,
                                     timeout_seconds_in=config.REQUEST_TIMEOUT_SECONDS, label_str_in=""):
        """
        Sends one historical bar request and waits for its result.

        Args:
            contract_in (Contract): Contract
            end_ts_in (pd.Timestamp): Exclusive end of the window (New York time)
            duration_seconds_int_in (int): Window length in seconds
            timeout_seconds_in (float): Maximum wait
            label_str_in (str): Free label

        Returns:
            dict: See get_request_result_dict
        """
        # IF THE APPLICATION IS NOT CONNECTED
        if not self.isConnected():
            # RETURN AN ERROR RESULT
            return {"req_id_int": None, "label_str": label_str_in, "status_str": "error", "bar_tuple_list": [],
                    "error_code_int": None, "error_str": "not connected", "request_seconds_float": 0.0}
        # SUBMIT THE REQUEST
        req_id_int = self.submit_historical_request_int(contract_in, end_ts_in, duration_seconds_int_in, label_str_in)
        # WAIT AND RETURN THE RESULT
        return self.wait_request_dict(req_id_int, timeout_seconds_in)

    """
    EWrapper Callbacks (called from the API thread)
    """

    # CALLBACK: RECEIVE THE FIRST VALID ID (THE HANDSHAKE IS COMPLETE)
    def nextValidId(self, orderId):
        """
        Args:
            orderId (int): First valid order id
        """
        # LOCK THE COUNTER
        with self.lock:
            # START THE REQUEST IDS AT THE FIRST VALID ID (ONLY ON THE FIRST HANDSHAKE OF THIS OBJECT)
            self.next_req_id_int = orderId if self.next_req_id_int is None else max(self.next_req_id_int, orderId)
        # SIGNAL THE HANDSHAKE
        self.connected_event.set()

    # CALLBACK: RECEIVE AN ERROR OR A STATUS MESSAGE
    def error(self, reqId, *args):
        """
        Args:
            reqId (int): Request id (-1 for messages not tied to a request)
            *args: Version-dependent arguments (see get_error_dict)
        """
        # NORMALIZE THE ARGUMENTS
        error_dict = get_error_dict(args)
        error_code_int, error_str = error_dict["error_code_int"], error_dict["error_str"]
        # CLASSIFY THE MESSAGE
        status_str = get_error_status_str(error_code_int, error_str)
        # LOCK THE REQUEST DICTIONARY
        with self.lock:
            # DEFINE WHETHER THE MESSAGE BELONGS TO A PENDING REQUEST
            pending_bool = self.request_dict.get(reqId, {}).get("status_str") == "pending"
        # IF THE MESSAGE BELONGS TO A PENDING REQUEST AND IS NOT INFORMATIONAL
        if pending_bool and status_str != "info":
            # FINISH THE REQUEST
            self.finish_request(reqId, status_str, error_code_int, error_str)
            # EXIT FUNCTION (THE CALLER REPORTS IT)
            return
        # STORE THE MESSAGE
        self.message_dict_list.append({"time_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "req_id_int": reqId,
                                       "error_code_int": error_code_int, "error_str": error_str})
        # DISPLAY INFORMATION
        print(f"ℹ️ IBKR [{error_code_int}] {error_str}" + (f" (reqId {reqId})" if reqId not in (-1, None) else "")) if self.alert_bool else None

    # CALLBACK: RECEIVE ONE HISTORICAL BAR
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
            # IF THE REQUEST IS UNKNOWN OR FINISHED (A LATE ANSWER TO A TIMED-OUT REQUEST)
            if request_state_dict is None or request_state_dict["status_str"] != "pending":
                # IGNORE THE BAR
                return
            # STORE THE BAR
            request_state_dict["bar_tuple_list"].append(get_bar_tuple(bar))

    # CALLBACK: THE HISTORICAL REQUEST IS COMPLETE
    def historicalDataEnd(self, reqId, start, end):
        """
        Args:
            reqId (int): Request id
            start (str): Window start reported by IBKR
            end (str): Window end reported by IBKR
        """
        # FINISH THE REQUEST (NO cancelHistoricalData: THE REQUEST IS ALREADY OVER)
        self.finish_request(reqId, "complete")
