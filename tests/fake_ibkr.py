import time
import threading
from decimal import Decimal
from datetime import datetime
import numpy as np
import pandas as pd
from ibapi.common import BarData
# IMPORT THE IBKR APPLICATION
from ingest.ibkr_client import IbkrApp

"""
Simulated IBKR Server (tests only)

FakeIbkrApp is an IbkrApp whose socket methods are replaced: connect / run / isConnected / disconnect simulate the
handshake, and reqHistoricalData answers from a background thread with synthetic 1-minute bars, calling the same
EWrapper callbacks (historicalData, historicalDataEnd, error) as the real API. The error callback is called with the
ibapi 10.45 argument layout (reqId, errorTime, code, text, json) or the 9.81 layout (reqId, code, text).

Behaviour per session date and attempt (scenario_dict[date_str] = list of behaviours, one per attempt; the last one repeats):
    "ok"        every minute of the requested window, then historicalDataEnd
    "partial"   the window without its first 30 minutes, then historicalDataEnd
    "extra"     the window plus one bar after it (16:00), then historicalDataEnd
    "dup"       the window with one minute sent twice, then historicalDataEnd
    "empty"     no bar, then historicalDataEnd
    "no_data"   error 162 "HMDS query returned no data"
    "pacing"    error 162 "Historical Market Data Service error message:API historical data query cancelled: ... pacing violation"
    "error"     error 200 "No security definition has been found for the request"
    "timeout"   a few bars, then nothing
    "late"      nothing until 0.4 s later, then every bar and historicalDataEnd (arrives after a short timeout)
"""

# CLASS: SIMULATED IBKR APPLICATION
class FakeIbkrApp(IbkrApp):
    """
    IbkrApp with a simulated server.
    """

    # METHOD: INITIALIZE
    def __init__(self, scenario_dict_in=None, error_layout_str_in="10.45", decimal_volume_bool_in=True, delay_seconds_in=0.01):
        """
        Args:
            scenario_dict_in (dict): date "YYYY-MM-DD" -> list of behaviours (default "ok")
            error_layout_str_in (str): "10.45" or "9.81" error callback layout
            decimal_volume_bool_in (bool): Send volumes as Decimal (ibapi 10) instead of int (ibapi 9.81)
            delay_seconds_in (float): Answer delay
        """
        # INITIALIZE THE APPLICATION WITHOUT ALERTS
        IbkrApp.__init__(self, alert_in=False)
        # STORE THE SIMULATION SETTINGS
        self.scenario_dict = scenario_dict_in or {}
        self.error_layout_str = error_layout_str_in
        self.decimal_volume_bool = decimal_volume_bool_in
        self.delay_seconds = delay_seconds_in
        self.fake_connected_bool = False
        # RECORD THE REQUESTS (req_id, date_str, end_str, duration_str, submit time) AND THE ATTEMPTS PER DATE
        self.sent_request_list, self.attempt_dict, self.cancel_list = [], {}, []
        self.max_open_int, self.open_int = 0, 0
        self.fake_lock = threading.Lock()

    # METHOD: SIMULATE THE CONNECTION
    def connect(self, host, port, clientId):
        # MARK AS CONNECTED AND SEND THE HANDSHAKE FROM ANOTHER THREAD
        self.fake_connected_bool = True
        threading.Timer(0.01, self.nextValidId, args=(100,)).start()

    # METHOD: SIMULATE THE READER LOOP
    def run(self):
        # NOTHING TO READ
        return

    # METHOD: SIMULATE THE CONNECTION STATE
    def isConnected(self):
        return self.fake_connected_bool

    # METHOD: SIMULATE THE SERVER VERSION
    def serverVersion(self):
        return 999

    # METHOD: SIMULATE THE DISCONNECTION
    def disconnect(self):
        self.fake_connected_bool = False

    # METHOD: RECORD A CANCEL
    def cancelHistoricalData(self, reqId):
        self.cancel_list.append(reqId)

    # METHOD: SEND AN ERROR WITH THE CONFIGURED LAYOUT
    def send_error(self, req_id_int_in, code_int_in, text_str_in):
        # IF THE LAYOUT IS IBAPI 10.45
        if self.error_layout_str == "10.45":
            self.error(req_id_int_in, int(time.time() * 1000), code_int_in, text_str_in, "")
        # IF THE LAYOUT IS IBAPI 9.81
        else:
            self.error(req_id_int_in, code_int_in, text_str_in)

    # METHOD: BUILD A BAR
    def get_bar(self, ts_in, price_float_in):
        # CREATE THE BAR (EPOCH SECONDS, formatDate=2)
        bar = BarData()
        bar.date = str(int(ts_in.timestamp()))
        bar.open, bar.high, bar.low, bar.close = price_float_in, round(price_float_in + 0.05, 2), round(price_float_in - 0.05, 2), round(price_float_in + 0.01, 2)
        bar.volume = Decimal(1000 + ts_in.minute) if self.decimal_volume_bool else 1000 + ts_in.minute
        return bar

    # METHOD: SIMULATE A HISTORICAL REQUEST
    def reqHistoricalData(self, reqId, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions):
        # PARSE THE WINDOW
        end_ts = pd.Timestamp(datetime.strptime(endDateTime.rsplit(" ", 1)[0], "%Y%m%d %H:%M:%S")).tz_localize("America/New_York")
        start_ts = end_ts - pd.Timedelta(seconds=int(durationStr.split()[0]))
        date_str = str(start_ts.date())
        # COLLECT THE BEHAVIOUR OF THIS ATTEMPT
        with self.fake_lock:
            attempt_int = self.attempt_dict.get(date_str, 0)
            self.attempt_dict[date_str] = attempt_int + 1
            behaviour_list = self.scenario_dict.get(date_str, ["ok"])
            behaviour_str = behaviour_list[min(attempt_int, len(behaviour_list) - 1)]
            self.sent_request_list.append((reqId, date_str, endDateTime, durationStr, time.time()))
            self.open_int += 1
            self.max_open_int = max(self.max_open_int, self.open_int)
        # ANSWER FROM ANOTHER THREAD
        threading.Thread(target=self.answer, args=(reqId, start_ts, end_ts, behaviour_str), daemon=True).start()

    # METHOD: ANSWER A REQUEST
    def answer(self, req_id_int_in, start_ts_in, end_ts_in, behaviour_str_in):
        # WAIT THE ANSWER DELAY
        time.sleep(self.delay_seconds + (0.4 if behaviour_str_in == "late" else 0))
        # DEFINE THE MINUTES OF THE WINDOW
        minute_index = pd.date_range(start_ts_in, end_ts_in - pd.Timedelta(minutes=1), freq="1min")
        price_arr = 100 + np.arange(len(minute_index)) * 0.01
        # IF THE ANSWER IS AN ERROR
        if behaviour_str_in in ["no_data", "pacing", "error"]:
            # SEND THE ERROR
            code_int, text_str = {"no_data": (162, "Historical Market Data Service error message:HMDS query returned no data: SPY@SMART Trades"),
                                  "pacing": (162, "Historical Market Data Service error message:API historical data query cancelled: Pacing violation"),
                                  "error": (200, "No security definition has been found for the request")}[behaviour_str_in]
            self.send_error(req_id_int_in, code_int, text_str)
        # IF THE ANSWER IS BARS
        else:
            # SELECT THE MINUTES TO SEND
            send_list = list(zip(minute_index, price_arr))
            if behaviour_str_in == "partial":
                send_list = send_list[30:]
            elif behaviour_str_in == "extra":
                send_list = send_list + [(end_ts_in, 200.0)]
            elif behaviour_str_in == "dup":
                send_list = send_list[:5] + [send_list[4]] + send_list[5:]
            elif behaviour_str_in in ["empty"]:
                send_list = []
            elif behaviour_str_in == "timeout":
                send_list = send_list[:3]
            # SEND AN INFORMATIONAL MESSAGE TIED TO THE REQUEST (MUST NOT END IT)
            self.send_error(req_id_int_in, 2176, "Warning: informational message")
            # SEND THE BARS
            for ts, price_float in send_list:
                self.historicalData(req_id_int_in, self.get_bar(ts, round(float(price_float), 2)))
            # SEND THE END (EXCEPT FOR A TIMEOUT)
            if behaviour_str_in != "timeout":
                self.historicalDataEnd(req_id_int_in, "", "")
        # CLOSE THE REQUEST
        with self.fake_lock:
            self.open_int -= 1
