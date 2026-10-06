import threading
import pandas as pd
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE IBKR CLIENT FUNCTIONS
from ingest.ibkr_client import get_contract
# IMPORT THE SESSION FUNCTIONS
from ingest.sessions import get_ny_now_ts, get_session_pdf
# IMPORT THE RAW FILE FUNCTIONS
from ingest.raw_files import write_text_pdf_atomic

"""
Live 1-Minute Bars During The Session

The old notebook (query_live_1min_ohlcv / determine_streaming_time) requested "35 S" of bars once a minute and advanced
its query minute by one per loop. Problems found:
    - the loop slept time.sleep(5) (a "TESTING" line left in), so it advanced one minute every 5 seconds and requested
      minutes that had not happened yet;
    - a missed or failed poll lost its minute for good (each poll asked for exactly one minute);
    - the bars went into the shared historical / live lists, mixed with any other request;
    - the start logic used the module-level ny_datetime_now instead of its own argument.

LiveBarStream (this module):
    - every poll asks for the last few minutes (STREAM_LOOKBACK_SECONDS) that are COMPLETE (a minute is requested
      STREAM_BAR_DELAY_SECONDS after it ends), so a missed poll heals itself at the next one;
    - the first poll back-fills the session from the open;
    - polls are aligned on the real clock (minute + delay), not on a sleep counter, so they never drift ahead;
    - it stops by itself after the session's last bar, or with stop();
    - bars are kept in the stream object and written to <staging>/live/ohlcv_live_YYYYMMDD.csv after every poll.

The live file is NEVER merged into the raw folder (its name does not match the ohlcv_data_ pattern). The record of a
session is always the after-close historical download (scripts/download_ibkr_ohlcv.py), which is checked minute by minute.
"""

# FUNCTION: GET THE MARKET STATUS
def get_market_status_dict(now_ny_ts_in=None):
    """
    Describes today's session relative to now.

    Args:
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)

    Returns:
        dict: now_ny_ts, session_today_bool, market_open_bool, first_bar_ts, last_bar_ts (today's or None),
              next_first_bar_ts (next session open after now), seconds_until_open_int, seconds_until_close_int
    """
    # COLLECT THE CURRENT TIME AND THE SESSIONS OF THE NEXT TWO WEEKS
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    session_pdf = get_session_pdf(now_ny_ts.date(), now_ny_ts.date() + pd.Timedelta(days=14))
    # COLLECT TODAY'S SESSION
    today_pdf = session_pdf[session_pdf["date"] == now_ny_ts.date()]
    first_bar_ts = today_pdf["first_bar_ts"].iloc[0] if len(today_pdf) else None
    last_bar_ts = today_pdf["last_bar_ts"].iloc[0] if len(today_pdf) else None
    # DEFINE WHETHER THE MARKET IS OPEN (FROM THE OPEN TO THE END OF THE LAST BAR)
    market_open_bool = first_bar_ts is not None and first_bar_ts <= now_ny_ts < last_bar_ts + pd.Timedelta(minutes=1)
    # COLLECT THE NEXT OPEN AFTER NOW
    future_pdf = session_pdf[session_pdf["first_bar_ts"] > now_ny_ts]
    next_first_bar_ts = future_pdf["first_bar_ts"].iloc[0] if len(future_pdf) else None
    # RETURN THE STATUS
    return {"now_ny_ts": now_ny_ts, "session_today_bool": first_bar_ts is not None, "market_open_bool": market_open_bool,
            "first_bar_ts": first_bar_ts, "last_bar_ts": last_bar_ts, "next_first_bar_ts": next_first_bar_ts,
            "seconds_until_open_int": int((next_first_bar_ts - now_ny_ts).total_seconds()) if next_first_bar_ts is not None and not market_open_bool else 0,
            "seconds_until_close_int": int((last_bar_ts + pd.Timedelta(minutes=1) - now_ny_ts).total_seconds()) if market_open_bool else 0}

# CLASS: LIVE BAR STREAM
class LiveBarStream:
    """
    Polls the completed 1-minute bars of today's session in a background thread.
    """

    # METHOD: INITIALIZE
    def __init__(self, app_in, contract_in=None, live_path_str_in=config.LIVE_OHLCV_PATH_STR, bar_delay_seconds_in=config.STREAM_BAR_DELAY_SECONDS,
                 lookback_seconds_in=config.STREAM_LOOKBACK_SECONDS, request_timeout_seconds_in=config.REQUEST_TIMEOUT_SECONDS,
                 now_func_in=get_ny_now_ts, wait_func_in=None, alert_in=True):
        """
        Args:
            app_in (IbkrApp): Connected application (use its own client id: config.IBKR_STREAM_CLIENT_ID_INT)
            contract_in (Contract | None): Contract (None = the configured SPY contract)
            live_path_str_in (str): Folder of the live file
            bar_delay_seconds_in (float): Delay after a minute ends before its bar is requested
            lookback_seconds_in (int): Window of every poll after the first
            request_timeout_seconds_in (float): Maximum wait per poll
            now_func_in (callable): Returns the current New York time (replaced in the tests)
            wait_func_in (callable | None): Waits a number of seconds and returns True if stopped (None = stop_event.wait;
                                            replaced in the tests by a simulated clock)
            alert_in (bool): Display each poll
        """
        # STORE THE SETTINGS
        self.app = app_in
        self.contract = contract_in if contract_in is not None else get_contract()
        self.live_path_str = live_path_str_in
        self.bar_delay_td = pd.Timedelta(seconds=bar_delay_seconds_in)
        self.lookback_seconds_int = int(lookback_seconds_in)
        self.request_timeout_seconds = request_timeout_seconds_in
        self.now_func = now_func_in
        self.wait_func = wait_func_in if wait_func_in is not None else (lambda seconds_float: self.stop_event.wait(timeout=seconds_float))
        self.alert_bool = alert_in
        # DEFINE THE STATE
        self.bar_dict = {}
        self.poll_dict_list = []
        self.session_row = None
        self.stop_event = threading.Event()
        self.thread = None

    # METHOD: START THE STREAM
    def start(self):
        """
        Starts the background thread (no effect if it is already running).

        Returns:
            bool: True if a stream is running after the call
        """
        # IF THE STREAM IS ALREADY RUNNING
        if self.is_running():
            # DISPLAY INFORMATION AND RETURN
            print("⚠️ Live stream already running") if self.alert_bool else None
            return True
        # COLLECT TODAY'S SESSION
        now_ny_ts = self.now_func()
        session_pdf = get_session_pdf(now_ny_ts.date(), now_ny_ts.date())
        # IF THERE IS NO SESSION TODAY OR IT IS OVER
        if session_pdf.empty or now_ny_ts > session_pdf["last_bar_ts"].iloc[0] + pd.Timedelta(minutes=1) + self.bar_delay_td + pd.Timedelta(minutes=5):
            # DISPLAY INFORMATION AND RETURN
            print("❌ No session to stream (closed today, or already over): use the historical download.") if self.alert_bool else None
            return False
        # STORE THE SESSION AND START THE THREAD
        self.session_row = next(session_pdf.itertuples(index=False))
        self.stop_event.clear()
        self.thread = threading.Thread(target=self.run, name="live_bar_stream", daemon=True)
        self.thread.start()
        # RETURN TRUE
        return True

    # METHOD: STOP THE STREAM
    def stop(self, timeout_seconds_in=None):
        """
        Stops the background thread and waits for it.

        Args:
            timeout_seconds_in (float | None): Maximum wait (None = until the current poll ends)
        """
        # SIGNAL THE STOP
        self.stop_event.set()
        # WAIT FOR THE THREAD
        if self.thread is not None:
            self.thread.join(timeout=timeout_seconds_in)
        # DISPLAY INFORMATION
        print(f"⛔ Live stream stopped ({len(self.bar_dict)} bars)") if self.alert_bool else None

    # METHOD: CHECK WHETHER THE STREAM IS RUNNING
    def is_running(self):
        """
        Returns:
            bool: True while the background thread is alive
        """
        # RETURN THE THREAD STATE
        return self.thread is not None and self.thread.is_alive()

    # METHOD: GET THE LAST COMPLETE BAR START AT A TIME
    def get_last_complete_bar_ts(self, now_ny_ts_in):
        """
        Args:
            now_ny_ts_in (pd.Timestamp): Current New York time

        Returns:
            pd.Timestamp: Start of the latest bar that ended at least bar_delay ago (clipped to the session's last bar)
        """
        # RETURN THE LATEST COMPLETE BAR
        return min((now_ny_ts_in - self.bar_delay_td).floor("min") - pd.Timedelta(minutes=1), self.session_row.last_bar_ts)

    # METHOD: POLL ONCE
    def poll(self):
        """
        Requests the completed bars up to now (the whole session so far on the first poll, the lookback afterwards),
        stores them and writes the live file.

        Returns:
            dict: poll time, window, status, bars received, total bars, missing minutes so far
        """
        # COLLECT THE LATEST COMPLETE BAR
        now_ny_ts = self.now_func()
        last_bar_ts = self.get_last_complete_bar_ts(now_ny_ts)
        # IF NO BAR IS COMPLETE YET
        if last_bar_ts < self.session_row.first_bar_ts:
            # RETURN A WAITING POLL
            return {"poll_ts": now_ny_ts, "status_str": "waiting", "bar_count_int": 0, "total_bar_count_int": len(self.bar_dict), "missing_count_int": 0}
        # DEFINE THE WINDOW (FROM THE OPEN ON THE FIRST POLL)
        window_first_ts = self.session_row.first_bar_ts if not self.bar_dict else max(self.session_row.first_bar_ts, last_bar_ts - pd.Timedelta(seconds=self.lookback_seconds_int - 60))
        end_ts = last_bar_ts + pd.Timedelta(minutes=1)
        # REQUEST THE BARS
        result_dict = self.app.request_historical_bars_dict(self.contract, end_ts, int((end_ts - window_first_ts).total_seconds()),
                                                            self.request_timeout_seconds, f"live {last_bar_ts:%H:%M}")
        # STORE THE COMPLETE BARS OF THE SESSION (A LATER POLL REPLACES AN EARLIER VERSION OF THE SAME MINUTE)
        for bar_tup in result_dict["bar_tuple_list"]:
            if self.session_row.first_bar_ts <= bar_tup[0] <= last_bar_ts:
                self.bar_dict[bar_tup[0]] = bar_tup
        # WRITE THE LIVE FILE
        live_pdf = self.get_live_pdf()
        if not live_pdf.empty:
            write_text_pdf_atomic(live_pdf.assign(timestamp=live_pdf["timestamp"].map(lambda ts: ts.isoformat(sep=" "))),
                                  f"{self.live_path_str}ohlcv_live_{self.session_row.date:%Y%m%d}.csv")
        # COUNT THE MISSING MINUTES SO FAR
        missing_count_int = len(pd.date_range(self.session_row.first_bar_ts, last_bar_ts, freq="1min").difference(pd.DatetimeIndex(list(self.bar_dict))))
        # DEFINE THE POLL RECORD
        poll_dict = {"poll_ts": now_ny_ts, "window_str": f"{window_first_ts:%H:%M}-{last_bar_ts:%H:%M}", "status_str": result_dict["status_str"],
                     "bar_count_int": len(result_dict["bar_tuple_list"]), "total_bar_count_int": len(self.bar_dict), "missing_count_int": missing_count_int}
        # DISPLAY INFORMATION
        print(f"📡 {now_ny_ts:%H:%M:%S} bars {poll_dict['window_str']}: {result_dict['status_str']} ({poll_dict['bar_count_int']} received, "
              f"{poll_dict['total_bar_count_int']} total, {missing_count_int} missing)" + (f" {result_dict['error_str']}" if result_dict["error_str"] else "")) if self.alert_bool else None
        # RETURN THE POLL RECORD
        return poll_dict

    # METHOD: RUN THE POLLING LOOP
    def run(self):
        """
        Polls once per minute (minute + bar delay) until the session's last bar is stored or stop() is called.
        """
        # DISPLAY INFORMATION
        print(f"📡 Live stream for {self.session_row.date}: bars {self.session_row.first_bar_ts:%H:%M}-{self.session_row.last_bar_ts:%H:%M}, "
              f"each requested {self.bar_delay_td.total_seconds():.0f} s after it ends") if self.alert_bool else None
        # LOOP UNTIL STOPPED
        while not self.stop_event.is_set():
            # POLL (A FAILED POLL IS RECORDED, THE NEXT ONE COVERS ITS MINUTES)
            try:
                self.poll_dict_list.append(self.poll())
            except Exception as poll_error:
                self.poll_dict_list.append({"poll_ts": self.now_func(), "status_str": f"exception: {poll_error}"})
                print(f"❌ Poll failed: {poll_error}") if self.alert_bool else None
            # IF THE SESSION'S LAST BAR IS STORED
            if self.session_row.last_bar_ts in self.bar_dict:
                # DISPLAY INFORMATION AND STOP
                print(f"🏁 Session complete in the live stream ({len(self.bar_dict)} bars). The record is the after-close download.") if self.alert_bool else None
                break
            # IF THE LAST POSSIBLE POLL HAS PASSED (THE LAST BAR NEVER CAME)
            now_ny_ts = self.now_func()
            if now_ny_ts > self.session_row.last_bar_ts + pd.Timedelta(minutes=1) + self.bar_delay_td + pd.Timedelta(minutes=5):
                # DISPLAY INFORMATION AND STOP
                print(f"⚠️ Session over; the live stream is missing {self.poll_dict_list[-1].get('missing_count_int', '?')} minute(s).") if self.alert_bool else None
                break
            # WAIT FOR THE NEXT POLL TIME (NEXT MINUTE + DELAY, BUT NOT BEFORE THE FIRST BAR IS COMPLETE)
            next_poll_ts = max(now_ny_ts.floor("min") + pd.Timedelta(minutes=1), self.session_row.first_bar_ts + pd.Timedelta(minutes=1)) + self.bar_delay_td
            self.wait_func(max(0.0, (next_poll_ts - self.now_func()).total_seconds()))

    # METHOD: GET THE LIVE BARS
    def get_live_pdf(self):
        """
        Returns:
            pd.DataFrame: timestamp, open, high, low, close, volume of the bars received so far (time order)
        """
        # RETURN THE BARS
        return pd.DataFrame(sorted(self.bar_dict.values()), columns=["timestamp", "open", "high", "low", "close", "volume"])
