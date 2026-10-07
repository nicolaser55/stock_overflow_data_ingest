"""
Index Alignment Tests (simulated IBKR server, no connection)

Run from the workspace root:   python tests/test_index_alignment.py

What is verified:
    1. Daily vs minute check: matches, mismatches, the bar that carries the daily open / close, missing bars.
    2. Returns: no return across a gap.
    3. Lag correlation: a simulated index whose bars are aligned with SPY (best lag 0), shifted +1 minute (best lag +1, "future information"
       verdict) and delayed by 1 minute (best lag -1, "LATE" verdict, no look-ahead).
    4. The run: request counts, the end date format per contract (SPY: US/Eastern, index: UTC, no wasted retries), the untouched window refusal.
"""

import os
import sys
import subprocess

# ADD THE TESTS FOLDER TO THE PATH AND IMPORT THE SIMULATED PROBE SERVER (THIS ALSO POINTS THE DATA ROOT TO A TEMPORARY FOLDER)
TESTS_PATH_STR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_PATH_STR)
from test_vix_probe import FakeProbeApp, passed

import numpy as np
import pandas as pd
from ibapi.common import BarData
# IMPORT THE ALIGNMENT CHECK
from ingest import config
from ingest.index_alignment import (get_bar_pdf, get_return_series, get_session_check_dict, get_lag_corr_pdf, run_alignment_tuple,
                                    get_alignment_verdict_str_list, ALIGN_CHECK_COL_STR_LIST)

# CLASS: SIMULATED SERVER WITH SPY-LIKE AND INDEX-LIKE MINUTE BARS
class FakeAlignApp(FakeProbeApp):
    """
    SPY: random 1-minute returns, bars 09:30 -> 15:59 labelled by start. Index: bars 09:31 -> 16:14 whose return at label t is -20 times the SPY return
    at label t + shift (shift +1: the index holds the future of its label; -1: it is late). Daily bars are built from the minute bars. Like the real
    indices, the simulated indices reject every end date format except UTC (error 10314).
    """

    # METHOD: INITIALIZE
    def __init__(self, shift_dict_in, layout_str_in="10.45"):
        FakeProbeApp.__init__(self, {}, layout_str_in)
        self.shift_dict = shift_dict_in

    # METHOD: BUILD THE MINUTE BARS OF A SYMBOL AND A DATE
    def get_minute_pdf(self, symbol_str_in, date_str_in):
        # DEFINE THE SPY RETURNS OF THE DATE
        rng = np.random.default_rng(int(date_str_in.replace("-", "")))
        spy_index = pd.date_range(f"{date_str_in} 09:30", f"{date_str_in} 15:59", freq="1min", tz=config.NY_TZ_STR)
        spy_ret_series = pd.Series(rng.normal(0, 0.0005, len(spy_index)), index=spy_index)
        # IF THE SYMBOL IS SPY
        if symbol_str_in == "SPY":
            index, ret_arr = spy_index, spy_ret_series.to_numpy()
        # IF THE SYMBOL IS AN INDEX
        else:
            index = pd.date_range(f"{date_str_in} 09:31", f"{date_str_in} 16:14", freq="1min", tz=config.NY_TZ_STR)
            ret_arr = -20 * spy_ret_series.reindex(index + pd.Timedelta(minutes=self.shift_dict.get(symbol_str_in, 0))).fillna(0).to_numpy() + rng.normal(0, 0.002, len(index))
        # BUILD THE BARS
        close_arr = 20 * np.exp(np.cumsum(ret_arr))
        open_arr = np.concatenate([[20.0], close_arr[:-1]])
        return pd.DataFrame({"open": open_arr, "high": np.maximum(open_arr, close_arr) + 0.01, "low": np.minimum(open_arr, close_arr) - 0.01, "close": close_arr}, index=index)

    # METHOD: BUILD A BAR
    def make_bar(self, date_str_in, row_in):
        bar = BarData()
        bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume = date_str_in, float(row_in["open"]), float(row_in["high"]), float(row_in["low"]), float(row_in["close"]), 0
        return bar

    # METHOD: ANSWER A HISTORICAL REQUEST
    def answer_bars(self, req_id_int_in, contract_in, end_str_in, duration_str_in, bar_size_str_in, use_rth_int_in):
        # IF AN INDEX IS ASKED WITH ANOTHER FORMAT THAN UTC
        if contract_in.secType == "IND" and "-" not in end_str_in:
            self.send_error(req_id_int_in, 10314, "End Date/Time: The date, time, or time-zone entered is invalid.")
            return
        # PARSE THE END (UTC "YYYYMMDD-HH:MM:SS" OR NEW YORK "YYYYMMDD HH:MM:SS US/Eastern")
        if "-" in end_str_in:
            end_ts = pd.to_datetime(end_str_in, format="%Y%m%d-%H:%M:%S").tz_localize("UTC").tz_convert(config.NY_TZ_STR)
        else:
            end_ts = pd.to_datetime(" ".join(end_str_in.split(" ")[:2]), format="%Y%m%d %H:%M:%S").tz_localize(config.NY_TZ_STR)
        date_str = str(end_ts.date())
        minute_pdf = self.get_minute_pdf(contract_in.symbol, date_str)
        # IF THE BARS ARE DAILY: FOUR DUMMY DAYS AND THE SESSION'S BAR BUILT FROM ITS MINUTE BARS
        if bar_size_str_in == "1 day":
            for day_ts in pd.bdate_range(end=pd.Timestamp(date_str) - pd.Timedelta(days=1), periods=4):
                self.historicalData(req_id_int_in, self.make_bar(day_ts.strftime("%Y%m%d"), {"open": 19, "high": 21, "low": 18, "close": 20}))
            daily_row = {"open": minute_pdf["open"].iloc[0], "high": minute_pdf["high"].max(), "low": minute_pdf["low"].min(), "close": minute_pdf["close"].iloc[-1]}
            self.historicalData(req_id_int_in, self.make_bar(date_str.replace("-", ""), daily_row))
        # IF THE BARS ARE 1-MINUTE BARS (EPOCH LABELS)
        else:
            for ts, row in minute_pdf.iterrows():
                self.historicalData(req_id_int_in, self.make_bar(str(int(ts.timestamp())), row))
        # SEND THE END
        self.historicalDataEnd(req_id_int_in, "", "")

# FUNCTION: RUN THE CHECK ON A SIMULATED SERVER
def run_fake(shift_dict_in, symbol_str_list_in, date_str_list_in, layout_str_in="10.45"):
    app = FakeAlignApp(shift_dict_in, layout_str_in)
    assert app.connect_app()
    result_tuple = run_alignment_tuple(app, symbol_str_list_in, date_str_list_in, timeout_seconds_in=5, pause_seconds_in=0, pacing_wait_seconds_in=0,
                                       sleep_func_in=lambda s: None, alert_in=False)
    app.disconnect_app()
    return app, result_tuple

# TEST: DAILY vs MINUTE
def test_session_check():
    # THREE MINUTE BARS AND A DAILY BAR THAT MATCHES THEM
    minute_pdf = get_bar_pdf([(str(int(pd.Timestamp(f"2015-08-24 {t}", tz="America/New_York").timestamp())), o, h, l, c, 0)
                              for t, o, h, l, c in [("09:31", 20.0, 21.0, 19.0, 20.5), ("09:32", 20.5, 22.0, 20.0, 21.0), ("16:14", 21.0, 21.5, 20.8, 21.2)]])
    daily_pdf = get_bar_pdf([("20150824", 20.0, 22.0, 19.0, 21.2, 0)])
    row_dict = get_session_check_dict("VIX", "2015-08-24", daily_pdf, minute_pdf)
    assert list(row_dict) == ALIGN_CHECK_COL_STR_LIST
    assert row_dict["open_equals_first"] and row_dict["close_equals_last"] and row_dict["high_equals_max"] and row_dict["low_equals_min"]
    assert (row_dict["first_bar"], row_dict["last_bar"], row_dict["close_match_bar"], row_dict["open_match_bar"]) == \
           ("2015-08-24 09:31", "2015-08-24 16:14", "2015-08-24 16:14", "2015-08-24 09:31")
    # A DAILY OPEN THAT IS THE OPEN OF THE SECOND BAR AND A DAILY CLOSE THAT IS THE CLOSE OF THE FIRST BAR
    row_dict = get_session_check_dict("VIX", "2015-08-24", get_bar_pdf([("20150824", 20.5, 22.0, 19.0, 20.5, 0)]), minute_pdf)
    assert not row_dict["open_equals_first"] and row_dict["open_match_bar"] == "2015-08-24 09:32"
    assert not row_dict["close_equals_last"] and row_dict["close_match_bar"] == "2015-08-24 09:31"
    # MISSING DAILY BAR / MISSING MINUTE BARS
    assert get_session_check_dict("VIX", "2015-08-25", daily_pdf, minute_pdf)["note"].strip() == "no daily bar"
    assert get_session_check_dict("VIX", "2015-08-24", daily_pdf, get_bar_pdf([]))["note"].strip() == "no minute bars"
    # NO RETURN ACROSS A GAP (09:31, 09:32, then 09:40)
    gap_pdf = get_bar_pdf([(str(int(pd.Timestamp(f"2015-08-24 {t}", tz="America/New_York").timestamp())), 1, 1, 1, c, 0) for t, c in [("09:31", 20.0), ("09:32", 20.2), ("09:40", 25.0)]])
    assert len(get_return_series(gap_pdf)) == 1
    passed("daily vs minute check, returns without gaps")

# TEST: LAG DETECTION
def test_lags():
    date_list = ["2015-08-24", "2018-02-05", "2020-03-16"]
    # ALIGNED (VIX), SHIFTED +1 (VIX3M)
    app, (check_pdf, lag_pdf, edge_pdf) = run_fake({"VIX": 0, "VIX3M": 1}, ["VIX", "VIX3M"], date_list)
    best_dict = {symbol_str: int(lag_pdf[lag_pdf["symbol"] == symbol_str].set_index("lag")["corr"].abs().idxmax()) for symbol_str in ["VIX", "VIX3M"]}
    assert best_dict == {"VIX": 0, "VIX3M": 1}, lag_pdf
    verdict_list = get_alignment_verdict_str_list(check_pdf, lag_pdf)
    assert any(line.startswith("VIX VERDICT: no minute shift") for line in verdict_list), verdict_list
    assert any(line.startswith("VIX3M VERDICT: SHIFT of +1 min") and "future information" in line for line in verdict_list), verdict_list
    # DAILY vs MINUTE: THE SIMULATED DAILY BARS ARE BUILT FROM THE MINUTE BARS, SO EVERYTHING MATCHES
    assert check_pdf[["open_equals_first", "close_equals_last", "high_equals_max", "low_equals_min"]].all().all()
    assert set(check_pdf["first_bar"].str[-5:]) == {"09:31"} and set(check_pdf["last_bar"].str[-5:]) == {"16:14"} and (check_pdf["minute_bars"] == 404).all()
    # THE REQUESTS: PER SESSION 1 SPY + 2 PER INDEX = 5; EACH SENT ONCE (SPY IN THE US/EASTERN FORMAT, THE INDICES IN UTC: NO WASTED RETRIES)
    assert len(app.sent_list) == 5 * len(date_list), len(app.sent_list)
    assert len(edge_pdf) == len(date_list) * 3 * 6
    # DELAYED INDEX (-1) WITH THE 9.81 ERROR LAYOUT
    _, (check_pdf, lag_pdf, _) = run_fake({"VIX": -1}, ["VIX"], date_list, "9.81")
    verdict_list = get_alignment_verdict_str_list(check_pdf, lag_pdf)
    assert any(line.startswith("VIX VERDICT: the index is LATE by 1 min") and "No look-ahead" in line for line in verdict_list), verdict_list
    # TOO FEW PAIRS: NO CORRELATION
    assert get_lag_corr_pdf("VIX", [])["corr"].isna().all()
    passed("lag detection (aligned, +1 shift, -1 delay), request counts")

# TEST: THE SCRIPT
def test_script():
    root_str = os.path.dirname(TESTS_PATH_STR)
    script_str = os.path.join(root_str, "scripts", "probe_index_alignment.py")
    env_dict = {**os.environ, "PYTHONPATH": root_str, "PYTHONIOENCODING": "utf-8"}
    refused = subprocess.run([sys.executable, script_str, "--dates", "2026-06-01"], capture_output=True, text=True, env=env_dict, encoding="utf-8")
    assert refused.returncode != 0 and "untouched window" in (refused.stdout + refused.stderr)
    listed = subprocess.run([sys.executable, script_str, "--list"], capture_output=True, text=True, env=env_dict, encoding="utf-8")
    assert listed.returncode == 0 and "Requests: 30" in listed.stdout, listed.stdout + listed.stderr
    passed("script: untouched window refused, --list (30 requests)")

# RUN THE TESTS
if __name__ == "__main__":
    test_session_check()
    test_lags()
    test_script()
    print("\nAll index alignment tests passed ✅")
