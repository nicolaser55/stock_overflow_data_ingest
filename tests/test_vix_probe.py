"""
VIX Probe Tests (simulated IBKR server, no connection, no real data folder)

Run from the workspace root:   python tests/test_vix_probe.py   (or python tests/run_all_tests.py)

What is verified:
    1. Bar parsing: daily "YYYYMMDD" dates are not read as epoch seconds; epoch dates become New York time; the summary counts missing
       minutes and classifies unset / zero / present volumes (ibapi 10 sends 2**127 - 1 for "no volume").
    2. Diagnosis: data returned / permission missing / no data / contract not found / pacing / timeout.
    3. The plan: sample dates mapped to NYSE sessions, request count, extended and control requests.
    4. A full probe run on a simulated server with three scenarios per symbol (index available, permission missing, contract not found)
       with both error layouts (ibapi 10.45 and 9.81): rows, verdict lines, contract details table, a pacing retry.
    5. The script refuses sample dates inside the untouched window.
"""

import os
import sys
import time
import tempfile
import threading
import subprocess
import warnings
from decimal import Decimal

# POINT THE INGEST DATA ROOT TO A TEMPORARY FOLDER (BEFORE ingest IS IMPORTED)
os.environ["SO_INGEST_DATA_PATH"] = tempfile.mkdtemp(prefix="so_probe_test_").replace("\\", "/") + "/"
# IGNORE WARNINGS FROM LIBRARIES
warnings.filterwarnings("ignore")

import pandas as pd
from ibapi.common import BarData
from ibapi.contract import ContractDetails
# IMPORT THE PROBE
from ingest import config
from ingest.index_probe import (ProbeApp, get_probe_bar_ts, get_bar_summary_dict, get_diagnosis_str, get_sample_session_date_list, get_probe_step_list,
                                run_probe_tuple, get_verdict_str_list, get_probe_end_str, PROBE_COL_STR_LIST, PROBE_UNSET_VOLUME_FLOAT)

# FUNCTION: PRINT A PASSED TEST
def passed(name_str_in):
    print(f"✅ {name_str_in}")

# CLASS: PROBE APPLICATION WITH A SIMULATED SERVER
class FakeProbeApp(ProbeApp):
    """
    ProbeApp whose socket methods are replaced. scenario_dict maps a symbol to "ok", "permission", "missing", "pacing_once", "gap" or "utc_only" (error 10314 for every end format except UTC).
    """

    # METHOD: INITIALIZE
    def __init__(self, scenario_dict_in, error_layout_str_in="10.45"):
        # INITIALIZE THE APPLICATION WITHOUT ALERTS
        ProbeApp.__init__(self, alert_in=False)
        # STORE THE SIMULATION SETTINGS
        self.scenario_dict, self.error_layout_str, self.connected_bool = scenario_dict_in, error_layout_str_in, False
        self.pacing_sent_set, self.sent_list = set(), []

    # METHOD: SIMULATE THE CONNECTION
    def connect(self, host, port, clientId):
        # MARK AS CONNECTED AND SEND THE HANDSHAKE
        self.connected_bool = True
        threading.Timer(0.01, self.nextValidId, args=(100,)).start()

    # METHOD: SIMULATE THE READER LOOP, THE CONNECTION STATE, THE SERVER VERSION AND THE DISCONNECTION
    def run(self):
        return
    def isConnected(self):
        return self.connected_bool
    def serverVersion(self):
        return 999
    def disconnect(self):
        self.connected_bool = False
    def cancelHistoricalData(self, reqId):
        return
    def cancelHeadTimeStamp(self, reqId):
        return

    # METHOD: SEND AN ERROR WITH THE CONFIGURED LAYOUT
    def send_error(self, req_id_int_in, code_int_in, text_str_in):
        # SEND THE LAYOUT
        if self.error_layout_str == "10.45":
            self.error(req_id_int_in, int(time.time() * 1000), code_int_in, text_str_in, "")
        else:
            self.error(req_id_int_in, code_int_in, text_str_in)

    # METHOD: ANSWER FROM ANOTHER THREAD
    def reply(self, func, *args):
        threading.Thread(target=lambda: (time.sleep(0.005), func(*args)), daemon=True).start()

    # METHOD: SIMULATE A CONTRACT DETAILS REQUEST
    def reqContractDetails(self, reqId, contract):
        self.sent_list.append(("details", contract.symbol))
        self.reply(self.answer_details, reqId, contract)

    # METHOD: ANSWER A CONTRACT DETAILS REQUEST
    def answer_details(self, req_id_int_in, contract_in):
        # DEFINE THE SCENARIO
        scenario_str = self.scenario_dict.get(contract_in.symbol, "ok")
        # IF THE CONTRACT DOES NOT EXIST
        if scenario_str == "missing":
            self.send_error(req_id_int_in, 200, "No security definition has been found for the request")
            return
        # SEND THE DETAILS
        details = ContractDetails()
        details.contract.conId, details.contract.symbol, details.contract.secType = 13455763, contract_in.symbol, contract_in.secType
        details.contract.exchange, details.contract.currency, details.contract.localSymbol = contract_in.exchange, "USD", contract_in.symbol
        details.longName, details.timeZoneId, details.minTick, details.validExchanges = "CBOE Volatility Index", "US/Central", 0.01, "CBOE"
        details.tradingHours, details.liquidHours = "20260106:0830-1515", "20260106:0830-1515"
        self.contractDetails(req_id_int_in, details)
        self.contractDetailsEnd(req_id_int_in)

    # METHOD: SIMULATE A HEAD TIMESTAMP REQUEST
    def reqHeadTimeStamp(self, reqId, contract, whatToShow, useRTH, formatDate):
        self.sent_list.append(("head", contract.symbol))
        self.reply(self.answer_head, reqId, contract)

    # METHOD: ANSWER A HEAD TIMESTAMP REQUEST
    def answer_head(self, req_id_int_in, contract_in):
        # IF THE SYMBOL HAS NO PERMISSION
        if self.scenario_dict.get(contract_in.symbol, "ok") == "permission":
            self.send_error(req_id_int_in, 162, "Historical Market Data Service error message:No market data permissions for CBOE IND")
            return
        # SEND THE EARLIEST DATA POINT (1990-01-02 14:30 UTC)
        self.headTimestamp(req_id_int_in, str(int(pd.Timestamp("1990-01-02 14:30", tz="UTC").timestamp())))

    # METHOD: SIMULATE A HISTORICAL REQUEST
    def reqHistoricalData(self, reqId, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions):
        self.sent_list.append(("bars", contract.symbol, barSizeSetting, useRTH))
        self.reply(self.answer_bars, reqId, contract, endDateTime, durationStr, barSizeSetting, useRTH)

    # METHOD: ANSWER A HISTORICAL REQUEST
    def answer_bars(self, req_id_int_in, contract_in, end_str_in, duration_str_in, bar_size_str_in, use_rth_int_in):
        # DEFINE THE SCENARIO
        scenario_str = self.scenario_dict.get(contract_in.symbol, "ok")
        # IF THE SYMBOL HAS NO PERMISSION
        if scenario_str == "permission":
            self.send_error(req_id_int_in, 162, "Historical Market Data Service error message:No market data permissions for CBOE IND")
            return
        # IF THE FIRST ANSWER MUST BE A PACING VIOLATION (ONCE PER SYMBOL AND BAR SIZE)
        if scenario_str == "pacing_once" and (contract_in.symbol, bar_size_str_in) not in self.pacing_sent_set:
            self.pacing_sent_set.add((contract_in.symbol, bar_size_str_in))
            self.send_error(req_id_int_in, 162, "Historical Market Data Service error message:API historical data query cancelled: Pacing violation")
            return
        # IF THE SYMBOL ACCEPTS ONLY THE UTC END FORMAT ("YYYYMMDD-HH:MM:SS") OR NO END, ANY OTHER FORMAT IS AN INVALID END DATE
        if scenario_str == "utc_only" and end_str_in != "" and "-" not in end_str_in:
            self.send_error(req_id_int_in, 10314, "End Date/Time: The date, time, or time-zone entered is invalid.\nThe correct format is yyyymmdd hh:mm:ss xx/xxxx")
            return
        # PARSE THE END (NEW YORK): "" = NOW (2026-10-06 16:30), "YYYYMMDD-HH:MM:SS" = UTC, "YYYYMMDD HH:MM:SS [TZ]" = NEW YORK
        if end_str_in == "":
            end_ts = pd.Timestamp("2026-10-06 16:30", tz=config.NY_TZ_STR)
        elif "-" in end_str_in:
            end_ts = pd.to_datetime(end_str_in, format="%Y%m%d-%H:%M:%S").tz_localize("UTC").tz_convert(config.NY_TZ_STR)
        else:
            end_ts = pd.to_datetime(" ".join(end_str_in.split(" ")[:2]), format="%Y%m%d %H:%M:%S").tz_localize(config.NY_TZ_STR)
        # IF THE BARS ARE DAILY
        if bar_size_str_in == "1 day":
            # SEND 20 DAILY BARS ENDING ON THE END DATE (DATES AS YYYYMMDD TEXT, VOLUME UNSET LIKE IBAPI 10 DOES FOR AN INDEX)
            for day_ts in pd.bdate_range(end=end_ts.tz_localize(None).normalize(), periods=20):
                self.historicalData(req_id_int_in, self.get_bar(day_ts.strftime("%Y%m%d"), Decimal(2 ** 127 - 1)))
        else:
            # DEFINE THE WINDOW: THE INDEX CALCULATES 09:30 -> 16:14 (EXTENDED: FROM 03:15), SPY 09:30 -> 15:59
            start_ts = end_ts.normalize() + pd.Timedelta(hours=3, minutes=15 if use_rth_int_in == 0 else 390)
            last_ts = end_ts.normalize() + (pd.Timedelta(hours=15, minutes=59) if "SPY" in contract_in.symbol else pd.Timedelta(hours=16, minutes=14))
            # SEND THE MINUTES (ONE MISSING MINUTE AT 10:00 FOR THE "gap" SYMBOL)
            for minute_ts in pd.date_range(start_ts, last_ts, freq="1min"):
                if scenario_str == "gap" and minute_ts.strftime("%H:%M") == "10:00":
                    continue
                self.historicalData(req_id_int_in, self.get_bar(str(int(minute_ts.timestamp())), Decimal(0)))
        # SEND THE END
        self.historicalDataEnd(req_id_int_in, "", "")

    # METHOD: BUILD A BAR
    def get_bar(self, date_str_in, volume_in):
        bar = BarData()
        bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume = date_str_in, 20.0, 21.0, 19.5, 20.5, volume_in
        return bar

# FUNCTION: BUILD CONNECTED APP
def get_app(scenario_dict_in, layout_str_in="10.45"):
    app = FakeProbeApp(scenario_dict_in, layout_str_in)
    assert app.connect_app(), "handshake failed"
    return app

# TEST: BAR PARSING AND SUMMARY
def test_bar_parsing():
    # A DAILY DATE IS A DATE, NOT AN EPOCH (20250102 AS EPOCH SECONDS WOULD BE IN 1970)
    daily_ts = get_probe_bar_ts("20250102")
    assert daily_ts == pd.Timestamp("2025-01-02") and daily_ts.tzinfo is None
    # AN EPOCH IS CONVERTED TO NEW YORK TIME (2026-07-17 13:30 UTC = 09:30 EDT)
    assert str(get_probe_bar_ts(str(int(pd.Timestamp("2026-07-17 13:30", tz="UTC").timestamp())))) == "2026-07-17 09:30:00-04:00"
    # THE SUMMARY OF 1-MINUTE BARS WITH ONE MISSING MINUTE AND UNSET VOLUMES
    bar_list = [(str(int(pd.Timestamp(f"2026-07-17 {t}", tz="America/New_York").timestamp())), 1, 2, 0.5, 1.5, float(2 ** 127 - 1)) for t in ["09:30", "09:31", "09:33"]]
    summary_dict = get_bar_summary_dict(bar_list, True)
    assert (summary_dict["bars"], summary_dict["first_bar"], summary_dict["last_bar"], summary_dict["missing_minutes"]) == (3, "2026-07-17 09:30", "2026-07-17 09:33", 1)
    assert summary_dict["volume_info"] == "unset" and summary_dict["first_bar_ohlc"] == "1/2/0.5/1.5"
    # ZERO AND PRESENT VOLUMES, AND AN EMPTY ANSWER
    assert get_bar_summary_dict([("20250102", 1, 2, 1, 2, 0)], False)["volume_info"] == "zero_or_negative"
    assert get_bar_summary_dict([("20250102", 1, 2, 1, 2, 5)], False)["volume_info"] == "present"
    assert get_bar_summary_dict([], True)["bars"] == 0 and PROBE_UNSET_VOLUME_FLOAT < 2 ** 127
    passed("bar parsing and summary")

# TEST: DIAGNOSIS
def test_diagnosis():
    assert get_diagnosis_str("complete", None, "", 5) == "data returned"
    assert get_diagnosis_str("complete", None, "", 0) == "completed with nothing returned"
    assert get_diagnosis_str("error", 162, "Historical Market Data Service error message:No market data permissions for CBOE IND", 0) == "market data permission missing"
    assert get_diagnosis_str("error", 354, "Requested market data is not subscribed.", 0) == "market data permission missing"
    assert get_diagnosis_str("no_data", 162, "HMDS query returned no data", 0).startswith("no data for this window")
    assert get_diagnosis_str("error", 200, "No security definition has been found", 0).startswith("contract not found")
    assert get_diagnosis_str("pacing", 162, "Pacing violation", 0).startswith("pacing")
    assert get_diagnosis_str("timeout", None, "", 0).startswith("no answer")
    assert get_diagnosis_str("error", 321, "Error validating request", 0) == "IBKR error (see error_text)"
    passed("diagnosis")

# TEST: PLAN
def test_plan():
    # SAMPLE DATES ARE MAPPED TO SESSIONS (2005-01-01 IS A SATURDAY AND 2005-01-03 THE FIRST SESSION; DUPLICATES REMOVED)
    assert get_sample_session_date_list(["2005-01-01", "2005-01-03", "2008-10-10"]) == ["2005-01-03", "2008-10-10"]
    default_list = get_sample_session_date_list()
    assert len(default_list) == 6 and default_list[0] == "2005-01-04" and default_list[-1] == "2026-05-13"
    # THE PLAN: PER SYMBOL 2 + 2 LATEST + 2 PER DATE + 1 EXTENDED; PLUS 2 CONTROL REQUESTS
    step_list = get_probe_step_list(["VIX", "VIX3M"], default_list)
    assert len(step_list) == 2 * (2 + 2 + 2 * 6 + 1) + 2
    assert len(get_probe_step_list(["VIX"], default_list, control_bool_in=False, extended_bool_in=False)) == 2 + 2 + 2 * 6
    assert [s["end_ts"] for s in step_list if s["test"].endswith("_latest")] == [None] * 4
    # THE CONTRACT IS AN INDEX ON CBOE WITHOUT A PRIMARY EXCHANGE; THE CONTROL IS SPY
    contract = step_list[0]["contract"]
    assert (contract.symbol, contract.secType, contract.exchange, contract.primaryExchange, contract.currency) == ("VIX", "IND", "CBOE", "", "USD")
    assert step_list[-1]["contract"].symbol == "SPY" and step_list[-1]["symbol"] == "SPY (control)"
    # THE EXTENDED REQUEST USES useRTH = 0
    assert [s["use_rth_int"] for s in step_list if s["test"] == "minute_bars_extended"] == [0, 0]
    passed("plan")

# TEST: A FULL RUN FOR BOTH ERROR LAYOUTS
def test_run(layout_str_in):
    # THREE SCENARIOS: VIX AVAILABLE, VIX3M WITHOUT PERMISSION, UNKNOWN INDEX NOT FOUND
    app = get_app({"VIX": "ok", "VIX3M": "permission", "NOPE": "missing"}, layout_str_in)
    date_list = ["2008-10-10", "2026-05-13"]
    step_list = get_probe_step_list(["VIX", "VIX3M", "NOPE"], date_list)
    row_pdf, detail_pdf = run_probe_tuple(app, step_list, timeout_seconds_in=5, pause_seconds_in=0, sleep_func_in=lambda s: None, alert_in=False)
    app.disconnect_app()
    assert list(row_pdf.columns) == PROBE_COL_STR_LIST and len(row_pdf) == len(step_list)
    # VIX: EVERYTHING RETURNED; THE 1-MINUTE BARS RUN 09:30 -> 16:14 (405 BARS), THE INDEX HAS NO VOLUME
    vix_pdf = row_pdf[row_pdf["symbol"] == "VIX"].set_index(["test", "target_date"])
    assert (vix_pdf["diagnosis"] == "data returned").all()
    minute_row = vix_pdf.loc[("minute_bars", "2008-10-10")]
    assert (minute_row["bars"], minute_row["first_bar"], minute_row["last_bar"], minute_row["missing_minutes"], minute_row["volume_info"]) == \
           (405, "2008-10-10 09:30", "2008-10-10 16:14", 0, "zero_or_negative")
    assert vix_pdf.loc[("earliest_data", "")]["first_bar"] == "1990-01-02 09:30"
    daily_row = vix_pdf.loc[("daily_bars", "2008-10-10")]
    assert daily_row["bars"] == 20 and daily_row["last_bar"] == "2008-10-10" and daily_row["volume_info"] == "unset"
    extended_row = vix_pdf.loc[("minute_bars_extended", "2026-05-13")]
    assert extended_row["first_bar"] == "2026-05-13 03:15" and extended_row["bars"] > 405
    # VIX3M: PERMISSION MISSING EVERYWHERE EXCEPT THE CONTRACT DETAILS; NOPE: CONTRACT NOT FOUND
    vix3m_pdf = row_pdf[row_pdf["symbol"] == "VIX3M"]
    assert (vix3m_pdf[vix3m_pdf["test"] != "contract_details"]["diagnosis"] == "market data permission missing").all()
    assert (row_pdf[row_pdf["symbol"] == "NOPE"]["diagnosis"].str.startswith("contract not found")).any()
    # THE CONTROL (SPY 09:30 -> 15:59 = 390 BARS)
    control_pdf = row_pdf[row_pdf["symbol"] == "SPY (control)"].set_index("test")
    assert (control_pdf["diagnosis"] == "data returned").all() and control_pdf.loc["minute_bars", "bars"] == 390
    # THE CONTRACT DETAILS TABLE (VIX AND VIX3M)
    assert sorted(detail_pdf["requested_symbol"]) == ["VIX", "VIX3M"] and (detail_pdf["con_id"] == 13455763).all()
    # THE VERDICT
    verdict_list = get_verdict_str_list(row_pdf)
    assert verdict_list[0].startswith("VIX: contract found; earliest data 1990-01-02 09:30; daily bars on 2/2 sample dates; 1-minute bars on 2/2")
    assert "permission errors: 0" in verdict_list[0] and "permission errors: 8" in verdict_list[1]
    assert verdict_list[2].startswith("NOPE: contract NOT found") and "missing market data permissions" in verdict_list[-1]
    # THE DISTINCT IBKR ERRORS ARE LISTED (8 PERMISSION ERRORS OF VIX3M, 1 CONTRACT ERROR + 14 DEPENDENT OF NOPE)
    error_line_list = [line for line in verdict_list if line.startswith("IBKR error")]
    assert any(line.startswith("IBKR error 162 x8: ") and "No market data permissions" in line for line in error_line_list), error_line_list
    assert any(line.startswith("IBKR error 200 x") for line in error_line_list)
    # THE REGULAR SESSION WINDOW IS ONE DAY, NOT A NUMBER OF SECONDS
    assert {s["duration_str"] for s in step_list if s["test"] == "minute_bars"} == {"1 D"}
    passed(f"full probe run (error layout {layout_str_in})")

# TEST: PACING RETRY, MISSING MINUTES, FAILED CONTROL
def test_pacing_gap_control():
    # A PACING VIOLATION IS RETRIED ONCE AFTER A WAIT; A MISSING MINUTE IS COUNTED
    app = get_app({"VIX": "pacing_once", "VIX3M": "gap"})
    sleep_list = []
    row_pdf, _ = run_probe_tuple(app, get_probe_step_list(["VIX", "VIX3M"], ["2015-08-24"], control_bool_in=False, extended_bool_in=False), timeout_seconds_in=5,
                                 pause_seconds_in=0, pacing_wait_seconds_in=15, sleep_func_in=sleep_list.append, alert_in=False)
    app.disconnect_app()
    assert (row_pdf["diagnosis"].isin(["data returned"])).all(), row_pdf[["symbol", "test", "diagnosis"]]
    assert sleep_list.count(15) == 2
    gap_pdf = row_pdf[(row_pdf["symbol"] == "VIX3M") & (row_pdf["test"] == "minute_bars")]
    assert gap_pdf["missing_minutes"].iloc[0] == 1 and gap_pdf["bars"].iloc[0] == 404
    # IF THE CONTROL FAILS TOO, THE VERDICT BLAMES THE CONNECTION, NOT VIX
    app = get_app({"VIX": "permission", "SPY": "permission"})
    row_pdf, _ = run_probe_tuple(app, get_probe_step_list(["VIX"], ["2015-08-24"]), timeout_seconds_in=5, pause_seconds_in=0, sleep_func_in=lambda s: None, alert_in=False)
    app.disconnect_app()
    assert get_verdict_str_list(row_pdf)[-1].startswith("CONTROL FAILED")
    passed("pacing retry, missing minutes, failed control")

# TEST: END DATE FORMATS (ERROR 10314), LATEST BARS WITHOUT PRICES
def test_end_formats():
    # A SYMBOL THAT ACCEPTS ONLY THE UTC END FORMAT: THE PROBE TRIES THE FORMATS IN ORDER AND NOTES THE ONE THAT WORKED
    app = get_app({"VIX": "utc_only"})
    row_pdf, _ = run_probe_tuple(app, get_probe_step_list(["VIX"], ["2015-08-24"], control_bool_in=False, extended_bool_in=False), timeout_seconds_in=5,
                                 pause_seconds_in=0, sleep_func_in=lambda s: None, alert_in=False)
    app.disconnect_app()
    assert (row_pdf["diagnosis"] == "data returned").all(), row_pdf[["test", "diagnosis", "error_code"]]
    format_dict = row_pdf.set_index(["test", "target_date"])["end_format"].to_dict()
    assert format_dict[("daily_bars", "2015-08-24")] == "utc" and format_dict[("minute_bars", "2015-08-24")] == "utc"
    assert format_dict[("daily_bars_latest", "")] == "now"
    # THE 10314 REQUESTS BEFORE THE UTC FORMAT WERE SENT IN THE EXPECTED ORDER (US/Eastern, America/New_York, UTC)
    daily_request_count = sum(1 for sent in app.sent_list if sent[:3] == ("bars", "VIX", "1 day"))
    assert daily_request_count == 1 + 3, daily_request_count
    # THE VERDICT TELLS THE END FORMAT; THE LATEST BARS KEEP NO PRICES
    assert any(line.startswith("END FORMAT") and "utc" in line for line in get_verdict_str_list(row_pdf))
    assert (row_pdf[row_pdf["test"].str.endswith("_latest")]["first_bar_ohlc"] == "").all()
    assert (row_pdf[row_pdf["test"] == "daily_bars"]["first_bar_ohlc"] != "").all()
    # EVERY FORMAT IS A VALID IBKR STRING FOR THE SAME INSTANT (16:30 NEW YORK IN SUMMER = 20:30 UTC)
    end_ts = pd.Timestamp("2015-08-24 16:30", tz=config.NY_TZ_STR)
    assert [get_probe_end_str(end_ts, v) for v in ["us_eastern", "america_new_york", "utc", "no_timezone", "now"]] == \
           ["20150824 16:30:00 US/Eastern", "20150824 16:30:00 America/New_York", "20150824-20:30:00", "20150824 16:30:00", ""]
    passed("end date formats (10314) and latest bars")

# TEST: THE SCRIPT REFUSES THE UNTOUCHED WINDOW AND LISTS THE PLAN WITHOUT A CONNECTION
def test_script():
    # DEFINE THE SCRIPT AND THE WORKSPACE ROOT
    root_str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script_str = os.path.join(root_str, "scripts", "probe_ibkr_vix.py")
    env_dict = {**os.environ, "PYTHONPATH": root_str, "PYTHONIOENCODING": "utf-8"}
    # A DATE INSIDE THE UNTOUCHED WINDOW IS REFUSED (BEFORE ANY CONNECTION)
    refused = subprocess.run([sys.executable, script_str, "--dates", "2026-06-01"], capture_output=True, text=True, env=env_dict, encoding="utf-8")
    assert refused.returncode != 0 and "untouched window" in (refused.stdout + refused.stderr)
    # --list PLANS WITHOUT CONNECTING
    listed = subprocess.run([sys.executable, script_str, "--list"], capture_output=True, text=True, env=env_dict, encoding="utf-8")
    assert listed.returncode == 0 and "Requests planned: 36" in listed.stdout and "VIX3M" in listed.stdout, listed.stdout + listed.stderr
    passed("script: untouched window refused, --list")

# RUN THE TESTS
if __name__ == "__main__":
    test_bar_parsing()
    test_diagnosis()
    test_plan()
    test_run("10.45")
    test_run("9.81")
    test_pacing_gap_control()
    test_end_formats()
    test_script()
    print("\nAll VIX probe tests passed ✅")
