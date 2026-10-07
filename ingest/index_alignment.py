import time
import numpy as np
import pandas as pd
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE IBKR CLIENT HELPER AND THE PROBE
from ingest.ibkr_client import get_contract
from ingest.index_probe import (get_probe_bar_ts, get_probe_ts_str, PROBE_SEC_TYPE_STR, PROBE_EXCHANGE_STR, PROBE_CURRENCY_STR, PROBE_WINDOW_END_TIME_STR,
                                PROBE_REGULAR_DURATION_STR, PROBE_MINUTE_BAR_SIZE_STR, PROBE_DAILY_BAR_SIZE_STR, PROBE_TIMEOUT_SECONDS,
                                PROBE_PAUSE_SECONDS, PROBE_PACING_WAIT_SECONDS)

"""
Index Alignment Check: are the 1-minute bars of VIX / VIX3M labelled like the 1-minute bars of SPY?

Why: the first probe (docs: claude/VIX_PROBE_RESULTS_2026-10-06.md) found that the index sessions start at 09:31 (SPY: 09:30) and end at
16:14 (SPY: 15:59). SPY bars are labelled by their START time. If an index bar were labelled differently, joining it with SPY at a decision
minute would use a minute of the future (look-ahead) or a stale value. Two independent checks, both read-only:

    1. DAILY vs MINUTE. The daily bar of a session is built from the same prints as its minute bars. If the daily open equals the open of
       the first minute bar, the daily close equals the close of the last minute bar, and the daily high / low equal the maximum / minimum of
       the minute highs / lows, the minute bars cover the whole session and the first / last bar labels tell where the session begins and ends.
       If the daily close equals the close of the bar labelled 16:14, that bar ends at the final 16:15 value (start-labelled, like SPY).
    2. LAG vs SPY. Over volatile sessions, the 1-minute return of the index is correlated with the 1-minute return of SPY (negatively for
       VIX). The correlation is computed for lags of -3 .. +3 minutes: corr(index return at label t, SPY return at label t + lag). With the
       same labelling the strongest correlation is at lag 0.
           lag +k: the index bar labelled t holds the move of the SPY bar labelled t + k. Using it at decision minute t would use
                   information from the future. Treat it as available only from t + k.
           lag -k: the index bar labelled t holds the move of the SPY bar labelled t - k: the index is late (stale). No look-ahead, but
                   it can be a real property of the index (calculated from option quotes) rather than a labelling convention.

Only dates before the untouched window of the research (2026-05-14) are accepted. Nothing is written to the raw or staging folders.
"""

"""
Settings
"""

# DEFINE THE INDEX SYMBOLS, THE SAMPLE DATES (VOLATILE SESSIONS GIVE THE MOST INFORMATIVE CORRELATIONS) AND THE LAGS (minutes)
ALIGN_SYMBOL_STR_LIST = ["VIX", "VIX3M"]
ALIGN_DATE_STR_LIST = ["2008-10-10", "2011-08-08", "2015-08-24", "2018-02-05", "2020-03-16", "2024-08-05"]
ALIGN_LAG_INT_LIST = [-3, -2, -1, 0, 1, 2, 3]
# DEFINE THE PRICE TOLERANCE OF A MATCH (INDEX LEVELS HAVE TWO DECIMALS), THE MINIMUM NUMBER OF RETURN PAIRS OF A CORRELATION AND THE MARGIN
# BY WHICH A LAG MUST BEAT LAG 0 (ABSOLUTE CORRELATION) TO BE REPORTED AS A SHIFT
ALIGN_PRICE_TOLERANCE_FLOAT = 0.005
ALIGN_MIN_PAIR_COUNT_INT = 100
ALIGN_MARGIN_FLOAT = 0.03
# DEFINE THE END DATE FORMAT OF EACH CONTRACT TYPE (THE FIRST PROBE: INDICES NEED UTC, SPY THE US/EASTERN FORMAT)
ALIGN_INDEX_END_VARIANT_STR_LIST = ["utc"]
ALIGN_SPY_END_VARIANT_STR_LIST = ["us_eastern"]
# DEFINE THE DURATION OF A DAILY REQUEST (A WEEK OF DAILY BARS ENDING ON THE SAMPLE DATE)
ALIGN_DAILY_DURATION_STR = "1 W"
# DEFINE THE COLUMNS OF THE CHECK TABLE
ALIGN_CHECK_COL_STR_LIST = ["symbol", "date", "note", "minute_bars", "first_bar", "last_bar", "daily_open", "first_bar_open", "open_equals_first", "open_match_bar",
                            "daily_close", "last_bar_close", "close_equals_last", "close_match_bar", "daily_high", "minute_max_high", "high_equals_max",
                            "daily_low", "minute_min_low", "low_equals_min"]

"""
Bars
"""

# FUNCTION: CONVERT RAW BARS TO A DATAFRAME
def get_bar_pdf(bar_tuple_list_in):
    """
    Args:
        bar_tuple_list_in (list): Raw bars (date field, open, high, low, close, volume) of a probe request

    Returns:
        pd.DataFrame: open, high, low, close indexed by the bar timestamp (New York for 1-minute bars, a naive date for daily bars), sorted
    """
    # IF THERE ARE NO BARS
    if not bar_tuple_list_in:
        # RETURN AN EMPTY DATAFRAME
        return pd.DataFrame(columns=["open", "high", "low", "close"], dtype=float)
    # CREATE THE DATAFRAME
    bar_pdf = pd.DataFrame([bar_tuple[1:5] for bar_tuple in bar_tuple_list_in], columns=["open", "high", "low", "close"],
                           index=pd.Index([get_probe_bar_ts(bar_tuple[0]) for bar_tuple in bar_tuple_list_in]))
    # RETURN THE SORTED DATAFRAME WITHOUT DUPLICATED TIMESTAMPS
    return bar_pdf[~bar_pdf.index.duplicated(keep="first")].sort_index()

# FUNCTION: GET THE 1-MINUTE LOG RETURNS OF A BAR DATAFRAME
def get_return_series(minute_pdf_in):
    """
    Log return of the close from the previous bar, only where the previous bar is exactly one minute earlier (no return across a gap).

    Args:
        minute_pdf_in (pd.DataFrame): 1-minute bars (see get_bar_pdf)

    Returns:
        pd.Series: Log returns indexed by the bar timestamp
    """
    # IF THERE ARE NO BARS
    if minute_pdf_in.empty:
        # RETURN AN EMPTY SERIES
        return pd.Series(dtype=float)
    # COMPUTE THE RETURNS
    return_series = np.log(minute_pdf_in["close"]).diff()
    # KEEP THE RETURNS WITH A PREVIOUS BAR ONE MINUTE EARLIER
    consecutive_mask = minute_pdf_in.index.to_series().diff() == pd.Timedelta(minutes=1)
    # RETURN THE RETURNS
    return return_series[consecutive_mask].replace([np.inf, -np.inf], np.nan).dropna()

"""
Check 1: Daily Bar vs Minute Bars
"""

# FUNCTION: COMPARE THE DAILY BAR WITH THE MINUTE BARS OF A SESSION
def get_session_check_dict(symbol_str_in, date_str_in, daily_pdf_in, minute_pdf_in, tolerance_float_in=ALIGN_PRICE_TOLERANCE_FLOAT):
    """
    Args:
        symbol_str_in (str): Index symbol
        date_str_in (str): Session date "YYYY-MM-DD"
        daily_pdf_in (pd.DataFrame): Daily bars (get_bar_pdf), indexed by naive dates
        minute_pdf_in (pd.DataFrame): 1-minute bars of the session (get_bar_pdf)
        tolerance_float_in (float): Maximum price difference of a match

    Returns:
        dict: One row of the check table (ALIGN_CHECK_COL_STR_LIST); open_match_bar / close_match_bar = the first bar whose open / the last bar whose
              close equals the daily open / close
    """
    # DEFINE THE EMPTY ROW
    row_dict = {col_str: "" for col_str in ALIGN_CHECK_COL_STR_LIST}
    row_dict.update(symbol=symbol_str_in, date=date_str_in, minute_bars=len(minute_pdf_in))
    # FIND THE DAILY BAR OF THE SESSION
    daily_ts = pd.Timestamp(date_str_in)
    # IF THE DAILY OR THE MINUTE BARS ARE MISSING
    if daily_ts not in daily_pdf_in.index or minute_pdf_in.empty:
        # RETURN THE ROW WITH A NOTE
        row_dict["note"] = ("no daily bar" if daily_ts not in daily_pdf_in.index else "") + (" no minute bars" if minute_pdf_in.empty else "")
        return row_dict
    # COLLECT THE DAILY BAR AND THE MINUTE BAR FACTS
    daily_bar = daily_pdf_in.loc[daily_ts]
    first_bar, last_bar = minute_pdf_in.iloc[0], minute_pdf_in.iloc[-1]
    max_high_float, min_low_float = float(minute_pdf_in["high"].max()), float(minute_pdf_in["low"].min())
    # FIND THE BARS THAT CARRY THE DAILY OPEN AND CLOSE
    open_match_index = minute_pdf_in.index[(minute_pdf_in["open"] - daily_bar["open"]).abs() <= tolerance_float_in]
    close_match_index = minute_pdf_in.index[(minute_pdf_in["close"] - daily_bar["close"]).abs() <= tolerance_float_in]
    # STORE THE FACTS
    row_dict.update(first_bar=get_probe_ts_str(minute_pdf_in.index[0]), last_bar=get_probe_ts_str(minute_pdf_in.index[-1]),
                    daily_open=float(daily_bar["open"]), first_bar_open=float(first_bar["open"]),
                    open_equals_first=bool(abs(daily_bar["open"] - first_bar["open"]) <= tolerance_float_in),
                    open_match_bar=get_probe_ts_str(open_match_index[0]) if len(open_match_index) else "",
                    daily_close=float(daily_bar["close"]), last_bar_close=float(last_bar["close"]),
                    close_equals_last=bool(abs(daily_bar["close"] - last_bar["close"]) <= tolerance_float_in),
                    close_match_bar=get_probe_ts_str(close_match_index[-1]) if len(close_match_index) else "",
                    daily_high=float(daily_bar["high"]), minute_max_high=max_high_float,
                    high_equals_max=bool(abs(daily_bar["high"] - max_high_float) <= tolerance_float_in),
                    daily_low=float(daily_bar["low"]), minute_min_low=min_low_float,
                    low_equals_min=bool(abs(daily_bar["low"] - min_low_float) <= tolerance_float_in))
    # RETURN THE ROW
    return row_dict

"""
Check 2: Lag vs SPY
"""

# FUNCTION: PAIR THE RETURNS OF AN INDEX WITH THE RETURNS OF SPY AT A LAG
def get_lag_pair_pdf(index_return_series_in, spy_return_series_in, lag_int_in):
    """
    Args:
        index_return_series_in (pd.Series): Index returns by label t
        spy_return_series_in (pd.Series): SPY returns by label t
        lag_int_in (int): Lag in minutes

    Returns:
        pd.DataFrame: index_ret (index at t) and spy_ret (SPY at t + lag), for the labels present in both
    """
    # SHIFT THE SPY LABELS SO THAT THE SPY RETURN OF t + lag SITS AT t
    shifted_series = spy_return_series_in.copy()
    shifted_series.index = shifted_series.index - pd.Timedelta(minutes=lag_int_in)
    # JOIN THE SERIES
    return pd.concat([index_return_series_in.rename("index_ret"), shifted_series.rename("spy_ret")], axis=1, join="inner").dropna()

# FUNCTION: GET THE LAG CORRELATIONS OF A SYMBOL OVER SEVERAL SESSIONS
def get_lag_corr_pdf(symbol_str_in, return_series_list_in, lag_int_list_in=ALIGN_LAG_INT_LIST):
    """
    Args:
        symbol_str_in (str): Index symbol
        return_series_list_in (list): One tuple (index returns, SPY returns) per session
        lag_int_list_in (list): Lags in minutes

    Returns:
        pd.DataFrame: symbol, lag, pairs, corr (pooled over the sessions; NaN with too few pairs)
    """
    # DEFINE THE ROWS
    row_dict_list = []
    # ITERATE OVER THE LAGS
    for lag_int in lag_int_list_in:
        # POOL THE PAIRS OF EVERY SESSION
        pair_pdf = pd.concat([get_lag_pair_pdf(index_series, spy_series, lag_int) for index_series, spy_series in return_series_list_in]) \
                   if return_series_list_in else pd.DataFrame(columns=["index_ret", "spy_ret"])
        # STORE THE CORRELATION
        corr_float = float(pair_pdf["index_ret"].corr(pair_pdf["spy_ret"])) if len(pair_pdf) >= ALIGN_MIN_PAIR_COUNT_INT else np.nan
        row_dict_list.append({"symbol": symbol_str_in, "lag": lag_int, "pairs": len(pair_pdf), "corr": corr_float})
    # RETURN THE TABLE
    return pd.DataFrame(row_dict_list, columns=["symbol", "lag", "pairs", "corr"])

"""
Run
"""

# FUNCTION: REQUEST BARS FOR ONE SESSION WITH ONE PACING RETRY
def request_session_bars_pdf(app_in, symbol_str_in, date_str_in, daily_bool_in, exchange_str_in=PROBE_EXCHANGE_STR, timeout_seconds_in=PROBE_TIMEOUT_SECONDS,
                             pacing_wait_seconds_in=PROBE_PACING_WAIT_SECONDS, sleep_func_in=None):
    """
    Args:
        app_in (ProbeApp): Connected application
        symbol_str_in (str): "SPY" or an index symbol
        date_str_in (str): Session date
        daily_bool_in (bool): True for the daily bars of the week ending on the date, False for the 1-minute bars of the session
        exchange_str_in (str): Index exchange
        timeout_seconds_in (float): Maximum wait
        pacing_wait_seconds_in (float): Wait before the retry after a pacing violation
        sleep_func_in (callable): Sleep function

    Returns:
        tuple: (bar DataFrame, request result dict)
    """
    # DEFINE THE CONTRACT AND THE END DATE FORMAT (SPY: THE EXISTING DOWNLOADER'S; INDEX: UTC)
    is_spy_bool = symbol_str_in == config.CONTRACT_SYMBOL_STR
    contract = get_contract() if is_spy_bool else get_contract(symbol_str_in, PROBE_SEC_TYPE_STR, PROBE_CURRENCY_STR, exchange_str_in, "")
    end_ts = pd.Timestamp(f"{date_str_in} {PROBE_WINDOW_END_TIME_STR}", tz=config.NY_TZ_STR)
    # SEND THE REQUEST (A PACING VIOLATION IS RETRIED ONCE)
    for attempt_int in range(2):
        # REQUEST THE BARS
        result_dict = app_in.request_probe_bars_dict(contract, end_ts, ALIGN_DAILY_DURATION_STR if daily_bool_in else PROBE_REGULAR_DURATION_STR,
                                                     PROBE_DAILY_BAR_SIZE_STR if daily_bool_in else PROBE_MINUTE_BAR_SIZE_STR, 1,
                                                     timeout_seconds_in=timeout_seconds_in, label_str_in=f"{symbol_str_in} {date_str_in}",
                                                     end_variant_list_in=ALIGN_SPY_END_VARIANT_STR_LIST if is_spy_bool else ALIGN_INDEX_END_VARIANT_STR_LIST)
        # IF THE ANSWER IS NOT A PACING VIOLATION
        if result_dict["status_str"] != "pacing":
            # STOP
            break
        # WAIT BEFORE THE RETRY
        sleep_func_in(pacing_wait_seconds_in) if sleep_func_in else None
    # RETURN THE BARS AND THE RESULT
    return get_bar_pdf(result_dict["bar_tuple_list"]), result_dict

# FUNCTION: RUN THE ALIGNMENT CHECK
def run_alignment_tuple(app_in, symbol_str_list_in, date_str_list_in, exchange_str_in=PROBE_EXCHANGE_STR, timeout_seconds_in=PROBE_TIMEOUT_SECONDS,
                        pause_seconds_in=PROBE_PAUSE_SECONDS, pacing_wait_seconds_in=PROBE_PACING_WAIT_SECONDS, sleep_func_in=None, alert_in=True):
    """
    Per session: the SPY 1-minute bars once, then for each index the daily bars and the 1-minute bars.

    Args:
        app_in (ProbeApp): Connected application
        symbol_str_list_in (list): Index symbols
        date_str_list_in (list): Session dates
        exchange_str_in (str): Index exchange
        timeout_seconds_in (float): Maximum wait per request
        pause_seconds_in (float): Pause between two requests
        pacing_wait_seconds_in (float): Wait before the retry after a pacing violation
        sleep_func_in (callable | None): Sleep function (default time.sleep; replaced in tests)
        alert_in (bool): Display one line per request

    Returns:
        tuple: (check DataFrame, lag correlation DataFrame, edge bar DataFrame with the first and last three bars of every session)
    """
    # DEFINE THE SLEEP FUNCTION
    sleep_func = sleep_func_in or time.sleep
    # DEFINE THE RESULTS
    check_dict_list, edge_pdf_list, return_dict = [], [], {symbol_str: [] for symbol_str in symbol_str_list_in}
    # ITERATE OVER THE SESSIONS
    for date_str in date_str_list_in:
        # REQUEST THE SPY MINUTE BARS
        spy_pdf, spy_result_dict = request_session_bars_pdf(app_in, config.CONTRACT_SYMBOL_STR, date_str, False, exchange_str_in, timeout_seconds_in, pacing_wait_seconds_in, sleep_func)
        print(f"  SPY    {date_str} 1-minute: {spy_result_dict['status_str']} ({len(spy_pdf)} bars)") if alert_in else None
        sleep_func(pause_seconds_in)
        # STORE THE EDGE BARS OF SPY (ONCE PER SESSION)
        edge_pdf = pd.concat([spy_pdf.head(3), spy_pdf.tail(3)])
        edge_pdf_list.append(edge_pdf.assign(symbol=config.CONTRACT_SYMBOL_STR, date=date_str, label=[get_probe_ts_str(ts) for ts in edge_pdf.index]))
        # ITERATE OVER THE INDICES
        for symbol_str in symbol_str_list_in:
            # REQUEST THE DAILY AND THE MINUTE BARS
            daily_pdf, daily_result_dict = request_session_bars_pdf(app_in, symbol_str, date_str, True, exchange_str_in, timeout_seconds_in, pacing_wait_seconds_in, sleep_func)
            sleep_func(pause_seconds_in)
            minute_pdf, minute_result_dict = request_session_bars_pdf(app_in, symbol_str, date_str, False, exchange_str_in, timeout_seconds_in, pacing_wait_seconds_in, sleep_func)
            sleep_func(pause_seconds_in)
            # COMPARE THE DAILY BAR WITH THE MINUTE BARS
            row_dict = get_session_check_dict(symbol_str, date_str, daily_pdf, minute_pdf)
            # ADD THE IBKR ERROR TEXT OF A FAILED REQUEST TO THE NOTE
            for result_dict in (daily_result_dict, minute_result_dict):
                row_dict["note"] = (row_dict["note"] + f" [IBKR {result_dict['error_code_int']}: {result_dict['error_str'][:80]}]").strip() if result_dict["error_str"] else row_dict["note"]
            check_dict_list.append(row_dict)
            print(f"  {symbol_str:<6} {date_str} daily {daily_result_dict['status_str']} ({len(daily_pdf)} bars), 1-minute {minute_result_dict['status_str']} "
                  f"({len(minute_pdf)} bars) {row_dict['note']}") if alert_in else None
            # STORE THE RETURNS OF THE SESSION (FOR THE LAG CORRELATION) AND THE EDGE BARS OF THE INDEX
            if len(minute_pdf) and len(spy_pdf):
                return_dict[symbol_str].append((get_return_series(minute_pdf), get_return_series(spy_pdf)))
            edge_pdf = pd.concat([minute_pdf.head(3), minute_pdf.tail(3)])
            edge_pdf_list.append(edge_pdf.assign(symbol=symbol_str, date=date_str, label=[get_probe_ts_str(ts) for ts in edge_pdf.index]))
    # BUILD THE TABLES
    check_pdf = pd.DataFrame(check_dict_list, columns=ALIGN_CHECK_COL_STR_LIST)
    lag_pdf = pd.concat([get_lag_corr_pdf(symbol_str, return_dict[symbol_str]) for symbol_str in symbol_str_list_in], ignore_index=True)
    edge_pdf = pd.concat(edge_pdf_list, ignore_index=True)[["symbol", "date", "label", "open", "high", "low", "close"]] if edge_pdf_list else pd.DataFrame()
    # RETURN THE TABLES
    return check_pdf, lag_pdf, edge_pdf

"""
Verdict
"""

# FUNCTION: SUMMARIZE THE ALIGNMENT RESULTS IN PLAIN WORDS
def get_alignment_verdict_str_list(check_pdf_in, lag_pdf_in):
    """
    Args:
        check_pdf_in (pd.DataFrame): Check table of run_alignment_tuple
        lag_pdf_in (pd.DataFrame): Lag correlation table

    Returns:
        list: Lines
    """
    # DEFINE THE LINES
    line_str_list = []
    # ITERATE OVER THE SYMBOLS
    for symbol_str in check_pdf_in["symbol"].unique():
        # COLLECT THE SESSIONS WITH BOTH DAILY AND MINUTE BARS
        symbol_pdf = check_pdf_in[(check_pdf_in["symbol"] == symbol_str) & (check_pdf_in["first_bar"] != "")]
        session_count_int = len(symbol_pdf)
        # IF THERE IS NO SESSION
        if session_count_int == 0:
            # STORE THE LINE AND CONTINUE
            line_str_list.append(f"{symbol_str}: no session with both daily and 1-minute bars (see the note column)")
            continue
        # STORE THE DAILY vs MINUTE FACTS
        line_str_list.append(f"{symbol_str} daily vs minute ({session_count_int} sessions): open = first bar's open on {int(symbol_pdf['open_equals_first'].sum())}, "
                             f"close = last bar's close on {int(symbol_pdf['close_equals_last'].sum())}, high = max of minute highs on {int(symbol_pdf['high_equals_max'].sum())}, "
                             f"low = min of minute lows on {int(symbol_pdf['low_equals_min'].sum())}")
        # STORE WHERE THE DAILY CLOSE WAS FOUND
        line_str_list.append(f"{symbol_str}   last bar labels {sorted(set(symbol_pdf['last_bar'].str[-5:]))}; bar carrying the daily close {sorted(set(symbol_pdf['close_match_bar'].str[-5:]))}; "
                             f"first bar labels {sorted(set(symbol_pdf['first_bar'].str[-5:]))}; bar carrying the daily open {sorted(set(symbol_pdf['open_match_bar'].str[-5:]))}")
        # COLLECT THE LAG CORRELATIONS
        lag_pdf = lag_pdf_in[(lag_pdf_in["symbol"] == symbol_str) & lag_pdf_in["corr"].notna()]
        # IF THERE ARE NO CORRELATIONS
        if lag_pdf.empty:
            # STORE THE LINE AND CONTINUE
            line_str_list.append(f"{symbol_str} lag vs SPY: too few return pairs (< {ALIGN_MIN_PAIR_COUNT_INT})")
            continue
        # FIND THE STRONGEST LAG AND THE CORRELATION AT LAG 0
        best_row = lag_pdf.loc[lag_pdf["corr"].abs().idxmax()]
        zero_corr_float = float(lag_pdf[lag_pdf["lag"] == 0]["corr"].iloc[0]) if (lag_pdf["lag"] == 0).any() else float("nan")
        best_lag_int = int(best_row["lag"])
        # STORE THE CORRELATIONS
        line_str_list.append(f"{symbol_str} lag vs SPY (pooled, {int(best_row['pairs'])} pairs per lag): " +
                             ", ".join(f"{int(lag_row['lag']):+d}: {lag_row['corr']:+.3f}" for _, lag_row in lag_pdf.iterrows()))
        # CLASSIFY THE RESULT
        if best_lag_int == 0 or abs(best_row["corr"]) - abs(zero_corr_float) < ALIGN_MARGIN_FLOAT:
            line_str_list.append(f"{symbol_str} VERDICT: no minute shift against SPY detectable (strongest at lag {best_lag_int:+d}, corr {best_row['corr']:+.3f}; "
                                 f"lag 0: {zero_corr_float:+.3f}).")
        elif best_lag_int > 0:
            line_str_list.append(f"{symbol_str} VERDICT: SHIFT of +{best_lag_int} min. The index bar labelled t holds the move of the SPY bar labelled t+{best_lag_int}: using it at "
                                 f"decision minute t would use future information. Treat the bar as available only from t+{best_lag_int}.")
        else:
            line_str_list.append(f"{symbol_str} VERDICT: the index is LATE by {-best_lag_int} min (its bar t holds the move of SPY bar t{best_lag_int}). No look-ahead; the value is stale "
                                 f"(a real property of the index or a label convention: this check cannot tell which).")
    # RETURN THE LINES
    return line_str_list
