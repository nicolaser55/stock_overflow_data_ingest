import os
import argparse
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE PROBE AND THE ALIGNMENT CHECK
from ingest.index_probe import ProbeApp, PROBE_EXCHANGE_STR, UNTOUCHED_WINDOW_START_STR, get_sample_session_date_list
from ingest.index_alignment import ALIGN_SYMBOL_STR_LIST, ALIGN_DATE_STR_LIST, run_alignment_tuple, get_alignment_verdict_str_list
from ingest.sessions import get_ny_now_ts

"""
Alignment check of the VIX / VIX3M 1-minute bars against SPY and against their own daily bars (read-only)

Prerequisite: IB Gateway (or TWS) running and logged in, API enabled on the port below.

    python scripts/probe_index_alignment.py                      # VIX and VIX3M, 6 volatile sessions: 30 requests, about 1 minute
    python scripts/probe_index_alignment.py --list               # only list the sessions and the number of requests (no connection)
    python scripts/probe_index_alignment.py --symbols VIX --dates 2015-08-24 2018-02-05
    python scripts/probe_index_alignment.py --port 4002          # paper Gateway

Output: a verdict on screen and three CSV files in probe_output/ (the daily vs minute checks, the lag correlations, the first and last three bars
of every session). Dates from 2026-05-14 on (untouched window of the research) are refused. See ingest/index_alignment.py for the method.
"""

# FUNCTION: PARSE THE COMMAND LINE ARGUMENTS
def get_args():
    """
    Returns:
        argparse.Namespace: Command line arguments
    """
    # CREATE THE PARSER
    parser = argparse.ArgumentParser(description="Check the alignment of index 1-minute bars against SPY (read-only).")
    parser.add_argument("--symbols", nargs="*", default=ALIGN_SYMBOL_STR_LIST, help="index symbols (default: VIX VIX3M)")
    parser.add_argument("--exchange", default=PROBE_EXCHANGE_STR, help="index exchange (default: CBOE)")
    parser.add_argument("--dates", nargs="*", help="sample dates, each mapped to the next NYSE session (default: 6 volatile sessions from 2008 to 2024)")
    parser.add_argument("--list", action="store_true", help="list the sessions and stop (no connection)")
    parser.add_argument("--host", default=config.IBKR_HOST_STR)
    parser.add_argument("--port", type=int, default=config.IBKR_PORT_INT, help="4001 Gateway live, 4002 Gateway paper, 7496 / 7497 TWS")
    parser.add_argument("--client-id", type=int, default=config.IBKR_DOWNLOAD_CLIENT_ID_INT)
    parser.add_argument("--out", default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "probe_output").replace("\\", "/"),
                        help="folder for the CSV files (default: probe_output/ in the workspace)")
    # RETURN THE ARGUMENTS
    return parser.parse_args()

# FUNCTION: MAIN
def main():
    """
    Maps the dates to sessions, connects, runs the check, disconnects, reports.
    """
    # PARSE THE ARGUMENTS
    args = get_args()
    # MAP THE DATES TO SESSIONS AND REFUSE THE UNTOUCHED WINDOW
    session_date_str_list = get_sample_session_date_list(args.dates or ALIGN_DATE_STR_LIST)
    if any(date_str >= UNTOUCHED_WINDOW_START_STR for date_str in session_date_str_list):
        raise SystemExit(f"❌ Dates from {UNTOUCHED_WINDOW_START_STR} on are inside the untouched window of the research; pick earlier dates.")
    # DISPLAY INFORMATION
    request_count_int = len(session_date_str_list) * (1 + 2 * len(args.symbols))
    print(f"Sessions: {session_date_str_list}\nSymbols: {args.symbols}\nRequests: {request_count_int}")
    # IF ONLY A LIST WAS ASKED FOR
    if args.list:
        return
    # CONNECT
    app = ProbeApp()
    if not app.connect_app(args.host, args.port, args.client_id):
        raise SystemExit(1)
    # RUN THE CHECK, THEN ALWAYS DISCONNECT
    try:
        check_pdf, lag_pdf, edge_pdf = run_alignment_tuple(app, args.symbols, session_date_str_list, args.exchange)
    finally:
        app.disconnect_app()
    # SAVE THE RESULTS
    os.makedirs(args.out, exist_ok=True)
    stamp_str = get_ny_now_ts().strftime("%Y%m%d_%H%M%S")
    path_str_list = [f"{args.out}/index_alignment_{stamp_str}_{name_str}.csv" for name_str in ("checks", "lags", "edge_bars")]
    for table_pdf, path_str in zip((check_pdf, lag_pdf, edge_pdf), path_str_list):
        table_pdf.to_csv(path_str, index=False)
    # DISPLAY THE RESULTS
    print("\nDaily vs minute bars")
    print(check_pdf[["symbol", "date", "minute_bars", "first_bar", "last_bar", "open_equals_first", "open_match_bar", "close_equals_last", "close_match_bar",
                     "high_equals_max", "low_equals_min", "note"]].to_string(index=False))
    print("\nVerdict")
    for line_str in get_alignment_verdict_str_list(check_pdf, lag_pdf):
        print(f"  {line_str}")
    print("\nSaved:\n  " + "\n  ".join(path_str_list))

# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
