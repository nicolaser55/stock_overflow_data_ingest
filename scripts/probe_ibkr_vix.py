import os
import argparse
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE PROBE
from ingest.index_probe import (ProbeApp, PROBE_SYMBOL_STR_LIST, PROBE_EXCHANGE_STR, UNTOUCHED_WINDOW_START_STR, get_sample_session_date_list,
                                get_probe_step_list, run_probe_tuple, get_verdict_str_list)
from ingest.sessions import get_ny_now_ts

"""
Probe IBKR for VIX / VIX3M (read-only: nothing is written to the raw or staging folders)

Prerequisite: IB Gateway (or TWS) running and logged in, API enabled on the port below.

    python scripts/probe_ibkr_vix.py                      # VIX and VIX3M, 6 sample sessions, SPY control: 36 requests, 1-2 minutes
    python scripts/probe_ibkr_vix.py --list               # only list the requests (no connection)
    python scripts/probe_ibkr_vix.py --symbols VIX        # one index
    python scripts/probe_ibkr_vix.py --dates 2010-05-06 2012-08-01
    python scripts/probe_ibkr_vix.py --port 4002          # paper Gateway

Output: a table on screen and two CSV files in probe_output/ (the results, and the contract details IBKR returned).
Dates inside the untouched window of the research (from 2026-05-14) are refused unless --allow-untouched-window is given.
"""

# FUNCTION: PARSE THE COMMAND LINE ARGUMENTS
def get_args():
    """
    Returns:
        argparse.Namespace: Command line arguments
    """
    # CREATE THE PARSER
    parser = argparse.ArgumentParser(description="Probe IBKR for index contracts and history (read-only).")
    parser.add_argument("--symbols", nargs="*", default=PROBE_SYMBOL_STR_LIST, help="index symbols (default: VIX VIX3M)")
    parser.add_argument("--exchange", default=PROBE_EXCHANGE_STR, help="index exchange (default: CBOE)")
    parser.add_argument("--dates", nargs="*", help="sample dates (each is mapped to the next NYSE session; default: 6 dates from 2005 to 2026-05-13)")
    parser.add_argument("--no-control", action="store_true", help="skip the SPY control requests")
    parser.add_argument("--no-extended", action="store_true", help="skip the extended-hours 1-minute request")
    parser.add_argument("--allow-untouched-window", action="store_true", help="allow sample dates from 2026-05-14 on (not recommended)")
    parser.add_argument("--list", action="store_true", help="list the requests and stop (no connection)")
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
    Plans the requests, connects, runs them, disconnects, and reports.
    """
    # PARSE THE ARGUMENTS
    args = get_args()
    # MAP THE SAMPLE DATES TO SESSIONS
    session_date_str_list = get_sample_session_date_list(args.dates)
    # REFUSE DATES INSIDE THE UNTOUCHED WINDOW
    if not args.allow_untouched_window and any(date_str >= UNTOUCHED_WINDOW_START_STR for date_str in session_date_str_list):
        raise SystemExit(f"❌ Sample dates {[d for d in session_date_str_list if d >= UNTOUCHED_WINDOW_START_STR]} are inside the untouched window "
                         f"(from {UNTOUCHED_WINDOW_START_STR}). Pick earlier dates, or pass --allow-untouched-window on purpose.")
    # PLAN THE REQUESTS
    step_dict_list = get_probe_step_list(args.symbols, session_date_str_list, args.exchange, not args.no_control, not args.no_extended)
    # DISPLAY INFORMATION
    print(f"Sample sessions: {session_date_str_list}")
    print(f"Requests planned: {len(step_dict_list)}")
    # IF ONLY A LIST WAS ASKED FOR
    if args.list:
        # PRINT THE REQUESTS AND EXIT
        for step_dict in step_dict_list:
            print(f"  {step_dict['symbol']:<14} {step_dict['test']:<21} {step_dict['target_date']:<10} "
                  f"{step_dict.get('bar_size_str', ''):<6} {step_dict.get('duration_str', '')}")
        return
    # CONNECT
    app = ProbeApp()
    if not app.connect_app(args.host, args.port, args.client_id):
        raise SystemExit(1)
    # RUN THE PROBE, THEN ALWAYS DISCONNECT
    try:
        row_pdf, detail_pdf = run_probe_tuple(app, step_dict_list)
    finally:
        app.disconnect_app()
    # SAVE THE RESULTS
    os.makedirs(args.out, exist_ok=True)
    stamp_str = get_ny_now_ts().strftime("%Y%m%d_%H%M%S")
    result_path_str, detail_path_str = f"{args.out}/vix_probe_{stamp_str}.csv", f"{args.out}/vix_probe_{stamp_str}_contracts.csv"
    row_pdf.to_csv(result_path_str, index=False)
    detail_pdf.to_csv(detail_path_str, index=False)
    # DISPLAY THE RESULTS
    print("\n" + row_pdf[["symbol", "test", "target_date", "diagnosis", "bars", "first_bar", "last_bar", "missing_minutes", "volume_info", "end_format", "error_code"]].to_string(index=False))
    print("\nVerdict")
    for line_str in get_verdict_str_list(row_pdf):
        print(f"  {line_str}")
    print(f"\nSaved: {result_path_str}\n       {detail_path_str}")

# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
