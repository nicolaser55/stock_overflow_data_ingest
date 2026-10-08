import argparse
import pandas as pd
# IMPORT THE INGEST AND STOCK CONFIGURATION
from ingest import config
from ingest import stock_config
# IMPORT THE IBKR APPLICATIONS (1-MINUTE BARS: IbkrApp, DAILY BARS: IndexApp) AND THE STOCK DOWNLOAD
from ingest.ibkr_client import IbkrApp
from ingest.index_download import IndexApp
from ingest.stock_download import (get_stock_session_pdf, get_stock_daily_task_list, download_stock_minute_pdf, download_stock_daily_pdf,
                                   get_stock_problem_pdf)

"""
Download the 1-minute and daily bars of a stock (AAPL) from IBKR into its staging folders (nothing is added to the raw folders)

Prerequisite: IB Gateway (or TWS) running and logged in, API enabled on the port below.

    python scripts/download_ibkr_stock.py --list                                   # what would be downloaded (no connection)
    python scripts/download_ibkr_stock.py --dates 2024-08-05 2025-04-09            # a small first trial (1-minute and daily)
    python scripts/download_ibkr_stock.py --date1 2024-01-02                       # everything missing from that date (explicit dates are never moved)
    python scripts/download_ibkr_stock.py                                          # everything missing after the last date present (nothing present: from the start date in stock_config)
    python scripts/download_ibkr_stock.py --from-start                             # also fills gaps anywhere in the history
    python scripts/download_ibkr_stock.py --symbols AAPL --bars daily

Folders: <data root>/store01_rawzone/ibkr_aapl_1min_staging/ and ibkr_aapl_daily_staging/ (raw: ibkr_aapl_1min/, ibkr_aapl_daily/, see merge_stock_staging_into_raw.py).
Interrupted or failed runs are safe to repeat: finished requests are saved, and already staged or raw dates are skipped.
Speed: about 2 requests per second at most; the full 1-minute history is about one request per session (roughly 4,900 sessions from 2007).
"""


# FUNCTION: MAIN
def main():
    """
    Parses the arguments, plans the requests, downloads them into the staging folders.
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Download stock bars from IBKR into the staging folders.")
    parser.add_argument("--symbols", nargs="*", default=list(stock_config.STOCK_SPEC_DICT), choices=list(stock_config.STOCK_SPEC_DICT))
    parser.add_argument("--bars", nargs="*", default=stock_config.STOCK_BAR_KIND_LIST, choices=stock_config.STOCK_BAR_KIND_LIST, help="1min and / or daily")
    parser.add_argument("--date1", help="first date (default: the day after the last date present, or the start date in stock_config)")
    parser.add_argument("--date2", help="last date (default: today)")
    parser.add_argument("--dates", nargs="*", help="explicit dates (replaces the range)")
    parser.add_argument("--from-start", action="store_true", help="start at the start date in stock_config, so that gaps anywhere are filled")
    parser.add_argument("--redownload", action="store_true", help="request dates already present too (1-minute: the staging file is replaced)")
    parser.add_argument("--list", action="store_true", help="list the plan and stop (no connection)")
    parser.add_argument("--max-in-flight", type=int, default=config.MAX_IN_FLIGHT_REQUEST_COUNT)
    parser.add_argument("--host", default=config.IBKR_HOST_STR)
    parser.add_argument("--port", type=int, default=config.IBKR_PORT_INT, help="4001 Gateway live, 4002 Gateway paper, 7496 / 7497 TWS")
    parser.add_argument("--client-id", type=int, default=config.IBKR_DOWNLOAD_CLIENT_ID_INT)
    args = parser.parse_args()
    # PLAN THE SESSIONS OF EVERY STOCK AND BAR KIND (THE DAILY SESSIONS BECOME ONE REQUEST PER YEAR)
    plan_dict = {}
    for symbol_str in args.symbols:
        for bar_kind_str in args.bars:
            session_pdf = get_stock_session_pdf(symbol_str, bar_kind_str, args.date1, args.date2, args.dates, args.from_start, args.redownload)
            plan_dict[(symbol_str, bar_kind_str)] = get_stock_daily_task_list(symbol_str, session_pdf) if bar_kind_str == "daily" else session_pdf
    # DISPLAY THE PLAN
    print("\nPlan:")
    for (symbol_str, bar_kind_str), plan_item in plan_dict.items():
        unit_str = "year request(s)" if bar_kind_str == "daily" else "session(s)"
        date_str = f" ({plan_item['date'].min()} -> {plan_item['date'].max()})" if bar_kind_str == "1min" and len(plan_item) else ""
        print(f"  {symbol_str:<6} {bar_kind_str:<6} {len(plan_item):>6,} {unit_str}{date_str}  -> {stock_config.get_stock_folder_path_str(symbol_str, bar_kind_str, True)}")
    print(f"  Dates from {stock_config.STOCK_UNTOUCHED_WINDOW_START_STR} belong to the untouched window of the research: staged normally, never to be evaluated on.")
    # IF ONLY THE PLAN WAS ASKED FOR OR THERE IS NOTHING TO DO
    if args.list or not any(len(plan_item) for plan_item in plan_dict.values()):
        # EXIT FUNCTION
        return
    # LIST TO HOLD THE UNFINISHED REQUESTS
    problem_pdf_list = []
    # DOWNLOAD THE DAILY BARS (INDEX APPLICATION), THEN THE 1-MINUTE BARS (SPY APPLICATION): EACH WITH ITS OWN CONNECTION, ALWAYS DISCONNECTED
    for bar_kind_str, app_class in [("daily", IndexApp), ("1min", IbkrApp)]:
        # SKIP THE BAR KIND WHEN NOTHING IS PLANNED FOR IT
        if not any(len(plan_item) for (_, plan_bar_kind_str), plan_item in plan_dict.items() if plan_bar_kind_str == bar_kind_str):
            continue
        # CONNECT
        app = app_class()
        if not app.connect_app(args.host, args.port, args.client_id):
            raise SystemExit(1)
        # DOWNLOAD EVERY STOCK, THEN ALWAYS DISCONNECT
        try:
            for (symbol_str, plan_bar_kind_str), plan_item in plan_dict.items():
                if plan_bar_kind_str != bar_kind_str or not len(plan_item):
                    continue
                if bar_kind_str == "daily":
                    summary_pdf = download_stock_daily_pdf(app, symbol_str, plan_item, max_in_flight_int_in=args.max_in_flight)
                else:
                    summary_pdf = download_stock_minute_pdf(app, symbol_str, plan_item, max_in_flight_int_in=args.max_in_flight)
                problem_pdf_list.append(get_stock_problem_pdf(symbol_str, bar_kind_str, summary_pdf))
        finally:
            app.disconnect_app()
    # DISPLAY THE PROBLEMS
    problem_pdf = pd.concat(problem_pdf_list, ignore_index=True) if problem_pdf_list else pd.DataFrame()
    print(f"\nNot complete: {len(problem_pdf)}")
    print(problem_pdf.to_string(index=False)) if not problem_pdf.empty else None
    print("Next: python scripts/merge_stock_staging_into_raw.py   (dry run), then add --apply")


# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
