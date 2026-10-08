import pandas as pd
# IMPORT THE STOCK CONFIGURATION AND STATUS
from ingest import config
from ingest import stock_config
from ingest.stock_status import get_stock_status_pdf, get_stock_missing_date_list, get_stock_log_problem_pdf

"""
Where is my stock data, how much is there, what is missing? (read-only: nothing is written or downloaded, no IBKR connection)

    python scripts/stock_data_status.py

Prints: the data root, a table per stock / bar size (files, rows, MB, dates, first and last date) for raw, staging and staging/merged, the sessions still missing
since the start date in stock_config, and the requests the download log says IBKR could not answer. Reading the 1-minute files over the network share can take a
minute.
"""


# FUNCTION: MAIN
def main():
    """
    Prints the status of the stock data.
    """
    # DISPLAY THE FOLDERS IN USE
    print(f"Data root:  {config.DATA_ROOT_PATH_STR}")
    print(f"Raw zone:   {stock_config.STOCK_RAWZONE_PATH_STR}")
    # DISPLAY THE FOLDER TABLE
    print("\nData by folder (place: raw = merged and final, staging = downloaded, not merged yet, staging/merged = staging files already added to raw)")
    status_pdf = get_stock_status_pdf()
    print(status_pdf.drop(columns=["path", "exists_bool"]).assign(folder=status_pdf["exists_bool"].map({True: "", False: "(folder missing)"})).to_string(index=False))
    # DISPLAY THE MISSING DATES (RAW + STAGING, CLOSED NYSE SESSIONS SINCE THE START DATE)
    print("\nMissing sessions (in neither raw nor staging; counted from the start date in stock_config, which is not verified against IBKR)")
    for symbol_str in stock_config.STOCK_SPEC_DICT:
        for bar_kind_str in stock_config.STOCK_BAR_KIND_LIST:
            expected_int, missing_date_list = get_stock_missing_date_list(symbol_str, bar_kind_str)
            head_str = f"{[str(d) for d in missing_date_list[:3]]} ... {[str(d) for d in missing_date_list[-3:]]}" if len(missing_date_list) > 6 else str([str(d) for d in missing_date_list])
            print(f"  {symbol_str:<6} {bar_kind_str:<6} {len(missing_date_list):>6,} of {expected_int:,} sessions missing  {head_str if missing_date_list else ''}")
    # DISPLAY THE LOG PROBLEMS
    problem_pdf = pd.concat([get_stock_log_problem_pdf(symbol_str, bar_kind_str) for symbol_str in stock_config.STOCK_SPEC_DICT for bar_kind_str in stock_config.STOCK_BAR_KIND_LIST],
                            ignore_index=True)
    print(f"\nRequests whose last attempt in the download log was not complete: {len(problem_pdf)}")
    if not problem_pdf.empty:
        print(problem_pdf.groupby(["symbol", "bar_kind", "status_str", "saved_bool"]).size().rename("requests").reset_index().to_string(index=False))
        print(problem_pdf.head(10).to_string(index=False))
    # DISPLAY THE NEXT COMMANDS
    print("\nTo collect what is missing:  python scripts/download_ibkr_stock.py --list        (plan only)")
    print("                             python scripts/download_ibkr_stock.py --from-start  (downloads the missing sessions, anywhere in the history)")


# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
