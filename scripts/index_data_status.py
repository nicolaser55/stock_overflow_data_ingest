import pandas as pd
# IMPORT THE INDEX CONFIGURATION AND STATUS
from ingest import config
from ingest import index_config
from ingest.index_status import get_index_status_pdf, get_other_folder_pdf, get_missing_date_list, get_log_problem_pdf

"""
Where is my VIX / VIX3M data, how much is there, what is missing? (read-only: nothing is written or downloaded, no IBKR connection)

    python scripts/index_data_status.py

Prints: the folders in use, a table per index / bar size (files, rows, MB, dates, first and last date) for raw, staging and staging/merged, other
folders of the raw zone that may hold index data (for example under an older folder name), the sessions still missing, and the requests the download
log says IBKR could not answer. Reading the 1-minute year files over the network share can take a minute.
"""

# FUNCTION: MAIN
def main():
    """
    Prints the status of the index data.
    """
    # DISPLAY THE FOLDERS IN USE
    print(f"Data root:     {config.DATA_ROOT_PATH_STR}")
    print(f"Index raw:     {index_config.INDEX_RAW_ROOT_PATH_STR}")
    print(f"Index staging: {index_config.INDEX_STAGING_ROOT_PATH_STR}")
    print(f"Download log:  {index_config.INDEX_DOWNLOAD_LOG_FILE_PATH_STR}")
    # DISPLAY THE FOLDER TABLE
    print("\nData by folder (place: raw = merged and final, staging = downloaded, not merged yet, staging/merged = staging files already added to raw)")
    status_pdf = get_index_status_pdf()
    print(status_pdf.drop(columns=["path", "exists_bool"]).assign(folder=status_pdf["exists_bool"].map({True: "", False: "(folder missing)"})).to_string(index=False))
    # DISPLAY THE OTHER FOLDERS
    other_pdf = get_other_folder_pdf()
    print("\nOther folders in the raw zone that may hold index data:")
    print(other_pdf.to_string(index=False) if not other_pdf.empty else "  none")
    # DISPLAY THE MISSING DATES (RAW + STAGING, FINISHED NYSE SESSIONS SINCE THE FIRST DATE IBKR HAS)
    print("\nMissing sessions (in neither raw nor staging)")
    for symbol_str in index_config.INDEX_SPEC_DICT:
        for bar_kind_str in index_config.INDEX_BAR_KIND_DICT:
            expected_int, missing_date_list = get_missing_date_list(symbol_str, bar_kind_str)
            head_str = f"{[str(d) for d in missing_date_list[:3]]} ... {[str(d) for d in missing_date_list[-3:]]}" if len(missing_date_list) > 6 else str([str(d) for d in missing_date_list])
            print(f"  {symbol_str:<6} {bar_kind_str:<6} {len(missing_date_list):>6,} of {expected_int:,} sessions missing  {head_str if missing_date_list else ''}")
    # DISPLAY THE LOG PROBLEMS
    problem_pdf = get_log_problem_pdf()
    print(f"\nRequests whose last attempt in the download log was not complete: {len(problem_pdf)}")
    if not problem_pdf.empty:
        print(problem_pdf.groupby(["symbol", "bar_kind", "status_str", "saved_bool"]).size().rename("requests").reset_index().to_string(index=False))
        print(problem_pdf.head(10).to_string(index=False))
    # DISPLAY THE NEXT COMMANDS
    print("\nTo collect what is missing:  python scripts/download_ibkr_index.py --list        (plan only)")
    print("                             python scripts/download_ibkr_index.py --from-start  (downloads the missing sessions, anywhere in the history)")
    pd.reset_option("display.max_rows")

# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
