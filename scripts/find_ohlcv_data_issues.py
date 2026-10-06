import argparse
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE MAINTENANCE FUNCTIONS
from ingest.raw_maintenance import get_raw_issue_pdf

"""
Report the data issues of the raw folder (and optionally the staging folder): read only

    python scripts/find_ohlcv_data_issues.py
    python scripts/find_ohlcv_data_issues.py --start 2005-01-03 --include-staging --save issues.csv

Reported per date: missing sessions, missing minutes, extra minutes (outside the session), duplicate rows, and bars on
dates that are not NYSE sessions. Only sessions that have closed (real New York time) are expected.
"""

# FUNCTION: MAIN
def main():
    """
    Parses the arguments and prints the report.
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Report missing / extra / duplicate minutes (read only).")
    parser.add_argument("--start", help="first date the data should cover (default: first date found)")
    parser.add_argument("--raw", default=config.RAW_OHLCV_PATH_STR)
    parser.add_argument("--include-staging", action="store_true", help="also read the staging folder")
    parser.add_argument("--save", help="CSV path for the full report")
    args = parser.parse_args()
    # DEFINE THE FOLDERS
    path_str_list = [args.raw.replace("\\", "/").rstrip("/") + "/"] + ([config.STAGING_OHLCV_PATH_STR] if args.include_staging else [])
    # FIND THE ISSUES
    issue_pdf = get_raw_issue_pdf(path_str_list, args.start)
    # SAVE THE REPORT
    if args.save and not issue_pdf.empty:
        issue_pdf.to_csv(args.save, index=False)
        print(f"✅ Report saved: {args.save}")

# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
