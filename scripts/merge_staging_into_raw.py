import argparse
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE MAINTENANCE FUNCTIONS
from ingest.raw_maintenance import merge_staging_into_raw_pdf

"""
Check the staged downloads and add them to the raw folder (ADD-ONLY: existing raw files and dates are never changed)

    python scripts/merge_staging_into_raw.py                      # DRY RUN: check every staged day, write nothing
    python scripts/merge_staging_into_raw.py --apply              # add the accepted days to the raw folder
    python scripts/merge_staging_into_raw.py --apply --include-partial

A day is added when it is a closed NYSE session, every minute is present (or --include-partial), the candlesticks pass
the repo's sanity check, and the raw folder does not have that date yet. Added files are moved to
<staging>/merged/ and listed in <staging>/merge_log.csv. Afterwards: optional 'python scripts/organize_ohlcv_data.py',
then the repo's bad tick check and pipeline step 00 in stock_overflow_workspace.
"""

# FUNCTION: MAIN
def main():
    """
    Parses the arguments and runs the merge.
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Add checked staging day files to the raw folder (dry run by default).")
    parser.add_argument("--apply", action="store_true", help="really copy the files (default: dry run)")
    parser.add_argument("--include-partial", action="store_true", help="also add sessions with missing minutes")
    parser.add_argument("--staging", default=config.STAGING_OHLCV_PATH_STR)
    parser.add_argument("--raw", default=config.RAW_OHLCV_PATH_STR)
    parser.add_argument("--skip-name-check", action="store_true", help="bypass the SPY raw folder name check (temporary data roots and tests)")
    args = parser.parse_args()
    # STOP WHEN THE SPY RAW FOLDER NAME AND so.paths DISAGREE (--skip-name-check IS FOR TEMPORARY DATA ROOTS AND TESTS)
    if not args.skip_name_check:
        # READ THE MESSAGE (EMPTY WHEN THE NAMES MATCH)
        folder_name_problem_str = config.get_folder_name_problem_str()
        # PRINT IT AND STOP BEFORE ANY WRITE OR IBKR CONNECTION
        if folder_name_problem_str:
            # PRINT THE MESSAGE
            print(folder_name_problem_str)
            # STOP
            raise SystemExit(1)
    # DEFINE THE FOLDERS (ENDING WITH "/")
    staging_path_str = args.staging.replace("\\", "/").rstrip("/") + "/"
    raw_path_str = args.raw.replace("\\", "/").rstrip("/") + "/"
    # RUN THE MERGE
    result_pdf = merge_staging_into_raw_pdf(args.apply, args.include_partial, staging_path_str, raw_path_str,
                                            f"{staging_path_str}merged/", f"{staging_path_str}merge_log.csv")
    # DISPLAY THE RESULT
    print(result_pdf.to_string(index=False)) if not result_pdf.empty else None

# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
