import argparse
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE MAINTENANCE FUNCTIONS
from ingest.raw_maintenance import clean_raw_orphans_pdf

"""
Repair: fold left-over day / month files into the month / year file that already exists for them

    python scripts/clean_ohlcv_data.py               # DRY RUN
    python scripts/clean_ohlcv_data.py --apply

A left-over (orphan) file is a day or month file whose data was rolled up, but which could not be deleted. The pipeline
reads every file, so its rows would be counted twice. Rows are compared by exact instant; the row already in the month /
year file wins. The old script defaulted to DRY_RUN = False; this one is a dry run unless --apply is given.
"""

# FUNCTION: MAIN
def main():
    """
    Parses the arguments and cleans the folder.
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Fold orphan day / month files into their existing month / year file (dry run by default).")
    parser.add_argument("--apply", action="store_true", help="really write (default: dry run)")
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
    # CLEAN THE FOLDER
    result_pdf = clean_raw_orphans_pdf(args.apply, args.raw.replace("\\", "/").rstrip("/") + "/")
    # DISPLAY THE RESULT
    print(result_pdf.to_string(index=False)) if not result_pdf.empty else None

# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
