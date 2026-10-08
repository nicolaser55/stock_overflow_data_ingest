import argparse
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE MAINTENANCE FUNCTIONS
from ingest.raw_maintenance import organize_raw_pdf

"""
Roll up the raw folder: day files of closed months -> month files; day / month files of closed years -> year files

    python scripts/organize_ohlcv_data.py            # DRY RUN
    python scripts/organize_ohlcv_data.py --apply

Text is kept exactly, rows already in a month / year file win over a duplicate, every changed file is backed up into
<raw>_backup_YYYYMMDD_HHMMSS/, and a target is all-or-nothing (see ingest/raw_files.py). "Closed" uses the real New York
clock (the old script used the laptop's clock labelled as New York time).
"""

# FUNCTION: MAIN
def main():
    """
    Parses the arguments and organizes the folder.
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Roll up day / month files of closed periods (dry run by default).")
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
    # ORGANIZE THE FOLDER
    result_pdf = organize_raw_pdf(args.apply, args.raw.replace("\\", "/").rstrip("/") + "/")
    # DISPLAY THE RESULT
    print(result_pdf.to_string(index=False)) if not result_pdf.empty else None

# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
