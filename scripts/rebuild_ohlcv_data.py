import argparse
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE MAINTENANCE FUNCTIONS
from ingest.raw_maintenance import rebuild_raw_pdf

"""
Rebuild a folder into the organized layout (one file per year / closed month / day of the current month)

    python scripts/rebuild_ohlcv_data.py                                    # DRY RUN, raw -> <raw>_rebuilt/
    python scripts/rebuild_ohlcv_data.py --apply                            # write <raw>_rebuilt/
    python scripts/rebuild_ohlcv_data.py --apply --target <raw folder>      # in place (every replaced / removed file is backed up)

Rows of year files win over month files, which win over day files (the compiled files hold any correction). Text is kept
exactly. The old script wrote with DRY_RUN = False and deleted "stale" files by default; here both are opt-in.
"""

# FUNCTION: MAIN
def main():
    """
    Parses the arguments and rebuilds the folder.
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Rebuild a folder into the organized layout (dry run by default).")
    parser.add_argument("--apply", action="store_true", help="really write (default: dry run)")
    parser.add_argument("--source", default=config.RAW_OHLCV_PATH_STR)
    parser.add_argument("--target", default=config.RAW_OHLCV_PATH_STR.rstrip("/") + "_rebuilt/")
    parser.add_argument("--delete-stale", action="store_true", help="move target OHLCV files that are not rebuilt to the backup folder")
    args = parser.parse_args()
    # REBUILD THE FOLDER
    rebuild_raw_pdf(args.source.replace("\\", "/").rstrip("/") + "/", args.target.replace("\\", "/").rstrip("/") + "/", args.apply, args.delete_stale)

# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
