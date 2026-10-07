import argparse
# IMPORT THE INDEX CONFIGURATION AND MERGE
from ingest import index_config
from ingest.index_merge import merge_index_staging_into_raw_pdf

"""
Check the staged VIX / VIX3M files and add them to the raw folders (ADD-ONLY: raw dates are never replaced)

    python scripts/merge_index_staging_into_raw.py                   # DRY RUN: check every staged file, write nothing
    python scripts/merge_index_staging_into_raw.py --apply           # add the accepted files
    python scripts/merge_index_staging_into_raw.py --apply --include-partial

1-minute files are copied as new day files (a date already in raw is skipped). Daily files are merged into the raw file of the same year with the
text-preserving engine (rows already in raw win; backup first). Afterwards the folders can be rolled up with
'python scripts/organize_ohlcv_data.py --raw <folder>' (dry run first): day files of closed months become month files, month files of closed years year files.
"""

# FUNCTION: MAIN
def main():
    """
    Parses the arguments and merges every index and bar kind.
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Add checked VIX / VIX3M staging files to the raw folders (dry run by default).")
    parser.add_argument("--apply", action="store_true", help="really write (default: dry run)")
    parser.add_argument("--include-partial", action="store_true", help="also add 1-minute sessions with missing minutes")
    parser.add_argument("--symbols", nargs="*", default=list(index_config.INDEX_SPEC_DICT), choices=list(index_config.INDEX_SPEC_DICT))
    parser.add_argument("--bars", nargs="*", default=list(index_config.INDEX_BAR_KIND_DICT), choices=list(index_config.INDEX_BAR_KIND_DICT))
    args = parser.parse_args()
    # MERGE EVERY INDEX AND BAR KIND
    for symbol_str in args.symbols:
        for bar_kind_str in args.bars:
            result_pdf = merge_index_staging_into_raw_pdf(symbol_str, bar_kind_str, args.apply, args.include_partial)
            print(result_pdf.to_string(index=False)) if not result_pdf.empty else None

# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
