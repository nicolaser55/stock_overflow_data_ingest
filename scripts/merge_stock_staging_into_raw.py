import argparse
# IMPORT THE STOCK CONFIGURATION AND MERGE
from ingest import stock_config
from ingest.stock_merge import merge_stock_staging_into_raw_pdf

"""
Check the staged stock files and add them to the raw folders (ADD-ONLY: raw dates are never replaced)

    python scripts/merge_stock_staging_into_raw.py                   # DRY RUN: check every staged file, write nothing
    python scripts/merge_stock_staging_into_raw.py --apply           # add the accepted files
    python scripts/merge_stock_staging_into_raw.py --apply --include-partial

1-minute files are copied as new day files after the SPY checks (closed NYSE session, every minute present unless --include-partial, candlestick sanity, bad
ticks counted); a date already in raw is skipped. Daily files are merged into the raw file of the same year with the text-preserving engine (rows already in
raw win; backup first, into <raw folder>_backup_<time>/). Afterwards the folders can be rolled up with
'python scripts/organize_ohlcv_data.py --raw <folder>' (dry run first): day files of closed months become month files, month files of closed years year files.
"""


# FUNCTION: MAIN
def main():
    """
    Parses the arguments and merges every stock and bar kind.
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Add checked stock staging files to the raw folders (dry run by default).")
    parser.add_argument("--apply", action="store_true", help="really write (default: dry run)")
    parser.add_argument("--include-partial", action="store_true", help="also add 1-minute sessions with missing minutes")
    parser.add_argument("--symbols", nargs="*", default=list(stock_config.STOCK_SPEC_DICT), choices=list(stock_config.STOCK_SPEC_DICT))
    parser.add_argument("--bars", nargs="*", default=stock_config.STOCK_BAR_KIND_LIST, choices=stock_config.STOCK_BAR_KIND_LIST)
    args = parser.parse_args()
    # MERGE EVERY STOCK AND BAR KIND
    for symbol_str in args.symbols:
        for bar_kind_str in args.bars:
            result_pdf = merge_stock_staging_into_raw_pdf(symbol_str, bar_kind_str, args.apply, args.include_partial)
            print(result_pdf.to_string(index=False)) if not result_pdf.empty else None


# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
