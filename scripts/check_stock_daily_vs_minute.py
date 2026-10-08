import argparse
import pandas as pd
# IMPORT THE STOCK CONFIGURATION AND STATUS
from ingest import stock_config
from ingest.stock_status import get_stock_daily_vs_minute_pdf

"""
Compare the daily bar of a stock with its 1-minute bars of the same dates (read-only: nothing is written, no IBKR connection)

    python scripts/check_stock_daily_vs_minute.py                                  # the last 10 dates present in both
    python scripts/check_stock_daily_vs_minute.py --dates 2024-08-05 2025-04-09

Per date: the daily open / high / low / close / volume next to the open of the first 1-minute bar, the highest high, the lowest low, the close of the last bar and the
sum of the 1-minute volumes. Prices that agree and a volume ratio of about 1 mean the two downloads are on the same price basis and the same volume unit. A ratio of
about 100 or 0.01 means a different volume unit; different prices mean a different adjustment (for example after a split). A small difference in volume or in the
high / low is possible (the daily bar may include late prints or auction trades that the 1-minute bars do not); a large one is a finding to look into.
"""


# FUNCTION: MAIN
def main():
    """
    Prints the comparison.
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Compare daily bars with the aggregated 1-minute bars of the same dates.")
    parser.add_argument("--symbol", default=list(stock_config.STOCK_SPEC_DICT)[0], choices=list(stock_config.STOCK_SPEC_DICT))
    parser.add_argument("--dates", nargs="*", help="session dates (default: the last 10 present in both)")
    args = parser.parse_args()
    # COMPARE
    compare_pdf = get_stock_daily_vs_minute_pdf(args.symbol, args.dates)
    # DISPLAY THE RESULT
    if compare_pdf.empty:
        print(f"No date with both a daily bar and 1-minute bars for {args.symbol} (raw + staging).")
        return
    pd.set_option("display.width", 250)
    pd.set_option("display.float_format", lambda value: f"{value:,.4f}")
    print(compare_pdf.to_string(index=False))


# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    main()
