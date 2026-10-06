import os
import re
import shutil
from datetime import datetime
import pandas as pd
# IMPORT LOCAL FILE MANAGEMENT FUNCTIONS OF THE RESEARCH WORKSPACE
from so.core.local_file_management import get_path_file_list
# IMPORT THE INGEST CONFIGURATION
from ingest import config

"""
Raw OHLCV Files: naming, text-preserving reading and writing, roll-up rules and the merge engine

The raw folder holds one CSV per period: ohlcv_data_YYYY.csv (a past year), ohlcv_data_YYYYMM.csv (a past month of the
current year) and ohlcv_data_YYYYMMDD.csv (a day of the current month). The research pipeline reads every CSV of the
folder whatever the layout (so.core.raw_data.read_raw_ohlcv_pdf), so the layout only keeps the folder tidy.

Rules of every function that changes files (organize, clean, rebuild, merge from staging):

    1. Text is preserved. Files are read with every cell as text and written back as text: prices, volumes and
       timestamps keep their exact characters. (The old scripts parsed and re-wrote the files with pandas, which turned
       every timestamp into UTC text and could change number formatting. The repo's bad tick correction,
       so.core.bad_ticks, documents that the raw files are kept "byte for byte".)
    2. Existing rows win. When two files hold the same minute (same instant, whatever the text format), the row already
       in the target file is kept, so a corrected bar (records/bad_tick_corrections.csv) is never replaced by a fresh,
       uncorrected copy. Conflicting duplicates are counted and reported.
    3. Dry run by default. Nothing is written unless apply_bool_in is True.
    4. Backups first. Before a file is overwritten or deleted, it is copied byte for byte into a sibling folder
       <raw folder>_backup_YYYYMMDD_HHMMSS/ (same convention as so.core.bad_ticks).
    5. Atomic writes. A file is written to "<name>.tmp" and then renamed over the target, so an interrupted run never
       leaves a half-written CSV.
    6. All or nothing per target. The source files are first MOVED into the backup folder (that is their delete: if the
       share refuses deletes, this fails before anything was written and the moved files are put back). Then the target
       is written and verified (re-read: every input minute present, no duplicate). If the write or the verification
       fails, the target and the sources are restored. The data is never left in two files, and never lost.
"""

"""
File Names
"""

# DEFINE THE FILE NAME PATTERN (ohlcv_data_ + 4, 6 OR 8 DIGITS)
OHLCV_FILE_NAME_PATTERN = re.compile(rf"^{config.OHLCV_FILE_PREFIX_STR}_(\d{{4}}(?:\d{{2}}(?:\d{{2}})?)?)\.csv$")
# DEFINE THE PERIOD KIND OF EACH DATE STRING LENGTH
PERIOD_KIND_DICT = {4: "year", 6: "month", 8: "day"}
# DEFINE THE NAME OF THE INTERNAL INSTANT COLUMN (NEVER WRITTEN)
TS_KEY_COL_STR = "_ts_utc"
# DEFINE THE PRICE AND VOLUME COLUMNS COMPARED WHEN DUPLICATES ARE FOUND
VALUE_COL_STR_LIST = ["open", "high", "low", "close", "volume"]

# FUNCTION: GET THE FILE NAME OF A PERIOD
def get_file_name_str(period_str_in):
    """
    Args:
        period_str_in (str): "YYYY", "YYYYMM" or "YYYYMMDD"

    Returns:
        str: File name, e.g. "ohlcv_data_202608.csv"
    """
    # RETURN THE FILE NAME
    return f"{config.OHLCV_FILE_PREFIX_STR}_{period_str_in}.csv"

# FUNCTION: GET THE FIRST AND LAST DATE OF A PERIOD
def get_period_date_tup(period_str_in):
    """
    Args:
        period_str_in (str): "YYYY", "YYYYMM" or "YYYYMMDD"

    Returns:
        tuple: (datetime.date first day, datetime.date last day)
    """
    # IF THE PERIOD IS A YEAR
    if len(period_str_in) == 4:
        # RETURN THE YEAR BOUNDS
        return pd.Timestamp(f"{period_str_in}-01-01").date(), pd.Timestamp(f"{period_str_in}-12-31").date()
    # IF THE PERIOD IS A MONTH
    if len(period_str_in) == 6:
        # COLLECT THE FIRST DAY
        first_ts = pd.Timestamp(f"{period_str_in[:4]}-{period_str_in[4:]}-01")
        # RETURN THE MONTH BOUNDS
        return first_ts.date(), (first_ts + pd.offsets.MonthEnd(0)).date()
    # RETURN THE DAY
    day_date = pd.Timestamp(f"{period_str_in[:4]}-{period_str_in[4:6]}-{period_str_in[6:]}").date()
    return day_date, day_date

# FUNCTION: GET THE OHLCV FILES OF A FOLDER
def get_ohlcv_file_pdf(path_str_in, alert_in=True):
    """
    Lists the OHLCV files of a folder and parses their periods. Other files are skipped (and listed when alert_in).

    Args:
        path_str_in (str): Folder ending with "/"
        alert_in (bool): Display the skipped file names

    Returns:
        pd.DataFrame: file_path_str, file_name_str, period_str, period_kind_str ("year"/"month"/"day"), first_date,
                      last_date; sorted by first_date then period length
    """
    # DEFINE THE COLUMNS
    col_str_list = ["file_path_str", "file_name_str", "period_str", "period_kind_str", "first_date", "last_date"]
    # IF THE FOLDER DOES NOT EXIST
    if not os.path.isdir(path_str_in):
        # RETURN AN EMPTY DATAFRAME
        return pd.DataFrame(columns=col_str_list)
    # LIST TO HOLD THE FILE ROWS AND THE SKIPPED NAMES
    row_dict_list, skipped_name_str_list = [], []
    # ITERATE OVER THE FILES OF THE FOLDER
    for file_path_str in get_path_file_list(path_str_in):
        # COLLECT THE FILE NAME
        file_name_str = os.path.basename(file_path_str)
        # MATCH THE NAME
        match = OHLCV_FILE_NAME_PATTERN.match(file_name_str)
        # IF THE NAME IS NOT AN OHLCV FILE NAME
        if match is None:
            # REMEMBER THE NAME (TEMPORARY FILES AND LOGS ARE EXPECTED)
            skipped_name_str_list.append(file_name_str)
            continue
        # COLLECT THE PERIOD AND ITS BOUNDS
        period_str = match.group(1)
        first_date, last_date = get_period_date_tup(period_str)
        # STORE THE ROW
        row_dict_list.append({"file_path_str": file_path_str, "file_name_str": file_name_str, "period_str": period_str,
                              "period_kind_str": PERIOD_KIND_DICT[len(period_str)], "first_date": first_date, "last_date": last_date})
    # DISPLAY THE SKIPPED FILES
    print(f"ℹ️ Skipped non-OHLCV files in {path_str_in}: {sorted(skipped_name_str_list)}") if alert_in and skipped_name_str_list else None
    # IF THERE ARE NO FILES
    if not row_dict_list:
        # RETURN AN EMPTY DATAFRAME
        return pd.DataFrame(columns=col_str_list)
    # CREATE THE DATAFRAME
    file_pdf = pd.DataFrame(row_dict_list, columns=col_str_list)
    # RETURN THE SORTED DATAFRAME
    return file_pdf.assign(_len=file_pdf["period_str"].str.len()).sort_values(["first_date", "_len"]).drop(columns="_len").reset_index(drop=True)

"""
Reading And Writing Text
"""

# FUNCTION: PARSE TIMESTAMP TEXT TO UTC INSTANTS
def get_ts_utc_series(timestamp_series_in):
    """
    Parses timestamp text with any UTC offset ("2005-01-03 09:30:00-05:00", "2005-01-03 14:30:00+00:00") to UTC instants.

    Args:
        timestamp_series_in (pd.Series): Timestamp text

    Returns:
        pd.Series: UTC timestamps
    """
    # RETURN THE PARSED INSTANTS
    return pd.to_datetime(timestamp_series_in, utc=True, format="ISO8601")

# FUNCTION: READ AN OHLCV FILE AS TEXT
def read_ohlcv_text_pdf(file_path_str_in):
    """
    Reads an OHLCV CSV with every cell kept as text (empty cells stay empty strings) and adds the UTC instant column
    TS_KEY_COL_STR (used for sorting and duplicates; never written).

    Args:
        file_path_str_in (str): CSV file path

    Returns:
        pd.DataFrame: Text cells + TS_KEY_COL_STR
    """
    # READ THE FILE AS TEXT
    text_pdf = pd.read_csv(file_path_str_in, dtype=str, keep_default_na=False)
    # DROP REPEATED COLUMN NAMES (pandas RENAMES THEM "date.1"; THE REPO READER DROPS THEM TOO)
    text_pdf = text_pdf.loc[:, [not re.fullmatch(r".+\.\d+", col_str) for col_str in text_pdf.columns]]
    # ADD THE INSTANT COLUMN
    text_pdf[TS_KEY_COL_STR] = get_ts_utc_series(text_pdf["timestamp"])
    # RETURN THE DATAFRAME
    return text_pdf

# FUNCTION: WRITE A TEXT DATAFRAME ATOMICALLY
def write_text_pdf_atomic(text_pdf_in, file_path_str_in):
    """
    Writes a DataFrame of text cells to CSV through a temporary file and a rename (never a half-written target).
    The internal instant column is not written.

    Args:
        text_pdf_in (pd.DataFrame): Text cells (TS_KEY_COL_STR is dropped if present)
        file_path_str_in (str): Target CSV path
    """
    # CREATE THE FOLDER IF NEEDED
    os.makedirs(os.path.dirname(file_path_str_in), exist_ok=True)
    # DEFINE THE TEMPORARY PATH
    tmp_file_path_str = f"{file_path_str_in}.tmp"
    # WRITE THE TEMPORARY FILE
    text_pdf_in.drop(columns=[TS_KEY_COL_STR], errors="ignore").to_csv(tmp_file_path_str, index=False)
    # RENAME IT OVER THE TARGET
    os.replace(tmp_file_path_str, file_path_str_in)

# FUNCTION: APPEND ROWS TO A LOG CSV
def append_log_rows(row_dict_list_in, file_path_str_in, col_str_list_in):
    """
    Appends rows to a CSV log (the header is written when the file is created). Logs are never rewritten.

    Args:
        row_dict_list_in (list[dict]): Rows
        file_path_str_in (str): Log path
        col_str_list_in (list[str]): Column order
    """
    # IF THERE ARE NO ROWS
    if not row_dict_list_in:
        # EXIT FUNCTION
        return
    # CREATE THE FOLDER IF NEEDED
    os.makedirs(os.path.dirname(file_path_str_in), exist_ok=True)
    # DEFINE WHETHER THE HEADER MUST BE WRITTEN
    header_bool = not os.path.isfile(file_path_str_in)
    # APPEND THE ROWS
    pd.DataFrame(row_dict_list_in, columns=col_str_list_in).to_csv(file_path_str_in, mode="a", header=header_bool, index=False)

"""
Present Dates
"""

# FUNCTION: GET THE SESSION DATES PRESENT IN FOLDERS WITHIN A DATE RANGE
def get_present_date_set(path_str_list_in, date1_in=None, date2_in=None):
    """
    Collects the New York session dates that have at least one bar in the OHLCV files of the folders. Day files count
    from their name; month and year files overlapping the range are read (timestamp column only).

    Args:
        path_str_list_in (list[str]): Folders ending with "/"
        date1_in (str | date | None): First date (None = no bound)
        date2_in (str | date | None): Last date (None = no bound)

    Returns:
        set[datetime.date]: Present dates within the range
    """
    # CONVERT THE BOUNDS
    date1 = pd.Timestamp(date1_in).date() if date1_in is not None else pd.Timestamp.min.date()
    date2 = pd.Timestamp(date2_in).date() if date2_in is not None else pd.Timestamp.max.date()
    # SET TO HOLD THE DATES
    present_date_set = set()
    # ITERATE OVER THE FOLDERS
    for path_str in path_str_list_in:
        # COLLECT THE FILES OVERLAPPING THE RANGE
        file_pdf = get_ohlcv_file_pdf(path_str, alert_in=False)
        file_pdf = file_pdf[(file_pdf["last_date"] >= date1) & (file_pdf["first_date"] <= date2)]
        # ITERATE OVER THE FILES
        for file_row in file_pdf.itertuples():
            # IF THE FILE IS A DAY FILE
            if file_row.period_kind_str == "day":
                # ADD ITS DATE (ONLY NON-EMPTY DAY FILES ARE EVER WRITTEN)
                present_date_set.add(file_row.first_date)
                continue
            # READ THE TIMESTAMPS
            timestamp_series = pd.read_csv(file_row.file_path_str, usecols=["timestamp"], dtype=str)["timestamp"]
            # ADD THE NEW YORK DATES
            present_date_set.update(get_ts_utc_series(timestamp_series).dt.tz_convert(config.NY_TZ_STR).dt.date.unique())
    # RETURN THE DATES WITHIN THE RANGE
    return {date_object for date_object in present_date_set if date1 <= date_object <= date2}

"""
Roll-Up Rules
"""

# FUNCTION: GET THE TARGET PERIOD OF A DATE (ORGANIZE RULE)
def get_organized_period_str(date_in, now_ny_ts_in):
    """
    Organize rule: the current month stays in day files, earlier months of the current year go to month files, earlier
    years go to year files. (Dates after the current month, which should not exist, stay in day files.)

    Args:
        date_in (date): Session date
        now_ny_ts_in (pd.Timestamp): Current New York time

    Returns:
        str: Target period ("YYYYMMDD", "YYYYMM" or "YYYY")
    """
    # CONVERT THE DATE
    date_ts = pd.Timestamp(date_in)
    # IF THE DATE IS IN AN EARLIER YEAR
    if date_ts.year < now_ny_ts_in.year:
        # RETURN THE YEAR
        return date_ts.strftime("%Y")
    # IF THE DATE IS IN AN EARLIER MONTH OF THE CURRENT YEAR
    if date_ts.year == now_ny_ts_in.year and date_ts.month < now_ny_ts_in.month:
        # RETURN THE MONTH
        return date_ts.strftime("%Y%m")
    # RETURN THE DAY
    return date_ts.strftime("%Y%m%d")

# FUNCTION: GET THE TARGET PERIOD OF A FILE (ORGANIZE RULE)
def get_organized_file_period_str(period_str_in, now_ny_ts_in):
    """
    Applies the organize rule to a whole file (a day or month file of a closed month moves up).

    Args:
        period_str_in (str): Period of the file
        now_ny_ts_in (pd.Timestamp): Current New York time

    Returns:
        str: Target period
    """
    # IF THE FILE IS A YEAR FILE
    if len(period_str_in) == 4:
        # IT STAYS
        return period_str_in
    # COLLECT THE TARGET OF THE FILE'S FIRST DAY
    target_period_str = get_organized_period_str(get_period_date_tup(period_str_in)[0], now_ny_ts_in)
    # A MONTH FILE NEVER GOES DOWN TO A DAY FILE
    return period_str_in if len(target_period_str) > len(period_str_in) else target_period_str

# FUNCTION: GET THE TARGET PERIOD OF A FILE (ORPHAN RULE OF THE CLEAN SCRIPT)
def get_orphan_file_period_str(period_str_in, existing_period_str_set_in):
    """
    Orphan rule: a day or month file whose year file already exists belongs in the year file; a day file whose month
    file exists belongs in the month file. (Left-over sources of an interrupted roll-up.)

    Args:
        period_str_in (str): Period of the file
        existing_period_str_set_in (set[str]): Periods of every file in the folder

    Returns:
        str: Target period
    """
    # IF THE YEAR FILE EXISTS AND THIS IS A SMALLER FILE
    if len(period_str_in) > 4 and period_str_in[:4] in existing_period_str_set_in:
        # RETURN THE YEAR
        return period_str_in[:4]
    # IF THE MONTH FILE EXISTS AND THIS IS A DAY FILE
    if len(period_str_in) == 8 and period_str_in[:6] in existing_period_str_set_in:
        # RETURN THE MONTH
        return period_str_in[:6]
    # RETURN THE FILE'S OWN PERIOD
    return period_str_in

"""
Merge Engine
"""

# FUNCTION: MERGE TEXT DATAFRAMES (EXISTING ROWS WIN)
def get_merged_text_pdf_tup(text_pdf_list_in):
    """
    Concatenates text DataFrames (the first one is the existing target), drops duplicate minutes keeping the first
    occurrence, and sorts by time. Columns are the union, in order of first appearance.

    Args:
        text_pdf_list_in (list[pd.DataFrame]): Text DataFrames from read_ohlcv_text_pdf, existing target first

    Returns:
        tuple: (merged pd.DataFrame, info dict: input_row_count, output_row_count, duplicate_row_count,
                conflict_minute_count, conflict_ts_list (up to 10 New York timestamps))
    """
    # CONCATENATE THE DATAFRAMES (MISSING COLUMNS BECOME EMPTY TEXT)
    combined_pdf = pd.concat(text_pdf_list_in, axis=0, ignore_index=True, sort=False)
    combined_pdf = combined_pdf.fillna({col_str: "" for col_str in combined_pdf.columns if col_str != TS_KEY_COL_STR})
    # FIND THE MINUTES THAT APPEAR MORE THAN ONCE
    duplicate_mask = combined_pdf.duplicated(subset=[TS_KEY_COL_STR], keep=False)
    # COLLECT THE DUPLICATES WITH DIFFERENT PRICES OR VOLUMES (COMPARED AS NUMBERS: "119.4" EQUALS "119.40")
    value_col_str_list = [col_str for col_str in VALUE_COL_STR_LIST if col_str in combined_pdf.columns]
    duplicate_value_pdf = combined_pdf.loc[duplicate_mask, [TS_KEY_COL_STR] + value_col_str_list]
    duplicate_value_pdf[value_col_str_list] = duplicate_value_pdf[value_col_str_list].apply(pd.to_numeric, errors="coerce")
    conflict_ts_series = duplicate_value_pdf.groupby(TS_KEY_COL_STR)[value_col_str_list].nunique(dropna=False).gt(1).any(axis=1)
    conflict_ts_list = sorted(conflict_ts_series.index[conflict_ts_series.values])
    # DROP THE DUPLICATES (FIRST OCCURRENCE = EXISTING TARGET ROW) AND SORT BY TIME
    merged_pdf = combined_pdf.drop_duplicates(subset=[TS_KEY_COL_STR], keep="first").sort_values(TS_KEY_COL_STR, kind="stable").reset_index(drop=True)
    # DEFINE THE INFORMATION DICTIONARY
    info_dict = {"input_row_count": len(combined_pdf), "output_row_count": len(merged_pdf),
                 "duplicate_row_count": len(combined_pdf) - len(merged_pdf), "conflict_minute_count": len(conflict_ts_list),
                 "conflict_ts_list": [ts.tz_convert(config.NY_TZ_STR) for ts in conflict_ts_list[:10]]}
    # RETURN THE MERGED DATAFRAME AND THE INFORMATION
    return merged_pdf, info_dict

# FUNCTION: GET A NEW BACKUP FOLDER PATH
def get_backup_path_str(raw_path_str_in):
    """
    Args:
        raw_path_str_in (str): Folder being changed (ending with "/")

    Returns:
        str: Sibling backup folder "<folder>_backup_YYYYMMDD_HHMMSS/" that does not exist yet (a suffix "_2", "_3", ...
             is added when two runs start in the same second, so a backup is never overwritten)
    """
    # DEFINE THE BASE PATH
    base_path_str = f"{raw_path_str_in.rstrip('/')}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    # DEFINE THE FIRST FREE PATH
    backup_path_str, suffix_int = base_path_str, 1
    while os.path.exists(backup_path_str):
        suffix_int += 1
        backup_path_str = f"{base_path_str}_{suffix_int}"
    # RETURN THE BACKUP FOLDER PATH
    return f"{backup_path_str}/"

# FUNCTION: MERGE SOURCE FILES INTO A TARGET FILE
def merge_files_into_target_dict(target_file_path_str_in, source_file_path_str_list_in, apply_bool_in, backup_path_str_in, alert_in=True):
    """
    Merges source files into a target file (created if missing) and deletes the sources, following the rules of the
    module docstring (text kept, existing rows win, backup, atomic write, verification, all or nothing).

    Args:
        target_file_path_str_in (str): Target CSV (may not exist yet)
        source_file_path_str_list_in (list[str]): Source CSVs (the target itself is ignored if listed)
        apply_bool_in (bool): False = dry run (read and report only)
        backup_path_str_in (str): Backup folder for this run (created on first use)
        alert_in (bool): Display information

    Returns:
        dict: target, sources, status ("dry_run", "merged", "rolled_back", "failed"), message and the merge counts
    """
    # REMOVE THE TARGET FROM THE SOURCES
    source_file_path_str_list = [path_str for path_str in source_file_path_str_list_in if path_str != target_file_path_str_in]
    # DEFINE WHETHER THE TARGET EXISTS
    target_exists_bool = os.path.isfile(target_file_path_str_in)
    # READ THE TARGET AND THE SOURCES
    text_pdf_list = ([read_ohlcv_text_pdf(target_file_path_str_in)] if target_exists_bool else []) + \
                    [read_ohlcv_text_pdf(path_str) for path_str in source_file_path_str_list]
    # MERGE THEM
    merged_pdf, info_dict = get_merged_text_pdf_tup(text_pdf_list)
    # DEFINE THE RESULT DICTIONARY
    result_dict = {"target_str": os.path.basename(target_file_path_str_in), "source_str_list": [os.path.basename(p) for p in source_file_path_str_list],
                   "target_existed_bool": target_exists_bool, **{k: v for k, v in info_dict.items() if k != "conflict_ts_list"},
                   "status_str": "dry_run", "message_str": ""}
    # DISPLAY INFORMATION
    if alert_in:
        print(f"{'🔎 [DRY RUN] ' if not apply_bool_in else ''}{result_dict['target_str']} <- {result_dict['source_str_list']}: "
              f"{info_dict['input_row_count']:,} rows in, {info_dict['output_row_count']:,} out "
              f"({info_dict['duplicate_row_count']:,} duplicates, {info_dict['conflict_minute_count']:,} with different values)")
        print(f"\t⚠️ Different values at (existing row kept): {[str(ts) for ts in info_dict['conflict_ts_list']]}") if info_dict["conflict_minute_count"] else None
    # IF THIS IS A DRY RUN
    if not apply_bool_in:
        # RETURN THE RESULT
        return result_dict
    # CREATE THE BACKUP FOLDER
    os.makedirs(backup_path_str_in, exist_ok=True)
    # BACK UP THE TARGET (BYTE FOR BYTE COPY)
    if target_exists_bool:
        shutil.copy2(target_file_path_str_in, f"{backup_path_str_in}{os.path.basename(target_file_path_str_in)}")
    # LIST TO HOLD THE SOURCES ALREADY MOVED INTO THE BACKUP FOLDER
    moved_path_str_list = []
    # TRY TO MOVE THE SOURCES INTO THE BACKUP FOLDER (THIS IS THE DELETE; IT FAILS FIRST IF DELETES ARE NOT ALLOWED)
    try:
        # ITERATE OVER THE SOURCES
        for path_str in source_file_path_str_list:
            # MOVE THE SOURCE
            shutil.move(path_str, f"{backup_path_str_in}{os.path.basename(path_str)}")
            moved_path_str_list.append(path_str)
    # IF A MOVE FAILS (NOTHING HAS BEEN WRITTEN YET)
    except OSError as move_error:
        # MOVE BACK THE SOURCES ALREADY MOVED
        move_back_files(backup_path_str_in, moved_path_str_list)
        # RETURN A ROLLBACK
        result_dict.update(status_str="rolled_back", message_str=f"could not remove {os.path.basename(path_str)} ({move_error}); nothing changed")
        print(f"\t❌ {result_dict['message_str']}") if alert_in else None
        return result_dict
    # TRY TO WRITE AND VERIFY THE TARGET
    try:
        # WRITE THE TARGET
        write_text_pdf_atomic(merged_pdf, target_file_path_str_in)
        # VERIFY THE WRITTEN TARGET (EVERY INPUT MINUTE PRESENT, NO DUPLICATE)
        check_pdf = read_ohlcv_text_pdf(target_file_path_str_in)
        expected_ts_set = set(pd.concat([text_pdf[TS_KEY_COL_STR] for text_pdf in text_pdf_list]))
        # IF THE VERIFICATION FAILS
        if set(check_pdf[TS_KEY_COL_STR]) != expected_ts_set or check_pdf[TS_KEY_COL_STR].duplicated().any():
            # RAISE AN ERROR
            raise ValueError("verification of the written target failed")
    # IF THE WRITE OR THE VERIFICATION FAILS
    except (OSError, ValueError) as write_error:
        # RESTORE THE TARGET (OR REMOVE THE NEW ONE) AND MOVE THE SOURCES BACK
        if target_exists_bool:
            shutil.copy2(f"{backup_path_str_in}{os.path.basename(target_file_path_str_in)}", target_file_path_str_in)
        elif os.path.isfile(target_file_path_str_in):
            os.remove(target_file_path_str_in)
        move_back_files(backup_path_str_in, moved_path_str_list)
        # RETURN A FAILURE
        result_dict.update(status_str="failed", message_str=f"{write_error}; target and sources restored")
        print(f"\t❌ {result_dict['message_str']}") if alert_in else None
        return result_dict
    # RETURN A SUCCESS
    result_dict.update(status_str="merged")
    print(f"\t✅ Written and verified; {len(moved_path_str_list)} source file(s) moved to the backup folder {backup_path_str_in}") if alert_in else None
    return result_dict

# FUNCTION: MOVE FILES BACK FROM A BACKUP FOLDER
def move_back_files(backup_path_str_in, original_file_path_str_list_in):
    """
    Moves files back from the backup folder to their original paths.

    Args:
        backup_path_str_in (str): Backup folder
        original_file_path_str_list_in (list[str]): Original paths of the moved files
    """
    # ITERATE OVER THE FILES
    for path_str in original_file_path_str_list_in:
        # MOVE THE FILE BACK
        shutil.move(f"{backup_path_str_in}{os.path.basename(path_str)}", path_str)
