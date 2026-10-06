import os
import shutil
from datetime import datetime, timezone
import pandas as pd
# IMPORT THE RESEARCH WORKSPACE'S BAD TICK RULE AND CANDLESTICK SANITY CHECK (THE SAME CHECKS AS PIPELINE STEP 00)
from so.core.bad_ticks import get_bad_tick_pdf
from so.features.ohlcv_data_utils import get_ohlcv_sanity_issue_pdf
# IMPORT THE INGEST CONFIGURATION
from ingest import config
# IMPORT THE SESSION FUNCTIONS
from ingest.sessions import get_ny_now_ts, get_session_pdf, get_closed_session_pdf, get_session_check_dict, get_ohlcv_issue_pdf
# IMPORT THE RAW FILE FUNCTIONS
from ingest.raw_files import (TS_KEY_COL_STR, get_file_name_str, get_ohlcv_file_pdf, get_ts_utc_series, read_ohlcv_text_pdf,
                              write_text_pdf_atomic, append_log_rows, get_present_date_set, get_organized_period_str,
                              get_organized_file_period_str, get_orphan_file_period_str, get_merged_text_pdf_tup,
                              get_backup_path_str, merge_files_into_target_dict)

"""
Raw Folder Maintenance: merge staging into raw, organize, clean orphans, rebuild, find issues

Every function that changes files is a DRY RUN unless apply_bool_in is True, and follows the rules of ingest.raw_files
(text kept, existing rows win, backups, atomic writes, all or nothing).

    merge_staging_into_raw_pdf   ADD-ONLY: copies checked staging day files into the raw folder as new day files. It never
                                 changes an existing raw file and never adds a date the raw folder already has.
    organize_raw_pdf             replaces organize_ohlcv_data.py: day files of closed months -> month files, month files of
                                 closed years -> year files.
    clean_raw_orphans_pdf        replaces clean_ohlcv_data.py: folds left-over day / month files into the month / year file
                                 that already exists for them.
    rebuild_raw_pdf              replaces rebuild_ohlcv_data.py: rewrites a whole folder into the organized layout, into a
                                 DIFFERENT folder by default.
    get_raw_issue_pdf            replaces find_ohlcv_data_issues.py: missing sessions, missing / extra minutes, duplicates,
                                 bars on non-session dates.

Research note: the research pipeline (stock_overflow_workspace, so.core.raw_data) reads every CSV of the raw folder. A merge
or a roll-up changes its input; re-run pipeline step 00 afterwards, and the repo's bad tick check
(python scripts/fix_raw_bad_ticks.py, dry run first) when new days were added.
"""

# DEFINE THE MERGE LOG COLUMNS
MERGE_LOG_COL_STR_LIST = ["merged_at_utc", "date", "status_str", "bar_count_int", "missing_count_int", "bad_tick_count_int",
                          "staging_file_str", "raw_file_str"]

"""
Merge Staging Into Raw
"""

# FUNCTION: CHECK AND MERGE THE STAGING DAY FILES INTO THE RAW FOLDER
def merge_staging_into_raw_pdf(apply_bool_in=False, include_partial_bool_in=False, staging_path_str_in=config.STAGING_OHLCV_PATH_STR,
                               raw_path_str_in=config.RAW_OHLCV_PATH_STR, merged_path_str_in=config.STAGING_MERGED_PATH_STR,
                               log_file_path_str_in=config.MERGE_LOG_FILE_PATH_STR, now_ny_ts_in=None, alert_in=True):
    """
    Checks every staging day file (session schedule, candlestick sanity, bad ticks, already in raw) and, with
    apply_bool_in, copies the accepted ones into the raw folder as new day files, then moves them to the merged folder.

    Args:
        apply_bool_in (bool): False = dry run
        include_partial_bool_in (bool): Also add sessions with missing minutes (IBKR gaps)
        staging_path_str_in (str): Staging folder
        raw_path_str_in (str): Raw folder
        merged_path_str_in (str): Where merged staging files are moved
        log_file_path_str_in (str): Merge log
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)
        alert_in (bool): Display information

    Returns:
        pd.DataFrame: One row per staging file: date, check_status_str, sanity_issue_count_int, bad_tick_count_int,
                      in_raw_bool, decision_str ("add", "added", "skip: ..."), file_name_str
    """
    # COLLECT THE STAGING DAY FILES
    file_pdf = get_ohlcv_file_pdf(staging_path_str_in, alert_in=False)
    file_pdf = file_pdf[file_pdf["period_kind_str"] == "day"].reset_index(drop=True)
    # IF THERE ARE NO FILES
    if file_pdf.empty:
        # DISPLAY INFORMATION AND RETURN
        print(f"ℹ️ No staging day files in {staging_path_str_in}") if alert_in else None
        return pd.DataFrame()
    # COLLECT THE SESSIONS AND THE DATES ALREADY IN RAW
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    session_pdf = get_closed_session_pdf(get_session_pdf(file_pdf["first_date"].min(), file_pdf["first_date"].max()), now_ny_ts)
    session_row_dict = {session_row.date: session_row for session_row in session_pdf.itertuples(index=False)}
    raw_date_set = get_present_date_set([raw_path_str_in], file_pdf["first_date"].min(), file_pdf["first_date"].max())
    # LIST TO HOLD THE RESULTS AND THE LOG ROWS
    row_dict_list, log_dict_list = [], []
    # ITERATE OVER THE STAGING FILES
    for file_row in file_pdf.itertuples(index=False):
        # READ THE FILE AS TEXT
        text_pdf = read_ohlcv_text_pdf(file_row.file_path_str)
        # PARSE THE NUMBERS FOR THE CHECKS
        number_pdf = text_pdf.assign(timestamp=text_pdf[TS_KEY_COL_STR].dt.tz_convert(config.NY_TZ_STR))
        number_pdf[["open", "high", "low", "close", "volume"]] = number_pdf[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
        # CHECK THE SESSION MINUTES
        session_row = session_row_dict.get(file_row.first_date)
        check_dict = get_session_check_dict(number_pdf["timestamp"], session_row) if session_row is not None else {"status_str": "no_session", "missing_count_int": 0}
        # CHECK THE CANDLESTICKS AND THE BAD TICKS (REPO RULES)
        sanity_issue_count_int = len(get_ohlcv_sanity_issue_pdf(number_pdf))
        bad_tick_count_int = len(get_bad_tick_pdf(number_pdf))
        # DEFINE WHETHER THE DATE IS ALREADY IN RAW (OR A FILE OF THE SAME NAME EXISTS)
        raw_file_path_str = f"{raw_path_str_in}{file_row.file_name_str}"
        in_raw_bool = file_row.first_date in raw_date_set or os.path.isfile(raw_file_path_str)
        # DECIDE
        if in_raw_bool:
            decision_str = "skip: date already in raw (raw data is never replaced)"
        elif check_dict["status_str"] == "no_session":
            decision_str = "skip: not a closed NYSE session"
        elif sanity_issue_count_int:
            decision_str = "skip: candlestick sanity issues"
        elif check_dict["status_str"] == "partial" and not include_partial_bool_in:
            decision_str = f"skip: partial ({check_dict['missing_count_int']} missing minutes; use --include-partial)"
        else:
            decision_str = "add"
        # IF THE FILE MUST BE ADDED FOR REAL
        if decision_str == "add" and apply_bool_in:
            # COPY IT INTO RAW (TEMPORARY NAME, THEN RENAME: THE FILE APPEARS COMPLETE OR NOT AT ALL)
            shutil.copy2(file_row.file_path_str, f"{raw_file_path_str}.tmp")
            os.replace(f"{raw_file_path_str}.tmp", raw_file_path_str)
            # VERIFY THE COPY
            if not read_ohlcv_text_pdf(raw_file_path_str)[TS_KEY_COL_STR].equals(text_pdf[TS_KEY_COL_STR]):
                raise OSError(f"verification of {raw_file_path_str} failed")
            # MOVE THE STAGING FILE TO THE MERGED FOLDER
            os.makedirs(merged_path_str_in, exist_ok=True)
            shutil.move(file_row.file_path_str, f"{merged_path_str_in}{file_row.file_name_str}")
            decision_str = "added"
            # STORE THE LOG ROW
            log_dict_list.append({"merged_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "date": str(file_row.first_date),
                                  "status_str": check_dict["status_str"], "bar_count_int": len(text_pdf), "missing_count_int": check_dict["missing_count_int"],
                                  "bad_tick_count_int": bad_tick_count_int, "staging_file_str": file_row.file_name_str, "raw_file_str": file_row.file_name_str})
        # STORE THE RESULT
        row_dict_list.append({"date": file_row.first_date, "check_status_str": check_dict["status_str"], "bar_count_int": len(text_pdf),
                              "missing_count_int": check_dict["missing_count_int"], "sanity_issue_count_int": sanity_issue_count_int,
                              "bad_tick_count_int": bad_tick_count_int, "in_raw_bool": in_raw_bool, "decision_str": decision_str,
                              "file_name_str": file_row.file_name_str})
    # WRITE THE MERGE LOG
    append_log_rows(log_dict_list, log_file_path_str_in, MERGE_LOG_COL_STR_LIST)
    # CREATE THE RESULT DATAFRAME
    result_pdf = pd.DataFrame(row_dict_list)
    # DISPLAY THE SUMMARY
    if alert_in:
        print(("" if apply_bool_in else "🔎 [DRY RUN] ") + "Staging -> raw: " + ", ".join(f"{k}: {v}" for k, v in result_pdf["decision_str"].value_counts().items()))
        print(f"⚠️ {int(result_pdf['bad_tick_count_int'].sum())} bad tick(s) in the checked files: after adding them, run the repo's "
              f"'python scripts/fix_raw_bad_ticks.py' (dry run first).") if result_pdf["bad_tick_count_int"].sum() else None
        print("ℹ️ Re-run pipeline step 00 in stock_overflow_workspace: the raw folder changed.") if apply_bool_in and (result_pdf["decision_str"] == "added").any() else None
    # RETURN THE RESULT
    return result_pdf

"""
Organize And Clean
"""

# FUNCTION: MERGE FILE GROUPS INTO THEIR TARGETS
def merge_file_groups_pdf(file_pdf_in, target_period_str_list_in, path_str_in, apply_bool_in, alert_in=True):
    """
    Groups the files whose target period differs from their own and merges each group into its target file.

    Args:
        file_pdf_in (pd.DataFrame): Files (get_ohlcv_file_pdf)
        target_period_str_list_in (list[str]): Target period of each file (same order)
        path_str_in (str): Folder
        apply_bool_in (bool): False = dry run
        alert_in (bool): Display information

    Returns:
        pd.DataFrame: One row per target (merge_files_into_target_dict results)
    """
    # ADD THE TARGETS AND KEEP THE FILES THAT MOVE
    move_pdf = file_pdf_in.assign(target_period_str=target_period_str_list_in)
    move_pdf = move_pdf[move_pdf["target_period_str"] != move_pdf["period_str"]]
    # IF NOTHING MOVES
    if move_pdf.empty:
        # DISPLAY INFORMATION AND RETURN
        print("✅ Nothing to do: every file is already where it belongs.") if alert_in else None
        return pd.DataFrame()
    # DEFINE THE BACKUP FOLDER OF THIS RUN
    backup_path_str = get_backup_path_str(path_str_in)
    # MERGE EVERY GROUP (OLDEST TARGET FIRST)
    result_dict_list = [merge_files_into_target_dict(f"{path_str_in}{get_file_name_str(target_period_str)}", group_pdf["file_path_str"].tolist(),
                                                     apply_bool_in, backup_path_str, alert_in)
                        for target_period_str, group_pdf in move_pdf.groupby("target_period_str", sort=True)]
    # RETURN THE RESULTS
    return pd.DataFrame(result_dict_list)

# FUNCTION: ORGANIZE THE RAW FOLDER
def organize_raw_pdf(apply_bool_in=False, raw_path_str_in=config.RAW_OHLCV_PATH_STR, now_ny_ts_in=None, alert_in=True):
    """
    Rolls day files of closed months into month files, and day / month files of closed years into year files.

    Args:
        apply_bool_in (bool): False = dry run
        raw_path_str_in (str): Raw folder
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)
        alert_in (bool): Display information

    Returns:
        pd.DataFrame: One row per target file
    """
    # COLLECT THE CURRENT NEW YORK TIME AND THE FILES
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    file_pdf = get_ohlcv_file_pdf(raw_path_str_in, alert_in)
    # DISPLAY INFORMATION
    print(f"{'' if apply_bool_in else '🔎 [DRY RUN] '}Organize {raw_path_str_in} (New York time {now_ny_ts:%Y-%m-%d %H:%M}, {len(file_pdf)} files)") if alert_in else None
    # MERGE THE GROUPS
    return merge_file_groups_pdf(file_pdf, [get_organized_file_period_str(p, now_ny_ts) for p in file_pdf["period_str"]], raw_path_str_in, apply_bool_in, alert_in)

# FUNCTION: FOLD LEFT-OVER FILES INTO THEIR EXISTING MONTH / YEAR FILE
def clean_raw_orphans_pdf(apply_bool_in=False, raw_path_str_in=config.RAW_OHLCV_PATH_STR, alert_in=True):
    """
    Folds every day / month file whose month / year file already exists into that file (left-overs of an interrupted
    roll-up, which would otherwise be read twice by the pipeline).

    Args:
        apply_bool_in (bool): False = dry run
        raw_path_str_in (str): Raw folder
        alert_in (bool): Display information

    Returns:
        pd.DataFrame: One row per target file
    """
    # COLLECT THE FILES AND THEIR PERIODS
    file_pdf = get_ohlcv_file_pdf(raw_path_str_in, alert_in)
    period_str_set = set(file_pdf["period_str"])
    # DISPLAY INFORMATION
    print(f"{'' if apply_bool_in else '🔎 [DRY RUN] '}Clean orphans in {raw_path_str_in} ({len(file_pdf)} files)") if alert_in else None
    # MERGE THE GROUPS
    return merge_file_groups_pdf(file_pdf, [get_orphan_file_period_str(p, period_str_set) for p in file_pdf["period_str"]], raw_path_str_in, apply_bool_in, alert_in)

"""
Rebuild
"""

# FUNCTION: REBUILD A FOLDER INTO THE ORGANIZED LAYOUT
def rebuild_raw_pdf(source_path_str_in, target_path_str_in, apply_bool_in=False, delete_stale_bool_in=False, now_ny_ts_in=None, alert_in=True):
    """
    Reads every OHLCV file of the source folder, merges them (rows of year files win over month files, which win over day
    files), and writes one file per organized period into the target folder.

    Args:
        source_path_str_in (str): Source folder
        target_path_str_in (str): Target folder (the same folder rebuilds in place: every changed file is backed up first)
        apply_bool_in (bool): False = dry run
        delete_stale_bool_in (bool): Remove (move to the backup folder) target OHLCV files that are not part of the rebuilt set
                                     (always True in place)
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)
        alert_in (bool): Display information

    Returns:
        pd.DataFrame: One row per written file: file_name_str, row_count_int, action_str
    """
    # IN PLACE, THE FILES THAT ARE NOT REBUILT MUST GO (OTHERWISE THEIR ROWS WOULD BE IN TWO FILES)
    delete_stale_bool_in = delete_stale_bool_in or os.path.normcase(os.path.abspath(source_path_str_in)) == os.path.normcase(os.path.abspath(target_path_str_in))
    # COLLECT THE CURRENT NEW YORK TIME AND THE SOURCE FILES (YEAR FILES FIRST: THEIR ROWS WIN)
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    file_pdf = get_ohlcv_file_pdf(source_path_str_in, alert_in)
    file_pdf = file_pdf.assign(_len=file_pdf["period_str"].str.len()).sort_values(["_len", "first_date"])
    # IF THERE ARE NO FILES
    if file_pdf.empty:
        # DISPLAY INFORMATION AND RETURN
        print(f"ℹ️ No OHLCV files in {source_path_str_in}") if alert_in else None
        return pd.DataFrame()
    # READ AND MERGE EVERY FILE
    merged_pdf, info_dict = get_merged_text_pdf_tup([read_ohlcv_text_pdf(path_str) for path_str in file_pdf["file_path_str"]])
    # DISPLAY INFORMATION
    print(f"{'' if apply_bool_in else '🔎 [DRY RUN] '}Rebuild {source_path_str_in} -> {target_path_str_in}: {info_dict['input_row_count']:,} rows read, "
          f"{info_dict['duplicate_row_count']:,} duplicates dropped ({info_dict['conflict_minute_count']:,} with different values, first kept)") if alert_in else None
    # ASSIGN EVERY ROW ITS TARGET FILE
    date_series = merged_pdf[TS_KEY_COL_STR].dt.tz_convert(config.NY_TZ_STR).dt.date
    period_dict = {date_object: get_organized_period_str(date_object, now_ny_ts) for date_object in date_series.unique()}
    merged_pdf["_target_str"] = date_series.map(period_dict).map(get_file_name_str)
    # DEFINE THE BACKUP FOLDER (USED ONLY WHEN A TARGET FILE IS REPLACED OR REMOVED)
    backup_path_str = get_backup_path_str(target_path_str_in)
    # LIST TO HOLD THE RESULTS
    row_dict_list = []
    # ITERATE OVER THE TARGET FILES
    for target_name_str, group_pdf in merged_pdf.groupby("_target_str", sort=True):
        # DEFINE THE TARGET PATH AND THE ACTION
        target_file_path_str = f"{target_path_str_in}{target_name_str}"
        action_str = "replace" if os.path.isfile(target_file_path_str) else "create"
        # IF THIS IS NOT A DRY RUN
        if apply_bool_in:
            # BACK UP A FILE THAT IS REPLACED
            if action_str == "replace":
                os.makedirs(backup_path_str, exist_ok=True)
                shutil.copy2(target_file_path_str, f"{backup_path_str}{target_name_str}")
            # WRITE THE FILE
            write_text_pdf_atomic(group_pdf.drop(columns=["_target_str"]), target_file_path_str)
        # STORE THE RESULT
        row_dict_list.append({"file_name_str": target_name_str, "row_count_int": len(group_pdf), "action_str": action_str})
    # DEFINE THE REBUILT FILE NAMES
    rebuilt_name_set = {row_dict["file_name_str"] for row_dict in row_dict_list}
    # IF STALE FILES MUST BE REMOVED
    if delete_stale_bool_in:
        # ITERATE OVER THE TARGET OHLCV FILES THAT ARE NOT REBUILT
        for file_row in get_ohlcv_file_pdf(target_path_str_in, alert_in=False).itertuples():
            if file_row.file_name_str not in rebuilt_name_set:
                # MOVE IT TO THE BACKUP FOLDER (APPLY) OR REPORT IT (DRY RUN)
                if apply_bool_in:
                    os.makedirs(backup_path_str, exist_ok=True)
                    shutil.move(file_row.file_path_str, f"{backup_path_str}{file_row.file_name_str}")
                row_dict_list.append({"file_name_str": file_row.file_name_str, "row_count_int": 0, "action_str": "remove (stale)"})
    # CREATE THE RESULT DATAFRAME
    result_pdf = pd.DataFrame(row_dict_list)
    # DISPLAY THE SUMMARY
    if alert_in:
        print(result_pdf.to_string(index=False))
        print(f"ℹ️ Replaced / removed files are in {backup_path_str}") if apply_bool_in and (result_pdf["action_str"] != "create").any() else None
    # RETURN THE RESULT
    return result_pdf

"""
Find Issues
"""

# FUNCTION: FIND THE DATA ISSUES OF ONE OR MORE FOLDERS
def get_raw_issue_pdf(path_str_list_in=None, expected_start_date_in=None, now_ny_ts_in=None, alert_in=True):
    """
    Reads the timestamps of every OHLCV file of the folders and reports, for every closed NYSE session since the start:
    fully missing sessions, missing minutes, extra minutes, duplicate rows, and bars on dates that are not sessions.

    Args:
        path_str_list_in (list[str] | None): Folders (None = the raw folder)
        expected_start_date_in (str | date | None): First date the data should cover (None = the first date found; a gap
                                                    at the very start cannot be detected then)
        now_ny_ts_in (pd.Timestamp | None): Current New York time (None = the real clock)
        alert_in (bool): Display the summary

    Returns:
        pd.DataFrame: Problem dates (ingest.sessions.get_ohlcv_issue_pdf)
    """
    # DEFINE THE FOLDERS AND THE CURRENT TIME
    path_str_list = path_str_list_in or [config.RAW_OHLCV_PATH_STR]
    now_ny_ts = now_ny_ts_in if now_ny_ts_in is not None else get_ny_now_ts()
    # READ THE TIMESTAMPS OF EVERY FILE
    timestamp_series_list = [pd.read_csv(path_str, usecols=["timestamp"], dtype=str)["timestamp"]
                             for folder_str in path_str_list for path_str in get_ohlcv_file_pdf(folder_str, alert_in)["file_path_str"]]
    # IF THERE IS NO DATA
    if not timestamp_series_list:
        # DISPLAY INFORMATION AND RETURN
        print("ℹ️ No data found.") if alert_in else None
        return pd.DataFrame()
    # PARSE THE TIMESTAMPS
    ts_series = get_ts_utc_series(pd.concat(timestamp_series_list, ignore_index=True))
    # DEFINE THE START OF THE EXPECTED COVERAGE
    start_date = pd.Timestamp(expected_start_date_in).date() if expected_start_date_in is not None else ts_series.min().tz_convert(config.NY_TZ_STR).date()
    # COLLECT THE CLOSED SESSIONS SINCE THE START
    session_pdf = get_closed_session_pdf(get_session_pdf(start_date, now_ny_ts.date()), now_ny_ts)
    # FIND THE ISSUES (BARS BEFORE THE START ARE IGNORED)
    issue_pdf = get_ohlcv_issue_pdf(ts_series[ts_series.dt.tz_convert(config.NY_TZ_STR).dt.date >= start_date], session_pdf)
    # DISPLAY THE SUMMARY
    if alert_in:
        print(f"Rows: {len(ts_series):,} | sessions expected: {len(session_pdf):,} ({start_date} -> {session_pdf['date'].max() if len(session_pdf) else '-'}) | problem dates: {len(issue_pdf):,}")
        for issue_str in ["missing_session", "missing_minutes", "extra_minutes", "duplicate_rows", "no_session"]:
            date_list = [str(d) for d in issue_pdf.loc[issue_pdf["issue_str"].str.contains(issue_str), "date"]] if not issue_pdf.empty else []
            print(f"  {issue_str} ({len(date_list)}): {date_list[:30]}" + (f" ... (+{len(date_list) - 30} more)" if len(date_list) > 30 else ""))
    # RETURN THE ISSUES
    return issue_pdf
