import argparse
import csv
import hashlib
import os
import subprocess
import sys
from datetime import datetime
# IMPORT THE INGEST CONFIGURATION (FOLDER NAMES LIVE THERE)
from ingest import config
from ingest import index_config
# IMPORT THE RESEARCH PATHS (THE RAW-ZONE FOLDER NAME IS TAKEN FROM THERE)
from so import paths as so_paths

"""
Rename the SPY and VIX / VIX3M raw and staging folders, and the sibling backups of each raw folder.

A rename is a move (os.rename), never a copy and never a delete. File bytes are not opened for writing.
Dry run by default: it writes a manifest and prints the rollback commands, and it does not rename.
--apply renames, reads the new folders, and prints "identical" or the differences against the manifest.

    python scripts/rename_data_folders.py
    python scripts/rename_data_folders.py --apply
    python scripts/rename_data_folders.py --data-root C:/temp/fake_data/

The manifest (relative path, size, CSV row count, SHA-256) is written in the working directory before any rename.
If a new folder already exists, or the dry run finds any other problem, nothing is renamed and no manifest is written.
If a download or merge script may be running, the script stops unless you type YES.
"""

# DEFINE THE SCRIPT NAMES THAT DOWNLOAD OR MERGE (A RUNNING ONE BLOCKS THE RENAME UNTIL YOU CONFIRM)
JOB_SCRIPT_NAME_LIST = ["download_ibkr_ohlcv.py", "download_ibkr_index.py", "merge_staging_into_raw.py", "merge_index_staging_into_raw.py"]
# DEFINE THE MANIFEST COLUMNS
MANIFEST_COL_STR_LIST = ["relative_path", "size_bytes", "row_count", "sha256"]


# FUNCTION: GET THE RAW ZONE UNDER A DATA ROOT
def get_rawzone_path_str(data_root_str_in):
    """
    Args:
        data_root_str_in (str): Data root (the share, or a temporary folder)

    Returns:
        str: store01_rawzone folder ending with "/"
    """
    # TAKE THE RAW-ZONE FOLDER NAME FROM THE RESEARCH PATH (store01_rawzone/...)
    relative_str = so_paths.LOCAL_OHLCV_DATA_FILE_PATH_STR[len(so_paths.LOCAL_PATH_STR):]
    rawzone_name_str = relative_str.split("/")[0]
    # RETURN THE RAW ZONE UNDER THE GIVEN ROOT
    return data_root_str_in.replace("\\", "/").rstrip("/") + f"/{rawzone_name_str}/"


# FUNCTION: GET THE FOUR FOLDER RENAMES
def get_folder_pair_list():
    """
    Returns:
        list[tuple]: (old name, new name, True when sibling <old>_backup_* folders are renamed with the raw folder)
    """
    # RETURN RAW THEN STAGING FOR SPY AND FOR THE INDICES
    return [(config.SPY_LEGACY_RAW_FOLDER_NAME_STR, config.SPY_RAW_FOLDER_NAME_STR, True),
            (config.SPY_LEGACY_STAGING_FOLDER_NAME_STR, config.SPY_STAGING_FOLDER_NAME_STR, False),
            (index_config.INDEX_LEGACY_RAW_FOLDER_NAME_STR, index_config.INDEX_RAW_FOLDER_NAME_STR, True),
            (index_config.INDEX_LEGACY_STAGING_FOLDER_NAME_STR, index_config.INDEX_STAGING_FOLDER_NAME_STR, False)]


# FUNCTION: LIST THE FOLDERS THAT WILL BE RENAMED
def get_rename_plan_dict_list(rawzone_path_str_in):
    """
    Args:
        rawzone_path_str_in (str): Raw zone ending with "/"

    Returns:
        tuple: (plan dicts with old_name_str, new_name_str, old_path_str, new_path_str; old names that are not present)
    """
    # LIST TO HOLD THE PLAN AND THE NAMES THAT ARE NOT ON DISK
    plan_dict_list, skipped_name_list = [], []
    # LIST THE RAW ZONE ONCE (BACKUP FOLDERS ARE SIBLINGS OF THE RAW FOLDER)
    entry_name_list = sorted(os.listdir(rawzone_path_str_in))
    # ITERATE OVER THE FOUR FOLDERS
    for old_name_str, new_name_str, backup_bool in get_folder_pair_list():
        # DEFINE THE OLD AND NEW PATHS
        old_path_str = f"{rawzone_path_str_in}{old_name_str}"
        # KEEP A FOLDER THAT IS PRESENT
        if os.path.isdir(old_path_str):
            plan_dict_list.append({"old_name_str": old_name_str, "new_name_str": new_name_str, "old_path_str": old_path_str,
                                   "new_path_str": f"{rawzone_path_str_in}{new_name_str}"})
        # RECORD A FOLDER THAT IS NOT PRESENT
        else:
            skipped_name_list.append(old_name_str)
        # SIBLING BACKUPS ARE NAMED <raw folder>_backup_... AND TAKE THE NEW RAW FOLDER'S PREFIX
        if backup_bool:
            prefix_str = f"{old_name_str}_backup_"
            for entry_name_str in entry_name_list:
                # KEEP A BACKUP DIRECTORY OF THIS RAW FOLDER
                if entry_name_str.startswith(prefix_str) and os.path.isdir(f"{rawzone_path_str_in}{entry_name_str}"):
                    suffix_str = entry_name_str[len(old_name_str):]
                    plan_dict_list.append({"old_name_str": entry_name_str, "new_name_str": f"{new_name_str}{suffix_str}",
                                           "old_path_str": f"{rawzone_path_str_in}{entry_name_str}",
                                           "new_path_str": f"{rawzone_path_str_in}{new_name_str}{suffix_str}"})
    # RETURN THE PLAN AND THE SKIPPED NAMES
    return plan_dict_list, skipped_name_list


# FUNCTION: FIND REASONS TO REFUSE THE RENAME
def get_plan_problem_str_list(plan_dict_list_in):
    """
    Args:
        plan_dict_list_in (list[dict]): Rename plan

    Returns:
        list[str]: Problems (a new folder already exists, or two folders would land on the same name)
    """
    # LIST TO HOLD THE PROBLEMS
    problem_str_list, new_name_set = [], set()
    # ITERATE OVER THE PLAN
    for plan_dict in plan_dict_list_in:
        # TWO SOURCES MUST NOT SHARE A DESTINATION
        if plan_dict["new_name_str"] in new_name_set:
            problem_str_list.append(f"Two folders would be renamed to {plan_dict['new_name_str']}")
        # RECORD THE DESTINATION NAME
        new_name_set.add(plan_dict["new_name_str"])
        # A DESTINATION THAT ALREADY EXISTS WOULD MAKE os.rename FAIL, SO REFUSE BEFORE ANY MOVE
        if os.path.exists(plan_dict["new_path_str"]):
            problem_str_list.append(f"New folder already exists: {plan_dict['new_path_str']}")
    # RETURN THE PROBLEMS
    return problem_str_list


# FUNCTION: LIST PYTHON COMMAND LINES THAT LOOK LIKE A DOWNLOAD OR A MERGE
def get_running_job_str_list():
    """
    Returns:
        list[str]: Command lines (one entry describing the failure when the process list cannot be read)
    """
    # ASK WINDOWS FOR EVERY PYTHON COMMAND LINE
    result = subprocess.run(["powershell", "-NoProfile", "-Command",
                             "Get-CimInstance Win32_Process | Where-Object { $_.Name -like 'python*' } | Select-Object -ExpandProperty CommandLine"],
                            capture_output=True, text=True, encoding="utf-8", errors="replace")
    # IF THE PROCESS LIST CANNOT BE READ, TREAT THAT AS A POSSIBLE RUNNING JOB
    if result.returncode != 0:
        detail_str = (result.stderr or result.stdout or "no detail").strip()
        return [f"Could not list processes: {detail_str}"]
    # KEEP THE LINES THAT NAME A DOWNLOAD OR A MERGE SCRIPT
    return [line_str.strip() for line_str in result.stdout.splitlines()
            if line_str.strip() and any(name_str in line_str for name_str in JOB_SCRIPT_NAME_LIST)]


# FUNCTION: ASK BEFORE CONTINUING WHEN A DOWNLOAD OR A MERGE MAY BE RUNNING
def confirm_running_job_bool(job_str_list_in):
    """
    Args:
        job_str_list_in (list[str]): Command lines that may be a download or a merge

    Returns:
        bool: True when the rename may continue
    """
    # NO MATCHING PROCESS
    if not job_str_list_in:
        # CONTINUE
        return True
    # SHOW THE PROCESSES
    print("A download or merge may be running:")
    for job_str in job_str_list_in:
        print(f"  {job_str}")
    # A NON-INTERACTIVE RUN CANNOT CONFIRM
    if not sys.stdin.isatty():
        # REFUSE
        print("Stopped. Nothing was changed. Re-run in an interactive terminal and type YES to continue.")
        return False
    # ASK FOR CONFIRMATION
    answer_str = input("Type YES to continue: ")
    # ACCEPT ONLY AN EXACT YES
    if answer_str.strip() != "YES":
        # REFUSE
        print("Stopped. Nothing was changed.")
        return False
    # CONTINUE
    return True


# FUNCTION: READ ONE FILE'S SIZE, CSV ROW COUNT AND SHA-256
def get_file_record_dict(file_path_str_in, relative_path_str_in):
    """
    Args:
        file_path_str_in (str): File to read (bytes are not written)
        relative_path_str_in (str): Path relative to the raw zone, forward slashes

    Returns:
        dict: relative_path, size_bytes, row_count (CSV data rows, else ""), sha256
    """
    # HASH THE BYTES AND COUNT THE LINES IN ONE READ
    hasher = hashlib.sha256()
    size_int, newline_count_int, ends_with_newline_bool = 0, 0, True
    with open(file_path_str_in, "rb") as file_object:
        while True:
            chunk_bytes = file_object.read(1024 * 1024)
            # STOP AT THE END OF THE FILE
            if not chunk_bytes:
                break
            hasher.update(chunk_bytes)
            size_int += len(chunk_bytes)
            newline_count_int += chunk_bytes.count(b"\n")
            ends_with_newline_bool = chunk_bytes.endswith(b"\n")
    # A LAST LINE WITHOUT A NEWLINE STILL COUNTS AS A LINE
    line_count_int = newline_count_int + (1 if size_int and not ends_with_newline_bool else 0)
    # CSV ROW COUNT IS THE LINES MINUS THE HEADER (OTHER FILES LEAVE THE CELL EMPTY)
    row_count_str = str(max(line_count_int - 1, 0)) if relative_path_str_in.lower().endswith(".csv") else ""
    # RETURN THE RECORD
    return {"relative_path": relative_path_str_in, "size_bytes": str(size_int), "row_count": row_count_str, "sha256": hasher.hexdigest()}


# FUNCTION: BUILD THE MANIFEST ROWS OF EVERY FILE UNDER THE PLANNED FOLDERS
def get_manifest_row_dict_list(rawzone_path_str_in, plan_dict_list_in):
    """
    Args:
        rawzone_path_str_in (str): Raw zone ending with "/"
        plan_dict_list_in (list[dict]): Rename plan

    Returns:
        list[dict]: One row per file, sorted by relative path
    """
    # LIST TO HOLD THE ROWS
    row_dict_list = []
    # ITERATE OVER THE FOLDERS
    for plan_dict in plan_dict_list_in:
        print(f"Reading {plan_dict['old_name_str']}")
        for dir_path_str, dir_name_list, file_name_list in os.walk(plan_dict["old_path_str"]):
            # WALK IN A STABLE ORDER
            dir_name_list.sort()
            for file_name_str in sorted(file_name_list):
                file_path_str = os.path.join(dir_path_str, file_name_str).replace("\\", "/")
                relative_path_str = os.path.relpath(file_path_str, rawzone_path_str_in).replace("\\", "/")
                row_dict_list.append(get_file_record_dict(file_path_str, relative_path_str))
    # RETURN THE ROWS IN PATH ORDER
    return sorted(row_dict_list, key=lambda row_dict: row_dict["relative_path"])


# FUNCTION: WRITE THE MANIFEST CSV
def write_manifest_csv(manifest_path_str_in, row_dict_list_in):
    """
    Args:
        manifest_path_str_in (str): CSV path (created or replaced)
        row_dict_list_in (list[dict]): Manifest rows
    """
    # WRITE THE HEADER AND THE ROWS (CRLF)
    with open(manifest_path_str_in, "w", newline="") as file_object:
        writer = csv.DictWriter(file_object, fieldnames=MANIFEST_COL_STR_LIST, lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(row_dict_list_in)


# FUNCTION: TRANSLATE A MANIFEST PATH THROUGH THE RENAME
def get_translated_relative_path_str(relative_path_str_in, name_pair_list_in):
    """
    Args:
        relative_path_str_in (str): Path relative to the raw zone
        name_pair_list_in (list[tuple]): (old folder name, new folder name), top folder only

    Returns:
        str: The same path with the top folder renamed
    """
    # SPLIT THE TOP FOLDER FROM THE REST
    top_str, _, rest_str = relative_path_str_in.partition("/")
    # FIND THE RENAME OF THAT TOP FOLDER
    for old_name_str, new_name_str in name_pair_list_in:
        if top_str == old_name_str:
            # RETURN THE TRANSLATED PATH
            return f"{new_name_str}/{rest_str}" if rest_str else new_name_str
    # RETURN THE PATH UNCHANGED WHEN IT IS NOT ONE OF THE RENAMED FOLDERS
    return relative_path_str_in


# FUNCTION: COMPARE THE MANIFEST WITH THE FOLDERS AFTER THE RENAME
def get_manifest_difference_str_list(old_row_dict_list_in, new_row_dict_list_in, name_pair_list_in):
    """
    Args:
        old_row_dict_list_in (list[dict]): Manifest rows (old relative paths)
        new_row_dict_list_in (list[dict]): Rows read from the new folders
        name_pair_list_in (list[tuple]): (old folder name, new folder name)

    Returns:
        list[str]: Differences (empty when the trees match)
    """
    # INDEX BOTH SIDES BY RELATIVE PATH
    old_dict = {get_translated_relative_path_str(row_dict["relative_path"], name_pair_list_in): row_dict for row_dict in old_row_dict_list_in}
    new_dict = {row_dict["relative_path"]: row_dict for row_dict in new_row_dict_list_in}
    # LIST TO HOLD THE DIFFERENCES
    difference_str_list = []
    # A FILE IN THE MANIFEST MUST BE IN THE NEW TREE WITH THE SAME SIZE, ROW COUNT AND HASH
    for relative_path_str in sorted(old_dict):
        if relative_path_str not in new_dict:
            difference_str_list.append(f"missing {relative_path_str}")
            continue
        for col_str in ["size_bytes", "row_count", "sha256"]:
            if old_dict[relative_path_str][col_str] != new_dict[relative_path_str][col_str]:
                difference_str_list.append(f"{relative_path_str} {col_str} {old_dict[relative_path_str][col_str]} -> {new_dict[relative_path_str][col_str]}")
    # A FILE IN THE NEW TREE MUST HAVE BEEN IN THE MANIFEST
    for relative_path_str in sorted(set(new_dict) - set(old_dict)):
        difference_str_list.append(f"extra {relative_path_str}")
    # RETURN THE DIFFERENCES
    return difference_str_list


# FUNCTION: BUILD THE POWERSHELL COMMAND THAT UNDOES ONE RENAME
def get_rollback_command_str(plan_dict_in):
    """
    Args:
        plan_dict_in (dict): One planned rename

    Returns:
        str: Rename-Item command (new path back to the old name)
    """
    # POWERSHELL LITERAL PATHS USE BACKSLASHES
    literal_path_str = plan_dict_in["new_path_str"].replace("/", "\\")
    # RETURN THE COMMAND
    return f"Rename-Item -LiteralPath '{literal_path_str}' -NewName '{plan_dict_in['old_name_str']}'"


# FUNCTION: MAIN
def main():
    """
    Parses the arguments, writes the manifest, and renames only when --apply is set and no problem was found.

    Returns:
        int: 0 when the run is clean, 1 when it refused or the trees differ
    """
    # PARSE THE ARGUMENTS
    parser = argparse.ArgumentParser(description="Rename the SPY and VIX / VIX3M data folders (dry run by default, move only).")
    parser.add_argument("--apply", action="store_true", help="rename the folders (default: dry run, no rename)")
    parser.add_argument("--data-root", default=config.DATA_ROOT_PATH_STR, help="data root (default: SO_INGEST_DATA_PATH or the share)")
    parser.add_argument("--manifest", default="", help="manifest CSV path (default: rename_data_folders_manifest_<time>.csv in the working directory)")
    args = parser.parse_args()
    # DEFINE THE RAW ZONE
    rawzone_path_str = get_rawzone_path_str(args.data_root)
    print(f"Data root: {args.data_root.replace(chr(92), '/').rstrip('/')}/")
    print(f"Raw zone:  {rawzone_path_str}")
    # REFUSE WHEN THE RAW ZONE IS NOT THERE
    if not os.path.isdir(rawzone_path_str):
        print(f"Raw zone not found: {rawzone_path_str}")
        print("Nothing was changed.")
        return 1
    # BUILD THE PLAN
    plan_dict_list, skipped_name_list = get_rename_plan_dict_list(rawzone_path_str)
    print(f"Not present (skipped): {skipped_name_list or 'none'}")
    # STOP WHEN THERE IS NOTHING TO RENAME
    if not plan_dict_list:
        print("No previous folders to rename. Nothing was changed.")
        return 0
    # REFUSE WHEN A NEW FOLDER ALREADY EXISTS OR TWO RENAMES COLLIDE
    problem_str_list = get_plan_problem_str_list(plan_dict_list)
    if problem_str_list:
        for problem_str in problem_str_list:
            print(problem_str)
        print("Nothing was changed.")
        return 1
    # REFUSE WHEN A DOWNLOAD OR A MERGE MAY BE RUNNING, UNLESS YOU CONFIRM
    if not confirm_running_job_bool(get_running_job_str_list()):
        return 1
    # READ THE OLD FOLDERS (THIS DOES NOT WRITE THEM)
    row_dict_list = get_manifest_row_dict_list(rawzone_path_str, plan_dict_list)
    # DEFINE THE MANIFEST PATH
    manifest_path_str = args.manifest.replace("\\", "/") if args.manifest else \
        os.getcwd().replace("\\", "/") + f"/rename_data_folders_manifest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    # WRITE THE MANIFEST BEFORE ANY RENAME
    write_manifest_csv(manifest_path_str, row_dict_list)
    print(f"Manifest: {manifest_path_str} ({len(row_dict_list)} file(s))")
    # SHOW EACH RENAME AND THE COMMAND THAT UNDOES IT
    print("Rollback (after a rename):")
    for plan_dict in plan_dict_list:
        print(f"  {plan_dict['old_name_str']} -> {plan_dict['new_name_str']}")
        print(f"  {get_rollback_command_str(plan_dict)}")
    # A DRY RUN STOPS HERE
    if not args.apply:
        print("Dry run: nothing was renamed.")
        return 0
    # RENAME EACH FOLDER (A MOVE ON THE SAME VOLUME)
    done_dict_list = []
    for plan_dict in plan_dict_list:
        try:
            os.rename(plan_dict["old_path_str"], plan_dict["new_path_str"])
        except OSError as error:
            print(f"Rename failed: {plan_dict['old_path_str']} -> {plan_dict['new_path_str']}: {error}")
            print("Rollback for the folders already renamed:")
            for done_dict in done_dict_list:
                print(get_rollback_command_str(done_dict))
            return 1
        done_dict_list.append(plan_dict)
        print(f"Renamed {plan_dict['old_name_str']} -> {plan_dict['new_name_str']}")
    # READ THE NEW FOLDERS AND COMPARE THEM WITH THE MANIFEST
    new_row_dict_list = get_manifest_row_dict_list(rawzone_path_str, [{"old_name_str": plan_dict["new_name_str"], "old_path_str": plan_dict["new_path_str"]}
                                                                       for plan_dict in plan_dict_list])
    name_pair_list = [(plan_dict["old_name_str"], plan_dict["new_name_str"]) for plan_dict in plan_dict_list]
    difference_str_list = get_manifest_difference_str_list(row_dict_list, new_row_dict_list, name_pair_list)
    # THE OLD PATH MUST BE GONE AND THE NEW PATH MUST BE A FOLDER
    for plan_dict in plan_dict_list:
        if os.path.exists(plan_dict["old_path_str"]) or not os.path.isdir(plan_dict["new_path_str"]):
            difference_str_list.append(f"folder {plan_dict['old_name_str']} -> {plan_dict['new_name_str']}")
    # PRINT THE COMPARISON
    if difference_str_list:
        print("Differences:")
        for difference_str in difference_str_list:
            print(f"  {difference_str}")
        return 1
    print("identical")
    return 0


# IF THE FILE IS RUN DIRECTLY
if __name__ == "__main__":
    # RUN THE MAIN FUNCTION
    raise SystemExit(main())
