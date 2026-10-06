import os
import json
from json import JSONDecodeError
from io import BytesIO
import pandas as pd
# IMPORT DATA QUALITY FUNCTIONS OF THE RESEARCH WORKSPACE
from so.core.data_quality import pdf_is_empty

# DEFINE THE SERVICE ACCOUNT KEY PATH (ENVIRONMENT VARIABLE; THE KEY IS NEVER STORED IN THE WORKSPACE OR IN GIT)
KEY_PATH = os.environ.get("SO_GCS_KEY_PATH", "")
# DEFINE BUCKET NAME
BUCKET_NAME = "stock_overflow_bucket"
# DEFINE STORAGE NAME STR
STORAGE_NAME_STR = "Google Cloud Storage"
# DEFINE THE CLIENT AND BUCKET CACHE (CREATED ON FIRST USE, NOT AT IMPORT)
_gcs_cache_dict = {}

"""
Google Cloud Storage helpers (moved from GCS_file_management.py; the functions are unchanged except where noted)

Changes:
    - The client is created on first use (get_gcs_bucket) instead of at import: importing the module no longer fails
      when the key or the network is missing.
    - The key path comes from the environment variable SO_GCS_KEY_PATH (e.g. C:/Users/nico/keys/stock_overflow_gcs.json),
      outside the workspace. The previous key (GCS_key.json in the workspace folder) was shared in a zip: rotate it.
    - pyarrow is imported by pandas when needed (no unused import); pdf_is_empty comes from so.core.data_quality.
"""

# FUNCTION: GET THE BUCKET (CREATES THE CLIENT ON FIRST USE)
def get_gcs_bucket():
    """
    Returns the bucket object, creating the authenticated client on the first call.

    Returns:
        google.cloud.storage.Bucket: The bucket
    """
    # IF THE BUCKET IS NOT CACHED YET
    if "bucket" not in _gcs_cache_dict:
        # IF THE KEY PATH IS NOT SET OR DOES NOT EXIST
        if not KEY_PATH or not os.path.isfile(KEY_PATH):
            # RAISE AN ERROR
            raise FileNotFoundError("Set the environment variable SO_GCS_KEY_PATH to the service account key file (outside the workspace).")
        # IMPORT THE CLIENT LIBRARY
        from google.cloud import storage
        # AUTHENTICATE THE CLIENT AND CACHE THE CLIENT AND THE BUCKET
        _gcs_cache_dict["client"] = storage.Client.from_service_account_json(KEY_PATH)
        _gcs_cache_dict["bucket"] = _gcs_cache_dict["client"].bucket(BUCKET_NAME)
    # RETURN THE BUCKET
    return _gcs_cache_dict["bucket"]

"""
Note: Writes to this bucket will stop after cost reaches or exceeds $5

Commands to run within the GCP console (CLI):

Manually lock bucket
=====================
    curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" https://disable-bucket-writes service-597183480637.us-east1.run.app

Manually unlock bucket
=======================
    curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" https://enable-bucket-writes-service-597183480637.us-east1.run.app

Test budget alert bucket lock
=============================
    gcloud pubsub topics publish billing-budget-alerts --message='{"costAmount": 5.1, "budgetAmount":5.0}'

"""

# FUNCTION: GET BLOB FROM FILE PATH
def _get_blob(file_path_str_in: str):
    """
    Returns a Blob object for the given file path within the bucket.
    Note: Centralizes blob construction to avoid repetition across functions.

    Args:
        file_path_str_in: String path to the blob (blob name within bucket)

    Returns:
        Blob object for the specified path
    """
    # RETURN BLOB FROM BUCKET
    return get_gcs_bucket().blob(file_path_str_in)

# FUNCTION: CHECK IF FILE EXISTS IN BUCKET (GOOGLE CLOUD STORAGE)
def check_file_exists_GCS(file_path_str_in: str, alert_in: bool = False) -> bool:
    """
    Checks if a file exists at the specified path in Google Cloud Storage.

    Args:
        file_path_str_in: String path to check for file
        alert_in: Boolean to control progress messages

    Returns:
        Boolean indicating if file exists
    """
    # CHECK IF THE FILE EXISTS
    file_exists_bool = _get_blob(file_path_str_in).exists()
    # IF ALERT IS ENABLED
    if alert_in:
        # IF FILE EXISTS
        if file_exists_bool:
            # DISPLAY INFORMATION
            print(f"✅ File:\t'{file_path_str_in}' Found!\t(☁️ In {STORAGE_NAME_STR})")
        # IF THE FILE DOES NOT EXIST
        else:
            # DISPLAY INFORMATION
            print(f"⚠️ File:\t'{file_path_str_in}' Not Found!\t(☁️ In {STORAGE_NAME_STR})")
    # RETURN TRUE IF FILE EXISTS
    return file_exists_bool

# FUNCTION: DELETE FILE FROM BUCKET (GOOGLE CLOUD STORAGE)
def delete_file_from_path_GCS(file_path_str_in: str, alert_in: bool = True) -> bool:
    """
    Deletes a file from Google Cloud Storage.

    Args:
        file_path_str_in: The path to the file (blob) to delete in the bucket
        alert_in: Boolean to control progress messages

    Returns:
        Boolean indicating if file was deleted
    """
    # GET BLOB FROM FILE PATH
    blob = _get_blob(file_path_str_in)
    # IF FILE EXISTS
    if blob.exists():
        # DISPLAY INFORMATION
        print(f"✅ File:\t'{file_path_str_in}' Found!\n🗑️ Deleting File...\t(☁️ From {STORAGE_NAME_STR})") if alert_in else None
        # DELETE FILE
        blob.delete()
        # RETURN TRUE
        return True
    # IF THE FILE DOES NOT EXIST
    else:
        # DISPLAY INFORMATION
        print(f"⚠️ File:\t'{file_path_str_in}' Not Found!\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
    # RETURN FALSE
    return False

# FUNCTION: CHECK THAT A STRING HAS A VALID FILE SPECIFIED
def check_valid_file_path(file_path_str_in, required_extension):
    """
    Validates that a file path contains a file with the required extension.

    Args:
        file_path_str_in (str): The full file path to validate
        required_extension (str): The required file extension (without the dot)

    Returns:
        bool: True if validation passes

    Raises:
        Exception: If file path is missing extension or has wrong extension
        
    Example:
        >>> check_valid_file_path("/path/to/file.csv", "csv")
        True
        >>> check_valid_file_path("/path/to/file.txt", "csv") 
        Exception: ⚠️ File extension '.txt' is not valid...
    """
    # GET THE FILE NAME FROM THE PATH
    filename = os.path.basename(file_path_str_in)
    # CHECK IF FILE HAS AN EXTENSION
    if '.' not in filename:
        # RAISE EXCEPTION
        raise Exception(f"⚠️ File Path '{file_path_str_in}' is not valid. Requires a file with extension '.{required_extension}'")
    # GET AND VALIDATE EXTENSION
    extension = filename.split('.')[-1]
    # IF THE REQUIRED EXTENSION IS NOT THE SAME AS THE FILE EXTENSION
    if extension != required_extension:
        # RAISE EXCEPTION
        raise Exception(f"⚠️ File extension '.{extension}' is not valid. Required extension is '.{required_extension}'")
    # RETURN TRUE
    return True

# FUNCTION: LIST FILES IN A DIRECTORY (PREFIX IN GCS)
def get_path_file_list_GCS(file_path_str_in: str) -> list:
    """
    Gets list of files (blobs) in a specified Google Cloud Storage directory (prefix).
    Note: Automatically appends a trailing slash if not present to ensure prefix-based listing.

    Args:
        file_path_str_in: String path to directory/prefix in Google Cloud Storage

    Returns:
        List of full blob paths for all blobs under the given prefix
    """
    # ENSURE TRAILING SLASH FOR DIRECTORY PREFIX IF NOT PRESENT
    prefix = file_path_str_in if file_path_str_in.endswith("/") else file_path_str_in + "/"
    # COLLECT BLOBS FROM PREFIX AND RETURN FULL BLOB PATHS (NAMES)
    return [blob.name for blob in get_gcs_bucket().client.list_blobs(BUCKET_NAME, prefix=prefix)]

# FUNCTION: READ CSV AS DATAFRAME FROM GOOGLE CLOUD STORAGE
def read_csv_file_from_path_GCS(file_path_str_in: str, alert_in: bool = True) -> pd.DataFrame | None:
    """
    Reads a CSV file from Google Cloud Storage into a pandas DataFrame.
    Note: Catches broad read errors in addition to blob-not-found cases,
    returning None and printing a warning in all failure cases.

    Args:
        file_path_str_in: The path to the file in the bucket (blob name)
        alert_in: Boolean to control progress messages

    Returns:
        DataFrame containing CSV data or None if file not found or unreadable
    """
    # CHECK IF THE FILE PATH IS VALID
    check_valid_file_path(file_path_str_in, "csv")
    # ATTEMPT TO READ THE FILE
    try:
        # GET BLOB FROM FILE PATH
        blob = _get_blob(file_path_str_in)
        # CHECK IF THE BLOB EXISTS
        if not blob.exists():
            # DISPLAY INFORMATION
            print(f"⚠️ File:\t'{file_path_str_in}' Not Found!\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
            # RETURN NONE
            return None
        # DOWNLOAD AND READ THE BLOB
        read_pdf = pd.read_csv(BytesIO(blob.download_as_bytes()))
        # DISPLAY INFORMATION
        print(f"✅ File:\t'{file_path_str_in}' Found!\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
        # RETURN THE DATAFRAME
        return read_pdf
    # CATCH EXCEPTION: FILE EXISTS BUT CANNOT BE READ (CORRUPT, ENCODING ERROR, ETC.)
    except Exception as e:
        # DISPLAY INFORMATION
        print(f"⚠️ File:\t'{file_path_str_in}' Could Not Be Read! ({e})\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
        # RETURN NONE
        return None

# FUNCTION: SAVE DATAFRAME AS CSV TO GOOGLE CLOUD STORAGE
def write_csv_file_to_path_GCS(pdf_in: pd.DataFrame, file_path_str_in: str, access_mode_in: str = "I", alert_in: bool = True) -> bool:
    """
    Writes a pandas DataFrame as a CSV file to Google Cloud Storage.
    Note: In 'W' (overwrite) mode the file is never read from storage first — it is written directly.

    Args:
        pdf_in: DataFrame to save
        file_path_str_in: The path (blob name) in the bucket
        access_mode_in: String indicating write mode ('W'=overwrite, 'A'=append, 'N'=no write, 'I'=ignore if exists)
        alert_in: Boolean to control progress messages

    Returns:
        Boolean indicating success
    """
    # COLLECT THE ACCESS MODE
    access_mode_str = access_mode_in.upper()
    # VALIDATE THAT THE ACCESS MODE IS VALID
    if access_mode_str not in ["W", "A", "N", "I"]:
        # RAISE VALUE ERROR
        raise ValueError(f"Access mode '{access_mode_in}' is invalid. Must be one of: 'W', 'A', 'N', 'I'")
    # ACCESS MODE MEANING
    access_mode_dict = {"W": "Overwrite", "A": "Append", "I": "Ignore", "N": "No-Write"}
    # IF access_mode_str IS "N"
    if access_mode_str == "N":
        # DISPLAY INFORMATION
        print("⚠️ Not Saving DataFrame") if alert_in else None
        # EXIT FUNCTION
        return False
    # CHECK IF THE FILE PATH IS VALID
    check_valid_file_path(file_path_str_in, "csv")
    # CHECK IF THE DATAFRAME IS EMPTY
    if pdf_is_empty(pdf_in):
        # EXIT FUNCTION
        return False
    # DISPLAY INFORMATION
    print(f"ℹ️ Saving In {access_mode_dict[access_mode_str]} Mode.\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
    # DEFINE ICON DICTIONARY
    icon_dict = {"Writing": "📝", "Appending": "➕", "Ignoring": "⚠️", "Overwriting": "♻️"}
    # IN OVERWRITE MODE, SKIP READING THE FILE — WRITE DIRECTLY
    if access_mode_str == "W":
        # CHECK IF FILE EXISTS
        file_exists_bool = check_file_exists_GCS(file_path_str_in)
        # SET THE FILE EXISTS AND ACTION STRINGS
        file_exists_str, action_str = ("Exists", "Overwriting") if file_exists_bool else ("Does Not Exist", "Writing")
        # DISPLAY INFORMATION
        print(f"\t{icon_dict[action_str]} File:\t'{file_path_str_in}' {file_exists_str}! {action_str}...") if alert_in else None
        # DEFINE SAVE DATAFRAME AS INPUT DATAFRAME
        save_pdf = pdf_in
    # IN ALL OTHER MODES, READ THE FILE FIRST TO DETERMINE ACTION
    else:
        # READ THE CSV FROM BLOB (REUSED FOR BOTH EXISTENCE CHECK AND APPEND)
        read_pdf = read_csv_file_from_path_GCS(file_path_str_in, alert_in=False)
        # DATA IS VALID BOOLEAN
        data_is_valid_bool = isinstance(read_pdf, pd.DataFrame)
        # DEFINE DATA EXISTS STRING
        data_exists_str = "Exists" if data_is_valid_bool else "Does Not Exist"
        # DEFINE ACTION STRING
        action_str = "Writing" if (not data_is_valid_bool) else "Appending" if (access_mode_str == "A") else "Ignoring"
        # DISPLAY INFORMATION
        print(f"\t{icon_dict[action_str]} File:\t'{file_path_str_in}' {data_exists_str}! {action_str}...") if alert_in else None
        # IF THE ACTION IS IGNORE
        if action_str == "Ignoring":
            # EXIT FUNCTION
            return False
        # DEFINE SAVE DATAFRAME
        save_pdf = pdf_in
        # IF THE ACTION IS APPEND
        if action_str == "Appending":
            # CONCATENATE THE DATAFRAMES
            save_pdf = pd.concat([read_pdf, pdf_in], axis=0)
    # TRY TO SAVE CSV TO BLOB STORAGE
    try:
        # CONVERT DATAFRAME TO CSV BUFFER
        csv_buffer = BytesIO()
        # WRITE DATAFRAME TO BUFFER
        save_pdf.to_csv(csv_buffer, index=False, encoding="utf-8")
        # RESET POINTER
        csv_buffer.seek(0)
        # UPLOAD THE CSV TO THE BLOB
        _get_blob(file_path_str_in).upload_from_file(csv_buffer, content_type="text/csv")
        # DISPLAY INFORMATION
        print(f"✅ File:\t'{file_path_str_in}' Has Been Saved!") if alert_in else None
        # RETURN TRUE
        return True
    # CATCH EXCEPTION: ERROR SAVING CSV
    except Exception as e:
        # DISPLAY INFORMATION
        print(f"❌ Error saving CSV file '{file_path_str_in}': {e}") if alert_in else None
        # RETURN FALSE
        return False

# FUNCTION: COLLECT CSV DATA FROM LIST OF PATHS
def read_pdf_from_csv_file_path_list_GCS(list_in: list) -> pd.DataFrame | None:
    """
    Reads multiple CSV files from Google Cloud Storage and combines them into a single DataFrame.
    Files that cannot be read (missing, corrupt, malformed) are skipped with a warning
    rather than crashing the entire batch.

    Args:
        list_in: List of blob paths to CSV files

    Returns:
        Combined DataFrame from all successfully read CSV files, or None if none succeeded
    """
    # CREATE A LIST OF DATAFRAMES FROM PATH LIST
    pdf_list = []
    # ITERATE OVER EACH FILE PATH
    for data_file_path_str in list_in:
        # ATTEMPT TO READ THE CSV FILE
        try:
            # READ THE CSV FILE
            pdf_list.append(pd.read_csv(BytesIO(_get_blob(data_file_path_str).download_as_bytes())))
        # CATCH EXCEPTION: FILE NOT FOUND OR UNREADABLE
        except Exception as e:
            # DISPLAY WARNING AND SKIP THE FILE
            print(f"⚠️ Skipping File:\t'{data_file_path_str}' — Could Not Be Read. ({e})")
    # IF THERE ARE DATAFRAMES IN THE LIST
    if pdf_list:
        # CONCATENATE ALL DATAFRAMES VERTICALLY AND RESET THE INDEX
        return pd.concat(pdf_list, axis=0).reset_index(drop=True)
    # IF THERE ARE NO ITEMS
    print("There are 0 DataFrame items in the list")
    # RETURN NONE
    return None

# FUNCTION: READ CSV DATA FROM PATH
def read_csv_files_from_path_GCS(file_path_str_in: str) -> pd.DataFrame | None:
    """
    Reads all CSV files in a Google Cloud Storage directory into a single DataFrame.
    Note: Requires a trailing slash to denote a directory prefix.

    Args:
        file_path_str_in: String path to directory (prefix) containing CSV blobs

    Returns:
        Combined DataFrame from all CSV blobs in directory
    """
    # VALIDATE TRAILING SLASH FOR DIRECTORY PREFIX
    if not file_path_str_in.endswith("/"):
        # RAISE VALUE ERROR
        raise ValueError(f"❌ File path '{file_path_str_in}' is not valid.\nFile path must end with '/'.")
    # COLLECT ALL THE FILES IN THE DIRECTORY
    file_path_str_list = get_path_file_list_GCS(file_path_str_in)
    # FILTER OUT NON-CSV FILES
    valid_file_path_str_list = [f for f in file_path_str_list if f.endswith(".csv")]
    # CALL FUNCTION TO COLLECT DATA
    return read_pdf_from_csv_file_path_list_GCS(valid_file_path_str_list)

# FUNCTION: READ PARQUET AS DATAFRAME FROM GOOGLE CLOUD STORAGE
def read_parquet_file_from_path_GCS(file_path_str_in: str, alert_in: bool = True) -> pd.DataFrame | None:
    """
    Reads a Parquet file from Google Cloud Storage into a pandas DataFrame.
    Note: Catches broad read errors (schema errors, etc.) in addition to blob-not-found cases,
    returning None and printing a warning in all failure cases.

    Args:
        file_path_str_in: The path to the file in the bucket (blob name)
        alert_in: Boolean to control progress messages

    Returns:
        DataFrame containing Parquet data or None if file not found or unreadable
    """
    # CHECK IF THE FILE PATH IS VALID
    check_valid_file_path(file_path_str_in, "parquet")
    # ATTEMPT TO READ THE FILE
    try:
        # GET BLOB FROM FILE PATH
        blob = _get_blob(file_path_str_in)
        # CHECK IF THE BLOB EXISTS
        if not blob.exists():
            # DISPLAY INFORMATION
            print(f"⚠️ File:\t'{file_path_str_in}' Not Found!\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
            # RETURN NONE
            return None
        # DOWNLOAD AND READ THE BLOB
        read_pdf = pd.read_parquet(BytesIO(blob.download_as_bytes()), engine="pyarrow")
        # DISPLAY INFORMATION
        print(f"✅ File:\t'{file_path_str_in}' Found!\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
        # RETURN THE DATAFRAME
        return read_pdf
    # CATCH EXCEPTION: FILE EXISTS BUT CANNOT BE READ (CORRUPT, SCHEMA ERROR, ETC.)
    except Exception as e:
        # DISPLAY INFORMATION
        print(f"⚠️ File:\t'{file_path_str_in}' Could Not Be Read! ({e})\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
        # RETURN NONE
        return None

# FUNCTION: SAVE DATAFRAME AS PARQUET TO GOOGLE CLOUD STORAGE
def write_parquet_file_to_path_GCS(pdf_in: pd.DataFrame, file_path_str_in: str, access_mode_in: str = "I", alert_in: bool = True) -> bool:
    """
    Writes a pandas DataFrame as a Parquet file to Google Cloud Storage.
    Note: In 'W' (overwrite) mode the file is never read from storage first — it is written directly.

    Args:
        pdf_in: DataFrame to save
        file_path_str_in: The path (blob name) in the bucket
        access_mode_in: String indicating write mode ('W'=overwrite, 'A'=append, 'N'=no write, 'I'=ignore if exists)
        alert_in: Boolean to control progress messages

    Returns:
        Boolean indicating success
    """
    # COLLECT THE ACCESS MODE
    access_mode_str = access_mode_in.upper()
    # VALIDATE THAT THE ACCESS MODE IS VALID
    if access_mode_str not in ["W", "A", "N", "I"]:
        # RAISE VALUE ERROR
        raise ValueError(f"Access mode '{access_mode_in}' is invalid. Must be one of: 'W', 'A', 'N', 'I'")
    # ACCESS MODE MEANING
    access_mode_dict = {"W": "Overwrite", "A": "Append", "I": "Ignore", "N": "No-Write"}
    # IF access_mode_str IS "N"
    if access_mode_str == "N":
        # DISPLAY INFORMATION
        print("⚠️ Not Saving DataFrame") if alert_in else None
        # EXIT FUNCTION
        return False
    # CHECK IF THE FILE PATH IS VALID
    check_valid_file_path(file_path_str_in, "parquet")
    # CHECK IF THE DATAFRAME IS EMPTY
    if pdf_is_empty(pdf_in):
        # EXIT FUNCTION
        return False
    # DISPLAY INFORMATION
    print(f"ℹ️ Saving In {access_mode_dict[access_mode_str]} Mode.\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
    # DEFINE ICON DICTIONARY
    icon_dict = {"Writing": "📝", "Appending": "➕", "Ignoring": "⚠️", "Overwriting": "♻️"}
    # IN OVERWRITE MODE, SKIP READING THE FILE — WRITE DIRECTLY
    if access_mode_str == "W":
        # CHECK IF FILE EXISTS
        file_exists_bool = check_file_exists_GCS(file_path_str_in)
        # SET THE FILE EXISTS AND ACTION STRINGS
        file_exists_str, action_str = ("Exists", "Overwriting") if file_exists_bool else ("Does Not Exist", "Writing")
        # DISPLAY INFORMATION
        print(f"\t{icon_dict[action_str]} File:\t'{file_path_str_in}' {file_exists_str}! {action_str}...") if alert_in else None
        # DEFINE SAVE DATAFRAME AS INPUT DATAFRAME
        save_pdf = pdf_in
    # IN ALL OTHER MODES, READ THE FILE FIRST TO DETERMINE ACTION
    else:
        # READ THE PARQUET FROM BLOB (REUSED FOR BOTH EXISTENCE CHECK AND APPEND)
        read_pdf = read_parquet_file_from_path_GCS(file_path_str_in, alert_in=False)
        # DATA IS VALID BOOLEAN
        data_is_valid_bool = isinstance(read_pdf, pd.DataFrame)
        # DEFINE DATA EXISTS STRING
        data_exists_str = "Exists" if data_is_valid_bool else "Does Not Exist"
        # DEFINE ACTION STRING
        action_str = "Writing" if not data_is_valid_bool else "Appending" if access_mode_str == "A" else "Ignoring"
        # DISPLAY INFORMATION
        print(f"\t{icon_dict[action_str]} File:\t'{file_path_str_in}' {data_exists_str}! {action_str}...") if alert_in else None
        # IF THE ACTION IS IGNORE
        if action_str == "Ignoring":
            # EXIT FUNCTION
            return False
        # DEFINE SAVE DATAFRAME
        save_pdf = pdf_in
        # IF THE ACTION IS APPEND
        if action_str == "Appending":
            # CONCATENATE THE DATAFRAMES
            save_pdf = pd.concat([read_pdf, pdf_in], axis=0)
    # TRY TO SAVE PARQUET TO BLOB STORAGE
    try:
        # CONVERT DATAFRAME TO PARQUET BUFFER
        parquet_buffer = BytesIO()
        # WRITE DATAFRAME TO BUFFER
        save_pdf.astype(str).to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")
        # RESET POINTER
        parquet_buffer.seek(0)
        # UPLOAD THE PARQUET TO THE BLOB
        _get_blob(file_path_str_in).upload_from_file(parquet_buffer, content_type="application/octet-stream")
        # DISPLAY INFORMATION
        print(f"✅ File:\t'{file_path_str_in}' Has Been Saved!") if alert_in else None
        # RETURN TRUE
        return True
    # CATCH EXCEPTION: ERROR SAVING PARQUET
    except Exception as e:
        # DISPLAY INFORMATION
        print(f"❌ Error saving Parquet file '{file_path_str_in}': {e}") if alert_in else None
        # RETURN FALSE
        return False

# FUNCTION: COLLECT DATA FROM LIST OF PATHS
def read_pdf_from_parquet_file_path_list_GCS(list_in: list) -> pd.DataFrame | None:
    """
    Reads multiple Parquet files from Google Cloud Storage and combines them into a single DataFrame.
    Files that cannot be read (missing, corrupt, malformed) are skipped with a warning
    rather than crashing the entire batch.

    Args:
        list_in: List of blob paths to Parquet files

    Returns:
        Combined DataFrame from all successfully read Parquet files, or None if none succeeded
    """
    # CREATE A LIST OF DATAFRAMES FROM PATH LIST
    pdf_list = []
    # ITERATE OVER EACH FILE PATH
    for data_file_path_str in list_in:
        # ATTEMPT TO READ THE PARQUET FILE
        try:
            # READ THE PARQUET FILE
            pdf_list.append(pd.read_parquet(BytesIO(_get_blob(data_file_path_str).download_as_bytes()), engine="pyarrow"))
        # CATCH EXCEPTION: FILE NOT FOUND OR UNREADABLE
        except Exception as e:
            # DISPLAY WARNING AND SKIP THE FILE
            print(f"⚠️ Skipping File:\t'{data_file_path_str}' — Could Not Be Read. ({e})")
    # IF THERE ARE DATAFRAMES IN THE LIST
    if pdf_list:
        # CONCATENATE ALL DATAFRAMES VERTICALLY AND RESET THE INDEX
        return pd.concat(pdf_list, axis=0).reset_index(drop=True)
    # IF THERE ARE NO ITEMS
    print("There are 0 DataFrame items in the list")
    # RETURN NONE
    return None

# FUNCTION: COLLECT PARQUET DATA FROM PATH
def read_parquet_files_from_path_GCS(file_path_str_in: str) -> pd.DataFrame | None:
    """
    Reads all Parquet files in a Google Cloud Storage directory into a single DataFrame.
    Note: Requires a trailing slash to denote a directory prefix.

    Args:
        file_path_str_in: String path to directory (prefix) containing Parquet blobs

    Returns:
        Combined DataFrame from all Parquet blobs in directory, or None if no files found
    """
    # VALIDATE TRAILING SLASH FOR DIRECTORY PREFIX
    if not file_path_str_in.endswith("/"):
        # RAISE VALUE ERROR
        raise ValueError(f"❌ File path '{file_path_str_in}' is not valid.\nFile path must end with '/'.")
    # COLLECT ALL THE FILES IN THE DIRECTORY
    file_path_str_list = get_path_file_list_GCS(file_path_str_in)
    # FILTER OUT NON-PARQUET FILES
    valid_file_path_str_list = [f for f in file_path_str_list if f.endswith(".parquet")]
    # CALL FUNCTION TO READ THE DATA
    return read_pdf_from_parquet_file_path_list_GCS(valid_file_path_str_list)

# FUNCTION: READ JSON AS DICTIONARY FROM GOOGLE CLOUD STORAGE
def read_json_file_from_path_GCS(file_path_str_in: str, alert_in: bool = True) -> dict | list | None:
    """
    Reads a JSON file from Google Cloud Storage into a dictionary.
    Note: If the JSON root is a list rather than a dict, it is returned as-is (not wrapped).
    Note: Decoded explicitly with UTF-8 for cross-platform consistency.

    Args:
        file_path_str_in: The path to the file in the bucket (blob name)
        alert_in: Boolean to control progress messages

    Returns:
        Dictionary (or list) containing JSON data, or None if file not found/corrupted/unreadable
    """
    # CHECK IF THE FILE PATH IS VALID
    check_valid_file_path(file_path_str_in, "json")
    # ATTEMPT TO READ THE FILE
    try:
        # GET BLOB FROM FILE PATH
        blob = _get_blob(file_path_str_in)
        # CHECK IF THE BLOB EXISTS
        if not blob.exists():
            # DISPLAY INFORMATION
            print(f"⚠️ File:\t'{file_path_str_in}' Not Found!\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
            # RETURN NONE
            return None
        # DOWNLOAD, DECODE, AND LOAD JSON
        data_dict = json.loads(blob.download_as_bytes().decode("utf-8"))
        # DISPLAY INFORMATION
        print(f"✅ File:\t'{file_path_str_in}' Found!\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
        # RETURN THE DICTIONARY
        return data_dict
    # CATCH JSON DECODE ERROR
    except JSONDecodeError:
        # DISPLAY INFORMATION
        print(f"⚠️ File:\t'{file_path_str_in}' Corrupted!\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
        # RETURN NONE
        return None
    # CATCH EXCEPTION: PERMISSION DENIED, ENCODING ERROR, OR OTHER IO FAILURE
    except Exception as e:
        # DISPLAY INFORMATION
        print(f"⚠️ File:\t'{file_path_str_in}' Could Not Be Read! ({e})\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
        # RETURN NONE
        return None

# FUNCTION: SAVE DICTIONARY AS JSON TO GOOGLE CLOUD STORAGE
def write_json_file_to_path_GCS(data_dict_in: dict, file_path_str_in: str, access_mode_in: str = "I", alert_in: bool = True) -> bool:
    """
    Writes a dictionary as a JSON file to Google Cloud Storage.
    Note: Append mode performs a shallow merge — top-level keys from data_dict_in
    will overwrite matching keys in the existing file. Nested dicts are not deep-merged.
    Note: If the existing JSON blob contains a root-level list, it will be treated as
    non-existent (i.e. overwritten), since list-append is not supported.
    Note: Encoded explicitly with UTF-8 for cross-platform consistency.
    Note: In 'W' (overwrite) mode the file is never read from storage first — it is written directly.

    Args:
        data_dict_in: Dictionary to save
        file_path_str_in: The path (blob name) in the bucket
        access_mode_in: String indicating write mode ('W'=overwrite, 'A'=append, 'N'=no write, 'I'=ignore if exists)
        alert_in: Boolean to control progress messages

    Returns:
        Boolean indicating success
    """
    # COLLECT THE ACCESS MODE
    access_mode_str = access_mode_in.upper()
    # VALIDATE THAT THE ACCESS MODE IS VALID
    if access_mode_str not in ["W", "A", "N", "I"]:
        # RAISE VALUE ERROR
        raise ValueError(f"Access mode '{access_mode_in}' is invalid. Must be one of: 'W', 'A', 'N', 'I'")
    # ACCESS MODE MEANING
    access_mode_dict = {"W": "Overwrite", "A": "Append", "I": "Ignore", "N": "No-Write"}
    # IF access_mode_str IS "N"
    if access_mode_str == "N":
        # DISPLAY INFORMATION
        print("⚠️ Not saving dictionary.") if alert_in else None
        # RETURN FALSE
        return False
    # CHECK IF THE FILE PATH IS VALID
    check_valid_file_path(file_path_str_in, "json")
    # CHECK IF THE DATA DICTIONARY IS EMPTY
    if not data_dict_in:
        # EXIT FUNCTION
        return False
    # DISPLAY INFORMATION
    print(f"ℹ️ Saving In {access_mode_dict[access_mode_str]} Mode.\t(☁️ In {STORAGE_NAME_STR})") if alert_in else None
    # DEFINE ICON DICTIONARY
    icon_dict = {"Writing": "📝", "Appending": "➕", "Ignoring": "⚠️", "Overwriting": "♻️"}
    # IN OVERWRITE MODE, SKIP READING THE FILE — WRITE DIRECTLY
    if access_mode_str == "W":
        # CHECK IF FILE EXISTS
        file_exists_bool = check_file_exists_GCS(file_path_str_in)
        # SET THE FILE EXISTS AND ACTION STRINGS
        file_exists_str, action_str = ("Exists", "Overwriting") if file_exists_bool else ("Does Not Exist", "Writing")
        # DISPLAY INFORMATION
        print(f"\t{icon_dict[action_str]} File:\t'{file_path_str_in}' {file_exists_str}! {action_str}...") if alert_in else None
        # DEFINE SAVE DICTIONARY AS INPUT DICTIONARY
        save_dict = data_dict_in
    # IN ALL OTHER MODES, READ THE FILE FIRST TO DETERMINE ACTION
    else:
        # READ JSON FROM BLOB (REUSED FOR BOTH EXISTENCE CHECK AND APPEND)
        read_dict = read_json_file_from_path_GCS(file_path_str_in, alert_in=False)
        # DATA IS VALID BOOLEAN (ONLY DICT ROOT TYPE IS SUPPORTED FOR APPEND)
        data_is_valid_bool = isinstance(read_dict, dict)
        # DEFINE DATA EXISTS STRING
        data_exists_str = "Exists" if data_is_valid_bool else "Does Not Exist"
        # DEFINE ACTION STRING
        action_str = "Writing" if not data_is_valid_bool else "Appending" if access_mode_str == "A" else "Ignoring"
        # DISPLAY INFORMATION
        print(f"\t{icon_dict[action_str]} File:\t'{file_path_str_in}' {data_exists_str}! {action_str}...") if alert_in else None
        # IF THE ACTION IS IGNORE
        if action_str == "Ignoring":
            # EXIT FUNCTION
            return False
        # DEFINE SAVE DICTIONARY
        save_dict = data_dict_in
        # IF THE ACTION IS APPEND
        if action_str == "Appending":
            # CONCATENATE THE DICTIONARIES
            save_dict = {**read_dict, **data_dict_in}
    # WRITE JSON TO BLOB STORAGE
    try:
        # CONVERT THE DICTIONARY TO JSON BYTES WITH EXPLICIT ENCODING
        json_bytes = json.dumps(save_dict, indent=2, ensure_ascii=False).encode("utf-8")
        # UPLOAD THE JSON BYTES
        _get_blob(file_path_str_in).upload_from_string(json_bytes, content_type="application/json")
        # DISPLAY INFORMATION
        print(f"✅ File:\t'{file_path_str_in}' Has Been Saved!") if alert_in else None
        # RETURN TRUE
        return True
    # CATCH EXCEPTION: ERROR SAVING JSON
    except Exception as e:
        # DISPLAY INFORMATION
        print(f"❌ Error saving JSON file '{file_path_str_in}': {e}") if alert_in else None
        # RETURN FALSE
        return False