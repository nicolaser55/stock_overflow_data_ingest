"""
Notebook Smoke Test (simulated IBKR server, temporary data folder)

Run from the workspace root:   python tests/smoke_notebooks.py   (or python tests/run_all_tests.py)
Executes every code cell of notebooks/step01-03 in order, with ingest.ibkr_client.IbkrApp replaced by the simulated
server of tests/fake_ibkr.py and SO_INGEST_DATA_PATH pointing to a temporary folder holding a small synthetic raw folder.
It checks that the cells run without error (not the numbers; tests/test_ingest.py checks those). The real clock is used,
so step 02 streams only when the market is open; otherwise its start cell reports that there is no session to stream.
"""

import os
import sys
import json
import shutil
import tempfile
import warnings

# POINT THE INGEST DATA ROOT TO A TEMPORARY FOLDER (BEFORE ingest IS IMPORTED)
TEST_ROOT_PATH_STR = tempfile.mkdtemp(prefix="so_ingest_smoke_").replace("\\", "/") + "/"
os.environ["SO_INGEST_DATA_PATH"] = TEST_ROOT_PATH_STR
# ADD THE TESTS FOLDER TO THE PATH
TESTS_PATH_STR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_PATH_STR)
# IGNORE WARNINGS FROM LIBRARIES
warnings.filterwarnings("ignore")

import pandas as pd
# IMPORT INGEST MODULES AND THE SIMULATED SERVER
import ingest.ibkr_client
from ingest import config
from ingest.sessions import get_ny_now_ts, get_session_pdf
from fake_ibkr import FakeIbkrApp

# FUNCTION: EXECUTE THE CODE CELLS OF A NOTEBOOK
def run_notebook(notebook_path_str_in):
    # READ THE NOTEBOOK
    with open(notebook_path_str_in, encoding="utf-8") as file:
        notebook_dict = json.load(file)
    # DEFINE THE NAMESPACE (display PRINTS)
    namespace_dict = {"display": print, "__name__": "__main__"}
    # ITERATE OVER THE CODE CELLS
    for cell_idx, cell_dict in enumerate(notebook_dict["cells"]):
        if cell_dict["cell_type"] == "code":
            # EXECUTE THE CELL
            exec(compile("".join(cell_dict["source"]), f"{os.path.basename(notebook_path_str_in)}[{cell_idx}]", "exec"), namespace_dict)
    # RETURN THE NUMBER OF CELLS
    return len(notebook_dict["cells"])

# RUN THE SMOKE TEST
if __name__ == "__main__":
    # TRY TO RUN THE NOTEBOOKS, THEN REMOVE THE TEMPORARY FOLDER
    try:
        # REPLACE THE IBKR APPLICATION BY THE SIMULATED SERVER
        ingest.ibkr_client.IbkrApp = FakeIbkrApp
        # THE SMOKE ROOT USES THE LEAF FROM so.paths; ALIGN THE EXPECTED NAME SO THE NOTEBOOK NAME CHECK DOES NOT STOP THE RUN
        config.SPY_RAW_FOLDER_NAME_STR = os.path.basename(config.RAW_OHLCV_PATH_STR.rstrip("/"))
        # BUILD A SMALL RAW FOLDER: THE LAST 15 CALENDAR DAYS MINUS THE LAST 3 SESSIONS (THE NOTEBOOK DOWNLOADS THEM)
        now_ny_ts = get_ny_now_ts()
        session_pdf = get_session_pdf(now_ny_ts - pd.Timedelta(days=15), now_ny_ts - pd.Timedelta(days=5))
        os.makedirs(config.RAW_OHLCV_PATH_STR)
        for session_row in session_pdf.itertuples():
            minute_index = pd.date_range(session_row.first_bar_ts, session_row.last_bar_ts, freq="1min")
            pd.DataFrame({"timestamp": [ts.isoformat(sep=" ") for ts in minute_index], "open": 100.0, "high": 100.1, "low": 99.9, "close": 100.0,
                          "volume": 1000, "created_ts": "", "date": str(session_row.date)}) \
              .to_csv(f"{config.RAW_OHLCV_PATH_STR}ohlcv_data_{session_row.date:%Y%m%d}.csv", index=False)
        # RUN THE NOTEBOOKS
        notebook_folder_str = os.path.join(os.path.dirname(TESTS_PATH_STR), "notebooks")
        for notebook_name_str in ["step01_ibkr_download.ipynb", "step02_ibkr_live_stream.ipynb", "step03_raw_data_check.ipynb"]:
            print(f"\n===== {notebook_name_str} =====")
            cell_count_int = run_notebook(os.path.join(notebook_folder_str, notebook_name_str))
            print(f"✅ {notebook_name_str}: {cell_count_int} cells ran")
        print("\nNotebook smoke test passed ✅")
    finally:
        shutil.rmtree(TEST_ROOT_PATH_STR, ignore_errors=True)
