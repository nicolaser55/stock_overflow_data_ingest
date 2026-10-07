import os
import sys
import subprocess
import time

"""
Run Every Test Suite

Run from the workspace root:   python tests/run_all_tests.py
Each suite runs in its own interpreter (plain asserts; a failing suite stops with its error). Takes about a minute.
No connection to IBKR and no access to the real data folder (simulated server, temporary folders).

    tests/test_ingest.py         IBKR client, sessions, download, raw files, maintenance, live stream, repo reader compatibility
    tests/smoke_notebooks.py     every cell of notebooks/step01-03 runs (simulated server)
    tests/test_vix_probe.py, tests/test_index_alignment.py, tests/test_index_pipeline.py     the VIX / VIX3M probe, alignment check and download pipeline

    Output is forced to UTF-8 (the suites print emoji; a Windows console or pipe in cp1252 would otherwise crash the print).
"""

# DEFINE THE SUITES
SUITE_LIST = ["test_ingest.py", "smoke_notebooks.py", "test_vix_probe.py", "test_index_alignment.py", "test_index_pipeline.py"]
# DEFINE THE TESTS FOLDER
TESTS_PATH_STR = os.path.dirname(os.path.abspath(__file__))

# RUN THE SUITES
if __name__ == "__main__":
    # FORCE UTF-8 OUTPUT IN THIS PROCESS AND IN THE SUITES
    sys.stdout.reconfigure(encoding="utf-8")
    suite_env_dict = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    # ITERATE OVER THE SUITES
    for suite_str in SUITE_LIST:
        # DISPLAY INFORMATION
        print(f"\n===== {suite_str} =====", flush=True)
        start_time = time.time()
        # RUN THE SUITE
        result = subprocess.run([sys.executable, os.path.join(TESTS_PATH_STR, suite_str)], env=suite_env_dict)
        # STOP AT THE FIRST FAILURE
        if result.returncode != 0:
            print(f"\n❌ {suite_str} failed")
            raise SystemExit(result.returncode)
        # DISPLAY INFORMATION
        print(f"({time.time() - start_time:.0f} s)", flush=True)
    # DISPLAY THE RESULT
    print("\nAll test suites passed ✅")
