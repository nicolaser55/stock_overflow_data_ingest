"""
Stock Overflow Data Ingest

    ingest.config           every constant (data folders, IBKR connection, contract, request and pacing settings)
    ingest.ibkr_client      IBKR connection: per-request results, version-agnostic callbacks
    ingest.ibkr_download    session selection, concurrent download, minute-by-minute check, staging files, download log
    ingest.ibkr_stream      live 1-minute bars during the session (never merged; the after-close download is the record)
    ingest.sessions         New York clock, NYSE schedule, session checks, issue report
    ingest.raw_files        file names, text-preserving read / write, roll-up rules, merge engine
    ingest.raw_maintenance  merge staging into raw, organize, clean orphans, rebuild
    ingest.gcs_file_management  Google Cloud Storage helpers

Shared code comes from the research workspace (package so), installed in the same venv: pip install -e <workspace>.
"""
