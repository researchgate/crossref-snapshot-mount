import os
import json
import time
from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery

# CONFIG
PROJECT_ID = 'mlops-prd-384915'
DATASET_ID = 'crossref2025'
TABLE_ID = 'crossref_april_snapshot_mounted_v1'
SCHEMA_FILE_PATH = 'schema.json'
FAILED_LOG = 'failed_uploads.txt'
RETRY_FAILED_LOG = 'retry_failed_uploads.txt'
RETRY_ERROR_LOG = 'retry_error_log.txt'
MAX_WORKERS = os.cpu_count()
TABLE_UPDATE_LIMIT_PER_10S = 100
DELAY_BETWEEN_BATCHES = max((MAX_WORKERS / TABLE_UPDATE_LIMIT_PER_10S) * 10, 10.0)
FILES_PER_BATCH = 50  # Based on earlier estimate to stay within 1500 jobs
#decrease FILES_PER_BATCH slowly as and when big batches continue to succeed and only few files are left

client = bigquery.Client(project=PROJECT_ID)

def load_schema():
    with open(SCHEMA_FILE_PATH, 'r') as f:
        schema_list = json.load(f)
    return [bigquery.SchemaField.from_api_repr(field) for field in schema_list]

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=load_schema(),
    autodetect=False,
    ignore_unknown_values=True,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)

def upload_batch_to_bq(gcs_uris):
    try:
        load_job = client.load_table_from_uri(
            gcs_uris,
            f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
            job_config=job_config
        )
        load_job.result()
        print(f"✅ RETRY SUCCESS: Loaded {len(gcs_uris)} files.")
        time.sleep(DELAY_BETWEEN_BATCHES)
        return (gcs_uris, True, None)
    except Exception as e:
        print(f"❌ RETRY FAILED: {len(gcs_uris)} files - {str(e)[:100]}...")
        return (gcs_uris, False, str(e))

def main():
    if not os.path.exists(FAILED_LOG):
        print(f"❗ File {FAILED_LOG} not found.")
        return

    with open(FAILED_LOG, 'r') as f:
        failed_files = [line.strip() for line in f if line.strip()]

    total_files = len(failed_files)
    print(f"🔁 Retrying {total_files} failed files...")

    batches = [
        failed_files[i:i + FILES_PER_BATCH]
        for i in range(0, total_files, FILES_PER_BATCH)
    ]

    retry_failed = []
    retry_errors = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(upload_batch_to_bq, batch) for batch in batches]

        for future in as_completed(futures):
            gcs_uris, success, err = future.result()
            if not success:
                retry_failed.extend(gcs_uris)
                retry_errors.append(f"{gcs_uris} - {err}")

    with open(RETRY_FAILED_LOG, "w") as f:
        for fpath in retry_failed:
            f.write(fpath + "\n")

    with open(RETRY_ERROR_LOG, "w") as f:
        for msg in retry_errors:
            f.write(msg + "\n")

    print(f"\n📄 Retry complete. {len(retry_failed)} files still failed.")
    print(f"🔹 Retry errors in: {RETRY_ERROR_LOG}")
    print(f"🔹 Retry failures in: {RETRY_FAILED_LOG}")

if __name__ == "__main__":
    main()
