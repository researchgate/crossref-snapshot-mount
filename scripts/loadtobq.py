import os
import json
import time
from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery, storage

# CONFIG
PROJECT_ID = 'mlops-prd-384915'
DATASET_ID = 'crossref2025'
TABLE_ID = 'crossref_april_snapshot_mounted_v1'
BUCKET_NAME = 'crossref'
GCS_FOLDER = 'processed_for_bq'
SCHEMA_FILE_PATH = 'schema.json'
FAILED_LOG = 'failed_uploads.txt'
ERROR_LOG = 'error-log.log'
MAX_WORKERS = os.cpu_count()
MAX_TOTAL_LOAD_JOBS = 1500
TABLE_UPDATE_LIMIT_PER_10S = 100

# Throttle calculation to stay within 100 updates / 10s
DELAY_BETWEEN_BATCHES = max((MAX_WORKERS / TABLE_UPDATE_LIMIT_PER_10S) * 10, 10.0)

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
        print(f"✅ SUCCESS: Loaded {len(gcs_uris)} files.")
        time.sleep(DELAY_BETWEEN_BATCHES)  # Throttle to avoid rate limit
        return (gcs_uris, True, None)
    except Exception as e:
        print(f"❌ FAILED: {len(gcs_uris)} files - {str(e)[:100]}...")
        return (gcs_uris, False, str(e))

def main():
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=GCS_FOLDER + "/")
    all_files = [f"gs://{BUCKET_NAME}/{b.name}" for b in blobs if b.name.endswith(".jsonl.gz")]

    total_files = len(all_files)
    files_per_batch = ceil(total_files / MAX_TOTAL_LOAD_JOBS)

    print(f"🧾 Total files: {total_files}")
    print(f"📦 Batching ~{files_per_batch} files per job (up to {MAX_TOTAL_LOAD_JOBS} jobs)")
    print(f"🚀 Running with {MAX_WORKERS} workers")
    print(f"⏱️ Throttling with {DELAY_BETWEEN_BATCHES:.2f} seconds delay per batch")

    batches = [
        all_files[i:i + files_per_batch]
        for i in range(0, total_files, files_per_batch)
    ]

    failed_files = []
    error_messages = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(upload_batch_to_bq, batch) for batch in batches]

        for future in as_completed(futures):
            gcs_uris, success, err = future.result()
            if not success:
                failed_files.extend(gcs_uris)
                error_messages.append(f"{gcs_uris} - {err}")

    with open(FAILED_LOG, "w") as f:
        for fpath in failed_files:
            f.write(fpath + "\n")

    with open(ERROR_LOG, "w") as f:
        for msg in error_messages:
            f.write(msg + "\n")

    print(f"\n✅ Upload complete. {len(failed_files)} files failed.")
    print(f"🔹 Errors in: {ERROR_LOG}")
    print(f"🔹 Failed file list in: {FAILED_LOG}")

if __name__ == "__main__":
    main()
