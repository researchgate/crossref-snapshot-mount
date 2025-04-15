import json
import os
import datetime
import logging
import gzip
import tempfile
import shutil # Keep for potential future cleanup, though tempfile handles its dir
import time # For timing
from concurrent.futures import ProcessPoolExecutor, as_completed
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import exceptions # Import exceptions for detailed error handling

# --- Configuration ---
project_id = 'mlops-prd-384915'
dataset_id = 'crossref2025'
table_id = 'crossref_2025_04_10'
bucket_name = "crossref"
source_extracted_dir = "/home/rashmi_varma/extracted/April_2025_Public_Data_File_from_Crossref"
gcs_processed_folder = "processed_for_bq"
schema_file_path = 'schema.json'
# --- Parallelization Configuration ---
# Adjust based on the number of CPU cores on your VM (os.cpu_count())
# Leave as None to let ProcessPoolExecutor decide (usually uses os.cpu_count())
MAX_WORKERS = os.cpu_count() # Or set to a specific number like 4, 8 etc.

try:
    bigquery_client = bigquery.Client(project=project_id)
    storage_client = storage.Client() # Client used for existence check
    bucket = storage_client.bucket(bucket_name) # Bucket object for uploads
except Exception as e:
    print(f"FATAL: Failed to initialize Google Cloud clients: {e}")
    logging.error(f"FATAL: Failed to initialize Google Cloud clients: {e}")
    exit(1) # Exit if clients can't be created

log_file = "conversion_load_parallel.log"
logging.basicConfig(filename=log_file, level=logging.INFO, filemode='a',
                    format='%(asctime)s - %(levelname)s - [%(processName)s] - [%(funcName)s] - %(message)s')
print(f"Appending errors and info to: {log_file}")
logging.info("="*40 + f" SCRIPT START (Parallel, Workers={MAX_WORKERS}) " + "="*40)


def load_schema():
    """Loads the BigQuery schema from a JSON file."""
    logging.info(f"Loading schema from: {schema_file_path}")
    try:
        with open(schema_file_path, 'r') as schema_file:
            schema_list = json.load(schema_file)
        logging.info("Schema file read successfully.")
        bq_schema = [bigquery.SchemaField.from_api_repr(field) for field in schema_list]
        logging.info("Schema converted to BigQuery SchemaField objects.")
        return bq_schema
    except Exception as e:
        logging.error(f"Error loading or processing schema: {e}", exc_info=True)
        print(f"ERROR: Error loading or processing schema: {e}")
        raise

def date_parts_to_date(input_list: list):
    """Convert date parts to a date in YYYY-MM-DD format."""
    if not input_list or not isinstance(input_list, list) or not input_list[0]: return None
    if not isinstance(input_list[0], list) or len(input_list[0]) == 0: return None
    try:
        year_part = input_list[0][0]; month_part = input_list[0][1] if len(input_list[0]) > 1 else None; day_part = input_list[0][2] if len(input_list[0]) > 2 else None
        year = int(year_part) if year_part is not None else None; month = int(month_part) if month_part is not None else (1 if year is not None else None); day = int(day_part) if day_part is not None else (1 if year is not None else None)
        if year is not None and month is not None and day is not None:
            month = max(1, min(12, month)); day = max(1, min(31, day))
            try: return datetime.date(year, month, day).isoformat()
            except ValueError as date_err: logging.warning(f"Invalid date Y:{year} M:{month} D:{day}. Parts: {input_list}. Err: {date_err}"); return None
        else: return None
    except (ValueError, TypeError, IndexError) as e: logging.warning(f"Invalid date parts format/type: {input_list}. Err: {str(e)}"); return None

def recursively_flatten_date_parts(obj):
    """Recursively searches for 'date-parts' and converts to 'date'."""
    if isinstance(obj, dict):
        new_obj = {}; date_key_present = 'date-parts' in obj
        for key, value in obj.items():
            if key == 'date-parts': new_obj['date'] = date_parts_to_date(value)
            else: new_obj[key] = recursively_flatten_date_parts(value)
        return new_obj
    elif isinstance(obj, list): return [recursively_flatten_date_parts(item) for item in obj]
    else: return obj

def process_single_file(input_gz_path, output_gz_path):
    """Reads, transforms, and writes a single jsonl.gz file. Returns True/False."""
    lines_processed, lines_failed_json, lines_written = 0, 0, 0
    base_filename = os.path.basename(input_gz_path)
    try:
        with gzip.open(input_gz_path, 'rt', encoding='utf-8', errors='replace') as infile, \
                gzip.open(output_gz_path, 'wt', encoding='utf-8') as outfile:
            for line in infile:
                lines_processed += 1
                try:
                    stripped_line = line.strip();
                    if not stripped_line: continue
                    data = json.loads(stripped_line)
                    transformed_data = recursively_flatten_date_parts(data)
                    if transformed_data is not None:
                        outfile.write(json.dumps(transformed_data) + '\n')
                        lines_written +=1
                except json.JSONDecodeError: lines_failed_json += 1; logging.debug(f"Invalid JSON line {lines_processed} in {base_filename}"); continue
                except Exception as process_err: logging.warning(f"Error processing line {lines_processed} in {base_filename}: {process_err}"); continue
    except Exception as e: logging.error(f"Failed processing file {base_filename}: {str(e)}"); return False # Indicate file-level failure
    if lines_failed_json > 0: logging.warning(f"Processed {base_filename}. Read:{lines_processed}, Written:{lines_written}, FailedJSON:{lines_failed_json}")
    #else: logging.info(f"Processed {base_filename}. Read:{lines_processed}, Written:{lines_written}") # Reduce log noise
    return True

def upload_to_gcs(local_file_path, gcs_destination_path, bucket_obj):
    """Uploads a local file to GCS using a passed bucket object."""
    base_filename = os.path.basename(local_file_path)
    try:
        blob = bucket_obj.blob(gcs_destination_path) # Use the passed bucket object
        blob.upload_from_filename(local_file_path, timeout=300) # Add timeout
        logging.info(f"Uploaded {base_filename} to gs://{bucket_obj.name}/{gcs_destination_path}")
        return True
    except exceptions.Forbidden as e: logging.error(f"Permission denied uploading {base_filename} to GCS: {e}"); return False
    except Exception as e: logging.error(f"Failed to upload {base_filename} to GCS: {str(e)}"); return False

def list_processed_files_in_gcs(bucket_obj, folder_prefix):
    """Lists base filenames (without .jsonl.gz) in the specified GCS folder."""
    logging.info(f"Listing existing blobs in gs://{bucket_obj.name}/ with prefix '{folder_prefix}'...")
    file_names_set = set()
    blob_count = 0
    try:
        blobs_iterator = bucket_obj.list_blobs(prefix=folder_prefix)
        for blob in blobs_iterator:
            blob_count += 1
            if blob.name == folder_prefix or not blob.name.endswith('.jsonl.gz'): continue
            base_name = os.path.basename(blob.name).removesuffix('.jsonl.gz')
            file_names_set.add(base_name)
        logging.info(f"Checked {blob_count} blobs. Found {len(file_names_set)} unique processed base filenames.")
        print(f"Found {len(file_names_set)} previously processed files in GCS.")
    except Exception as e:
        logging.error(f"Error listing blobs with prefix '{folder_prefix}': {e}", exc_info=True)
        print(f"ERROR: Could not list existing files in GCS: {e}. Assuming no files exist.")
    return file_names_set


# --- Parallel Worker Function ---
def process_and_upload_worker(input_file_path, temp_processed_dir, local_source_dir, gcs_dest_folder, bucket_name, existing_processed_files_set):
    """
    Worker function for processing one file. Checks existence, processes, uploads.
    Returns tuple: (status, message/gcs_path)
    status: 'skipped', 'processed_empty', 'upload_failed', 'processing_failed', 'success'
    """
    try:
        # Derive paths and names needed
        filename = os.path.basename(input_file_path)
        filename_base = filename.removesuffix('.jsonl.gz')
        relative_path = os.path.relpath(input_file_path, local_source_dir)
        gcs_path = os.path.join(gcs_dest_folder, relative_path).replace("\\", "/")
        temp_output_file_path = os.path.join(temp_processed_dir, filename)

        # 1. Check if already processed (using the pre-fetched set)
        if filename_base in existing_processed_files_set:
            logging.info(f"Skipping {relative_path}: Exists in pre-fetched GCS list.")
            return ('skipped', filename)

        # 2. Process the file locally
        logging.debug(f"Processing {relative_path}...")
        if not process_single_file(input_file_path, temp_output_file_path):
            return ('processing_failed', filename) # process_single_file logs the error

        # 3. Check if processed file has content
        if os.path.getsize(temp_output_file_path) == 0:
            logging.warning(f"Skipping upload for {filename}: processed file is empty.")
            # Clean up empty temp file
            try: os.remove(temp_output_file_path)
            except OSError: pass
            return ('processed_empty', filename)

        # 4. Upload the processed file to GCS
        # Need a bucket object here. If global client init causes issues, create one here:
        # storage_client_local = storage.Client()
        # bucket_local = storage_client_local.bucket(bucket_name)
        # Use global bucket object for now
        logging.debug(f"Uploading processed {filename} to GCS...")
        if upload_to_gcs(temp_output_file_path, gcs_path, bucket): # Pass global bucket
            # Clean up successful temp file
            try: os.remove(temp_output_file_path)
            except OSError: pass
            return ('success', f"gs://{bucket_name}/{gcs_path}")
        else:
            # Upload failed (upload_to_gcs logs the error)
            # Keep the temp file for potential manual retry/debug
            logging.error(f"Upload failed for {filename}. Temp file kept at {temp_output_file_path}")
            return ('upload_failed', filename)

    except Exception as e:
        # Catch unexpected errors in the worker
        logging.exception(f"Critical error in worker processing {input_file_path}")
        return ('worker_error', f"{filename}: {str(e)}")


# --- Main Workflow Functions ---

def parallel_process_and_upload(local_source_dir, gcs_dest_folder, bucket_obj):
    """
    Finds local files, checks against GCS, processes and uploads new ones in parallel.
    """
    if not os.path.isdir(local_source_dir):
        logging.error(f"Source directory not found: {local_source_dir}")
        print(f"ERROR: Source directory not found: {local_source_dir}")
        return 0

    # 1. Get list of existing processed files from GCS ONCE
    folder_prefix = gcs_dest_folder + ('/' if not gcs_dest_folder.endswith('/') else '')
    # The existing_files_set logic can be omitted if you are running the execution for the first time
    # However, if you are running again and some files have already been cleaned up , this logic will help you skip processing those files again
    existing_files_set = list_processed_files_in_gcs(bucket_obj, folder_prefix)

    # 2. Prepare list of tasks for workers
    tasks_to_submit = []
    logging.info(f"Scanning local source directory: {local_source_dir}...")
    print(f"Scanning local source directory: {local_source_dir}...")
    file_scan_count = 0
    for root, _, files in os.walk(local_source_dir):
        for filename in files:
            if filename.endswith('.jsonl.gz'):
                file_scan_count += 1
                input_file_path = os.path.join(root, filename)
                # Add arguments for the worker function as a tuple
                tasks_to_submit.append((input_file_path, local_source_dir, gcs_dest_folder, bucket_obj.name, existing_files_set))

    logging.info(f"Found {file_scan_count} potential source files. {len(existing_files_set)} exist in GCS. Submitting ~{file_scan_count - len(existing_files_set)} tasks.")
    print(f"Found {file_scan_count} potential source files. Submitting tasks for processing/upload...")
    if not tasks_to_submit:
        print("No new files found to process.")
        return 0

    # 3. Execute tasks in parallel using a temporary directory for outputs
    processed_count = 0
    skipped_count = 0
    processing_failed_count = 0
    upload_failed_count = 0
    worker_error_count = 0
    start_time = time.time()

    # Use a single temporary directory managed here
    with tempfile.TemporaryDirectory(prefix="bq_parallel_processed_") as temp_processed_dir:
        logging.info(f"Using temporary directory for processed files: {temp_processed_dir}")
        print(f"Processing files using up to {MAX_WORKERS} workers...")

        # Use ProcessPoolExecutor for CPU-bound work
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Create futures by submitting tasks with the temp dir argument added
            futures = {executor.submit(process_and_upload_worker, task[0], temp_processed_dir, task[1], task[2], task[3], task[4]): task for task in tasks_to_submit}
            # futures = {executor.submit(process_and_upload_worker, input_path, temp_processed_dir, src_dir, gcs_folder, bucket_nm, existing_set): (input_path, src_dir, gcs_folder, bucket_nm, existing_set) for input_path, src_dir, gcs_folder, bucket_nm, existing_set in tasks_to_submit}


            total_tasks = len(futures)
            completed_tasks = 0
            # Process results as they complete
            for future in as_completed(futures):
                completed_tasks += 1
                original_task_args = futures[future] # Get args if needed for logging
                input_path_for_log = original_task_args[0] # Get input path from tuple
                try:
                    status, message = future.result()
                    if status == 'success':
                        processed_count += 1
                        logging.info(f"Success: {os.path.basename(input_path_for_log)} -> {message}")
                    elif status == 'skipped':
                        skipped_count += 1
                        # This count happens before submission now, but good to track results
                    elif status == 'processing_failed':
                        processing_failed_count += 1
                    elif status == 'upload_failed':
                        upload_failed_count += 1
                    elif status == 'processed_empty':
                        # Count as a processing failure or separate category? Let's count failure.
                        processing_failed_count += 1
                    elif status == 'worker_error':
                        worker_error_count += 1
                        logging.error(f"Worker error processing {message}") # Message contains details

                    if completed_tasks % 100 == 0 or completed_tasks == total_tasks: # Print progress periodically
                        print(f"Progress: {completed_tasks}/{total_tasks} tasks completed...")

                except Exception as exc:
                    worker_error_count += 1
                    logging.exception(f"Worker future for {input_path_for_log} generated an exception: {exc}")
                    print(f"ERROR: Worker task for {os.path.basename(input_path_for_log)} failed unexpectedly: {exc}")

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Parallel processing complete in {duration:.2f} seconds.")
    logging.info(f"Summary - Success: {processed_count}, Skipped (already in GCS): {skipped_count}, Processing Failed/Empty: {processing_failed_count}, Upload Failed: {upload_failed_count}, Worker Errors: {worker_error_count}")
    print(f"Parallel processing complete in {duration:.2f} seconds.")
    print(f"Summary - Success: {processed_count}, Skipped (already in GCS): {skipped_count}, Processing Failed/Empty: {processing_failed_count}, Upload Failed: {upload_failed_count}, Worker Errors: {worker_error_count}")

    # Return count of files successfully processed AND uploaded in this run
    return processed_count


# --- Main Execution ---
if __name__ == "__main__":
    main_start_time = time.time()
    logging.info("--- Starting Main Execution ---")
    print("--- Starting Main Execution ---")
    successful_upload_count = 0
    try:
        # 1. Process local files in parallel, skip existing GCS, upload new ones
        successful_upload_count = parallel_process_and_upload(source_extracted_dir, gcs_processed_folder, bucket) # Pass bucket object
    except Exception as e:
        logging.exception(f"A critical error occurred in the main workflow")
        print(f"CRITICAL ERROR occurred. Check logs: {log_file}")
    finally:
        main_end_time = time.time()
        logging.info(f"--- Workflow finished in {main_end_time - main_start_time:.2f} seconds ---")
        print(f"--- Workflow finished in {main_end_time - main_start_time:.2f} seconds ---")
        print(f"Script finished. Check log file for details: {log_file}")