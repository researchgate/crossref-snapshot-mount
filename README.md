# crossref-snapshot-mount
This repository contains the code useed to mount the April 2025 crossref snapshot to Bigquery

# Create a VM (with name crossref):

## why?
We will be working primarily with VM to download the latest snapshot and upload the cleaned up data to BigQuery.

## Configuration and setup
* consider enough space while creating VM ~700 GB (can be reduced later)
* while creating the VM, under `Identity and API access` -> Allow full access to all Cloud APIs. This will be required to write
  to bucket.
* Once your VM is setup, find out your service account
  ```gcloud compute instances describe crossref --zone="northamerica-northeast2-b" --format='value(serviceAccounts.email)'`
    * grant permission to your service account to be able to manage objects in bucket via VM
      ```gcloud storage buckets add-iam-policy-binding gs://crossref \
          --member=serviceAccount:830983394603-compute@developer.gserviceaccount.com \
          --role=roles/storage.objectAdmin
    * **restart** the VM for IAM policy to take effect

## Download the crossref yearly dump and set up the VM to process it

* Once your VM  is setup. Start the VM and install awscli

  ```sudo apt install awscli```

* Configure and download the snapshot
    ```aws configure
       AWS Access Key ID [None]:  $give_the_key_id
       AWS Secret Access Key [None]: $give_the_secret_access_key
       Default region name [None]: 
       Default output format [None]: 

* List the files by crossref on aws:
  ```
       aws s3 ls --request-payer requester s3://api-snapshots-reqpays-crossref

       should show something like:  
       2023-05-09 15:14:12 185897154560 April_2023_Public_Data_File_from_Crossref.tar
       2024-05-09 19:56:17 212156057600 April_2024_Public_Data_File_from_Crossref.tar
  ```

* Download the file to current location in your VM:

  ```aws s3api get-object --bucket api-snapshots-reqpays-crossref --request-payer requester --key  April_2024_Public_Data_File_from_Crossref.tar ./April_2024_Public_Data_File_from_Crossref.tar```

* upload the file to the crossref bucket to prevent any mishaps in case of VM deletion and data loss

  ```gsutil cp April_2024_Public_Data_File_from_Crossref.tar  gs://crossref```

* unzip the tar file in VM because we will cleanup the data with a python script that accesses extracted files in VM:

  ```tar -xvf April_2024_Public_Data_File_from_Crossref.tar```

* Python set up in VM
  ```sudo apt install python3-pip
      sudo apt install python3-venv
      python3 -m venv myenv
      source myenv/bin/activate

* BQ package install:

  ```pip3 install google-cloud-storage google-cloud-bigquery```

## Processing the yearly dump

We divide this process into two-steps
* cleaning the data
* loading the data to BQ

### Why?
The VM tends to lose session while the data is being cleaned/uploaded to buckets for loading to BQ. Therefore, it is much better to clean
the data first and then attempt the loading to BQ , because even the loading step may have several failures and with 30k+ files it is easy
to lose track of successful and failed file cleanups/uploads.

### Cleanup

* The minimum amount of cleanup that is a must is flattening the field "date-parts". In the crossref snapshots, date-parts are double arrays [["YYYY-MM-DDTHH:MM:S]] fields which BQ does not allow.
  We run a python script to flatten the date-parts field.

#### Running the cleanup script

* Setup gcloud CLI local machine.
  https://cloud.google.com/sdk/docs/install

* Make sure your VM is started. Now upload the script to your VM

``` 
  gcloud compute scp $path_to_cleanup_script \
   rashmi_varma@crossref:$path_to_where_you_want_your_script_in_VM \
   --zone="$zone-of-your-VM" \
   --tunnel-through-iap
```
   
   
* Make sure your venv environment is activated(source myenv/bin/activate) in the VM. Run the script in your VM
  ```
  python3 crossref_cleanup.py
  ``` 
  This can take very long time depending on the number of files and the size of the files.
  Full disclosure, this script was written with the help of Gemini. Feel free to improve it and write your own :).

* The script will create a folder called "processed_for_bq"  as specified by the variable : gcs_processed_folder 
  in the  bucket named crossref and shall upload all the cleaned up files potentially ready to be uploaded to a BQ table.

* Try your luck:  If you are feeling lucky , and the crossref data is super clean , you can directly upload all the .jsonl.gz files to a BQ table with the command
  ``` 
    bq load \
    --autodetect \
    --source_format=NEWLINE_DELIMITED_JSON \
    --max_bad_records=1000 \   # optional  I usually omit this argument to catch all the errors and get all the data
    dataset_id.table_name \
    'gs://crossref/extracted/*.jsonl.gz'`
  ```
Unfortunately, more often than not , the data is not clean and you will have to do further clean ups.

### Upload to BQ (patience required!!!)

* Now that first round of cleanup is done and the luck did not work out , you are here!!
* So, we can start uploading the cleaned up files step-wise to BQ , eliminating the problematic ones and handling them at the end.

* Upload the schema file to the VM (preferably in same location as script)
``` 
  gcloud compute scp $path_to_schema_file \
   rashmi_varma@crossref:$path_to_where_you_want_your_schema_in_VM \
   --zone="$zone-of-your-VM" \
   --tunnel-through-iap
```
* Upload the BQ load script
  ``` 
  gcloud compute scp /Users/rvarma/Documents/repositories/crossref-snapshot-mount/scripts/loadtobq.py 
  rashmi_varma@crossref:/home/rashmi_varma  \
  --zone="northamerica-northeast2-b" \
  --tunnel-through-iap
  ```

* **BQ caveats and limitations**
    BigQuery has rate limits on table update operations: 
    Max: 1,500 load jobs per table per day ✅ 

    BUT ALSO:
    ⛔️ Max: 100 table update operations per 10 seconds per table

   We try to circumvent those by diving the total number of files into batches , so that the 
   number of load jobs on the table we are creating is <1500
   We try to deal with the time limitation between table update operations by adding a DELAY. 
   I used a delay of 10 seconds and there were still several errors:

#### Running the upload script

  ```
  python3 loadtobq.py
  ``` 

  * Common Errors

   ```
   Rate Limiting: 
   FAILED: gs://crossref/processed_for_bq/10012.jsonl.gz - 429 Exceeded rate limits: too many table update operations for this table. For more information, see https://cloud.google.com/bigquery/docs/troubleshoot-quotas; reason: rateLimitExceeded, location: table.write, message: Exceeded rate limits: too many table update ope
  
   data error:
   
  FAILED: gs://crossref/processed_for_bq/14822.jsonl.gz - 400 Cannot return an invalid timestamp value of 569552083200000000 microseconds relative to the Unix epoch. The range of valid timestamp values is [0001-01-01 00:00:00, 9999-12-31 23:59:59.999999]; error in writing field updated-by.updated.date-time; error in writing field updated-by.updated; error in writing field updated-by; reason: invalidQuery, location: query, message: Cannot return an invalid timestamp value of 569552083200000000 microseconds relative to the Unix epoch. The range of valid timestamp values is [0001-01-01 00:00:00, 9999-12-31 23:59:59.999999]; error in writing field updated-by.updated.date-time; error in writing field updated-by.updated; error in writing field updated-by
  ``` 
  
* For the rate-limiting error, upload the **loadtobq_retry.py** script.
  This will attempt to reload the files listed in failed_uploads.txt. 
  For speed you can increase the size of the batch by increasing $FILES_PER_BATCH
  
  DISCLAIMER: you will have to re-run the **loadtobq_retry.py** couple of times until all that remains of the errors is bad data i.e. the 400s.

### Dealing with 400  (manual)

* Download the .jsonl.gz in question to your local machine from the "processed_for_bq" bucket in GCP
* Go through each file and error in the file referenced by $RETRY_ERROR_LOG in loadtobq_retry.py script.
  and fix the error manually. Sometimes there are only 1 or two records that have errors.
* re-upload the .jsonl to the bucket in a different folder(like manually_cleanedup) in crossref bucket for easier tracking.
* Run this command on GCP terminal. Upload the schema.json file to the terminal.
  ```
    bq load --source_format=NEWLINE_DELIMITED_JSON --schema=schema.json  dataset_id.table_id  gs://crossref/manually_cleanedup/14822.jsonl
  ```
  if it shows more errors , cleanup.
* do the process for all the files in question. 
  If the files in question are too many with a pattern in the errors, you can use `jq` tool to fix them via commandline.

