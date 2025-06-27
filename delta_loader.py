import os
import json
import gzip
import requests
import logging
import mysql.connector
from mysql.connector import pooling, Error
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import zipfile
import shutil
import argparse

# python3 -m venv venv; source venv/bin/activate; pip install mysql-connector-python; pip install requests
# python delta_loader.py

# Used to load incremental updates from DVLA MOT API, processing delta files and ignoring any that are already-laoded/processed.

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------- Environment Variables & Config --------------------
REQUIRED_ENV_VARS = [
    "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME",
    "DVLA_TENANT_ID", "DVLA_CLIENT_ID", "DVLA_CLIENT_SECRET", "DVLA_API_KEY"
]

def validate_env_vars() -> None:
    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        raise EnvironmentError(f"Missing required environment variables: {missing}")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "raise_on_warnings": True,
    "autocommit": False,
}

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "./downloads")
FAILED_DIR = os.getenv("FAILED_DIR", "./failed")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "8192"))
MAX_WORKERS = 3
POOL_SIZE = 20

# Updated API endpoint and headers to match mot_api_manual_download.py
DVLA_BASE_URL = "https://history.mot.api.gov.uk/v1/trade/vehicles"
BULK_DOWNLOAD_URL = f"{DVLA_BASE_URL}/bulk-download"
TAPI_SCOPE = "https://tapi.dvsa.gov.uk/.default"

TENANT_ID = os.getenv("DVLA_TENANT_ID")
CLIENT_ID = os.getenv("DVLA_CLIENT_ID")
CLIENT_SECRET = os.getenv("DVLA_CLIENT_SECRET")
API_KEY = os.getenv("DVLA_API_KEY")

# -------------------- Directory Management --------------------
def ensure_directories_exist():
    """Ensure both downloads and failed directories exist"""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR, exist_ok=True)
    logger.info(f"Ensured directories exist: {DOWNLOAD_DIR}, {FAILED_DIR}")

# -------------------- Utility --------------------
def clean_datetime(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    dt_str = dt_str.replace("T", " ").rstrip("Z")
    return dt_str.split(".")[0] if "." in dt_str else dt_str

def retry_request(func):
    def wrapper(*args, **kwargs):
        delay = 2
        for attempt in range(5):
            try:
                return func(*args, **kwargs)
            except requests.RequestException as e:
                logger.warning(f"Retry {attempt + 1} after error: {e}")
                time.sleep(delay)
                delay *= 2
        raise RuntimeError("Max retries exceeded")
    return wrapper

# -------------------- Auth --------------------
@retry_request
def get_access_token() -> str:
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": TAPI_SCOPE,
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# -------------------- DVLA --------------------
@retry_request
def list_dvla_files(access_token: str) -> List[Dict[str, Any]]:
    url = BULK_DOWNLOAD_URL
    headers = {"Authorization": f"Bearer {access_token}", "X-API-Key": API_KEY}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# -------------------- DB Connection --------------------
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mot_pool",
    pool_size=POOL_SIZE,
    **DB_CONFIG
)

# -------------------- Batch Inserts --------------------
def batch_insert_vehicles(cursor, vehicles):
    if not vehicles:
        return 0, 0
    placeholders = []
    values = []
    for v in vehicles:
        placeholders.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        values.extend([
            v.get("registration"),
            clean_datetime(v.get("firstUsedDate")),
            clean_datetime(v.get("registrationDate")),
            clean_datetime(v.get("manufactureDate")),
            v.get("primaryColour"),
            v.get("secondaryColour"),
            v.get("engineSize"),
            v.get("model"),
            v.get("make"),
            v.get("fuelType"),
            clean_datetime(v.get("lastMotTestDate")),
            v.get("lastUpdateTimestamp"),
            v.get("dataSource"),
            v.get("lastUpdateDate"),
            v.get("modification"),
        ])
    sql = f"""
        INSERT INTO vehicles (
            registration, first_used_date, registration_date, manufacture_date,
            primary_colour, secondary_colour, engine_size, model, make, fuel_type,
            last_mot_test_date, last_update_timestamp, data_source, last_update_date, modification
        )
        VALUES {', '.join(placeholders)} AS new_values
        ON DUPLICATE KEY UPDATE
            first_used_date = new_values.first_used_date,
            registration_date = new_values.registration_date,
            manufacture_date = new_values.manufacture_date,
            primary_colour = new_values.primary_colour,
            secondary_colour = new_values.secondary_colour,
            engine_size = new_values.engine_size,
            model = new_values.model,
            make = new_values.make,
            fuel_type = new_values.fuel_type,
            last_mot_test_date = new_values.last_mot_test_date,
            last_update_timestamp = new_values.last_update_timestamp,
            data_source = new_values.data_source,
            last_update_date = new_values.last_update_date,
            modification = new_values.modification
    """
    try:
        cursor.execute(sql, values)
        return len(vehicles), 0
    except Error as e:
        logger.error(f"❌ ERROR batch inserting vehicles: {e}")
        return 0, len(vehicles)

def batch_insert_mot_tests(cursor, mot_tests):
    if not mot_tests:
        return 0, 0
    placeholders = []
    values = []
    for mt in mot_tests:
        placeholders.append("(%s, %s, %s, %s, %s, %s, %s)")
        values.extend([
            mt["registration"],
            clean_datetime(mt.get("completedDate")),
            clean_datetime(mt.get("expiryDate")),
            mt.get("testResult"),
            mt.get("odometerValue"),
            mt.get("odometerUnit"),
            mt.get("odometerResultType"),
        ])
    sql = f"""
        INSERT INTO mot_tests (
            registration, completed_date, expiry_date, test_result,
            odometer_value, odometer_unit, odometer_result_type
        )
        VALUES {', '.join(placeholders)} AS new_values
        ON DUPLICATE KEY UPDATE
            completed_date = new_values.completed_date,
            expiry_date = new_values.expiry_date,
            test_result = new_values.test_result,
            odometer_value = new_values.odometer_value,
            odometer_unit = new_values.odometer_unit,
            odometer_result_type = new_values.odometer_result_type
    """
    try:
        cursor.execute(sql, values)
        return len(mot_tests), 0
    except Error as e:
        logger.error(f"❌ ERROR batch inserting MOT tests: {e}")
        return 0, len(mot_tests)

# -------------------- Import Tracking --------------------
def mark_file_status(conn, filename: str, status: str):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO import_log (filename, import_timestamp, status)
        VALUES (%s, %s, %s) AS new_values
        ON DUPLICATE KEY UPDATE import_timestamp = new_values.import_timestamp, status = new_values.status
        """,
        (filename, datetime.now(timezone.utc), status),
    )
    conn.commit()
    cursor.close()

def is_file_imported(conn, filename: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM import_log WHERE filename = %s AND status = 'COMPLETED' LIMIT 1",
        (filename,),
    )
    result = cursor.fetchone()
    cursor.close()
    return result is not None

def is_file_downloaded_or_completed(conn, filename: str) -> bool:
    """Check if file is already downloaded or completed"""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM import_log WHERE filename = %s AND status IN ('DOWNLOADED', 'COMPLETED') LIMIT 1",
        (filename,),
    )
    result = cursor.fetchone()
    cursor.close()
    return result is not None

def get_failed_files(conn) -> List[str]:
    """Get list of files marked as FAILED in the database"""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT filename FROM import_log WHERE status = 'FAILED'",
    )
    results = cursor.fetchall()
    cursor.close()
    return [row[0] for row in results]

def move_file_to_failed_dir(filepath: str) -> str:
    """Move failed file to failed directory and return new path"""
    filename = os.path.basename(filepath)
    failed_path = os.path.join(FAILED_DIR, filename)
    
    if os.path.exists(filepath):
        shutil.move(filepath, failed_path)
        logger.info(f"Moved failed file to: {failed_path}")
    
    return failed_path

def move_file_from_failed_dir(filename: str) -> Optional[str]:
    """Move file from failed directory back to downloads directory"""
    failed_path = os.path.join(FAILED_DIR, filename)
    if os.path.exists(failed_path):
        downloads_path = os.path.join(DOWNLOAD_DIR, filename)
        shutil.move(failed_path, downloads_path)
        logger.info(f"Moved file from failed directory back to downloads: {downloads_path}")
        return downloads_path
    return None

def remove_from_failed_dir(filename: str) -> bool:
    """Remove file from failed directory after successful retry"""
    failed_path = os.path.join(FAILED_DIR, filename)
    if os.path.exists(failed_path):
        os.remove(failed_path)
        logger.info(f"Removed successfully retried file from failed directory: {failed_path}")
        return True
    return False

# -------------------- Process File --------------------
def process_file(filepath: str):
    thread_name = threading.current_thread().name
    filename = os.path.basename(filepath)
    logger.info(f"[{thread_name}] Processing file: {filepath}")

    processed = 0
    inserted_vehicles = 0
    failed_vehicles = 0
    inserted_mot_tests = 0
    failed_mot_tests = 0

    try:
        with connection_pool.get_connection() as conn_status:
            mark_file_status(conn_status, filename, "STARTED")

        vehicles_batch = []
        mot_tests_batch = []
        defects_batch = []

        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            for line_number, line in enumerate(f, 1):
                try:
                    vehicle = json.loads(line)
                    registration = vehicle.get("registration")
                    if not registration:
                        logger.warning(f"[{thread_name}] Skipping line {line_number}: no registration found.")
                        continue

                    vehicles_batch.append(vehicle)

                    mot_tests = vehicle.get("motTests", [])
                    for mot_test in mot_tests:
                        mot_test_record = {
                            "registration": registration,
                            "completedDate": mot_test.get("completedDate"),
                            "expiryDate": mot_test.get("expiryDate"),
                            "testResult": mot_test.get("testResult"),
                            "odometerValue": mot_test.get("odometerValue"),
                            "odometerUnit": mot_test.get("odometerUnit"),
                            "odometerResultType": mot_test.get("odometerResultType"),
                        }
                        mot_tests_batch.append(mot_test_record)

                        # Defects will be linked after mot_tests inserted, so store temporarily with index
                        defects = mot_test.get("defects", [])
                        for defect in defects:
                            defects_batch.append(
                                {
                                    "mot_test_id": None,  # Will be updated after mot_tests insert
                                    "registration": registration,
                                    "defect": defect,
                                }
                            )

                    processed += 1

                    if processed % BATCH_SIZE == 0:
                        with connection_pool.get_connection() as conn:
                            with conn.cursor() as cursor:
                                inserted, failed = batch_insert_vehicles(cursor, vehicles_batch)
                                inserted_vehicles += inserted
                                failed_vehicles += failed

                                inserted_mt, failed_mt = batch_insert_mot_tests(cursor, mot_tests_batch)
                                inserted_mot_tests += inserted_mt
                                failed_mot_tests += failed_mt

                                conn.commit()

                        vehicles_batch.clear()
                        mot_tests_batch.clear()
                        defects_batch.clear()

                        logger.info(f"[{thread_name}] ✅ Committed {processed} vehicles so far...")

                except json.JSONDecodeError as e:
                    logger.error(f"[{thread_name}] JSON decode error at line {line_number}: {e}")
                except Exception as e:
                    logger.error(f"[{thread_name}] Unexpected error at line {line_number}: {e}")

        if vehicles_batch or mot_tests_batch:
            with connection_pool.get_connection() as conn:
                with conn.cursor() as cursor:
                    if vehicles_batch:
                        inserted, failed = batch_insert_vehicles(cursor, vehicles_batch)
                        inserted_vehicles += inserted
                        failed_vehicles += failed
                    if mot_tests_batch:
                        inserted_mt, failed_mt = batch_insert_mot_tests(cursor, mot_tests_batch)
                        inserted_mot_tests += inserted_mt
                        failed_mot_tests += failed_mt
                    conn.commit()

        logger.info(f"[{thread_name}] ✅ Finished processing {processed} vehicles from {filepath}")
        logger.info(f"[{thread_name}] Inserted vehicles: {inserted_vehicles}, Failed vehicles: {failed_vehicles}")
        logger.info(f"[{thread_name}] Inserted MOT tests: {inserted_mot_tests}, Failed MOT tests: {failed_mot_tests}")

        with connection_pool.get_connection() as conn_status:
            mark_file_status(conn_status, filename, "COMPLETED")
            
        logger.info(f"[{thread_name}] ✅ Marked file as COMPLETED: {filename}")

    except Exception as e:
        logger.error(f"[{thread_name}] Error processing file {filename}: {e}")
        try:
            with connection_pool.get_connection() as conn_status:
                mark_file_status(conn_status, filename, "FAILED")
            # Move failed file to failed directory
            move_file_to_failed_dir(filepath)
        except Exception as mark_err:
            logger.error(f"[{thread_name}] Error marking file status FAILED for {filename}: {mark_err}")

# -------------------- Retry Failed Files --------------------
def retry_failed_files():
    """Retry processing files that previously failed"""
    try:
        with connection_pool.get_connection() as conn:
            failed_files = get_failed_files(conn)
            
        if not failed_files:
            logger.info("No failed files found in database to retry")
            return
            
        logger.info(f"Found {len(failed_files)} failed files to retry")
        
        retried_files = []
        successful_retries = []
        failed_retries = []
        
        for filename in failed_files:
            logger.info(f"🔄 Attempting to retry failed file: {filename}")
            
            # Move file from failed directory back to processing
            processing_path = move_file_from_failed_dir(os.path.basename(filename))
            
            if not processing_path or not os.path.exists(processing_path):
                logger.warning(f"Failed file not found in failed directory, skipping: {filename}")
                continue
                
            retried_files.append(filename)
            
            try:
                # If it's a zip file, handle extraction
                if zipfile.is_zipfile(processing_path):
                    unzip_dir = os.path.join(DOWNLOAD_DIR, os.path.splitext(os.path.basename(filename))[0])
                    os.makedirs(unzip_dir, exist_ok=True)
                    logger.info(f"Unzipping retry file {processing_path} to {unzip_dir}...")
                    with zipfile.ZipFile(processing_path, 'r') as zip_ref:
                        zip_ref.extractall(unzip_dir)
                    logger.info(f"Finished unzipping retry file {processing_path}")
                    
                    # Process all json.gz files inside extracted directory
                    retry_success = True
                    for root, _, files in os.walk(unzip_dir):
                        for file in files:
                            if file.endswith(".json.gz"):
                                file_path = os.path.join(root, file)
                                try:
                                    logger.info(f"🔄 Retry processing file {file_path}...")
                                    process_file(file_path)
                                    logger.info(f"✅ Successfully retried file: {file}")
                                except Exception as e:
                                    logger.error(f"❌ Retry failed for file {file}: {e}")
                                    retry_success = False
                    
                    # Clean up extracted directory
                    if os.path.exists(unzip_dir):
                        shutil.rmtree(unzip_dir)
                        logger.info(f"Deleted extracted retry directory {unzip_dir}")
                        
                else:
                    # Process single file directly 
                    logger.info(f"🔄 Retry processing file {processing_path}...")
                    process_file(processing_path)
                    retry_success = True
                    
                if retry_success:
                    successful_retries.append(filename)
                    # Remove from failed directory after successful retry
                    remove_from_failed_dir(os.path.basename(filename))
                    logger.info(f"✅ Successfully retried and completed: {filename}")
                else:
                    failed_retries.append(filename)
                    # Move back to failed directory
                    move_file_to_failed_dir(processing_path)
                    logger.error(f"❌ Retry failed, moved back to failed directory: {filename}")
                    
            except Exception as e:
                logger.error(f"❌ Error during retry of {filename}: {e}")
                failed_retries.append(filename)
                # Move back to failed directory
                if os.path.exists(processing_path):
                    move_file_to_failed_dir(processing_path)
        
        # Log retry summary
        logger.info("===== RETRY PROCESS SUMMARY =====")
        logger.info(f"Total failed files found: {len(failed_files)}")
        logger.info(f"Files attempted to retry: {len(retried_files)}")
        logger.info(f"Successful retries: {len(successful_retries)}")
        for f in successful_retries:
            logger.info(f"  ✅ {f}")
        logger.info(f"Failed retries: {len(failed_retries)}")
        for f in failed_retries:
            logger.info(f"  ❌ {f}")
        logger.info("==================================")
        
    except Exception as e:
        logger.error(f"🚨 Fatal error during retry process: {e}")

# -------------------- Main --------------------
def main():
    parser = argparse.ArgumentParser(description="Import MOT incremental debug")
    parser.add_argument("--retry", action="store_true", help="Reprocess failed files")
    args = parser.parse_args()
    
    try:
        validate_env_vars()
        ensure_directories_exist()
        
        if args.retry:
            logger.info("🔄 RETRY MODE: Reprocessing failed files")
            retry_failed_files()
            return
        
        token = get_access_token()
        files_response = list_dvla_files(token)

        # The response contains keys 'bulk' and 'delta' with lists of files
        bulk_files = files_response.get("bulk", [])
        delta_files = files_response.get("delta", [])

        all_files = bulk_files + delta_files

        files_to_process = []
        files_skipped = []
        downloaded_files = []
        unzipped_dirs = []
        unzip_to_original_file = {}  # Map unzip directory to original file name

        with connection_pool.get_connection() as conn:
            for f in all_files:
                # mot_api_manual_download.py uses 'filename' key
                name = f.get("filename")
                url = f.get("downloadUrl")
                if not name or not url:
                    continue
                # Skip bulk files if already imported or to avoid re-downloading large bulk files
                if name.startswith("v1/public/bulk/"):
                    if is_file_imported(conn, name):
                        files_skipped.append(name)
                        logger.info(f"Skipping already imported bulk file: {name}")
                        continue
                    else:
                        logger.info(f"Skipping new bulk file to avoid large re-download: {name}")
                        continue

                if is_file_imported(conn, name):
                    files_skipped.append(name)
                    logger.info(f"Skipping already imported file: {name}")
                    continue
                files_to_process.append(name)
                logger.info(f"Preparing to download and process file: {name}")

                # Download file to downloads directory
                local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(name))
                if not os.path.exists(local_path):
                    logger.info(f"Downloading {name}...")
                    r = requests.get(url, stream=True)
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    downloaded = 0
                    with open(local_path, "wb") as f_out:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f_out.write(chunk)
                                downloaded += len(chunk)
                                if total_size > 0:
                                    percent = downloaded * 100 // total_size
                                    logger.info(f"Downloading {name}: {percent}% ({downloaded}/{total_size} bytes)")
                    logger.info(f"Finished downloading {name}")
                    downloaded_files.append(local_path)

                    # Record downloaded file in import_log as DOWNLOADED
                    mark_file_status(conn, name, "DOWNLOADED")
                else:
                    logger.info(f"File already downloaded: {name}")
                    downloaded_files.append(local_path)

                # Unzip the file if it is a zip archive
                if zipfile.is_zipfile(local_path):
                    unzip_dir = os.path.join(DOWNLOAD_DIR, os.path.splitext(os.path.basename(name))[0])
                    os.makedirs(unzip_dir, exist_ok=True)
                    logger.info(f"Unzipping {local_path} to {unzip_dir}...")
                    with zipfile.ZipFile(local_path, 'r') as zip_ref:
                        zip_ref.extractall(unzip_dir)
                    logger.info(f"Finished unzipping {local_path}")
                    unzipped_dirs.append(unzip_dir)
                    unzip_to_original_file[unzip_dir] = name  # Map unzip dir to original file name

        # Process all json.gz files inside each unzipped directory
        for unzip_dir in unzipped_dirs:
            original_file_name = unzip_to_original_file[unzip_dir]
            logger.info(f"Processing extracted files in {unzip_dir}...")
            processed_files = []
            for root, _, files in os.walk(unzip_dir):
                for file in files:
                    if file.endswith(".json.gz"):
                        file_path = os.path.join(root, file)
                        logger.info(f"Processing file {file_path}...")
                        process_file(file_path)
                        processed_files.append(file)
            
            # Mark the original downloaded file as COMPLETED after all extracted files are processed
            with connection_pool.get_connection() as conn:
                mark_file_status(conn, original_file_name, "COMPLETED")
            logger.info(f"Marked original file as COMPLETED: {original_file_name} (processed {len(processed_files)} extracted files)")
            
            # After processing all files, delete the extracted directory and contents
            logger.info(f"Deleting extracted directory {unzip_dir}...")
            shutil.rmtree(unzip_dir)
            logger.info(f"Deleted extracted directory {unzip_dir}")

        logger.info("===== DVLA Files Report =====")
        logger.info(f"Total files found: {len(all_files)}")
        logger.info(f"Files to process (not yet imported): {len(files_to_process)}")
        for f in files_to_process:
            logger.info(f"  - {f}")
        logger.info(f"Files skipped (already imported): {len(files_skipped)}")
        for f in files_skipped:
            logger.info(f"  - {f}")
        logger.info(f"Downloaded files: {len(downloaded_files)}")
        for f in downloaded_files:
            logger.info(f"  - {f}")
        logger.info(f"Processed and deleted extracted directories: {len(unzipped_dirs)}")
        for d in unzipped_dirs:
            logger.info(f"  - {d}")
        logger.info("=============================")

    except Exception as e:
        logger.error(f"🚨 Fatal error: {e}")

if __name__ == "__main__":
    main()