import mysql.connector
import json
import gzip
import os
import logging
from mysql.connector import pooling, Error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


# python3 -m venv venv; source venv/bin/activate; pip install mysql-connector-python; pip install requests
# python initial_loader.py

# Used to load the initial "bulk" file as a one-off, that should be over 40GB in size.


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = "/home/don/workspaces/mot_scripts/bulk-light-vehicle_16-06-2025"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "raise_on_warnings": True,
    "autocommit": False,
}

BATCH_SIZE = 1000
MAX_WORKERS = 3
POOL_SIZE = 20

# Create a connection pool
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mot_pool",
    pool_size=POOL_SIZE,
    **DB_CONFIG
)

def clean_datetime(dt_str):
    if not dt_str:
        return None
    dt_str = dt_str.replace('T', ' ')
    if dt_str.endswith('Z'):
        dt_str = dt_str[:-1]
    if '.' in dt_str:
        dt_str = dt_str.split('.')[0]
    return dt_str

def mark_file_status(conn, filename: str, status: str):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO import_log (filename, import_timestamp, status)
        VALUES (%s, %s, %s)
        AS new_values
        ON DUPLICATE KEY UPDATE import_timestamp = new_values.import_timestamp, status = new_values.status
    """, (filename, datetime.now(timezone.utc), status))
    conn.commit()
    cursor.close()

def is_file_imported(conn, filename: str) -> bool:
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM import_log WHERE filename = %s AND status = 'COMPLETED' LIMIT 1", (filename,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None

def batch_insert_vehicles(cursor, vehicles):
    if not vehicles:
        return 0, 0
    placeholders = []
    values = []
    for v in vehicles:
        placeholders.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        values.extend([
            v.get('registration'),
            clean_datetime(v.get('firstUsedDate')),
            clean_datetime(v.get('registrationDate')),
            clean_datetime(v.get('manufactureDate')),
            v.get('primaryColour'),
            v.get('secondaryColour'),
            v.get('engineSize'),
            v.get('model'),
            v.get('make'),
            v.get('fuelType'),
            clean_datetime(v.get('lastMotTestDate')),
            v.get('lastUpdateTimestamp'),
            v.get('dataSource'),
            v.get('lastUpdateDate'),
            v.get('modification')
        ])
    sql = f"""
        INSERT INTO vehicles
        (registration, first_used_date, registration_date, manufacture_date, primary_colour, secondary_colour,
         engine_size, model, make, fuel_type, last_mot_test_date, last_update_timestamp, data_source, last_update_date, modification)
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
            mt['registration'],
            clean_datetime(mt['completedDate']),
            clean_datetime(mt['expiryDate']),
            mt.get('testResult'),
            mt.get('odometerValue'),
            mt.get('odometerUnit'),
            mt.get('odometerResultType')
        ])
    sql = f"""
        INSERT INTO mot_tests
        (registration, completed_date, expiry_date, test_result, odometer_value, odometer_unit, odometer_result_type)
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

def batch_insert_defects(cursor, defects):
    if not defects:
        return
    placeholders = []
    values = []
    for d in defects:
        placeholders.append("(%s, %s, %s, %s)")
        values.extend([
            d['mot_test_id'],
            d.get('dangerous'),
            d.get('text'),
            d.get('type')
        ])
    sql = f"""
        INSERT INTO defects
        (mot_test_id, dangerous, text, type)
        VALUES {', '.join(placeholders)}
    """
    try:
        cursor.execute(sql, values)
    except Error as e:
        logger.error(f"❌ ERROR batch inserting defects: {e}")

def process_file(filepath):
    thread_name = threading.current_thread().name
    filename = os.path.basename(filepath)
    logger.info(f"[{thread_name}] Processing file: {filepath}")

    processed = 0
    inserted_vehicles = 0
    failed_vehicles = 0
    inserted_mot_tests = 0
    failed_mot_tests = 0

    try:
        # Mark file as started using a dedicated connection
        with connection_pool.get_connection() as conn_status:
            mark_file_status(conn_status, filename, "STARTED")

        vehicles_batch = []
        mot_tests_batch = []
        defects_batch = []

        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                try:
                    vehicle = json.loads(line)
                    registration = vehicle.get('registration')
                    if not registration:
                        logger.warning(f"[{thread_name}] Skipping line {line_number}: no registration found.")
                        continue

                    vehicles_batch.append(vehicle)

                    mot_tests = vehicle.get('motTests', [])
                    for mot_test in mot_tests:
                        mot_test_record = {
                            'registration': registration,
                            'completedDate': mot_test.get('completedDate'),
                            'expiryDate': mot_test.get('expiryDate'),
                            'testResult': mot_test.get('testResult'),
                            'odometerValue': mot_test.get('odometerValue'),
                            'odometerUnit': mot_test.get('odometerUnit'),
                            'odometerResultType': mot_test.get('odometerResultType')
                        }
                        mot_tests_batch.append(mot_test_record)

                        # Defects will be linked after mot_tests inserted, so store temporarily with index
                        defects = mot_test.get('defects', [])
                        for defect in defects:
                            defects_batch.append({
                                'mot_test_id': None,  # Will be updated after mot_tests insert
                                'registration': registration,
                                'defect': defect
                            })

                    processed += 1

                    if processed % BATCH_SIZE == 0:
                        # Use a fresh connection for batch inserts
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
                    vehicle = json.loads(line)
                    registration = vehicle.get('registration')
                try:
                    vehicle = json.loads(line)
                    registration = vehicle.get('registration')
                    if not registration:
                        logger.warning(f"[{thread_name}] Skipping line {line_number}: no registration found.")
                        continue

                    vehicles_batch.append(vehicle)

                    mot_tests = vehicle.get('motTests', [])
                    for mot_test in mot_tests:
                        mot_test_record = {
                            'registration': registration,
                            'completedDate': mot_test.get('completedDate'),
                            'expiryDate': mot_test.get('expiryDate'),
                            'testResult': mot_test.get('testResult'),
                            'odometerValue': mot_test.get('odometerValue'),
                            'odometerUnit': mot_test.get('odometerUnit'),
                            'odometerResultType': mot_test.get('odometerResultType')
                        }
                        mot_tests_batch.append(mot_test_record)

                        # Defects will be linked after mot_tests inserted, so store temporarily with index
                        defects = mot_test.get('defects', [])
                        for defect in defects:
                            defects_batch.append({
                                'mot_test_id': None,  # Will be updated after mot_tests insert
                                'registration': registration,
                                'defect': defect
                            })

                    processed += 1

                    if processed % BATCH_SIZE == 0:
                        # Use a fresh connection for batch inserts
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

        # Insert any remaining batches with fresh connection
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

        # Mark file as completed
        with connection_pool.get_connection() as conn_status:
            mark_file_status(conn_status, filename, "COMPLETED")

        # Query and log total vehicles count after processing file
        if thread_name == "ThreadPoolExecutor-0_0XXX":
            with connection_pool.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) AS total_vehicles FROM mot_data.vehicles;")
                    result = cursor.fetchone()
                    total_vehicles = result[0] if result else 0
                    logger.info(f"[{thread_name}] ✅ ✅ ✅ ✅ ✅  Total vehicles in database: {total_vehicles} ✅ ✅ ✅ ✅ ✅ ")

    except Exception as e:
        logger.error(f"[{thread_name}] Error processing file {filename}: {e}")
        try:
            with connection_pool.get_connection() as conn_status:
                mark_file_status(conn_status, filename, "FAILED")
        except Exception as mark_err:
            logger.error(f"[{thread_name}] Error marking file status FAILED for {filename}: {mark_err}")

def main():
    conn = connection_pool.get_connection()
    files_to_process = []
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.json.gz'):
            if is_file_imported(conn, filename):
                logger.info(f"⏭️ Skipping already imported file: {filename}")
                continue
            files_to_process.append(filename)
    conn.close()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for filename in files_to_process:
            filepath = os.path.join(DATA_DIR, filename)
            futures.append(executor.submit(process_file, filepath))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error processing file in thread: {e}")

if __name__ == '__main__':
    main()
