import psycopg2
import psycopg2.pool
import psycopg2.extras
import json
import gzip
import os
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import argparse
import csv
import tempfile

# For environment: pip install psycopg2-binary
# python PG_initial_loader.py --use-copy
# for the fastest bulk load, or omit --use-copy to use batch inserts.

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = os.getenv("DATA_DIR", "/home/don/workspaces/mot_scripts/bulk-light-vehicle_16-06-2025")

REQUIRED_DB_VARS = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]

def validate_db_env():
    missing = [var for var in REQUIRED_DB_VARS if not os.getenv(var)]
    if missing:
        logger.error(f"Missing required DB environment variables: {', '.join(missing)}")
        sys.exit(1)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

BATCH_SIZE = 1000
MAX_WORKERS = 3
POOL_SIZE = 20

# Parse CLI args for COPY mode
parser = argparse.ArgumentParser(description="PG Initial Loader with optional CSV+COPY bulk load")
parser.add_argument("--use-copy", action="store_true", help="Use CSV & COPY for faster bulk inserts")
args = parser.parse_args()
USE_COPY = args.use_copy

# Pool will be initialized in main() after env validation
pool = None

def clean_datetime(dt_str):
    if not dt_str:
        return None
    dt_str = dt_str.replace("T", " ")
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1]
    if "." in dt_str:
        dt_str = dt_str.split(".")[0]
    return dt_str

def mark_file_status(conn, filename: str, status: str):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO import_log (filename, import_timestamp, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (filename) DO UPDATE
              SET import_timestamp = EXCLUDED.import_timestamp,
                  status = EXCLUDED.status
            """,
            (filename, datetime.now(timezone.utc), status),
        )
    conn.commit()

def is_file_imported(conn, filename: str) -> bool:
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM import_log WHERE filename = %s AND status = 'COMPLETED' LIMIT 1",
            (filename,),
        )
        return cursor.fetchone() is not None

def batch_insert_vehicles(cursor, vehicles):
    if not vehicles:
        return 0, 0
    vals = []
    for v in vehicles:
        if is_duplicate_vehicle(cursor, v.get("registration")):
            logger.warning(f"Duplicate vehicle skipped: {v.get('registration')}")
            continue
        try:
            engine_size = int(v.get("engineSize")) if v.get("engineSize") else None
        except ValueError:
            logger.error(f"Invalid engine size for vehicle {v.get('registration')}")
            engine_size = None
        vals.append((
            v.get("registration"),
            clean_datetime(v.get("firstUsedDate")),
            clean_datetime(v.get("registrationDate")),
            clean_datetime(v.get("manufactureDate")),
            v.get("primaryColour"),
            v.get("secondaryColour"),
            engine_size,
            v.get("model"),
            v.get("make"),
            v.get("fuelType"),
            clean_datetime(v.get("lastMotTestDate")),
            v.get("lastUpdateTimestamp"),
            v.get("dataSource"),
            v.get("lastUpdateDate"),
            v.get("modification"),
        ))
    sql = """
    INSERT INTO vehicles
      (registration, first_used_date, registration_date, manufacture_date,
       primary_colour, secondary_colour, engine_size, model, make, fuel_type,
       last_mot_test_date, last_update_timestamp, data_source, last_update_date, modification)
    VALUES %s
    """
    try:
        psycopg2.extras.execute_values(cursor, sql, vals, template=None, page_size=BATCH_SIZE)
        return len(vals), 0
    except Exception as e:
        logger.error(f"❌ ERROR inserting vehicles: {e}")
        return 0, len(vals)

def batch_insert_mot_tests(cursor, mot_tests):
    if not mot_tests:
        return 0, 0
    vals = [
        (
            mt["registration"],
            clean_datetime(mt.get("completedDate")),
            clean_datetime(mt.get("expiryDate")),
            mt.get("testResult"),
            mt.get("odometerValue"),
            mt.get("odometerUnit"),
            mt.get("odometerResultType"),
        )
        for mt in mot_tests
    ]
    sql = """
    INSERT INTO mot_tests
      (registration, completed_date, expiry_date, test_result, odometer_value,
       odometer_unit, odometer_result_type)
    VALUES %s
    """
    try:
        psycopg2.extras.execute_values(cursor, sql, vals, template=None, page_size=BATCH_SIZE)
        return len(vals), 0
    except Exception as e:
        logger.error(f"❌ ERROR inserting MOT tests: {e}")
        return 0, len(vals)

def batch_insert_defects(cursor, defects):
    if not defects:
        return 0
    vals = [
        (d["mot_test_id"], d.get("dangerous"), d.get("text"), d.get("type"))
        for d in defects
    ]
    sql = """
    INSERT INTO defects
      (mot_test_id, dangerous, text, type)
    VALUES %s
    """
    try:
        psycopg2.extras.execute_values(cursor, sql, vals, template=None, page_size=BATCH_SIZE)
        return len(vals)
    except Exception as e:
        logger.error(f"❌ ERROR inserting defects: {e}")
        return 0
def process_file_copy(filepath, index, total):
    thread_name = threading.current_thread().name
    filename = os.path.basename(filepath)
    logger.info(f"[{thread_name}] Processing file {index}/{total} (COPY): {filepath}")

    v_file = mt_file = d_file = None
    try:
        # Prepare temp CSV files
        try:
            v_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, newline="")
            mt_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, newline="")
            d_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, newline="")
        except Exception as e:
            logger.error(f"[{thread_name}] Error creating temp files: {e}")
            return

        v_writer = csv.writer(v_file)
        mt_writer = csv.writer(mt_file)
        d_writer = csv.writer(d_file)

        try:
            with gzip.open(filepath, "rt", encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        reg = rec.get("registration")
                        if not reg:
                            continue
                        # Vehicles row
                        v_writer.writerow([
                            reg,
                            clean_datetime(rec.get("firstUsedDate")),
                            clean_datetime(rec.get("registrationDate")),
                            clean_datetime(rec.get("manufactureDate")),
                            rec.get("primaryColour"),
                            rec.get("secondaryColour"),
                            rec.get("engineSize"),
                            rec.get("model"),
                            rec.get("make"),
                            rec.get("fuelType"),
                            clean_datetime(rec.get("lastMotTestDate")),
                            rec.get("lastUpdateTimestamp"),
                            rec.get("dataSource"),
                            clean_datetime(rec.get("lastUpdateDate")),
                            rec.get("modification"),
                        ])
                        for mt in rec.get("motTests", []):
                            mt_writer.writerow([
                                reg,
                                clean_datetime(mt.get("completedDate")),
                                clean_datetime(mt.get("expiryDate")),
                                mt.get("testResult"),
                                mt.get("odometerValue"),
                                mt.get("odometerUnit"),
                                mt.get("odometerResultType"),
                            ])
                            for d in mt.get("defects", []):
                                d_writer.writerow([
                                    reg,
                                    clean_datetime(mt.get("completedDate")),
                                    d.get("dangerous"),
                                    d.get("text"),
                                    d.get("type"),
                                ])
                    except Exception as e:
                        logger.error(f"[{thread_name}] JSON error: {e}")
        except Exception as e:
            logger.error(f"[{thread_name}] Error opening or reading gzip file {filepath}: {e}")
            return

        # Flush and close files
        for tmp in (v_file, mt_file, d_file):
            tmp.flush()
            tmp.close()

        conn = pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.copy_expert(
                    "COPY vehicles (registration, first_used_date, registration_date, manufacture_date, primary_colour, secondary_colour, engine_size, model, make, fuel_type, last_mot_test_date, last_update_timestamp, data_source, last_update_date, modification) FROM STDIN WITH CSV",
                    open(v_file.name, "r"),
                )
                cursor.copy_expert(
                    "COPY mot_tests (registration, completed_date, expiry_date, test_result, odometer_value, odometer_unit, odometer_result_type) FROM STDIN WITH CSV",
                    open(mt_file.name, "r"),
                )
                cursor.execute("""
                    CREATE TEMP TABLE tmp_defects (
                        registration VARCHAR(20),
                        completed_date TIMESTAMP,
                        dangerous BOOLEAN,
                        text TEXT,
                        type VARCHAR(50)
                    ) ON COMMIT DROP;
                """)
                cursor.copy_expert(
                    "COPY tmp_defects (registration, completed_date, dangerous, text, type) FROM STDIN WITH CSV",
                    open(d_file.name, "r"),
                )
                cursor.execute("""
                    INSERT INTO defects (mot_test_id, dangerous, text, type)
                    SELECT mt.id, t.dangerous, t.text, t.type
                    FROM tmp_defects t
                    JOIN mot_tests mt
                      ON mt.registration = t.registration AND mt.completed_date = t.completed_date;
                """)
            conn.commit()
        except Exception as e:
            logger.error(f"[{thread_name}] COPY load error: {e}")
            conn.rollback()
        finally:
            pool.putconn(conn)
    finally:
        # Cleanup temp files
        for tmp in (v_file, mt_file, d_file):
            if tmp is not None:
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass

# End of process_file_copy definition

def process_file(filepath, index, total):
    thread_name = threading.current_thread().name
    filename = os.path.basename(filepath)
    logger.info(f"[{thread_name}] Processing file {index}/{total}: {filepath}")

    processed = inserted_vehicles = failed_vehicles = 0
    inserted_mot_tests = failed_mot_tests = 0
    inserted_defects = 0
    vehicles_batch = []
    mot_tests_batch = []
    defects_batch = []

    try:
        conn_status = pool.getconn()
        mark_file_status(conn_status, filename, "STARTED")
        pool.putconn(conn_status)

        try:
            with gzip.open(filepath, "rt", encoding="utf-8") as f:
                for line in f:
                    processed += 1
                    try:
                        v = json.loads(line)
                        reg = v.get("registration")
                        if not reg:
                            logger.warning(f"[{thread_name}] Skipping line {processed}: no registration.")
                            continue
                        vehicles_batch.append(v)
                        for mt in v.get("motTests", []):
                            mot_tests_batch.append({"registration": reg, **mt})
                            for d in mt.get("defects", []):
                                defects_batch.append({"registration": reg, "completedDate": mt.get("completedDate"), **d})
                    except json.JSONDecodeError as e:
                        logger.error(f"[{thread_name}] JSON error at line {processed}: {e}")

                    if processed % BATCH_SIZE == 0:
                        conn = pool.getconn()
                        with conn.cursor() as cursor:
                            ins, err = batch_insert_vehicles(cursor, vehicles_batch)
                            inserted_vehicles += ins; failed_vehicles += err
                            ins_mt, err_mt = batch_insert_mot_tests(cursor, mot_tests_batch)
                            inserted_mot_tests += ins_mt; failed_mot_tests += err_mt
                            defect_records = []
                            for d in defects_batch:
                                cdt = clean_datetime(d["completedDate"])
                                cursor.execute(
                                    "SELECT id FROM mot_tests WHERE registration=%s AND completed_date=%s LIMIT 1",
                                    (d["registration"], cdt),
                                )
                                res = cursor.fetchone()
                                if res:
                                    defect_records.append({"mot_test_id": res[0], "dangerous": d.get("dangerous"), "text": d.get("text"), "type": d.get("type")})
                            ins_def = batch_insert_defects(cursor, defect_records)
                            inserted_defects += ins_def
                            conn.commit()
                        pool.putconn(conn)
                        vehicles_batch.clear(); mot_tests_batch.clear(); defects_batch.clear()
                        logger.info(f"[{thread_name}] ✅ Committed {processed} records...")
        except Exception as e:
            logger.error(f"[{thread_name}] Error opening or reading gzip file {filepath}: {e}")
            raise

        if vehicles_batch or mot_tests_batch or defects_batch:
            conn = pool.getconn()
            with conn.cursor() as cursor:
                ins, err = batch_insert_vehicles(cursor, vehicles_batch)
                inserted_vehicles += ins; failed_vehicles += err
                ins_mt, err_mt = batch_insert_mot_tests(cursor, mot_tests_batch)
                inserted_mot_tests += ins_mt; failed_mot_tests += err_mt
                defect_records = []
                for d in defects_batch:
                    cdt = clean_datetime(d["completedDate"])
                    cursor.execute(
                        "SELECT id FROM mot_tests WHERE registration=%s AND completed_date=%s LIMIT 1",
                        (d["registration"], cdt),
                    )
                    res = cursor.fetchone()
                    if res:
                        defect_records.append({"mot_test_id": res[0], "dangerous": d.get("dangerous"), "text": d.get("text"), "type": d.get("type")})
                inserted_defects += batch_insert_defects(cursor, defect_records)
                conn.commit()
            pool.putconn(conn)

        conn_status = pool.getconn()
        mark_file_status(conn_status, filename, "COMPLETED")
        pool.putconn(conn_status)

        logger.info(f"[{thread_name}] Finished {processed} vehicles; inserted {inserted_vehicles} vehicles, {inserted_mot_tests} tests, {inserted_defects} defects")
    except Exception as e:
        logger.error(f"[{thread_name}] Error: {e}")
        try:
            conn_status = pool.getconn()
            mark_file_status(conn_status, filename, "FAILED")
            pool.putconn(conn_status)
        except Exception as e2:
            logger.error(f"[{thread_name}] Error marking file as FAILED: {e2}")

def disable_indexes(conn):
    with conn.cursor() as cursor:
        cursor.execute("ALTER TABLE vehicles DISABLE TRIGGER ALL;")
        cursor.execute("ALTER TABLE mot_tests DISABLE TRIGGER ALL;")
        cursor.execute("ALTER TABLE defects DISABLE TRIGGER ALL;")
    conn.commit()

def enable_indexes(conn):
    with conn.cursor() as cursor:
        cursor.execute("ALTER TABLE vehicles ENABLE TRIGGER ALL;")
        cursor.execute("ALTER TABLE mot_tests ENABLE TRIGGER ALL;")
        cursor.execute("ALTER TABLE defects ENABLE TRIGGER ALL;")
    conn.commit()

def is_duplicate_vehicle(cursor, registration):
    cursor.execute("SELECT 1 FROM vehicles WHERE registration = %s LIMIT 1", (registration,))
    return cursor.fetchone() is not None

def main():
    global pool
    validate_db_env()
    try:
        pool = psycopg2.pool.ThreadedConnectionPool(
            1,
            POOL_SIZE,
            host=DB_CONFIG["host"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
        )
    except Exception as e:
        logger.error(f"Failed to create DB connection pool: {e}")
        sys.exit(1)

    conn = pool.getconn()
    disable_indexes(conn)
    pool.putconn(conn)

    try:
        try:
            conn = pool.getconn()
            files = []
            for fn in os.listdir(DATA_DIR):
                if fn.endswith(".json.gz"):
                    try:
                        if is_file_imported(conn, fn):
                            logger.info(f"⏭️ Skipping imported {fn}")
                            continue
                        files.append(fn)
                    except Exception as e:
                        logger.error(f"Error checking import status for {fn}: {e}")
            pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error listing files in {DATA_DIR}: {e}")
            sys.exit(1)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            loader = process_file_copy if USE_COPY else process_file
            total_files = len(files)
            futures = [
                executor.submit(loader, os.path.join(DATA_DIR, fn), idx, total_files)
                for idx, fn in enumerate(files, 1)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Thread error: {e}")
    finally:
        conn = pool.getconn()
        enable_indexes(conn)
        pool.putconn(conn)
        if pool:
            pool.closeall()
        logger.info("DB connection pool closed.")

if __name__ == "__main__":
    main()