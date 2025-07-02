#!/usr/bin/env python3
"""
test_single_file.py - Test the actual import process on one file
"""
import os
import sys
import gzip
import json
import logging
from pathlib import Path
from mysql.connector import pooling

# python3 -m venv venv; source venv/bin/activate; pip install mysql-connector-python; pip install requests
# source envsetup.sh; python3 test_single_file.py

# or
# python3 initial_loader.py --file /home/don/workspaces/mot_scripts/bulk-light-vehicle_16-06-2025/bulk-light-vehicle_16-06-2025_100.json.gz


logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s')
logger = logging.getLogger(__name__)

def clean_datetime(dt_str):
    if not dt_str:
        return None
    dt_str = dt_str.replace("T", " ").rstrip("Z")
    return dt_str.split(".")[0] if "." in dt_str else dt_str

def test_actual_import():
    logger.info("🧪 Testing single-file import via initial_loader.process_file...")
    from pathlib import Path
    from initial_loader import process_file

    # Locate first bulk file
    bulk_dir = Path("./bulk-light-vehicle_16-06-2025")
    files = list(bulk_dir.glob("*.json.gz"))
    if not files:
        logger.error("No bulk files found in directory")
        return False
    test_file = files[0]
    logger.info(f"Processing file: {test_file.name}")

    try:
        # Invoke initial_loader's process_file to load vehicles, tests, and defects
        process_file(str(test_file))
        logger.info("✅ process_file completed successfully for single file")
        return True
    except Exception as e:
        logger.error(f"❌ process_file error: {e}")
        return False

if __name__ == "__main__":
    success = test_actual_import()
    sys.exit(0 if success else 1)
