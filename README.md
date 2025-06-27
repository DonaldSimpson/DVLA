# DVLA MOT Data Import Project

## Overview

This project automates the process of downloading, processing, and importing MOT (Ministry of Transport) vehicle data from the DVLA API into a MySQL database. 


It consists of several Python scripts that handle different stages of the workflow, including downloading data files, loading them into the database, and tracking progress.


To gain access to the DVLA's MOT history data via an API, you need to register for an API key here: 

https://documentation.history.mot.api.gov.uk/mot-history-api/register


This involves completing an application form detailing your organization and intended use of the data. Upon approval, you will receive an API key that allows you to access the data:



## Environment Variables

The following environment variables must be set for the scripts to function correctly:

- `DB_HOST` - Hostname of the MySQL database server
- `DB_USER` - Username for the MySQL database
- `DB_PASSWORD` - Password for the MySQL database user
- `DB_NAME` - Name of the MySQL database
- `DVLA_TENANT_ID` - Tenant ID for DVLA API authentication
- `DVLA_CLIENT_ID` - Client ID for DVLA API authentication
- `DVLA_CLIENT_SECRET` - Client secret for DVLA API authentication
- `DVLA_API_KEY` - API key for accessing the DVLA MOT data API
- `DOWNLOAD_DIR` (optional) - Directory to save downloaded files (default: `./downloads`)
- `FAILED_DIR` (optional) - Directory to store failed files (default: `./failed`)
- `BATCH_SIZE` (optional) - Number of records to process in each batch (default: 500)

## Directory Structure

The scripts automatically create and manage the following directories:

- `downloads/` - Contains downloaded data files from the DVLA API
- `failed/` - Contains files that failed to process (managed independently for easy recovery)

These directories are created automatically when the scripts run and can be configured via environment variables.

## Error Handling & Recovery

### Failed File Management

The `delta_loader.py` script includes robust error handling:

- Files that fail to process are automatically moved to the `failed/` directory
- Failed files are tracked in the database with status 'FAILED'
- Use `delta_loader.py --retry` to reprocess failed files
- Successfully retried files are automatically removed from the failed directory
- Failed retries are moved back to the failed directory for further investigation

### Retry Process

```bash
# Retry all failed files
python delta_loader.py --retry
```

The retry process will:
1. Query the database for files marked as 'FAILED'
2. Move files from the failed directory back to processing
3. Attempt to reprocess each file
4. Remove successfully processed files from the failed directory
5. Move files that fail again back to the failed directory
6. Provide a detailed summary report

## Scripts

### 1. `initial_bulk_download.py`

- Authenticates with the DVLA API using OAuth2 client credentials.
- Fetches the list of available bulk and delta data files.
- Downloads these files to the local filesystem.
- Logs progress and errors during the download process.

### 2. `initial_loader.py`

- Loads downloaded gzipped JSON data files from a specified local directory.
- Parses vehicle, MOT test, and defect data from the files.
- Inserts data into the MySQL database using batch inserts for efficiency.
- Tracks import status of each file in the database to avoid duplicate processing.
- Uses multithreading to process multiple files concurrently.
- Logs detailed progress and errors.

### 3. `delta_loader.py`

- Similar to `initial_loader.py` but designed to work incrementally with the DVLA API.
- Authenticates and lists available files from the DVLA API.
- Downloads new files that have not been imported yet.
- Processes and imports data into the database in batches.
- Tracks import status and generates import batch reports.
- Uses connection pooling and multithreading for performance.
- Includes retry logic for API requests.
- Supports retry functionality with `--retry` argument to reprocess failed files.
- Manages failed files in a separate directory for easy tracking and recovery.

### 4. `process_progress.py`

- Utility script to parse a `progress.txt` file.
- Calculates and prints the total size of all files, processed files, and percentage progress.
- Helps monitor the progress of data processing.

## Current Scripts Status

The following scripts are the actively maintained and recommended files:

- ✅ `initial_bulk_download.py` - Production ready
- ✅ `initial_loader.py` - Production ready  
- ✅ `delta_loader.py` - Production ready with retry functionality
- ℹ️ `process_progress.py` - Utility script for monitoring

Additional files in the `archived/` directory contain earlier versions and are kept for reference.

## Usage

1. Set the required environment variables.
2. Run `initial_bulk_download.py` to download the initial set of data files.
3. Run `initial_loader.py` to load the downloaded files into the database.
4. Use `delta_loader.py` to incrementally download and import new data files from the DVLA API.
   - Use `delta_loader.py --retry` to reprocess any failed files.
5. Optionally, use `process_progress.py` to monitor processing progress.

## Security

- No passwords or secrets are hardcoded in the code.
- All sensitive credentials must be provided via environment variables.
- Ensure environment variables are securely managed and not exposed in version control.

## Logging

- All scripts use Python's `logging` module to provide informative logs.
- Logs include info, warning, and error messages to help track the process and diagnose issues.

## Dependencies

- Python 3.x
- `requests` library for HTTP requests
- `mysql-connector-python` for MySQL database connectivity
- Other standard Python libraries (`json`, `gzip`, `threading`, `concurrent.futures`, etc.)


## Database Setup

### `schema.sql`

This SQL file contains the `CREATE TABLE` statements to create the necessary database schema for the project, including tables for vehicles, MOT tests, defects, import logs, and batch reports.

The schema includes the following tables:

- `vehicles`: Stores vehicle registration and related details.
- `mot_tests`: Stores MOT test records linked to vehicles.
- `defects`: Stores defects linked to MOT tests.
- `import_log`: Tracks the import status of each data file.
- `import_batch_report`: Records summary reports of each import batch.

Run this file against your MySQL server to create the schema:

```bash
mysql -u your_user -p your_database < schema.sql
```

## License

This project is provided as-is without warranty. Use responsibly and ensure compliance with DVLA API terms of use.