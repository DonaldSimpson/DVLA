import os
import requests
import logging
import json
import urllib.parse  # for URL decoding
import zipfile
import sys

# python3 -m venv venv; source venv/bin/activate; pip install mysql-connector-python; pip install requests
# python initial_bulk_download.py

# Used to fetch the initial "bulk" file as a one-off, that should be over 40GB in size.

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get environment variables
TENANT_ID = os.getenv('DVLA_TENANT_ID')
CLIENT_ID = os.getenv('DVLA_CLIENT_ID')
CLIENT_SECRET = os.getenv('DVLA_CLIENT_SECRET')
API_KEY = os.getenv('DVLA_API_KEY')

# Define URLs
TOKEN_URL = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token'
BULK_DOWNLOAD_URL = 'https://history.mot.api.gov.uk/v1/trade/vehicles/bulk-download'

# Request headers
headers = {
    'X-API-Key': API_KEY,
}

def get_access_token():
    """Request access token from Microsoft Authentication."""
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': 'https://tapi.dvsa.gov.uk/.default'
    }
    try:
        logger.debug('Requesting access token...')
        response = requests.post(TOKEN_URL, data=payload)
        response.raise_for_status()
        token = response.json().get('access_token')
        if token:
            logger.info('Access token retrieved successfully.')
            return token
        else:
            logger.error('Failed to retrieve access token.')
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f'Error fetching access token: {e}')
        return None

def fetch_file_list(access_token):
    """Fetch list of available files using the provided access token."""
    headers['Authorization'] = f'Bearer {access_token}'
    
    try:
        logger.debug('Sending GET request to fetch file list...')
        response = requests.get(BULK_DOWNLOAD_URL, headers=headers)
        response.raise_for_status()
        file_list = response.json()
        logger.info(f'File list retrieved successfully: {json.dumps(file_list, indent=2)}')
        return file_list
    except requests.exceptions.RequestException as e:
        logger.error(f'Error fetching available files: {e}')
        return None

def download_file(file_url, file_name):
    """Download a file from the provided URL and save it locally with progress feedback."""
    # Do NOT decode the URL; use as-is to preserve S3 signature
    try:
        logger.info(f'Downloading file: {file_name} from {file_url}')
        response = requests.get(file_url, stream=True)  # No additional headers
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        last_logged = 0
        # Save the file locally with progress logging
        with open(file_name, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                f.write(chunk)
                if total_size:
                    downloaded += len(chunk)
                    percent = int(downloaded * 100 / total_size)
                    if percent >= last_logged + 10:
                        last_logged += 10
                        logger.info(f'Download progress: {last_logged}%')
        logger.info(f'File downloaded successfully: {file_name}')
    except requests.exceptions.RequestException as e:
        logger.error(f'Error downloading file {file_name}: {e}')
        if e.response:
            logger.error(f'Response content: {e.response.text}')

def main():
    """Main function to execute the download process."""
    logger.info('Starting the MOT data download script.')

    # Get the access token
    access_token = get_access_token()
    if not access_token:
        logger.critical('Failed to obtain access token. Exiting.')
        return

    # Fetch the file list
    file_list = fetch_file_list(access_token)
    if not file_list:
        logger.critical('Failed to fetch file list.')
        return

    # Process and download only the latest bulk file
    bulk_files = file_list.get('bulk', [])
    if not bulk_files:
        logger.critical('No bulk files found. Exiting.')
        sys.exit(1)
    # Assuming bulk_files are sorted by date, take the last entry as the latest
    latest_bulk = bulk_files[-1]
    file_url = latest_bulk.get('downloadUrl')
    file_name = latest_bulk.get('filename').split('/')[-1]
    if not file_url or not file_name:
        logger.critical('Invalid bulk file entry. Exiting.')
        sys.exit(1)
    # Determine extraction directory name (without .zip extension)
    extract_dir = file_name.rstrip('.zip')
    if os.path.exists(extract_dir):
        logger.critical(f'Extraction directory already exists: {extract_dir}. Exiting.')
        sys.exit(1)
    # Download the file
    download_file(file_url, file_name)
    # Extract downloaded zip file
    logger.info(f'Extracting {file_name} into directory: {extract_dir}')
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    logger.info('Extraction completed successfully.')

    logger.info('Script completed.')

if __name__ == '__main__':
    main()
