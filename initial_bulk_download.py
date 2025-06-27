import os
import requests
import logging
import json
import urllib.parse  # for URL decoding

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
    """Download a file from the provided URL and save it locally."""
    # Do NOT decode the URL; use as-is to preserve S3 signature
    try:
        logger.info(f'Downloading file: {file_name} from {file_url}')
        response = requests.get(file_url, stream=True)  # No additional headers
        response.raise_for_status()

        # Save the file locally
        with open(file_name, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)

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

    # Process and download bulk files
    for file in file_list.get('bulk', []):  # Assuming 'bulk' is the key holding the list of bulk files
        file_url = file.get('downloadUrl')
        file_name = file.get('filename').split('/')[-1]  # Extract the file name from the path
        if file_url and file_name:
            download_file(file_url, file_name)

    # Process and download delta files
    for file in file_list.get('delta', []):  # Assuming 'delta' is the key holding the list of delta files
        file_url = file.get('downloadUrl')
        file_name = file.get('filename').split('/')[-1]  # Extract the file name from the path
        if file_url and file_name:
            download_file(file_url, file_name)

    logger.info('Script completed.')

if __name__ == '__main__':
    main()
