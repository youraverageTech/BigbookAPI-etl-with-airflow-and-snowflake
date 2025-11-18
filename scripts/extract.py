from requests.exceptions import ConnectionError
import logging
import time
import json
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def fetch_api_data(url: str, headers: dict, params: dict):
    """
    Fetch data from bigbookapi.com with offset pagination until no more data is returned
    or the API quota limit is reached.
    
    args:
        url (str): The API endpoint URL.
        headers (dict): The headers to include in the API request.
        params (dict): The query parameters for the API request.

    returns:
        list: A list of all fetched book data."""
    
    # Initialize an empty list to store all fetched data
    logger.info("Starting data extraction from BigBookAPI")
    starttime = time.time()
    data = []
    params_reqs = params.copy()

    # Loop to fetch data with pagination
    while True:
        # Make the API request
        try:
            response = requests.get(url, headers=headers, params=params_reqs, timeout=10)
            response.raise_for_status()  # Raise an error for bad status codes
        except ConnectionError as ce:
            logger.error(f"Connection error occurred: {ce}")
            time.sleep(2)
            continue
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            break
        
        # Parse the JSON response
        try:
            json_data = response.json()
            book = json_data.get('books', [])
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            break

        # Break the loop if no more data is returned
        if len(book) == 0:
            break
        
        # Append the fetched data to the main list
        data.extend(book)

        # Check the API quota used from response headers
        # if quota used reaches 50, stop fetching more data
        quota_used = int(float(response.headers.get('X-API-Quota-Used', 0)))
        if quota_used == 50:
            logger.warning("API quota limit reached (50)")
            break

        logger.info(f"Fetched {len(book)} records; Total so far: {len(data)}; Quota used: {quota_used}/50")
        
        # Update the offset for the next request
        params_reqs['offset'] += params_reqs.get('number', 100)

        # Sleep for 1 second to avoid hitting rate limits
        time.sleep(1)
    
    # Save the fetched data to a JSON file
    try:
        with open('/opt/airflow/output/raw_data.json', 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f"Data saved to {'/opt/airflow/output/raw_data.json'}")
    except Exception as e:
        logger.error(f"Error saving JSON file: {e}")
    
    logger.info(f"Total records fetched: {len(data)}")
    
    endtime = time.time()

    logger.info(f"Data extraction completed in {endtime - starttime:.2f} seconds")

    return data