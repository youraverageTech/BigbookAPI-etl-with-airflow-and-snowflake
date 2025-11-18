import pandas as pd
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def transform_data(data: list):
    """
    Transform the raw data into a structured pandas DataFrame, and validate data types.
    args:
        data (list): The raw data fetched from the API.
    returns:
        pd.DataFrame: A DataFrame containing the transformed data.
    """

    logger.info(f'Starting Transform Process...')
    starttime = time.time()

    # create a list of records for DataFrame
    records = []

    # Extract relevant fields and handle nested structures
    for book in data:
        b = book[0] if isinstance(book, (list, tuple)) else book
        authors = b.get('authors', []) or []
        author_ids = [a.get('id') for a in authors]
        author_names = [a.get('name') for a in authors]
        record = {
            'id': b.get('id'),
            'title': b.get('title'),
            'image': b.get('image'),
            'genres': b.get('genres'),
            'rating': (b.get('rating') or {}).get('average'),
            'author_id': author_ids,
            'author_name': author_names
        }
        records.append(record)

    # Create DataFrame and validate data types
    data_df = pd.DataFrame(records)
    data_df['id'] = data_df['id'].apply(lambda x: int(float(x)) if pd.notnull(x) else None)
    data_df['author_id'] = data_df['author_id'].apply(lambda x: [str(i) for i in x])
    data_df['rating'] = pd.to_numeric(data_df['rating'], errors='coerce')
    data_df['rating'] = data_df['rating'] * 100

    logger.info(f"Data transformed: {len(data_df)} records")

    endtime = time.time()
    logger.info(f'Transform Process Completed : {endtime - starttime} seconds')

    return data_df