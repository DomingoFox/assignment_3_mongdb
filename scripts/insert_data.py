import pandas as pd
import logging
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os
import time
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def insert_chunk(chunk, client, progress_bar, skip_counter):
    # Insert a chunk of data with retry logic.
    max_retries = 3
    retry_delay = 10
    db = client.assignment_3
    collection = db.vessel_db

    records = chunk.to_dict(orient="records")
    if not records:
        skip_counter[0] += 1
        if skip_counter[0] % 100 == 0:  # Log every 100th skipped chunk (1 million rows)
            logging.warning(f"Skipped empty chunk (total skipped: {skip_counter[0]})")
        return 0

    for attempt in range(max_retries):
        try:
            collection.insert_many(records, ordered=False)
            inserted_count = len(records)
            progress_bar.update(inserted_count)
            return inserted_count
        except BulkWriteError as bwe:
            if attempt < max_retries - 1:
                logging.warning(f"Retry {attempt + 1}/{max_retries} after bulk write error: {str(bwe)}")
                time.sleep(retry_delay * (2 ** attempt))
            else:
                logging.error(f"Failed to insert chunk after {max_retries} attempts: {str(bwe)}")
                # Log a sample of the failed records for debugging
                sample_records = random.sample(records, min(5, len(records)))
                logging.debug(f"Sample of failed records: {sample_records}")
                return 0
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Retry {attempt + 1}/{max_retries} after error: {str(e)}")
                time.sleep(retry_delay * (2 ** attempt))
            else:
                logging.error(f"Failed to insert chunk after {max_retries} attempts: {str(e)}")
                # Log a sample of the failed records for debugging
                sample_records = random.sample(records, min(5, len(records)))
                logging.debug(f"Sample of failed records: {sample_records}")
                return 0

def main():
    logging.info("Starting data insertion")
    client = MongoClient("mongodb://localhost:27141/", maxPoolSize=50)
    try:
        total_rows = 1_000_000
        chunksize = 10_000  # Smaller chunks for memory efficiency
        total_inserted = 0
        skip_counter = [0]  # Track skipped chunks

        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, '..', 'data', 'aisdk-2025-03-01.csv')

        # Get total number of rows for progress bar
        with open(file_path, 'r') as f:
            total_lines = sum(1 for _ in f) - 1  # Exclude header
        total_lines = min(total_lines, total_rows)

        df_iter = pd.read_csv(
            file_path,
            chunksize=chunksize,
            nrows=total_rows,
            low_memory=False,
            on_bad_lines='warn'  # Log bad lines instead of failing
        )

        with tqdm(total=total_lines, desc="Inserting records", unit="records") as pbar:
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(insert_chunk, chunk, client, pbar, skip_counter) for chunk in df_iter]
                for future in as_completed(futures):
                    total_inserted += future.result()

        logging.info(f"Completed insertion of {total_inserted:,} records")
        if skip_counter[0] > 0:
            logging.info(f"Skipped {skip_counter[0]:,} empty chunks")

    except FileNotFoundError:
        logging.error(f"CSV file not found at {file_path}")
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    main()