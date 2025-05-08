import logging
from pymongo import MongoClient, ASCENDING
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import random
import time
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def create_indexes():
    """Create indexes for efficient filtering."""
    client = MongoClient("mongodb://localhost:27141/", maxPoolSize=50)
    try:
        db = client.assignment_3
        collection = db.vessel_db
        filtered_collection = db.filtered_vessel_db

        # Create indexes
        collection.create_index([("MMSI", ASCENDING)])
        collection.create_index([("# Timestamp", ASCENDING)])
        filtered_collection.create_index([("MMSI", ASCENDING)])
        filtered_collection.create_index([("# Timestamp", ASCENDING)])
        logging.info("Indexes created successfully")
    except Exception as e:
        logging.error(f"Error creating indexes: {str(e)}")
    finally:
        client.close()

def validate_and_clean_record(record):
    """Validate fields and remove invalid ones, keeping valid fields."""
    cleaned_record = record.copy()  # Preserve original record
    required_fields = ["# Timestamp", "Navigational status", "MMSI", "Latitude", "Longitude", "ROT", "SOG", "COG", "Heading"]

    for field in required_fields:
        if field not in cleaned_record or cleaned_record[field] is None or cleaned_record[field] == "":
            cleaned_record.pop(field, None)
            continue

        try:
            if field == "MMSI":
                mmsi = int(cleaned_record[field])
                if mmsi <= 0:
                    cleaned_record.pop(field, None)
            elif field in ["Latitude", "Longitude", "ROT", "SOG", "COG", "Heading"]:
                value = float(cleaned_record[field])
                if field == "Latitude" and (value < -90 or value > 90):
                    cleaned_record.pop(field, None)
                elif field == "Longitude" and (value < -180 or value > 180):
                    cleaned_record.pop(field, None)
                elif field in ["ROT", "SOG", "COG", "Heading"] and value is None:
                    cleaned_record.pop(field, None)
            elif field == "Navigational status" and cleaned_record[field].lower() in ["unknown", ""]:
                cleaned_record.pop(field, None)
        except (ValueError, TypeError):
            cleaned_record.pop(field, None)

    return cleaned_record if cleaned_record else None

def filter_chunk(mmsi_group, client, checkpoint_file):
    """Process records for a single MMSI in batches, cleaning invalid fields."""
    mmsi, count = mmsi_group
    if count < 100:
        return 0, mmsi

    db = client.assignment_3
    collection = db.vessel_db
    filtered_collection = db.filtered_vessel_db
    batch_size = 10000
    max_retries = 3
    retry_delay = 1

    try:
        total_processed = 0
        skip = 0

        while True:
            cleaned_records = []
            for attempt in range(max_retries):
                try:
                    records = collection.find(
                        {"MMSI": mmsi},
                        projection={
                            "Navigational status": 1, "MMSI": 1, "Latitude": 1, "Longitude": 1,
                            "ROT": 1, "SOG": 1, "COG": 1, "Heading": 1, "# Timestamp": 1, "_id": 0
                        }
                    ).skip(skip).limit(batch_size)

                    for record in records:
                        cleaned = validate_and_clean_record(record)
                        if cleaned:
                            cleaned_records.append(cleaned)

                    if cleaned_records:
                        filtered_collection.insert_many(cleaned_records, ordered=False)
                        total_processed += len(cleaned_records)

                    skip += batch_size
                    if len(cleaned_records) < batch_size:
                        break  # No more records to process

                    break  # Exit retry loop on success

                except Exception as e:
                    if attempt < max_retries - 1:
                        logging.warning(f"Retry {attempt + 1}/{max_retries} for MMSI {mmsi}: {str(e)}")
                        time.sleep(retry_delay * (2 ** attempt))
                    else:
                        logging.error(f"Failed to process MMSI {mmsi} after {max_retries} attempts: {str(e)}")
                        sample_records = list(collection.find({"MMSI": mmsi}).limit(5))
                        logging.debug(f"Sample records for MMSI {mmsi}: {sample_records}")
                        return 0, mmsi

            if len(cleaned_records) < batch_size:
                break

        # Log processed MMSI to checkpoint file
        with open(checkpoint_file, 'a') as f:
            f.write(f"{mmsi}\n")

        return total_processed, mmsi

    except Exception as e:
        logging.error(f"Error processing MMSI {mmsi}: {str(e)}")
        sample_records = list(collection.find({"MMSI": mmsi}).limit(5))
        logging.debug(f"Sample records for MMSI {mmsi}: {sample_records}")
        return 0, mmsi

def main():
    logging.info("Starting data filtering")
    client = MongoClient("mongodb://localhost:27141/", maxPoolSize=50)
    checkpoint_file = "processed_mmsis.txt"

    try:
        db = client.assignment_3
        collection = db.vessel_db
        filtered_collection = db.filtered_vessel_db

        # Clear filtered_vessel_db to ensure clean run REMOVE THIS? Since it keep info.
        # filtered_collection.drop()
        # logging.info("Cleared filtered_vessel_db")

        # Create indexes
        create_indexes()

        # Load processed MMSIs to skip them
        processed_mmsis = set()
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                processed_mmsis = set(line.strip() for line in f)

        # Get MMSI groups with count of data points
        mmsi_counts = list(collection.aggregate([
            {"$group": {"_id": "$MMSI", "count": {"$sum": 1}}}
        ]))
        mmsi_counts = [mc for mc in mmsi_counts if str(mc["_id"]) not in processed_mmsis]

        total_filtered = 0
        skipped_vessels = []
        with tqdm(total=len(mmsi_counts), desc="Processing vessels", unit="vessels") as pbar:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(filter_chunk, (doc["_id"], doc["count"]), client, checkpoint_file) for doc in mmsi_counts]
                for future in as_completed(futures):
                    filtered_count, mmsi = future.result()
                    total_filtered += filtered_count
                    if filtered_count == 0:
                        skipped_vessels.append(mmsi)
                    pbar.update(1)

        logging.info(f"Completed filtering, {total_filtered:,} records inserted into filtered_vessel_db")
        if skipped_vessels:
            logging.info(f"Skipped {len(skipped_vessels):,} vessels with <100 records or no valid data")
            logging.debug(f"Skipped MMSIs: {random.sample(skipped_vessels, min(10, len(skipped_vessels)))}")

    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    main()