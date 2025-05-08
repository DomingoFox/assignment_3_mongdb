import logging
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def calculate_delta_t_for_mmsi(mmsi):
    client = MongoClient("mongodb://localhost:27141/")
    try:
        db = client.assignment_3
        collection = db.filtered_vessel_db
        
        # Records for MMSI sorted by timestamp.
        records = collection.find({"MMSI": mmsi}).sort("# Timestamp", 1)
        timestamps = []
        
        for record in records:
            try:
                ts = datetime.strptime(record["# Timestamp"], "%d/%m/%Y %H:%M:%S")
                timestamps.append(ts)
            except ValueError:
                continue
        
        # Delta t in milliseconds
        delta_ts = []
        for i in range(1, len(timestamps)):
            delta_t = (timestamps[i] - timestamps[i-1]).total_seconds() * 1000
            if delta_t >= 0:  # Safety since its sorted, negative differences ignored.
                delta_ts.append(delta_t)
        
        return delta_ts
    
    except Exception as e:
        logging.error(f"Error processing MMSI {mmsi}: {str(e)}")
        return []
    finally:
        client.close()

def main():
    logging.info("Starting delta t calculation and histogram generation")
    client = MongoClient("mongodb://localhost:27141/")
    try:
        db = client.assignment_3
        collection = db.filtered_vessel_db
        
        # Get unique MMSIs
        mmsis = collection.distinct("MMSI")
        
        all_delta_ts = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(calculate_delta_t_for_mmsi, mmsi) for mmsi in mmsis]
            for future in as_completed(futures):
                all_delta_ts.extend(future.result())
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, '..', 'data', 'delta_t_histogram.png')
        
        if all_delta_ts:
            max_display = np.percentile(all_delta_ts, 99)  # Show up to 99th percentile
            plt.figure(figsize=(10, 6))
            plt.hist(all_delta_ts, bins=50, range=(0, max_display), edgecolor='black')
            plt.title("Histogram of Delta t Between Vessel Data Points")
            plt.xlabel("Delta t (ms)")
            plt.ylabel("Frequency")
            plt.grid(True)
            plt.savefig(file_path)
            logging.info(f"Histogram saved as delta_t_histogram.png in /assignment_3_mongo_db/data/")
        else:
            logging.warning("No delta_t values to display in histogram")
        
        # Some histogram stats.
        mean_delta_t = np.mean(all_delta_ts) if all_delta_ts else 0
        median_delta_t = np.median(all_delta_ts) if all_delta_ts else 0
        logging.info(f"Mean delta t: {mean_delta_t:,.2f} ms")
        logging.info(f"Median delta t: {median_delta_t:,.2f} ms")
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    main()