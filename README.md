
# SETUP

1. Ensure Docker desktop and Ubuntu are set up for usage.
Docker desktop: https://www.docker.com/products/docker-desktop/
For Ubuntu: Go to Microsoft store in apps search for Ubuntu 20.04.06 LTS and install. Then from cmd: `wsl --install`
2. Enter Ubuntu
3. Clone/download the scripts:
`git clone https://github.com/DomingoFox/assignment_3_mongo_db.git`
`cd assignment_3_mongo_db` (change dir)
4. Download and unzip the vessel data (tested on `aisdk-2025-03-01.csv`) into the project data folder (`assignment_3_mongo_db/data`).
5. Setup containers/clusters/shards/replicas and similarly by running (This will also pull a mongo image):
`./setup_cluster.sh` (If for some reason its not executable: chmod +x setup_cluster.sh)

P.S. if you get an error like `-bash: ./setup_cluster.sh: /bin/bash^M: bad interpreter: No such file or directory`, ensure it is a unix file by doing these two commands:
`sudo apt install dos2unix`
`dos2unix setup_cluster.sh`
`dos2unix cleanup_cluster.sh`

Check output ensure you see Container ids and on status code "ok": 1
Check if you see docker containers running using: `docker ps -a`

7. Change the amount of data you want to insert to database (to not run out of memory), for this example we will use 1 million rows (worked with 5 million but almost reaches limit).

8. Install requirements:
`pip install -r requirements.txt`
9. Execute insertion of data to database:
`cd scripts`
`python3 insert_data.py`

10. Enter and check mongo shard distribution (Each of the three shards should have aproximately 33%):
`docker exec -it router-1 mongo`
`use assignment_3`
`db.vessel_db.getShardDistribution()`
`exit`

11. Process the data in same folder as inser_data.py (/scripts/), run:
`python3 filter_noise.py`

12. Check if filtered data is in place:
`docker exec -it router-1 mongo`
`use assignment_3`
`db.filtered_vessel_db.find().limit(5).pretty()`
`exit`

13. Run the computation of delta t histogram:
`python3 compute_delta_hist.py`
Final histogram generated.
