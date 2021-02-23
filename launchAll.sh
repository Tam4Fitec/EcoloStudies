#!/bin/bash

echo "Create cluster ..."
docker-compose up -d 


echo "Wait Hue is UP !! "
wget -q "http://localhost:8888/desktop/debug/is_alive"
process_id=$!

echo "Save data to HDFS ..."
wait $process_id
python3 SauvegardeHDFS.py
process_id2=$!

echo "Create database  ..."
wait $process_id2
docker exec -it quickstart.cloudera hive -f /home/cloudera/script/tables.sql
process_id3=$!

echo "Ingest data in Hive  ..."
wait $process_id3
docker exec -it quickstart.cloudera pig /home/cloudera/script/script_fill.pig -useHCatalog
