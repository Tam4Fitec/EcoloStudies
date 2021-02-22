# EcoloStudies
BigData 4 développement durable


0°) git clone https://github.com/Tam4Fitec/EcoloStudies.git

1°) cd EcoloStudies/

2°) python3 SauvegardeHDFS.py
3°) docker exec -it quickstart.cloudera hive -f /home/cloudera/script/tables.sql

4°) docker exec -it quickstart.cloudera pig /home/cloudera/script/script_fill.pig -useHCatalog
