# EcoloStudies
BigData 4 développement durable

Pré-requis : Permettre à l'utilisateur de lancer docker sans sudo

git clone https://github.com/Tam4Fitec/EcoloStudies.git

cd EcoloStudies/

=> Lancer le script launchAll.sh (après lui avoir accorder les droits d'exécution)
chmod +x launchAll.sh

Il s'occupera de faire 

1°) Monter l'architecure : docker-compose up -d

2°) Collect et dépots :  python3 SauvegardeHDFS.py

3°) Database : docker exec -it quickstart.cloudera hive -f /home/cloudera/script/tables.sql

4°) Ingestion : docker exec -it quickstart.cloudera pig /home/cloudera/script/script_fill.pig -useHCatalog