CREATE DATABASE pollutiondb;
CREATE TABLE IF NOT EXISTS pollutiondb.table_pays (id_pays int, code_pays string, libelle_pays string, latitude double, longitude double) STORED AS AVRO;


CREATE TABLE IF NOT EXISTS pollutiondb.table_indicateur (id_indicateur int, type_indicateur string, libelle_indic string) STORED AS AVRO;


CREATE TABLE IF NOT EXISTS pollutiondb.table_metric(id_metric string, value_unit string) STORED AS AVRO;


CREATE TABLE IF NOT EXISTS pollutiondb.table_annee (id_annee int, val_annee int) STORED AS AVRO;


CREATE TABLE IF NOT EXISTS pollutiondb.donnees (id_pays string, id_indicateur string, id_metric string, region string, id_annee int, valeur double) STORED AS AVRO;
