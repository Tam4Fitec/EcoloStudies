list_country =  load '/user/cloudera/DevDurable/output_countries_codes_and_coordinates.csv' using PigStorage('\t')  AS (label:chararray,codealpha2:chararray, codealpha3:chararray,numcode:int,latitude:double,longitude:double);

res_ctry =  FOREACH list_country GENERATE numcode as id_pays, codealpha2 as code_pays, label as libelle_pays, latitude as latitude, longitude as longitude;
store res_ctry into 'pollutiondb.table_pays' using org.apache.hive.hcatalog.pig.HCatStorer();

list_units =  load '/user/cloudera/DevDurable/output_unit.csv' using PigStorage('\t')  AS (label:chararray,definition:chararray, notation:chararray,status:chararray,acceptedDate:chararray);

res =  FOREACH list_units GENERATE notation as id_metric, label as value_unit;
store res into 'pollutiondb.table_metric' using org.apache.hive.hcatalog.pig.HCatStorer();

indic =  load '/user/cloudera/DevDurable/indicateur.csv' using PigStorage('\t')  AS (id_indicateur:chararray,libelle_indic:chararray);
store indic into 'pollutiondb.table_indicateur' using org.apache.hive.hcatalog.pig.HCatStorer();

sdg_07_11 =  load '/user/cloudera/DevDurable/output_sdg_07_11.tsv' using PigStorage('\t')  AS (id_pays:chararray, id_indicateur:chararray, id_metric:chararray, region:chararray, id_annee:int, valeur:double);

sdg_06_20 =  load '/user/cloudera/DevDurable/output_sdg_06_20.csv' using PigStorage('\t')  AS (id_pays:chararray, id_indicateur:chararray, id_metric:chararray, region:chararray, id_annee:int, valeur:double);
sdg_06_60 =  load '/user/cloudera/DevDurable/output_sdg_06_60.csv' using PigStorage('\t')  AS (id_pays:chararray, id_indicateur:chararray, id_metric:chararray, region:chararray, id_annee:int, valeur:double);

sdg_07_10 =  load '/user/cloudera/DevDurable/output_sdg_07_10.tsv' using PigStorage('\t')  AS (id_pays:chararray, id_indicateur:chararray, id_metric:chararray, region:chararray, id_annee:int, valeur:double);
sdg_13_20 =  load '/user/cloudera/DevDurable/output_sdg_13_20.tsv' using PigStorage('\t')  AS (id_pays:chararray, id_indicateur:chararray, id_metric:chararray, region:chararray, id_annee:int, valeur:double);
sdg_07_40 =  load '/user/cloudera/DevDurable/output_sdg_07_40.tsv' using PigStorage('\t')  AS (id_pays:chararray, id_indicateur:chararray, id_metric:chararray, region:chararray, id_annee:int, valeur:double);
sdg_15_10 =  load '/user/cloudera/DevDurable/output_sdg_15_10.tsv' using PigStorage('\t')  AS (id_pays:chararray, id_indicateur:chararray, id_metric:chararray, region:chararray, id_annee:int, valeur:double);

sdg_13_10 =  load '/user/cloudera/DevDurable/output_sdg_13_10.json' using JsonLoader('donnees: {(pays:chararray, indic:chararray, unit:chararray, region:chararray, annee:int, valeur:double)}');
B = FOREACH sdg_13_10 GENERATE FLATTEN($0);
res_sdg_13_10 =  FOREACH B GENERATE pays as id_pays, indic as id_indicateur, unit as id_metric, region as region, annee as id_annee, valeur as valeur;


store sdg_07_11 into 'pollutiondb.donnees' using org.apache.hive.hcatalog.pig.HCatStorer();

store sdg_06_20 into 'pollutiondb.donnees' using org.apache.hive.hcatalog.pig.HCatStorer();
store sdg_06_60 into 'pollutiondb.donnees' using org.apache.hive.hcatalog.pig.HCatStorer();

store sdg_07_40 into 'pollutiondb.donnees' using org.apache.hive.hcatalog.pig.HCatStorer();
store sdg_07_10 into 'pollutiondb.donnees' using org.apache.hive.hcatalog.pig.HCatStorer();
store sdg_13_20 into 'pollutiondb.donnees' using org.apache.hive.hcatalog.pig.HCatStorer();
store sdg_15_10 into 'pollutiondb.donnees' using org.apache.hive.hcatalog.pig.HCatStorer();
store res_sdg_13_10 into 'pollutiondb.donnees' using org.apache.hive.hcatalog.pig.HCatStorer(); 
