country =  load '/user/cloudera/country.csv' using PigStorage()  AS (country_code:chararray);
list_country =  load '/user/cloudera/countries_codes_and_coordinates.csv' using PigStorage('\t')  AS (label:chararray,codealpha2:chararray, codealpha3:chararray,numcode:int,latitude:double,longitude:double);

temp_ctry = JOIN country BY country_code, list_country BY codealpha2;

res_ctry =  FOREACH temp_ctry GENERATE list_country::numcode as id_pays, country::country_code as code_pays, list_country::label as libelle_pays, list_country::latitude as latitude, list_country::longitude as longitude;
store res_ctry into 'pollutiondb.table_pays' using org.apache.hive.hcatalog.pig.HCatStorer();


units =  load '/user/cloudera/unit.csv' using PigStorage()  AS (unit_id:chararray);
list_units =  load '/user/cloudera/outfile.csv' using PigStorage('\t')  AS (label:chararray,definition:chararray, notation:chararray,status:chararray,acceptedDate:chararray);

tempo = JOIN units BY unit_id, list_units BY notation;

res =  FOREACH tempo GENERATE units::unit_id as id_metric, list_units::label as value_unit;
store res into 'pollutiondb.table_metric' using org.apache.hive.hcatalog.pig.HCatStorer();

sdg_07 =  load '/user/cloudera/output_sdg_07_11_v2.csv' using PigStorage('\t')  AS (id_pays:chararray, id_indicateur:chararray, id_metric:chararray, region:chararray, id_annee:int, valeur:double);
store sdg_07 into 'pollutiondb.donnees' using org.apache.hive.hcatalog.pig.HCatStorer();
