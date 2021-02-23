#!/usr/bin/env python
# coding: utf-8

import requests
import shutil
import sys
import os
import json
import csv
from shutil import copyfile
import xml.etree.ElementTree as ET

def createHDFSFolder(hdfs_path, hdfs_host = r"http://localhost:50070/webhdfs/v1"):
    # Create a folder to store data in HDFS
    r = requests.put(hdfs_host + hdfs_path, params=r'user.name=cloudera&op=MKDIRS')
    if not r.ok:
        print(f'Failed to create folder {hdfs_path}')
        print(f'Reason: {r.reason}')
        return
    else:
        print(f'create path: {r.ok}')

def download_file(url, filename):
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as request:
        request.encoding = 'utf-8'
        with open(filename, 'wb') as fout:
            for chunk in request.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                fout.write(chunk)
    if not request.ok:
            print(f'download failed for {url}')
            print(f'reason: {request.reason}')
            return
    else:
        print(f'download succeded: {request.ok}')

def upload_file(filename, hdfs_path, hdfs_name, hdfs_host = r"http://localhost:50070/webhdfs/v1"):
    
    # Request path for file PUT
    r = requests.put(hdfs_host + hdfs_path, params=r'user.name=cloudera&op=CREATE', allow_redirects=False)
    
    # Parse path for file PUT
    reply_path = r.headers['Location'].replace('quickstart.cloudera', 'localhost').split('?')
    
    print(reply_path)
    
    with open(filename) as fin:
        # upload to hdfs
        to_hdfs = requests.put(reply_path[0] + hdfs_name, params=reply_path[1], data=fin.read().encode('utf-8'), stream=True)     

    if not to_hdfs.ok:
            print(f'hdfs upload failed for {hdfs_name}')
            print(f'reason: {to_hdfs.reason}')
            return
    else:
        print(f'hdfs upload succeded: {to_hdfs.ok}')

def sedFichierUnit(nomFichier):
    fichierOutput = 'output_' + nomFichier
    os.system('cut -d "," -f2-6 ' + nomFichier + ' > ' + fichierOutput)
    #print('sed -i -r \'s/\",\"/\"\t\"/g\' ' + fichierOutput)
    os.system('sed -i -r \'s/\",\"/\"\t\"/g\' ' + fichierOutput)
    #print('sed -i -r \'s/"//g\' ' + fichierOutput)
    os.system('sed -i -r \'s/"//g\' ' + fichierOutput)
    return fichierOutput

def sedFichierPays(nomFichier):
    fichierOutput = 'output_' + nomFichier
    lignesSup = '\nUnion européenne (agrégat variable en fonction du contexte)\tEU_V\tEU_V\t9990\t0\t0\n'
    lignesSup = lignesSup + 'Union européenne - 27 pays (à partir de 2020)\tEU27_2020\tEU27_2020\t9991\t0\t0\n'
    lignesSup = lignesSup + 'Union européenne - 28 pays (2013-2020)\tEU28\tEU28\t9992\t0\t0\n'
    lignesSup = lignesSup + 'Union européenne - 27 pays (2007-2013)\tEU27_2007\tEU27_2007\t9993\t0\t0\n'
    lignesSup = lignesSup + 'Zone euro - 19 pays (à partir de 2015)\tEA19\tEA19\t9994\t0\t0\n'
    copyfile(nomFichier, fichierOutput)
    os.system('sed -i -r \'s/\",\"/\"\t\"/g\' ' + fichierOutput)
    os.system('sed -i -r \'s/\", \"/\"\t\"/g\' ' + fichierOutput)
    os.system('sed -i -r \'s/"//g\' ' + fichierOutput)
    with open(fichierOutput, 'a') as f_object:
        f_object.write(lignesSup)
    return fichierOutput

def createIndicateur(fichier):
    print("Creating indicator file in CSV " + fichier + " tab-delimited file...")
    with open(fichier, 'w') as fout:
        lignes = 'Indicateur\tLibelle\n'
        lignes = lignes + 'GHG_I90\tIndice d\'émissions de gaz à effet de serre (en équivalent CO2), année de base 1990\n'
        lignes = lignes + 'GHG_T_HAB\tIndice d\'émissions de gaz à effet de serre - tonnes par tête\n'
        lignes = lignes + 'REN\tSources d\'énergie renouvelable\n'
        lignes = lignes + 'REN_ELC\tSources d\'énergie renouvelable dans l\'électricité\n'
        lignes = lignes + 'REN_HEAT_CL\tSources d\'énergie renouvelable dans le chauffage et le refroidissement\n'
        lignes = lignes + 'REN_TRA\tSources d\'énergie renouvelable dans les transports\n'
        lignes = lignes + 'LCC1\tForêt\n'
        lignes = lignes + 'LCC1_2\tForêt et autres terres boisées\n'
        lignes = lignes + 'LCC2\tAutres terres boisées\n'
        lignes = lignes + 'sdg_06_20\tPopulation connectée au moins à un traitement secondaire des eaux usées\n'
        lignes = lignes + 'sdg_06_60\tIndice d\'exploitation de l\'eau, plus (WEI+)\n'
        lignes = lignes + 'sdg_07_10\tConsommation d\'énergie primaire\n'
        lignes = lignes + 'sdg_07_11\tConsommation d\'énergie finale\n'
        lignes = lignes + 'sdg_13_20\tIntensité d´émissions de gaz à effet de serre par consommation d´énergie\n'
        fout.write(lignes)
    return fichier

def conversionCSV3Param(fichier):
    inputPath = fichier
    outputPath = "temp_" + inputPath
    outputPath2 = "output_" + inputPath
    print("Converting CSV " + inputPath + " to tab-delimited file...")
    with open(inputPath) as fin, open(outputPath, 'w') as fout:
        for line in fin:
            fout.write(line.replace(',', '\t').replace('geo'"\\"'TIME_PERIOD', 'pays\tannee\tvaleur'))

    first_ligne = True
    with open(outputPath) as fin, open(outputPath2, 'w') as fout:
        for line in fin:
            if (first_ligne):
                #print('First line : ' + line)
                first_data = line.split('\t')
                unit_label = first_data[1].rstrip('\n')
                #print('Unit : ' + unit)
                pays_label = first_data[2].rstrip('\n')
                #print('pays : ' + pays)
                annee_label = first_data[3].rstrip('\n')
                #print('value : ' + value)
                valeur_label = first_data[4].rstrip('\n')

                indicateur_label = "indicateur"
                region_label = "region"

                #print('annee : ' + annee)
                listeAnnees = []
                for i in range(5, len(first_data)):
                    #print(first_data[i])
                    listeAnnees.append(first_data[i].strip().rstrip('\t').rstrip('\n'))
                output_line = pays_label + '\t' + indicateur_label + '\t' + unit_label + '\t' + region_label + '\t' + annee_label + '\t' + valeur_label +'\n'
                #fout.write(output_line)
                first_ligne = False
            else:
                #print(len(listeAnnees))
                #print('Line : ' + line)
                line_data = line.split('\t')
                
                unit = line_data[1].strip().rstrip('\n')

                indicateur = inputPath.strip().rstrip('.tsv')

                region = ""

                #print('Unit : ' + unit)
                pays = line_data[2].strip().rstrip('\n')

                listeValeurs = []

                for i in range(3, len(line_data)):
                    #print(first_data[i])
                    listeValeurs.append(line_data[i].strip().rstrip('\t').rstrip('\n'))

                for i in range(0, len(listeValeurs)):
                    output_line = pays + '\t' + indicateur + '\t' + unit + '\t' + region + '\t' + listeAnnees[i] + '\t' + listeValeurs[i] +'\n'
                    fout.write(output_line)

    print("Conversion complete, file " + outputPath2 + " created.")
    return outputPath2

def conversionCSV4Param(fichier):
    inputPath = fichier
    outputPath = "temp_" + inputPath
    outputPath2 = "output_" + inputPath
    print("Converting CSV " + inputPath + " to tab-delimited file...")
    with open(inputPath) as fin, open(outputPath, 'w') as fout:
        for line in fin:
            fout.write(line.replace(': ', '').replace(',', '\t').replace('geo'"\\"'TIME_PERIOD', 'pays\tannee\tvaleur'))

    first_ligne = True
    with open(outputPath) as fin, open(outputPath2, 'w') as fout:
        for line in fin:
            if (first_ligne):
                #print('First line : ' + line)
                first_data = line.split('\t')
                indicateur_label = first_data[1].rstrip('\n')

                unit_label = first_data[2].rstrip('\n')
                #print('Unit : ' + unit)
                pays_label = first_data[3].rstrip('\n')
                #print('pays : ' + pays)
                annee_label = first_data[4].rstrip('\n')
                #print('value : ' + value)
                valeur_label = first_data[5].rstrip('\n')

                region_label = "region"

                #print('annee : ' + annee)
                listeAnnees = []
                for i in range(6, len(first_data)):
                    #print(first_data[i])
                    listeAnnees.append(first_data[i].strip().rstrip('\t').rstrip('\n'))
                output_line = pays_label + '\t' + indicateur_label + '\t' + unit_label + '\t' + region_label + '\t' + annee_label + '\t' + valeur_label +'\n'
                #fout.write(output_line)
                first_ligne = False
            else:
                #print(len(listeAnnees))
                #print('Line : ' + line)
                line_data = line.split('\t')
                
                indicateur = line_data[1].strip().rstrip('\n')

                unit = line_data[2].strip().rstrip('\n')

                region = ""

                #print('Unit : ' + unit)
                pays = line_data[3].strip().rstrip('\n')

                listeValeurs = []

                for i in range(4, len(line_data)):
                    #print(first_data[i])
                    listeValeurs.append(line_data[i].strip().rstrip(' u').rstrip(' p').rstrip('\t').rstrip('\n'))

                for i in range(0, len(listeValeurs)):
                    output_line = pays + '\t' + indicateur + '\t' + unit + '\t' + region + '\t' + listeAnnees[i] + '\t' + listeValeurs[i] +'\n'
                    fout.write(output_line)

    print("Conversion complete, file " + outputPath2 + " created.")
    return outputPath2

def conversionCSV5Param(fichier):
    inputPath = fichier
    outputPath = "temp_" + inputPath
    outputPath2 = "output_" + inputPath
    print("Converting CSV " + inputPath + " to tab-delimited file...")
    with open(inputPath) as fin, open(outputPath, 'w') as fout:
        for line in fin:
            fout.write(line.replace(': ', '').replace(',', '\t').replace('geo'"\\"'TIME_PERIOD', 'pays\tannee\tvaleur'))

    first_ligne = True
    with open(outputPath) as fin, open(outputPath2, 'w') as fout:
        for line in fin:
            if (first_ligne):
                #print('First line : ' + line)
                first_data = line.split('\t')
                unit_label = first_data[1].rstrip('\n')
                indicateur_label = first_data[2].rstrip('\n')
                #print('Unit : ' + unit)
                pays_label = first_data[3].rstrip('\n')
                #print('pays : ' + pays)
                annee_label = first_data[4].rstrip('\n')
                #print('value : ' + value)
                valeur_label = first_data[5].rstrip('\n')

                region_label = "region"

                #print('annee : ' + annee)
                listeAnnees = []
                for i in range(6, len(first_data)):
                    #print(first_data[i])
                    listeAnnees.append(first_data[i].strip().rstrip('\t').rstrip('\n'))
                output_line = pays_label + '\t' + indicateur_label + '\t' + unit_label + '\t' + region_label + '\t' + annee_label + '\t' + valeur_label +'\n'
                #fout.write(output_line)
                first_ligne = False
            else:
                #print(len(listeAnnees))
                #print('Line : ' + line)
                line_data = line.split('\t')
                
                unit = line_data[1].strip().rstrip('\n')
                indicateur = line_data[2].strip().rstrip('\n')

                region = ""

                #print('Unit : ' + unit)
                pays = line_data[3].strip().rstrip('\n')

                listeValeurs = []

                for i in range(4, len(line_data)):
                    #print(first_data[i])
                    listeValeurs.append(line_data[i].strip().rstrip(' u').rstrip(' p').rstrip('\t').rstrip('\n'))

                for i in range(0, len(listeValeurs)):
                    output_line = pays + '\t' + indicateur + '\t' + unit + '\t' + region + '\t' + listeAnnees[i] + '\t' + listeValeurs[i] +'\n'
                    fout.write(output_line)

    print("Conversion complete, file " + outputPath2 + " created.")
    return outputPath2

def conversionJSON(fichier):
    inputPath = fichier
    outputPath = "output_" + inputPath
    with open(inputPath) as fin, open(outputPath,'w') as fout:
        print("Converting JSON " + inputPath + " file...")
        data = json.load(fin)
        #print(data)
        #result = json_normalize(data, 'value')
        list_valeurs = []
        for key, value in data['value'].items():
            list_valeurs.append(value)

        list_ids = data['id']
        #print(list_ids)

        #freq_id = list_ids[0]
        indic_id = list_ids[1]
        geo_id = list_ids[2]
        time_id = list_ids[3]

        list_dimensions = data['dimension']
        #list_dim_freq = list_dimensions[freq_id]['category']['index']
        #for key in list_dim_freq.keys(): print(key)
        list_dim_indic = list_dimensions[indic_id]['category']['index']
        #for key in list_dim_indic.keys(): print(key)

        list_dim_geo = []
        for key, value in list_dimensions[geo_id]['category']['index'].items():
            list_dim_geo.append(key)
        list_dim_geo.sort()
        #for key in list_dim_geo.keys(): print(key)
        list_dim_time = list_dimensions[time_id]['category']['index']
        #for key in list_dim_time.keys(): print(key)
        
        unit = ""
        region = ""
        output_line = ""
        cpt = 0
        for key_indic in list_dim_indic.keys() :
            for i in range (0, len(list_dim_geo)):
                for key_annee in list_dim_time.keys() :
                    if(cpt == 0):
                        output_line = '{"pays" : "' + list_dim_geo[i] + '", "indic" : "' + key_indic + '", "unit" : "' + unit + '", "region" : "' + region + '", "annee" : ' + key_annee + ', "valeur" : ' + str(list_valeurs[cpt]) + '}'
                    else:    
                        output_line = output_line + ',{"pays" : "' + list_dim_geo[i] + '", "indic" : "' + key_indic + '", "unit" : "' + unit + '", "region" : "' + region + '", "annee" : ' + key_annee + ', "valeur" : ' + str(list_valeurs[cpt]) + '}'
                    cpt = cpt + 1
        
        output_line = '{ "donnees" : [' + output_line + ']}'
        fout.write(output_line)
        print("Conversion complete, file " + outputPath + " created.")
        return outputPath

def transformationXML(filename):
    os.system("sed -i -r -e 's/g://g' -e 's/m://g' " + filename)
    os.system("sed -i -r -e 's/<GenericData xmlns.*><Header>/<GenericData><Header>/' " + filename)
    return XMLToCSV(filename)


def XMLToCSV(filename):
    tree = ET.parse(filename)
    root = tree.getroot()

    for dataset in root.findall('DataSet'):
        default_indic = dataset.find('KeyFamilyRef').text
    indic = default_indic.lower()

    # open a file for writing
    newfilename = "output_" + filename[:-3] + "csv"
    Ecolo_data = open(newfilename, 'w')

    # create the csv writer object
    csvwriter = csv.writer(Ecolo_data,  delimiter='\t')

    tree_s = dataset.findall('Series')
    for s in tree_s:
        s_data = []
        s_data_all = []
        tree_sk = s.findall('SeriesKey')
        for sk in tree_sk :
            for v in sk.iter('Value'):
                if v.attrib['concept'] == 'geo':
                    geo = v.get('value')
                    s_data.append(geo)
                    s_data.append(indic)
                if v.attrib['concept'] == 'unit':
                    unit = v.get('value')
                    s_data.append(unit)
        s_data.append("")

        tree_obs = s.findall('Obs')
        for o in tree_obs:
            s_data_all = s_data[:]
            annee = o.find('Time').text
            obsval = o.find('ObsValue').get('value')
            s_data_all.append(annee)
            s_data_all.append(obsval)
            csvwriter.writerow(s_data_all)
    
    Ecolo_data.close()
    return newfilename


hdfs_path = "/user/cloudera/DevDurable/"
createHDFSFolder(hdfs_path)


urlPays = r'https://gist.githubusercontent.com/tadast/8827699/raw/f5cac3d42d16b78348610fc4ec301e9234f82821/countries_codes_and_coordinates.csv'
filePays = 'countries_codes_and_coordinates.csv'

download_file(urlPays, filePays)
fichierPaysOutput = sedFichierPays(filePays)
upload_file(fichierPaysOutput, hdfs_path, fichierPaysOutput)

urlUnit = r'http://dd.eionet.europa.eu/vocabulary/eurostat/unit/csv'
fileUnit = 'unit.csv'

download_file(urlUnit, fileUnit)
fichierUnitOutput = sedFichierUnit(fileUnit)
upload_file(fichierUnitOutput, hdfs_path, fichierUnitOutput)

fichierIndicOutput = createIndicateur('indicateur.csv')
upload_file(fichierIndicOutput, hdfs_path, fichierIndicOutput)

url6_20 = r'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/SDG_06_20/?format=sdmx_2.0_generic&compressed=true"'
file6_20 = 'sdg_06_20.xml'
download_file(url6_20, file6_20)
fichier6_20Output = transformationXML(file6_20)
upload_file(fichier6_20Output, hdfs_path, fichier6_20Output)

url6_60 = r'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/SDG_06_60/?format=sdmx_2.0_generic&compressed=true"'
file6_60 = 'sdg_06_60.xml'
download_file(url6_60, file6_60)
fichier6_60Output = transformationXML(file6_60)
upload_file(fichier6_60Output, hdfs_path, fichier6_60Output)

url7_10 = r'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/SDG_07_10/?format=TSV&compressed=true"'
file7_10 = 'sdg_07_10.tsv'
download_file(url7_10, file7_10)
fichier7_10Output = conversionCSV3Param(file7_10)
upload_file(fichier7_10Output, hdfs_path, fichier7_10Output)

url7_11 = r'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/SDG_07_11/?format=TSV&compressed=true"'
file7_11 = 'sdg_07_11.tsv'
download_file(url7_11, file7_11)
fichier7_11Output = conversionCSV3Param(file7_11)
upload_file(fichier7_11Output, hdfs_path, fichier7_11Output)

url7_40 = r'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/SDG_07_40/?format=TSV&compressed=true"'
file7_40 = 'sdg_07_40.tsv'
download_file(url7_40, file7_40)
fichier7_40Output = conversionCSV4Param(file7_40)
upload_file(fichier7_40Output, hdfs_path, fichier7_40Output)

url13_10 = r'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/SDG_13_10/?format=JSON&lang=fr'
file13_10 = 'sdg_13_10.json'
download_file(url13_10, file13_10)
fichier13_10Output = conversionJSON(file13_10)
upload_file(fichier13_10Output, hdfs_path, fichier13_10Output)

url13_20 = r'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/SDG_13_20/?format=TSV&compressed=true"'
file13_20 = 'sdg_13_20.tsv'
download_file(url13_20, file13_20)
fichier13_20Output = conversionCSV3Param(file13_20)
upload_file(fichier13_20Output, hdfs_path, fichier13_20Output)

url15_10 = r'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/SDG_15_10/?format=TSV&compressed=true"'
file15_10 = 'sdg_15_10.tsv'
download_file(url15_10, file15_10)
fichier15_10Output = conversionCSV5Param(file15_10)
upload_file(fichier15_10Output, hdfs_path, fichier15_10Output)
