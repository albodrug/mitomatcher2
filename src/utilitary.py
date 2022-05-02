#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 01/26/2022
# author: abodrug

# This script contains functions that are broadly used by main MitoMatcher scripts.
# Not intended to run as stand alone
import os
import re
import sys
import glob
import pymysql
import xlrd
import datetime
import inspect
#
import config
#
from cryptography.fernet import Fernet
#
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def connect2databse(password):
    '''Tries to connect to database, previously created in mysql:
    mysql> create database mitomatcher2;
    mysql> grant all privileges on mitomatcher2.* to mito_admin@localhost;
    Takes in password.
    Outputs database.
    '''
    try:
        database = pymysql.connect(db='mitomatcher2', user= config.USERADMIN, passwd=password)
    except:
        print(bcolors.FAIL + "Cannot connect to database." + bcolors.ENDC)
        sys.exit()
    else:
        print(bcolors.OKCYAN + "Connection to database successful." + bcolors.ENDC)
    return database
#
def executesqlinstruction(instruction, cursor):
    ''' Tries to execute the SQL instruction on the database cursor.
        Takes in instruction, cursor
        Returns nothing, but if successful database is modified.
    '''
    try:
        cursor.execute(instruction)
        print(bcolors.OKCYAN + "Successful SQL instruction." + bcolors.ENDC)
        print(bcolors.BOLD + bcolors.OKGREEN + instruction + bcolors.ENDC)
    except:
        print(bcolors.WARNING + "Could not execute SQL instruction." + bcolors.ENDC)
        print(bcolors.BOLD + bcolors.WARNING + bcolors.WARNING + instruction + bcolors.ENDC)
#
def executesqlmetadatainsertion(insertion, cursor):
    ''' Tries to execute the SQL insertion of the metadata on the database cursor.
        Takes in the table in insertion, cursor.
        Returns nothing, but if successful database is updated.
    '''
    try:
        cursor.execute(insertion)
        print(bcolors.OKGREEN + "Successful SQL insertion." + bcolors.ENDC)
        print(bcolors.BOLD + bcolors.OKGREEN + insertion + bcolors.ENDC)
    except:
        print(bcolors.WARNING + "Could not execute SQL insertion." + bcolors.ENDC)
        print(bcolors.BOLD + bcolors.WARNING + insertion + bcolors.ENDC)
#
def executeselectinsert(select, insert, cursor):
   ''' Tries to execute the SQL insertion. Tries to select before insertion.
       Takes in the catalog json/dict, cursor.
       Returns nothing, but if successful database is updated.
   '''
   # Function tries to select items, if none found in the database, it will attempt
   # to insert the items.
   try:
       cursor.execute(select)
       result = cursor.fetchall()[0]
       print(bcolors.OKCYAN + "[si]Entry already exists." + bcolors.ENDC)
       print(bcolors.BOLD + bcolors.OKCYAN + select + bcolors.ENDC)
   except:
       print(bcolors.WARNING + "[si]Could not execute SQL selection. Proceed to insertion!" + bcolors.ENDC)
       print(bcolors.WARNING + bcolors.BOLD + select + bcolors.ENDC)
       try:
           cursor.execute(insert)
           print(bcolors.OKGREEN + "[si]Successful SQL insertion." + bcolors.ENDC)
           print(bcolors.BOLD + bcolors.OKGREEN + insert + bcolors.ENDC)
       except:
           print(bcolors.WARNING + "[si]Could not execute SQL insertion." + bcolors.ENDC)
           print(bcolors.BOLD + bcolors.WARNING + insert + bcolors.ENDC)
#
def executeselect(select, cursor):
   ''' Tries to select from database.
       Takes in the catalog json/dict, cursor.
       Returns selection.
   '''
   try:
       cursor.execute(select)
       print(bcolors.OKCYAN + "[s]Sucessful SQL selection." + bcolors.ENDC)
       print(bcolors.BOLD + bcolors.OKCYAN + bcolors.OKCYAN + select + bcolors.ENDC)
       return cursor.fetchall()
   except:
       print(bcolors.WARNING + "[s]Could not execute SQL selection." + bcolors.ENDC)
       print(bcolors.BOLD + bcolors.WARNING + select + bcolors.ENDC)
       return None
#
def executeinsert(insert, cursor):
    ''' For when insertion should be done without check, or with an external check
        For exampe in the case of encrypted ids.
    '''
    try:
        cursor.execute(insert)
        print(bcolors.OKGREEN + "[i]Sucessful SQL insertion." + bcolors.ENDC)
        print(bcolors.BOLD + bcolors.OKGREEN + insert + bcolors.ENDC)
    except:
        print(bcolors.WARNING + "[i]SQL insertion failed (did you perform a check on wether the entry exists already?)." + bcolors.ENDC)
        print(bcolors.BOLD + bcolors.WARNING + insert + bcolors.ENDC)
#
def check_if_json_in_database(database, samplejson, encrypted):
    ''' Checks if json was already inserted into database
        Input: database, json
        Output: true or false
    '''
    status = True # default is true, present in database
    # i need proof to change the status to false
    cursor = database.cursor()
    # sample id and analysis date determine if json should be inserted
    sample = samplejson['Sample']
    sample_id_in_lab = sample['sample_id_in_lab']
    analysis = samplejson['Analysis']
    analysis_date = analysis['analysis_date']
    analysis_date_formatted = format_datetime(analysis_date, sample_id_in_lab, 'json')
    # select sample ids from database
    sqlselect = "SELECT id_sample_in_lab, date_analysis FROM Sample INNER JOIN Analysis WHERE Sample.id_sample = Analysis.id_sample;"
    list = executeselect(sqlselect, cursor) # this is a tuple of sample id / date of analysis
    list_sample_id_in_lab = []
    list_analysis_date = []
    list_sampleanalysisdate = []
    # when encrypted
    if encrypted == True:
        for el in list:
            list_sample_id_in_lab.append(el[0])
            list_analysis_date.append(el[1])
            dekr_el = decrypt(el[0], encrypted)
            list_sampleanalysisdate.append(dekr_el+"-"+str(el[1]))
        # decrypt ids
        dekr_list_sample_id_in_lab = []
        for el in list_sample_id_in_lab:
            dekr_el = decrypt(el, encrypted)
            #print(el)
            #print(dekr_el)
            dekr_list_sample_id_in_lab.append(dekr_el)
        # check if json sample id in decrypted list
        if sample_id_in_lab not in dekr_list_sample_id_in_lab:
            status = False
        else:
            if sample_id_in_lab+"-"+str(analysis_date_formatted) not in list_sampleanalysisdate:
                status = False
     # when not encrypted
    elif encrypted == False:
        for el in list:
            list_sample_id_in_lab.append(el[0])
            list_analysis_date.append(el[1])
            list_sampleanalysisdate.append(el[0]+"-"+str(el[1]))
        if sample_id_in_lab not in list_sample_id_in_lab:
            status = False
        else:
            if sample_id_in_lab+"-"+str(analysis_date_formatted) not in list_sampleanalysisdate:
                status = False
    # return status
    print("sample_id, analysis_date, present_in_db : ",sample_id_in_lab, analysis_date, status)
    return status
#
def get_id(file, source):
    ''' Parses files to get patient idientifiers
        Input is file path and file type
        Output is array of identifiers
    '''
    identifiers = []
    # Clinical STIC data from CLINIC_BDD_PSTIC_120219.xls
    if (source == 'stic-clinic'):
        book = xlrd.open_workbook(file)
        sheet = book.sheet_by_index(0)
        for row_index in range(1,sheet.nrows):
            patid = sheet.cell(rowx=row_index,colx=0).value
            if patid not in identifiers:
                identifiers.append(patid)
    # Surveyor data from SUVEYOR-Final.xls
    elif (source == 'stic-surveyor'):
        book = xlrd.open_workbook(file)
        sheet = book.sheet_by_index(0)
        for row_index in range(5, sheet.nrows):
            patid = str(sheet.cell(rowx=row_index,colx=0).value).replace(" ","")
            if patid not in identifiers:
                identifiers.append(patid)
    # Mitochip data
    elif (source == 'stic-mitochip'):
        mitochip_varfiles = glob.glob(file+"*2012-04-11.xls")
        for mfile in mitochip_varfiles:
            book = xlrd.open_workbook(mfile)
            # Homoplasmic call sheet
            sheet1 = book.sheet_by_index(0)
            # Heteroplasmy call sheet, three patients with only heteroplasmic calls: 08037FER, 02097ZOM, 01082HAM
            sheet2 = book.sheet_by_index(1)
            for sheet in [sheet1, sheet2]:
                for row_index in range(1, sheet.nrows):
                    centre = int(sheet.cell(rowx=row_index,colx=0).value)
                    if centre < 10:
                        s = "0" + str(centre)
                        centre = s
                    sample = int(sheet.cell(rowx=row_index,colx=1).value)
                    if sample < 10:
                        s = "00" + str(sample)
                        sample = s
                    elif (sample >=10 and sample <100):
                        s = "0" + str(sample)
                        sample = s
                    initials = ("".join(str(sheet.cell(rowx=row_index,colx=2).value).split())).replace("-","").replace("*","")
                    patid = str(centre) + str(sample) + str(initials)
                    if (patid not in identifiers):
                        identifiers.append(patid)
    return identifiers
#
def get_stic_haplogroup(patid):
    ''' Extracts sample haplogroup from Mitochip data.
        Input just patid
        Returns haplogroup
    '''
    haplogroup = ''
    centre = int(patid[0:2])
    patient = int(patid[2:5])
    initiales = str(patid[5:])
    # Parse caller excel files
    mitochip_file = config.STICMITOCHIPxls+'_c'+str(centre)+"_2012-04-11.xls"
    book = xlrd.open_workbook(mitochip_file)
    sheet1 = book.sheet_by_index(0)
    sheet2 = book.sheet_by_index(1)
    haplo_id = ''
    for sheet in [sheet1, sheet2]:
        for row_index in range(1,sheet.nrows):
            col1 = int(sheet.cell(rowx=row_index,colx=0).value)
            col2 = int(sheet.cell(rowx=row_index,colx=1).value)
            if col1 == centre and col2 == patient:
                haplo_id = str(sheet.cell(rowx=row_index,colx=4).value)
    # Parse the haplogroupe file
    haplogroup_file = config.STICMITOCHIPxls+'_c'+str(centre)+"_haplogroupe.xls"
    book = xlrd.open_workbook(haplogroup_file)
    sheet = book.sheet_by_index(0)
    for row_index in range(1, sheet.nrows):
        haplo_id2 = str(sheet.cell(rowx=row_index,colx=0).value)
        if haplo_id == haplo_id2:
            haplogroup = str(sheet.cell(rowx=row_index,colx=2).value)

    #print(centre, patient, initiales, haplo_id, haplogroup)
    return haplogroup
#
def encrypt(id):
    ''' Encrypts sample id before insertion into mmdb2
    '''
    bid = bytes(id, 'utf-8')
    key = open(config.FERNETKEY, 'rb').read()
    fernet = Fernet(key)
    kryptid = fernet.encrypt(bid)
    return kryptid
#
def decrypt(kryptid, encrypt):
    ''' Decrypts sample id
    '''
    id = ''
    if encrypt == True:
        key = open(config.FERNETKEY, 'rb').read()
        fernet = Fernet(key)
        # sometimes i input a byte, sometimes a str
        if not isinstance(kryptid, bytes):
            kryptid = bytes(kryptid, 'utf-8')
        # check if i can decrypt
        try:
            bid = fernet.decrypt(kryptid)
        except ValueError:
            print("Issue with encrypted id:", kryptid)
            exit()
        id = bid.decode("utf-8")
    elif encrypt == False:
        id = kryptid
    return id
#
def areEqual(arr1, arr2):
    ''' From: https://www.geeksforgeeks.org/check-if-two-arrays-are-equal-or-not/
    '''
    status = True
    n = len(arr1)
    m = len(arr2)
    if (n != m):
        status=False

    # Linearly compare elements
    for i in range(0, n):
        #print(arr1[i], arr2[i])
        #print(status)
        if (arr1[i] != arr2[i]):
            if arr1[i] != "Haplogroup" and  arr2[i] != "Haplogroupe":
                status=False

    # If all elements were same.
    #print(arr1)
    #print(arr2)
    #print(status)
    return status
#
def get_ionthermo_id(file, source):
    ''' This function takes in an excel sheet and outputs ids.
        The input is an xls from a RUN titled as "Tableau recapitulatif des
        resultats patients..."
        The output is a an array of dictinaries containing:
        vcfbarcodeid, sampleid, haplogroup, tissue and patientid.
    '''
    arrodict = []
    if source == "retrofisher-recap":
        book = xlrd.open_workbook(file)
        sheet = book.sheet_by_index(0)
        # check that the xls headers and well ordered and that the script will
        # extract what is needed
        titles = []
        runs_without_complete_recap = ['054', '055', '056', '057', '058', '059',
                                       '053', '052', '051', '049', '048', '047',
                                       '046', '045', '050']
        status = False
        for r in runs_without_complete_recap:
            if r not in file:
                pass
            else:
                status = True
        if status:
            titles = [sheet.cell(rowx=2,colx=0).value,
                      sheet.cell(rowx=2,colx=1).value,
                      sheet.cell(rowx=2,colx=2).value,
                      sheet.cell(rowx=2,colx=4).value]
        else:
            titles = [sheet.cell(rowx=1,colx=0).value,
                      sheet.cell(rowx=1,colx=1).value,
                      sheet.cell(rowx=1,colx=3).value,
                      sheet.cell(rowx=0,colx=5).value]
                      #sheet.cell(rowx=0,colx=6).value]
        titles_as_expected = ['Barcode ID', 'Sample Name', 'Haplogroupe', 'Identité du patient'] #, 'Type de prélèvement']
        titles_as_expected2 = ['Patients', 'Maladie Recherchée', 'Résultats', 'Remarque particulière']
        if (areEqual(titles, titles_as_expected) or areEqual(titles, titles_as_expected2)):
            pass
        else:
            print(bcolors.FAIL +  "Title of columns in the recap file not as expected." + bcolors.ENDC)
            #print(titles)
            exit()
        #
        if status:
            maladies = ['SYND', 'LEBER', 'MITOT', 'LEBER']
            # runs before 059 included
            for row_index in range(4, sheet.nrows):
                maladie = sheet.cell(rowx=row_index, colx=1).value
                if str(maladie).upper() in maladies:
                    sample_id = sheet.cell(rowx=row_index, colx=0).value
                    try:
                        sample_id = int(sample_id)
                    except:
                        print(bcolors.FAIL + "Issue with sample id: " +  bcolors.ENDC, sample_id)
                        exit()
                    info = { 'sample_id' : sample_id }
                    arrodict.append(info)

        else:
            # runs after 059 excluded
            for row_index in range(2, sheet.nrows):
                vcfbarcode_id = sheet.cell(rowx=row_index,colx=0).value
                sample_id = sheet.cell(rowx=row_index,colx=1).value
                haplogroup = sheet.cell(rowx=row_index,colx=3).value
                patient_id = sheet.cell(rowx=row_index,colx=5).value
                #tissue = sheet.cell(rowx=row_index,colx=6).value
                # series of characters that, if present in sample_id fields, needs to be excluded
                to_exclude = ['/', 'TPOS', 'BLC', 'EXCL']
                exclude = False
                for el in to_exclude:
                    if el in str(sample_id):
                        exclude = True
                #
                if patient_id != '' and sample_id != '' and patient_id !='/' and exclude == False:
                    if "F" in str(sample_id) or "M" in str(sample_id): # sometimes in the fiche récapitulative
                        # sample_id has the sex appended like - M or - F
                        sample_id = str(sample_id).split()[0]
                        sample_id = str(sample_id).split('-')[0]
                    try:
                        sample_id = int(sample_id)
                    except:
                        if "_REPASSE" in sample_id or '_REPLIG' in sample_id or 'RepliG' in sample_id:
                            print("Warning: RERUN/REPLICA, ", sample_id, " in ", file)
                            sample_id = sample_id.split('_')[0]
                            sample_id = sample_id.split('-')[0]
                            sample_id = int(sample_id)
                        elif "LONG PCR" in sample_id:
                            print("Warning: LONG PCR, ", sample_id, " in ", file)
                            sample_id = sample_id.split('_')[0]
                            sample_id = sample_id.split('-')[0]
                            sample_id = int(sample_id)
                        elif "_QIAGEN" in sample_id:
                            print("Warning: QUIAGEN, ", sample_id, " in ", file)
                            sample_id = sample_id.split('_')[0]
                            sample_id = sample_id.split('-')[0]
                            sample_id = int(sample_id)
                        elif "_EZ1" in sample_id:
                            print("Warning: EZ1, ", sample_id, " in ", file)
                            sample_id = sample_id.split('_')[0]
                            sample_id = sample_id.split('-')[0]
                            sample_id = int(sample_id)
                        elif "_HAMILTON" in sample_id:
                            print("Warning: HAMILTON, ", sample_id, " in ", file)
                            sample_id = sample_id.split('_')[0]
                            sample_id = sample_id.split('-')[0]
                            sample_id = int(sample_id)
                        else:
                            print(bcolors.FAIL + "Issue with sample id: " +  bcolors.ENDC, sample_id)
                            exit()

                    info = { 'vcfbarcode_id': vcfbarcode_id,
                             'sample_id' : sample_id,
                             'haplogroup': haplogroup,
                             'patient_id': patient_id,
                            }
                    arrodict.append(info)
    return arrodict
#
def translate_tissue(tissue, sample_id):
    ''' This function translates french tissue names into
        english tissue names.
    '''
    dict = {
    'sang' : 'blood',
    'urines' : 'urine',
    'ADN' : 'DNA',
    'adn' : 'DNA',
    'muscle' : 'muscle',
    'muscles' : 'muscle',
    'homogenat' : 'homogenate',
    'fibroblastes' : 'fibroblast',
    'fibroblaste' : 'fibroblast',
    'fibros' : 'fibroblast',
    'fibro' : 'fibroblast',
    'foie' : 'liver',
    'ecouvillon' : 'swab',
    'cellules' : 'cells',
    'biopsie' : 'biopsy',
    'salive' : 'saliva',
    'biopsie foie' : 'liver biopsy',
    'liquide' : 'liquid',
    'frottis buccal' : 'buccal smear',
    'nerf' : 'nerve'
    }
    # typos and special fields
    if 'urine' in tissue:
        dict[tissue] = 'urine'
    if 'muscle' in tissue:
        dict[tissue] = 'muscle'
    if 'adn' in tissue or 'ADN' in tissue:
        dict[tissue] = 'ADN'
    if 'rein' in tissue:
        dict[tissue] = 'kidney'
    try:
        dict[tissue]
    except:
        print("Issue with tissue in sample: ", sample_id, "(",tissue,")")
        exit()
    return dict[tissue]
#
def format_patient_name(nom, prenom):
    ''' Function formats nom et prenom to a name that will end up in the .json
        Makes everything uppercase, replaces empty spaces in nom or prenom with underscrores
    '''
    try:
        nom = nom.strip()
    except:
        pass
    try:
        prenom = prenom.strip()
    except:
        pass
    name = "-".join([str(nom).replace(" ", "_"), str(prenom).replace(" ", "_")])
    return name
#
def format_patient_id(sample_id):
    ''' Function formats nom, prenom and sample_id to create a patient id
        For patients having no patient id (GLIMS?)
    '''
    #name = format_patient_name(nom, prenom)
    info = retrieve_glims_info(sample_id, 'all')
    arr = [info['nom'], info['prenom'], str(info['date_of_birth'])]
    patient_id = "-".join(arr)
    return patient_id
#
def retrieve_glims_info(sample_id, type):
    ''' Function retrieves data thanks to sample_id
        and the GLIMs extraction xls.
        input is sample_id and type which is:
        date_of_birth, nom, prenom, sex date_of_sampling, or all
    '''
    info= {}
    infofile = config.GLIMSINFOfile
    book = xlrd.open_workbook(infofile)
    sheet = book.sheet_by_index(0)
    # objet_patient is defined outside the loop.
    # This way, it will keep the former value in cases
    # where the objt_patient row is empty because a patient had several samplings
    objet_patient = ''
    for row_index in range(10,sheet.nrows):
        # objet patient
        if sheet.cell(rowx=row_index,colx=1).value != '':
            objet_patient = sheet.cell(rowx=row_index,colx=1).value
        # sampling time
        sampling_time = str(sheet.cell(rowx=row_index,colx=4).value).split()[0]
        sampling_date = format_datetime(sampling_time, sample_id, 'euro')
        # sample_id is called dossier in the GLIMS xls
        dossier = sheet.cell(rowx=row_index,colx=2).value
        if int(sample_id) == int(dossier):
            try:
                nom, prenom_sex, date_of_birth = objet_patient.split(',')
            except:
                print("Issue with GLIMs info: ", objet_patient, dossier)
                exit()
            try:
                prenom, sex = prenom_sex.split('(')
            except:
                print("Issue with GLIMs info: ", objet_patient, dossier)
                exit()
            nom = nom.strip()
            prenom = prenom.strip()
            nom = re.sub('[- ]', '_', nom) # replace all non characters with _
            # this is done to avoid white space and '-' in names
            prenom = re.sub('[- ]', '_', prenom)
            sex = sex.replace(')','')
            datetime = format_datetime(date_of_birth, sample_id, 'euro')
            info = { 'date_of_birth' : datetime,
                     'sex' : sex.strip(),
                     'nom' : nom.strip(),
                     'prenom' : prenom.strip(),
                     'date_of_sampling' : sampling_date,
                   }
    try:
        a = info['date_of_birth']
        b = info['sex']
        c = info['nom']
        d = info['prenom']
        e = info['date_of_sampling']
    except:
        print("Issue with info extraction from glims.")
        print("info", info)
        print("sample id", sample_id)
        exit()

    if type == 'all':
        return info
    else:
        return info[type]
#
def format_datetime(datestring, sample_id, style):
    ''' Function formats a datastring extracted from a patient xls from a
        Ion Proton or a Ion 5S XL and returns a date with an appropriate
        format to be inserted in the database.
        Takes in datestring, sample_id and style (euro or us)
    '''
    # trying to split datestring
    try:
        datearr = re.split('[-/]', datestring)
    except:
        pass
    # checking it does indeed contain three parts
    if len(datearr) != 3:
        print("Issue with date, it does not have the three items day, month, year: ", datestring, sample_id)
        exit()
    # extract day, month and year according to date style
    day, month, year = [0, 0, 0]
    if style == 'euro':
        day, month, year = datearr
    elif style == 'us':
        month, day, year = datearr
    elif style == 'json':
        year, month, day = datearr
    # format year correctly
    if len(year) == 4:
        pass
    elif len(year) == 2 and int(year)>=8 and int(year)<=22:
        year = "20"+str(year)
    else:
        print(inspect.stack()[0][3],": Issue with date format, wrong year input: ", datestring)
        exit()
    # delete '0' in front of months and days
    if str(day).startswith('0'):
        day = str(day).replace('0', '')
    if str(month).startswith('0'):
        month = str(month).replace('0', '')
    # format as fullyear-month-day
    date = datetime.date(int(year), int(month), int(day))
    return date
#
def get_retrofisher_catalog(file):
    ''' Function extracts variants from the Niourk pipeline xls
        Returns variant catalog.
    '''
    # NB: This is the catalog for mitomatcher2
    # In mitomatcher1, as developped by achoury
    # the variants that were present in the retrodata
    # were those called with TVC only
    # In their header appeared:
    ##reference=file:///media/achoury/7b4d28c6-b261-433b-8d3a-47158373291e/DATA/references/mtDNA/mtDNA.fasta
    ##source="tvc 5.10-12 (47cf458e73-68363dea) - Torrent Variant Caller"
    # In mitomatcher2, it is the Niourk xls that is used to retrieve variants
    catalog = []
    book = xlrd.open_workbook(file)
    for sheet in book.sheets():
        if (sheet.name).lower() == 'niourk' or (sheet.name).lower() == 'all variants':
            # check xls headers
            header_nb_calls = str(sheet.cell(rowx=1,colx=45).value)
            header_type = str(sheet.cell(rowx=2,colx=9).value)
            header_pval = str(sheet.cell(rowx=2,colx=42).value)
            header_strandbias = str(sheet.cell(rowx=1,colx=13).value)
            if header_nb_calls == "nb call" and header_type == "type" and header_pval == 'pval' and "relative" in header_strandbias.lower():
                pass
            else:
                print(bcolors.FAIL + "Issue with sample xls headers in NIOURK sheet:" + bcolors.ENDC)
                print(sample_id, header_nb_calls, header_type, header_pval, header_strandbias)
                exit()
            for row_index in range(3, sheet.nrows):
                # want at least 4 callers
                try:
                    nb_calls = sheet.cell(rowx=row_index,colx=45).value
                    nc_calls = int(nb_calls)
                    #print(row_index, nb_calls, sheet.cell(rowx=row_index,colx=1).value)
                except:
                    print(bcolors.FAIL + "Issue with nb_calls format: " + bcolors.ENDC, nb_calls)
                    exit()
                # want to exclude frameshirt variants, that are very often artefacts
                try:
                    type = sheet.cell(rowx=row_index,colx=9).value
                except:
                    print(bcolors.FAIL + "Issue with type format: " + bcolors.ENDC, type)
                    exit()
                try:
                    pval = sheet.cell(rowx=row_index,colx=42).value
                except:
                    print(bcolors.FAIL + "Issue with pval format: " + bcolors.ENDC, pval)
                    exit()
                try:
                    strandbias = sheet.cell(rowx=row_index,colx=13).value
                except:
                    print(bcolors.FAIL + "Issue with strandbias format: " + bcolors.ENDC, strandbias)
                    exit()
                try:
                    AF = sheet.cell(rowx=row_index,colx=12).value # allele freq
                except:
                    print(bcolors.FAIL + "Issue with AF: " + bcolors.ENDC, AF)
                    exit()
                # extracting data
                pos = int(sheet.cell(rowx=row_index,colx=1).value) # pos column
                ref = sheet.cell(rowx=row_index,colx=4).value # ref column
                alt = sheet.cell(rowx=row_index,colx=5).value # alt column
                AF = float(AF)
                heteroplasmy_status = 'HOM'
                # The pipeline at the CHU d'Angers will rarely give Homoplasmic
                # variants because of artefacts, this is why we consider allele
                # frequencies above 95 to be homoplasmic
                if AF>4 and AF < 96:
                    heteroplasmy_status = 'HET'
                # These two criteria were advised by Valerie Desquirez, in order
                # to retain a list of trustworthy
                # with the pipeline at Angers Hospital
                thres = { 'nb_calls'    : nb_calls,
                          'type'        : type,
                          'pval'        : pval,
                          'strandbias'  : strandbias,
                          'pos'         : pos,
                          'ref'         : ref,
                          'alt'         : alt,
                          'AF'          : AF
                        }
                #print(thres)
                thresholds = pass_thresholds(thres)
                if thresholds == True:
                    '''
                    if len(ref)>1 or len(alt)>1:
                        print(bcolors.FAIL + "yes indel", str(pval) + bcolors.ENDC, pos, ref, alt)
                    else:
                        print(bcolors.OKBLUE + "not indel", str(pval) + bcolors.ENDC, pos, ref, alt)
                    '''
                    #
                    #if ((len(ref) != 1 or len(alt) != 1 or ref == '.') and pval < 4e-16) or (len(ref)<=1 and len(alt)<=1): # insertions or deletions
                    variant = {
                        'chr' : 'chrM',
                        'pos' : pos,
                        'ref' : ref,
                        'alt' : alt,
                        'heteroplasmy_rate' : AF,
                        'heteroplasmy_status' : heteroplasmy_status,
                        'depth' : '',
                        'quality' : ''
                    }
                    #
                    catalog.append(variant)
                    #else:
                    #    print(bcolors.WARNING + "WARNING: Excluded variation. \t\t", str(pos), ref, alt, str(pval), bcolors.ENDC,)
                        #print("position ", int(sheet.cell(rowx=row_index,colx=1).value), " in ", file)
                        #exit()
        elif (sheet.name).lower() == 'variant caller mtdna':
            position = str(sheet.cell(rowx=0,colx=0).value)
            reference = str(sheet.cell(rowx=0,colx=3).value)
            alternative = str(sheet.cell(rowx=0,colx=4).value)
            AF = str(sheet.cell(rowx=0,colx=5).value)
            titles = [position, reference, alternative, AF]
            titles_as_expected = ['Position', 'Ref', 'Variant', 'Var Freq']
            if areEqual(titles, titles_as_expected):
                for row_index in range(1, sheet.nrows):
                    pos = sheet.cell(rowx=row_index,colx=0).value
                    zyg = sheet.cell(rowx=row_index,colx=2).value
                    ref = sheet.cell(rowx=row_index,colx=3).value
                    alt = sheet.cell(rowx=row_index,colx=4).value
                    af  = sheet.cell(rowx=row_index,colx=5).value
                    if pos != "":
                        try:
                            pos = int(pos)
                            af = float(af)
                            zyg = zyg.upper()
                            try:
                                len(zyg) == 3
                            except:
                                print("Zygosity annotation is a word of more than 3 chars: ", zyg)
                                exit()
                            variant = {
                                'chr' : 'chrM',
                                'pos' : pos,
                                'ref' : ref,
                                'alt' : alt,
                                'heteroplasmy_rate' : af,
                                'heteroplasmy_status' : zyg,
                                'depth' : '',
                                'quality' : ''
                            }
                            catalog.append(variant)
                        except:
                            print("Issue while building catalog (RUN 55 and below).")
                            exit()
    return catalog
#
def pass_thresholds(values):
    ''' What is a valid variant among all Niourk output?
    '''
    verbose = False
    # thresholds
    thres = { 'nb_calls'    : 4,
        'type'        : ['frameshift'],
        'pval'        : 4e-16,
        'pval-indels' : 1e-18,
        'strandbias'  : [0.25, 0.75],
    }
    # at least 4 variant calls called the variant
    if int(values['nb_calls']) <= thres['nb_calls']:
        if verbose : print(bcolors.WARNING + "Variant rejected due to low nb of callers: \n" + bcolors.ENDC, values, "\n")
        return False
    # strans bias needs to be checked
    if float(values['strandbias']) < thres['strandbias'][0] or float(values['strandbias']) > thres['strandbias'][1]:
        if verbose : print(bcolors.WARNING + "Variant rejected due to SB: \n" + bcolors.ENDC, values, "\n")
        return False
    # p-value for all variants
    if values['pval'] == '.':
        return False
    if float(values['pval']) > thres['pval']:
        if verbose : print(bcolors.WARNING + "Variant rejected due to p-value: \n" + bcolors.ENDC, values, "\n")
        return False
    # type
    if values['type'].lower() in thres['type']:
        if verbose : print(bcolors.WARNING + "Variant rejected due to type: \n" + bcolors.ENDC, values, "\n")
        return False
    # p-value for indels specifically
    if len(values['ref']) != 1 or len(values['alt']) != 1 or values['ref'] != '.':
        if float(values['pval']) > thres['pval-indels']:
            if verbose : print(bcolors.WARNING + "Variant rejected due to p-value (indels): \n" + bcolors.ENDC, values, "\n")
            return False
    #
    if verbose : print(bcolors.OKBLUE + "Variant kept: \n"+ bcolors.ENDC, values, "\n")
    return True
#
def get_date_recapfile(sheet):
    ''' Extract date from recap file of a Run (proton or S5)
        Input: sheet
        Output: date
    '''
    dates = []
    for r in range(30,52): # row
        for c in range(2,4): # column
            for i in range(0,2): # first or second word within cell, in case initials are before date
                date = ''
                try:
                    date = str(sheet.cell(rowx=r,colx=c).value).split()[i]
                except:
                    pass
                if '/' in date:
                    dates.append(date)
    # checks if array of dates is not empty
    # and deletes characters from it in case
    # upon entry initials were glued to the date
    if len(dates) != 0:
        d = re.sub('[A-z]', '', dates[0])
        return d
    else:
        print("Issue with date. No date found. (2)")
        #print(dates)
        exit()
#
def get_recap_file(run):
    ''' Get recap file from a S5 or Proton run
        Input: run
        Output: recap_file
    '''
    recap_file = []
    try:
        recap_file = glob.glob(run + "/**/*capitulatif*.xls", recursive = True)
    except:
        print("No recap file found for run: ", run)
        exit()
    try:
        recap_file[0]
        print("\n################ Parsing recap file:", recap_file[0], "\n\n")
    except:
        print("The RUN ", run, " does not contain a recap file.")
        exit()
    if len(recap_file)>1:
        print("The RUN", run, "has two recap files. Beware.")
        print(recap_file)
        exit()
    else:
        return recap_file[0]
#
def get_age_at_sampling(date_of_birth, date_of_sampling):
    #d1 = datetime.datetime.strptime(date_of_birth, "%Y-%m-%d")
    #d2 = datetime.datetime.strptime(date_of_sampling, "%Y-%m-%d")
    age = int(int(abs((date_of_birth - date_of_sampling).days))/365.25)
    return age
