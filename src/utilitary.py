#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 01/26/2022
# author: abodrug

# This script contains functions that are broadly used by main MitoMatcher scripts.
# Not intended to run as stand alone
import os
import sys
import glob
import pymysql
import xlrd
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
        database = pymysql.connect(db='mitomatcher2', user='mito_admin', passwd=password)
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
        for row_index in range(7, sheet.nrows):
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
def decrypt(kryptid):
    ''' Decrypts sample id
    '''
    key = open(config.FERNETKEY, 'rb').read()
    fernet = Fernet(key)
    bid = fernet.decrypt(kryptid)
    id = bid.decode("utf-8")
    return id
#
def areEqual(arr1, arr2):
    ''' From: https://www.geeksforgeeks.org/check-if-two-arrays-are-equal-or-not/
    '''
    n = len(arr1)
    m = len(arr2)
    if (n != m):
        return False

    # Sort both arrays
    arr1.sort()
    arr2.sort()

    # Linearly compare elements
    for i in range(0, n - 1):
        if (arr1[i] != arr2[i]):
            return False

    # If all elements were same.
    return True
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
        titles = [sheet.cell(rowx=1,colx=0).value,
                  sheet.cell(rowx=1,colx=1).value,
                  sheet.cell(rowx=1,colx=3).value,
                  sheet.cell(rowx=0,colx=5).value,
                  sheet.cell(rowx=0,colx=6).value]
        titles_as_expected = ['Barcode ID', 'Sample Name', 'Haplogroupe', 'Identité du patient', 'Type de prélèvement']
        if (areEqual(titles, titles_as_expected)):
            pass
        else:
            print("Title of columns in the recap file not as expected.")
            exit()
        for row_index in range(2, sheet.nrows):
            vcfbarcode_id = sheet.cell(rowx=row_index,colx=0).value
            sample_id = sheet.cell(rowx=row_index,colx=1).value
            haplogroup = sheet.cell(rowx=row_index,colx=3).value
            patient_id = sheet.cell(rowx=row_index,colx=5).value
            tissue = sheet.cell(rowx=row_index,colx=6).value
            if patient_id != '':
                info = { 'vcfbarcode_id': vcfbarcode_id,
                         'sample_id' : int(sample_id),
                         'haplogroup': haplogroup,
                         'patient_id': patient_id,
                         'tissue': tissue
                        }
                arrodict.append(info)

    return arrodict
#
def translate_tissue(tissue):
    ''' This function translates french tissue names into
        english tissue names.
    '''
    dict = {
    'sang' : 'blood',
    'urines' : 'urine',
    'ADN' : 'DNA',
    'adn' : 'DNA',
    'muscle' : 'muscle',
    'homogenat' : 'homogenate'
    }
    return dict[tissue]
#
def format_patient_name(nom, prenom):
    ''' Function formats nom et prenom to a name that will end up in the .json
        Makes everything uppercase, replaces empty spaces in nom or prenom with underscrores
    '''
    name = "-".join([str(nom.upper()).replace(" ", "_"), str(prenom.upper()).replace(" ", "_")])
    return name
#
def format_patient_id(nom, prenom, sample_id):
    ''' Function formats nom, prenom and sample_id to create a patient id
        For patients having no patient id (GLIMS?)
    '''
    name = format_patient_name(nom, prenom)
    patient_id = "-".join([name, sample_id])
    return patient_id
#
def format_datetime(datestring, sample_id):
    ''' Function formats a datastring extracted from a patient xls from a
        Ion Proton or a Ion 5S XL and returns a date with an appropriate
        format to be inserted in the database.
    '''
    date = ''
    day, month, year = datestring.split('/')
    if len(year) == 4:
        pass
    elif len(year) == 2:
        year = "20"+str(year)
    else:
        print("Issue with date format: ", datestring)
        print("Trying to rescue sample: ", sample_id)
        try:
            datestring = datestring[:-3] # sometimes people forget the space
            # between the date and their initials in the validations section
            day, month, year = datestring.split('/')
        except:
            print("Rescue failed.")
            exit()
    if str(day).startswith('0'):
        day = str(day).replace('0', '')
    if str(month).startswith('0'):
        month = str(month).replace('0', '')
    # format as fullyear-month-day
    date = str(year)+"-"+month+"-"+day
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
        if sheet.name == 'NIOURK':
            for row_index in range(3, sheet.nrows):
                # want at least 4 callers
                nb_calls = int(sheet.cell(rowx=row_index,colx=45).value)
                # want to exclude frameshirt variants, that are very often artefacts
                type = sheet.cell(rowx=row_index,colx=9).value
                # These two criteria were advised by Valerie Desquirez, in order
                # to retain a list of trustworthy
                # with the pipeline at Angers Hospital
                freq_project = float(sheet.cell(rowx=row_index,colx=14).value)
                if nb_calls > 3 and 'frameshift' not in type and freq_project <=30:
                    pos = int(sheet.cell(rowx=row_index,colx=1).value) # pos column
                    ref = sheet.cell(rowx=row_index,colx=4).value # ref column
                    alt = sheet.cell(rowx=row_index,colx=5).value # alt column
                    AF = float(sheet.cell(rowx=row_index,colx=12).value) # allele freq
                    heteroplasmy_status = 'HOM'
                    if AF>2 and AF < 98:
                        heteroplasmy_status = 'HET'

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
                    catalog.append(variant)
    return catalog
#
def get_date_recapfile(sheet):
    ''' Extract date from recap file of a Run (proton or S5)
        Input: sheet
        Output: date
    '''
    for r in range(30,40):
        try:
            date = str(sheet.cell(rowx=r,colx=2).value).split()[0]
            return date
        except:
            if r == 39:
                print("Issue with date.")
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
