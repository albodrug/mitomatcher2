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
