#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 27/04/2022
# author: abodrug

# This script contains functions that will look for co-occurences in the database
import os
import re
import sys
#
import pymysql
import xlrd
#
sys.path.insert(1, '/home/abodrug/mitomatcher2_dev/app/')
import config
import utilitary
#

def format_xlsfile(file):
    # array of Variants
    vararr = []
    #
    book = xlrd.open_workbook(file)
    sheet_count = book.nsheets
    #
    for sheet_index in range(sheet_count):
        sheet = book.sheet_by_index(sheet_index)
        for row_index in range(2, sheet.nrows):
            pos = sheet.cell(rowx=row_index,colx=2).value
            nuclchange = sheet.cell(rowx=row_index,colx=3).value
            if pos != '' and nuclchange != '':
                try:
                    ref = nuclchange.split('-')[0]
                    alt = nuclchange.split('-')[1]
                    if alt == 'del':
                        alt = '.'
                    var = { 'pos' : int(pos),
                            'ref' : ref,
                            'alt' : alt
                          }
                    vararr.append(var)
                except:
                    print("Could not add variant to list.")
                    print(int(pos), nuclchange)
                    exit()
    return vararr
#
def format_listfile(file):
    #
    vararr = []
    #
    lines = open(file, "r")
    next(lines)
    for line in lines:
        variant = line.strip().split('\t')[0]
        if '>' in variant:
            try:
                pos =  re.sub("[^0-9]", "", variant)
                ref = variant.split('>')[0].split(pos)[1]
                alt = variant.split('>')[1]
                var = { 'pos' : pos,
                        'ref' : ref,
                        'alt' : alt}
                vararr.append(var)
            except:
                print("Could not parse variant")
                print(variant)
                exit()
    return vararr
#
def merge_lists(list1, list2):
    megalist = []
    #
    for el1 in list1:
        if el1 not in megalist:
            megalist.append(el1)
    for el2 in list2:
        if el2 not in megalist:
            megalist.append(el2)
    #
    #for el in megalist:
    #    print(el['pos'], el['ref'], el['alt'])
    return megalist
#
def get_idvariant(cursor, list):
    ''' Gets lst of id_variants from the database.
        Outputs array of dictionaries (variants).
    '''
    #
    idvariant_list = []
    #
    verbose = False
    #
    for variant in list:
        pos = variant['pos']
        sqlselect = ("SELECT id_variant FROM Variant WHERE pos="+\
        str(variant['pos'])+\
        " AND ref='"+variant['ref']+\
        "' AND alt='"+variant['alt']+"';")
        #
        sqlresult = cursor.execute(sqlselect)
        sqlresult = cursor.fetchall()
        try:
            id_variant = sqlresult[0][0]
            variant['id_variant'] = id_variant
            idvariant_list.append(variant)
            if verbose:
                print("Variant exists in Mitomatcher: ",\
                str(variant['pos'])+variant['ref']+">"+variant['alt'])
        except:
            if verbose:
                print("Variant does not exist in Mitomatcher: ",\
                str(variant['pos'])+variant['ref']+">"+variant['alt'])
            variant['id_variant'] = -1
            idvariant_list.append(variant)
    return idvariant_list
#
def get_idpatient(cursor, idvariant_list):
    ''' Get patients with at least two variants in the list (cooccurences)
        Returns list of patients.
    '''
    # preparing list to be returned
    idpatient_list = []
    # clean the idvariant_list
    variant_list = []
    for v in idvariant_list:
        if v['id_variant'] != -1:
            variant_list.append(v)
    ######################
    # build joined table of data records with a least one cooccurence #
    ######################
    id_list = []
    for v in variant_list:
        id_list.append(str(v['id_variant']))
    strid_list = ','.join(id_list)
    #print(strid_list)
    # table joins several tables with variant_ids in list of interest
    sqlinstruction = ('CREATE TEMPORARY TABLE tempSampleVariantClinic'
    ' select Sample.*, Variant.*, Clinic.*, Analysis.date_analysis'
    ' from Sample '
    ' inner join Analysis on Sample.id_sample=Analysis.id_sample '
    ' inner join Variant_Call on Variant_Call.id_analysis = Analysis.id_analysis '
    ' inner join Variant on Variant.id_variant = Variant_Call.id_variant '
    ' inner join Sample_Clinic on Sample_Clinic.id_sample = Sample.id_sample '
    ' inner join Clinic on Clinic.id_patient = Sample_Clinic.id_patient;'
    )
    # ' where Variant.id_variant in ('+strid_list+');'
    cursor.execute(sqlinstruction)
    sqlinstruction2 = ('CREATE TEMPORARY TABLE tempSamplePathovariantClinic'
    ' SELECT * FROM tempSampleVariantClinic '
    ' WHERE id_variant IN ('+strid_list+');'
    )
    cursor.execute(sqlinstruction2)
    #################
    # extract id_patient_in_lab
    #################
    sqlselect = ('SELECT id_patient_in_lab FROM Clinic;')
    cursor.execute(sqlselect)
    result = cursor.fetchall()
    #########################
    # looping over patients #
    ########################
    patient_with_cooccuring_pathovars = []
    #
    for p in result:
        #
        id_patient_in_lab = p[0]
        sqlselect2 = ('SELECT * FROM tempSamplePathovariantClinic WHERE '+\
                     'id_patient_in_lab="'+id_patient_in_lab+'" AND '+\
                     'id_variant in ('+strid_list+');')
        cursor.execute(sqlselect2)
        result2 = cursor.fetchall()
        #
        catalog = []
        for el in result2:
            v = { 'chr' : el[9],
                  'pos' : el[10],
                  'ref' : el[11],
                  'alt' : el[12]
                }
            if v not in catalog:
                catalog.append(v)
        #
        if len(catalog) > 1:
            patient_info = { 'id_patient_in_lab' : id_patient_in_lab,
                             'id_sample_in_lab' : result2[0][1],
                             'tissue' : result2[0][2],
                             'haplogroup' : result2[0][3],
                             'patho_catlog' : catalog
                           }
            patient_with_cooccuring_pathovars.append(patient_info)
    ##################
    # Printing results
    ##################
    for el in patient_with_cooccuring_pathovars:
        if "STICCCCCCC" not in el['id_patient_in_lab']:
            print("Patient", utilitary.bcolors.FAIL+el['id_patient_in_lab']+utilitary.bcolors.ENDC,
                  "(sample:", utilitary.bcolors.FAIL+el['id_sample_in_lab']+utilitary.bcolors.ENDC,
                  ", tissue:",el['tissue'],", haplogroup:", el['haplogroup'],")",
                  "carries variants: ", end = ''
                 )
            for record in el['patho_catlog']:
                vchr = record['chr']
                vpos = record['pos']
                vref = record['ref']
                valt = record['alt']
                print(vchr+':'+str(vpos)+vref+'>'+valt," ", end = '')
            print('\n')
    #
    return patient_with_cooccuring_pathovars
#
if __name__ == "__main__":
    #############################
    # Getting the variant lists #
    #############################
    print("Processing started.")
    #
    print("Reading xls file.")
    file1 = config.VARIANTLIST_RANDA
    var_list1 = format_xlsfile(file1)
    #
    print("Reading txt file.")
    file2 = config.VARIANTLIST_ALICE
    var_list2 = format_listfile(file2)
    #
    variant_list = merge_lists(var_list1, var_list2)
    #
    ############################
    # Querying in the database #
    ############################
    database = utilitary.connect2databse(config.PWDADMIN)
    cursor = database.cursor()
    #
    idvariant_list = get_idvariant(cursor, variant_list)
    idpatient_list = get_idpatient(cursor, variant_list)
