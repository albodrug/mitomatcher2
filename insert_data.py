#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 02/2022
# abodrug

# This script inserts data into MitoMatcherDB.v2 using json files
# Only json files for STIC data.
# json files and vcf files for mdenis and other data (? maybe)

import sys, glob, os
import argparse as ap
import getpass
import json
#
import config
sys.path.append(config.SOURCE)
import utilitary
#
from cryptography.fernet import Fernet
#


####################
# help description #
####################
for el in sys.argv:
    if el in ["-h", "--help", "-help", "getopt", "usage"]:
        sys.exit('''
        -t  --type  :   Type of data to format. Each dataset has its own formats and has to be parsed separately.
                        choices "[stic, mdenis, genbank]"
        -v  --verbose : Make script verbose.
        ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-t", "--type", required=True, choices=['stic','mdenis','genbank'])
p.add_argument("-v", "--verbose", required=False, default=False, action='store_true')
args = p.parse_args()

#############
# functions #
#############
#
def insert_stic(database):
    ''' This is the description of the function.
        Takes in: database
        Returns: nothing
    '''
    # To see how the json files are formatted, either open one of them
    # in the input/ folders or check out the format_data.py script
    # that generates them
    # Notes about insertion order and logic:
    # The following tables are filled with the json info:
    # Variant, Gene_Variant (a Variant can only relate to a single Gene)
    # Sample, Sample_Ontology (a Sample can only relate to a single set of Ontologies)
    # Clinic, Sample_Clinic (a Clinic can have several Samples)
    # Analysis, Variant_Call ()
    input_files = glob.glob(config.PATIENTINPUTsurveyormitochip+"*.json")
    for file in input_files:
        samplejson = json.load(open(file))
        if args.verbose:
            print("Parsing:", file)
            input("Press Enter to insert sample data.")
        ######################
        # insert into tables Sample, Sample_Ontology
        sample = samplejson['Sample']
        id_sample = insert_sample(database, sample)

        if args.verbose:
            input("Press Enter to insert ontology data.")
        #######################
        # insert into tables Sample_Ontology
        ontology = samplejson['Ontology']
        insert_ontology(database, ontology, id_sample)

        if args.verbose:
            input("Press Enter to insert clinical data.")
        #######################
        # insert into tables Clinic, Sample_Clinic
        clinic = samplejson['Clinical']
        insert_clinic(database, clinic, id_sample)

        if args.verbose:
            input("Press Enter to insert variant data.")
        #######################
        # insert into tables Variant, Gene_Variant
        catalog = samplejson['Catalog']
        insert_catalog(database, catalog)

        if args.verbose:
            input("Press Enter to insert analysis data.")
        #######################
        # insert into tables Analysis, Variant_Call
        analysis = samplejson['Analysis']
        insert_analysis(database, catalog, analysis, id_sample, sample)

    return 0
#
def insert_sample(database, sample):
    ''' Inserts sample data from the .json into Sample.
        If sample not in database: Returns id_sample, database modified when successful.
        If sample in database: Returns id_sample, database not modified.
    '''
    #########################
    # For the function to return
    id_sample_index = None
    #########################
    # encrypt id_sample_in_lab
    id = sample['sample_id_in_lab']
    kryptid = encrypt(id)
    encrypted_id_for_database=str(kryptid)[2:-1]

    #############################
    # insert into ############################################################# Sample
    cursor = database.cursor()
    #############################
    # check the id_sample_in_lab in the database and decrypt them
    dekr_ids = {} # correspondance decrypted, id_sample (index)
    sqlselect = ("SELECT id_sample_in_lab, id_sample FROM Sample WHERE"+\
    " id_labo="+str(sample['laboratory_of_sampling']) + \
    " AND sample_date="+"'"+str(sample['date_of_sampling'])+"'"+ \
    " AND type="+"'"+str(sample['type'])+"'"+ \
    " AND haplogroup="+'"'+str(sample['haplogroup'])+'"'+ \
    ";")
    kr_ids = utilitary.executeselect(sqlselect, cursor) # tuple
    kr_ids_dekr_ids = {} # correspondance crypted and decrypted ids
    # gather decrypted ids, kryptid[0][2:-1]-> getting rid of byte chars in the str
    for k in kr_ids:
        bk = bytes(str(k[0]),'utf-8') # adding the b' and ' and the end
        try:
            id = decrypt(bk)
        except:
            print("warning: Wrongly encrypted token?:", bk)
        if id not in dekr_ids:
            dekr_ids[id] = k[1]
            kr_ids_dekr_ids[k[0]] = id
    #############################
    # check if sample already in database, if not insert it
    if sample['sample_id_in_lab'] in dekr_ids.keys():
        print("Sample already present in database:", sample['sample_id_in_lab'])
        #print(kr_ids) # crypted ids : id_sample
        #print(dekr_ids) # decrypted ids : id_sample
        #print(kr_ids_dekr_ids) # crypted ids : decrypted ids
        id_sample_index = dekr_ids[sample['sample_id_in_lab']] # the function returns this
    else:
        try:
            sqlinsert = ("INSERT INTO Sample (id_sample_in_lab, tissue, haplogroup, id_labo, sample_date, type) " \
            "VALUES (" + ",".join(['"'+str(encrypted_id_for_database)+'"', '"'+sample['tissue']+'"', \
            '"'+sample['haplogroup']+'"',str(sample['laboratory_of_sampling']), \
            '"'+sample['date_of_sampling']+'"','"'+sample['type']+'"']) \
            +");")
            utilitary.executeinsert(sqlinsert, cursor)
            database.commit()
            sqlselect = ("SELECT id_sample FROM Sample WHERE " + \
            " id_sample_in_lab="+"'"+str(encrypted_id_for_database)+"'" + \
            " AND id_labo="+str(sample['laboratory_of_sampling']) + \
            " AND sample_date="+"'"+str(sample['date_of_sampling'])+"'"+ \
            " AND type="+"'"+str(sample['type'])+"'"+ \
            " AND haplogroup="+'"'+str(sample['haplogroup'])+'"'+ \
            ";")
            query_result = utilitary.executeselect(sqlselect, cursor)
            id_sample_index = query_result[0][0]
        except:
            print("Warning: Could not insert into or select from Sample:", sample['sample_id_in_lab'])
            exit()

    return id_sample_index
#
def insert_ontology(datavase, ontology, id_sample):
    ''' Insert into Sample_Ontology.
        Returns nothing, modifies the database.
    '''
    #############################
    # insert into ############################################################# Sample_Ontology
    cursor = database.cursor()
    ################
    # HPO ontologies
    hpo = ontology['hpo']
    for hpo_term in hpo.keys():
        hpo_status = hpo[hpo_term]
        hpo_id = None
        # get hpo_id from Ontology table
        sqlselect = ("SELECT id_ontology FROM Ontology WHERE id_ontologyterm='"+hpo_term+"';")
        try:
            hpo_id = utilitary.executeselect(sqlselect, cursor)[0][0]
        except:
            print("Warning: HPO term might not be in Ontology table: ", hpo_term)
            exit()
        sqlinsert = ("INSERT INTO Sample_Ontology (id_ontology, id_sample, annot) VALUES (" +\
        ",".join([str(hpo_id), str(id_sample), "'"+hpo_status+"'"]) \
        +");")
        utilitary.executeinsert(sqlinsert, cursor)
        database.commit()
    #################
    # MONDO ontologies
    # TO CODE WHEN ONTOLOGIES ARISE
    return 0
#
def insert_clinic(database, clinic, id_sample):
    ''' Insert into Clinic, Sample_Clinic
    '''
    #############################
    # insert into ############################################################# Clinic
    cursor = database.cursor()
    ################
    # Clinic

    # encrypt id_patient_in_lab
    patient_id_in_lab = clinic['patient_id']
    kryptid = encrypt(patient_id_in_lab)
    encrypted_id_for_database=str(kryptid)[2:-1]

    # check if patient already exists in database
    sqlselect = "SELECT id_patient_in_lab FROM Clinic;"
    krypt_ids = utilitary.executeselect(sqlselect, cursor)
    kr_dekr_ids = {}
    for el in krypt_ids:
        kr = el[0]
        bkr = bytes(str(kr), 'utf-8')
        dekr = decrypt(bkr)
        kr_dekr_ids[dekr] = kr
    # if patient not in database
    if patient_id_in_lab not in kr_dekr_ids.keys():
        # insert into Clinic
        array = ["'"+encrypted_id_for_database+"'", "'"+clinic['sex']+"'", "'"+str(clinic['age_of_onset'])+"'", "'"+clinic['cosanguinity']+"'"]
        sqlinsert = ("INSERT INTO Clinic (id_patient_in_lab, sex, age_of_onset, cosanguinity) VALUES ("+ \
        ",".join(array) \
        +");")
        utilitary.executeinsert(sqlinsert, cursor)
        database.commit()
        # insert into Clinic_Sample
        sqlselect = ("SELECT id_patient FROM Clinic WHERE id_patient_in_lab='"+encrypted_id_for_database+"';")
        id_patient = utilitary.executeselect(sqlselect, cursor)[0][0]
        sqlinsert = ("INSERT INTO Sample_Clinic (id_sample, id_patient) VALUES ("+\
        ",".join([str(id_sample), str(id_patient)])+");")
        utilitary.executeinsert(sqlinsert, cursor)
        database.commit()
    # if patient in database
    else:
        print("Warning: Patient already present in database:", clinic['patient_id'])
        #print("Inserting correspondance in table Sample_Clinic.")
        encrypted_id_in_database = kr_dekr_ids[patient_id_in_lab]
        sqlselect = ("SELECT id_patient FROM Clinic WHERE id_patient_in_lab='"+str(encrypted_id_in_database)+"';")
        id_patient = utilitary.executeselect(sqlselect, cursor)[0][0]
        sqlinsert = ("INSERT INTO Sample_Clinic (id_sample, id_patient) VALUES ("+\
        ",".join([str(id_sample), str(id_patient)])+");")
        utilitary.executeinsert(sqlinsert, cursor)
        database.commit()

    return 0
#
def insert_catalog(database, catalog):
    ''' Inserts catalog data from the .json into Gene, Variant and Gene_Variant Tables
        No return, database modified when successful.
    '''
    cursor = database.cursor()
    for v in catalog:
        chr = v['chr']
        pos = v['pos']
        ref = v['ref']
        alt = v['alt']
        sqlselect = ("SELECT * FROM Variant WHERE "+ \
        "chr="+"'"+chr+"'"+ \
        " AND pos="+str(pos)+ \
        " AND ref="+"'"+ref+"'"+ \
        " AND alt="+"'"+alt+"'"+";")
        sqlinsert = ("INSERT INTO Variant (chr, pos, ref, alt)"+ \
        " VALUES ("+",".join(["'"+chr+"'", str(pos), "'"+ref+"'", "'"+alt+"'"])+");")
        # select from Variant to check if variant exists, if not insert it
        utilitary.executeselectinsert(sqlselect, sqlinsert, cursor)
        # commit the change
        database.commit()
        # create link between gene and variant
        # id_gene
        sql1 = ("SELECT id_gene FROM Gene WHERE "+ \
        "chr="+"'"+chr+"'"+ \
        " AND start<="+str(pos)+ \
        " AND end>="+str(pos)+";")
        id_gene = None
        try:
            id_gene = utilitary.executeselect(sql1, cursor)[0][0]
        except:
            pass
        # id_variant
        sql2 = ("SELECT id_variant FROM Variant WHERE "+ \
        "chr="+"'"+chr+"'"+ \
        " AND pos="+str(pos)+";")
        id_variant = None
        try:
            id_variant = utilitary.executeselect(sql2, cursor)[0][0]
        except:
            pass
        # make link in Gene_Variant table
        if id_gene != None:
            sqlselect = ("SELECT * FROM Gene_Variant WHERE " + \
            "id_variant="+str(id_variant)+ \
            " AND id_gene="+str(id_gene)+";")
            sqlinsert = ("INSERT INTO Gene_Variant (id_gene, id_variant) " + \
            "VALUES ("+",".join([str(id_gene), str(id_variant)])+ \
            ");")
            utilitary.executeselectinsert(sqlselect, sqlinsert, cursor)
            database.commit()
    return 0
#
def insert_analysis(database, catalog, analysis, id_sample, sample):
    ''' Insert into Analysis and Variant_Call
        Returns nothing, modifies database
    '''
    cursor = database.cursor()
    # insert analysis data
    id_tech = analysis['technique']
    analysis_date = analysis['analysis_date']

    # user info
    user_mail = sample['data_handler_email']
    sqlselect = ("SELECT id_user FROM User WHERE mail='"+str(user_mail)+"';")
    id_user = utilitary.executeselect(sqlselect, cursor)[0][0]

    # select, insert into Analysis
    sqlselect = ("SELECT id_analysis FROM Analysis WHERE "+ \
    "id_tech="+str(id_tech)+ \
    " AND id_sample="+str(id_sample)+ \
    ";")
    sqlinsert = ("INSERT INTO Analysis (date_analysis, id_sample, id_tech, id_user) VALUES ("+\
    ",".join(["'"+str(analysis_date)+"'", "'"+str(id_sample)+"'", "'"+str(id_tech)+"'", "'"+str(id_user)+"'"]) + \
    ");")
    utilitary.executeinsert(sqlinsert, cursor)
    database.commit()
    # get analysis id
    id_analysis=''
    try:
        id_analysis = utilitary.executeselect(sqlselect, cursor)[0][0]
    except:
        print("Warning: could not select id_analysis from Analysis.")
        print(sqlinsert)
        exit()


    # browse catalog, insert in Variant_Call
    heteroplasmy_rate = 'NULL'
    heteroplasmy_status = 'NULL'
    for v in catalog:
        chr = v['chr']
        pos = v['pos']
        ref = v['ref']
        alt = v['alt']
        if v['heteroplasmy_rate'] != '':
            heteroplasmy_rate = v['heteroplasmy_rate']
        if v['heteroplasmy_status'] != '':
            heteroplasmy_satus = v['heteroplasmy_status']
        sqlselect = ("SELECT id_variant FROM Variant WHERE pos="+str(pos)+ \
        " AND alt='"+str(alt)+"' AND chr='"+str(chr)+"';")
        id_variant = utilitary.executeselect(sqlselect, cursor)[0][0]

        array = [str(heteroplasmy_rate), "'"+str(heteroplasmy_satus)+"'", "'"+str(id_user)+"'", str(id_analysis), str(id_variant)]
        sqlinsert = ("INSERT INTO Variant_Call (heteroplasmy_rate, heteroplasmy, \
        id_user, id_analysis, id_variant) VALUES ("+\
        ",".join(array) + \
        ");")
        utilitary.executeinsert(sqlinsert, cursor)
        database.commit()

    return 0
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
########
# main #
########
# NB: to empty sample data:
# delete from Sample_Clinic; delete from Sample_Ontology;
# delete from Variant_Call; delete from Analysis;
# delete from Sample; delete from Gene_Variant; delete from Variant;
# delete from Clinic
if __name__ == "__main__":
    if args.type == 'stic':
        print("Insertion into database initiated. Put on your safety gear and brace.")
        password = config.PWDADMIN  #getpass.getpass()
        database = utilitary.connect2databse(str(password))
        insert_stic(database)
