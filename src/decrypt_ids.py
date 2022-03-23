#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 01/26/2022
# author: abodrug

# This script is a database integrity verification script
# It verifies that identifiers in lab, in Clinic or Sample, are unique

#
import sys, glob, os
import argparse as ap
import getpass
#
sys.path.append('/home/abodrug/mitomatcher2_dev/')
import config
sys.path.append(config.SOURCE)
import utilitary
#
from cryptography.fernet import Fernet
from difflib import SequenceMatcher
#

####################
# help description #
####################
for el in sys.argv:
    if el in ["-h", "--help", "-help", "getopt", "usage"]:
        sys.exit('''
        -v  --verbose : Make script verbose. Notably print IDs if duplicates found.
        ''')

###################
# argument parser #
###################

p = ap.ArgumentParser()
p.add_argument("-v", "--verbose", required=False, default=False, action='store_true', help="verbose log")
p.add_argument("-s", "--score", required=False, default=0.8, help="similariy score cutoff")
args = p.parse_args()

#############
# functions #
#############
#
def check_Sample_table(database):
    '''
    '''
    cursor = database.cursor()
    sqlselect = ("SELECT id_sample_in_lab FROM Sample;")
    result = utilitary.executeselect(sqlselect, cursor)
    enkr_list = []
    for el in result:
        enkr_list.append(el[0])
    dekr_list = []
    for el in enkr_list:
        bel = bytes(str(el), 'utf-8')
        dekr = utilitary.decrypt(bel)
        dekr_list.append(dekr)
    dekr_set = set(dekr_list)
    contains_duplicates = len(dekr_list) != len(dekr_set)
    if contains_duplicates:
        print("Warning: Sample IDs contain duplicates.")
        if args.verbose: print(dekr_list)
    else:
        print('''Sample IDs do not have exact duplicates. You can seek for similar
        IDs if you enable verbose and choose a similarity score cutoff.''')
        if args.verbose:
            for id1 in dekr_list:
                if "STIC-" in id1:
                    for id2 in dekr_list:
                        if id1 != id2 and "STIC-" in id2:
                            similarity_score = similar(id1[5:], id2[5:])
                            if similarity_score > args.score:
                                print(id1, id2, similarity_score)
    return list
#
def check_Clinic_table(database):
    '''
    '''
    cursor = database.cursor()
    sqlselect = ("SELECT id_patient_in_lab FROM Clinic;")
    result = utilitary.executeselect(sqlselect, cursor)
    enkr_list = []
    for el in result:
        enkr_list.append(el[0])
    dekr_list = []
    for el in enkr_list:
        bel = bytes(str(el), 'utf-8')
        dekr = utilitary.decrypt(bel)
        dekr_list.append(dekr)
    dekr_set = set(dekr_list)
    contains_duplicates = len(dekr_list) != len(dekr_set)
    if contains_duplicates:
        print("Warning: Clinic IDs contain duplicates.")
        if args.verbose: print(dekr_list)
    else:
        print('''Clinic IDs do not have exact duplicates. You can seek for similar
        IDs if you enable verbose and choose a similarity score cutoff.''')
        if args.verbose:
            for id1 in dekr_list:
                if "STIC-" in id1:
                    for id2 in dekr_list:
                        if id1 != id2 and "STIC-" in id2:
                            similarity_score = similar(id1[5:], id2[5:])
                            if similarity_score > args.score:
                                print(id1, id2, similarity_score)
    return list
#
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()
#
########
# main #
########
if __name__ == "__main__":
    print("Checking the unicity of ids in Sample and Clinic table.")
    password = config.PWDADMIN
    database = utilitary.connect2databse(str(password))
    ids = check_Sample_table(database)
