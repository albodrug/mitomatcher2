#!/usr/bin/python3
# -*- coding: utf-8 -*-
#01/26/2022
#author: abodrug, achoury

# This script contains functions that are broadly used by main MitoMatcher scripts.
# Not intended to run as stand alone
import sys
import pymysql
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
        print(instruction)
    except:
        print(bcolors.WARNING + "Could not execute SQL instruction." + bcolors.ENDC)
        print(instruction)
#
def executesqlmetadatainsertion(insertion, cursor):
    ''' Tries to execute the SQL insertion of the metadata on the database cursor.
        Takes in the table in insertion, cursor.
        Returns nothing, but if successful database is updated.
    '''
    try:
        cursor.execute(insertion)
        print(bcolors.OKCYAN + "Successful SQL insertion." + bcolors.ENDC)
        print(insertion)
    except:
        print(bcolors.WARNING + "Could not execute SQL insertion." + bcolors.ENDC)
        print(insertion)
#
def getsticlinic(df):
    ''' Parses the xlsx STIC (Bannwarth et al. 2013) clinical data
        Takes in empty MitoMatcherDB json style dataframe
        Outputs it with the available info.
    '''
    file = config.STICCLINICxlsx

    return df
