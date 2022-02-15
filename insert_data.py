#!/usr/bin/python3
# -*- coding: utf-8 -*-
#15/02/2022
#abodrug

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


####################
# help description #
####################
if len(sys.argv) == 1:
    sys.argv.append("--help")
if sys.argv[1] in ["-h", "--help", "-help", "getopt", "usage"]:
    sys.exit('''
    -t  --type  :   Type of data to format. Each dataset has its own formats and has to be parsed separately.
                    choices "[stic, mdenis, genbank]"
    ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-t", "--type", required=True, choices=['stic','mdenis','genbank'])
args = p.parse_args()

#############
# functions #
#############
#
def insert_stic(database):
    ''' This is the description of the function.
        Takes in:
        Returns:
    '''
    # To see how the json files are formatted, either open one of them
    # in the input/ folders or check out the format_data.py script
    # that generates them
    input_files = glob.glob(config.PATIENTINPUTsurveyor+"surveyor*json")
    for file in input_files:
        samplejson = json.load(open(file))
        print(json.dumps(samplejson))
    return 0

########
# main #
########
if __name__ == "__main__":
    if args.type == 'stic':
        print("Insertion into database initiated. Put on your safety gear and brace.")
        password = 'Mimas' #getpass.getpass()
        database = utilitary.connect2databse(str(password))
        insert_stic(database)
