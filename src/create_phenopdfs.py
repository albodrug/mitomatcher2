#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 21/03/2022
# abodrug

# This script generates Mitomatcher phenotype pdfs
# prefilled with clinical information and sample information
# that awaits details about the symptoms of a patient-sample
# in ontology format (ordo, mondo, hpo)

import sys, glob, os
import argparse as ap

####################
# help description #
####################
if len(sys.argv) == 1:
    sys.argv.append("--help")
if sys.argv[1] in ["-h", "--help", "-help", "getopt", "usage"]:
    sys.exit('''
    -a  --argument  :   This is the description of the argument.
                        default "", choices "[]"
    ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-a", "--argument", nargs="+", required=False, default=["d1", "d2"])
args = p.parse_args()

#############
# functions #
#############
#
def get_json_list ():
    ''' This is the description of the function.
        Takes in: nothing
        Returns: list of jsons
    '''
    return 0

########
# main #
########
if __name__ == "__main__":
    print("Processing start.")
