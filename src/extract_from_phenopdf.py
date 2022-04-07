#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 31/03/2022
# abodrug

# This script extracts information from phenopdfs of Mitomatcher
# This extracts info to complete the jsons with hpos
# i.e. the jsons

import sys, glob, os
import shutil
import argparse as ap
import json
#
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdftypes import resolve1
from fillpdf import fillpdfs
#
sys.path.insert(1, '/home/abodrug/mitomatcher2_dev/')
import config
sys.path.append(config.SOURCE)
import utilitary

####################
# help description #
####################
for el in sys.argv:
    if el in ["-h", "--help", "-help", "getopt", "usage"]:
        sys.exit('''
        -j  --jsonfile  :   Path to the json file that needs to be transformed
                        to pdf.
        ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-j", "--jsonfile", required=False)
args = p.parse_args()

#############
# functions #
#############
#
def get_phenopdf_list():
    ''' Retrieves list of phenopdfs to parse.
        In, nothing. Out list of files.
    '''
    return 0
#
def extract_infofrom_phenopdf(pdffile):
    ''' Retrieves data from phenopdf.
        In phenopdft, out json.
    '''
    print(pdffile)
    field_name = fillpdfs.get_form_fields(pdffile)

    phf = open(pdffile, 'rb'', encoding="utf8", errors='ignore'')
    parser = PDFParser(phf)
    doc = PDFDocument(parser)
    fields = resolve1(doc.catalog['AcroForm'])['Fields']
    for i in fields:
        field = resolve1(i)
        bname, bvalue = field.get('T'), field.get('V')
        name = bname
        value = bvalue
        if isinstance(name, bytes):
            name = bname.decode("utf-8")
        if isinstance(value, bytes):
            value = bvalue.decode("utf-8")
        print("Name: ", name, type(name), " \t\t\t Value: ", value, type(value))

    return 0
#
########
# main #
########
if __name__ == "__main__":
    print("Processing start.")
    #
    if not args.jsonfile:
        phenopdf_list = get_phenopdf_list()
        for json_file in json_list:
            print("Building pheno pdf for: ", json_file)
            extract_infofrom_phenopdf(json_file)
    else:
        json_file = args.jsonfile
        extract_infofrom_phenopdf(json_file)
