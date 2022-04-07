#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 21/03/2022
# abodrug

# This script generates Mitomatcher phenotype pdfs
# prefilled with clinical information and sample information
# that awaits details about the symptoms of a patient-sample
# in ontology format (ordo, mondo, hpo)

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
def get_retrofisher_json_list ():
    ''' This is the description of the function.
        Takes in: nothing
        Returns: list of jsons
    '''
    dir = config.PATIENTINPUTretrofisher
    file_list = glob.glob(dir+"/**/*json")
    return file_list
#
def build_phenopdf(json_file):
    ''' Builds and partially fills in the pheno/clinical pdf file
        associated to a json file. Backengineering basically.
        Takes in json, outputs pdf.
    '''
    # loading the json and fetching the pdf model
    json_frame = json.load(open(json_file))
    phenopdfmodel = config.PHENOPDFMODEL
    # sample json_from for relevant info to insert into the pdf
    info = subset_info_for_phenopdf(json_frame)
    load_data_into_phenopdf(info, phenopdfmodel)

    return 0
#
def load_data_into_phenopdf(info, phenopdfmodel):
    ''' Filling in pdf model with subset of info from the json_frame.
        In: info, phenopdf
        Out: pdf
    '''
    # create new pdf with a name containing sample, analysis date and name
    pdfname = config.PHENOPDFDIR+"phenopdf_"+info['sample_id']+"_"+info['analysis_date']+"_"+info['patient_name']+".pdf"
    shutil.copyfile(phenopdfmodel, pdfname)
    # filling in
    field_name = fillpdfs.get_form_fields(pdfname)
    data_dict = { 'Reference center'      : get_reference_center(info['reference_center']),
                  "Collector's e-mail"    : info['collector_email'],
                  'Patient_ID'            : info['patient_id'],
                  'Patient_Name'          : info['patient_name'],
                  'Sex'                   : get_full_sex(info['sex']),
                  'Age_at_sampling'       : get_phf_age(info['age_at_sampling']),
                  'Age_of_onset'          : info['age_of_onset'],
                  'Consanguinity'         : info['cosanguinity'],
                  'Sample_ID'             : info['sample_id'],
                  'Date of collection'    : info['date_of_sampling'],
                  'Haplorgoup'            : info['haplogroup'],
                  'Tissue'                : info['tissue'],
                  'Library'               : info['library'],
                  'Sequencer'             : info['sequencer'],
                  'Mapper/Caller/Pipeline': info['mapper_caller_pipeline'],
                  'Date of analysis'      : info['analysis_date'],
                  'Date of birth'         : info['date_of_birth']
                }
    fillpdfs.write_fillable_pdf(phenopdfmodel, pdfname, data_dict)
    # extracting
    '''
    phf = open(pdfname, 'rb')
    parser = PDFParser(phf)
    doc = PDFDocument(parser)
    fields = resolve1(doc.catalog['AcroForm'])['Fields']
    for i in fields:
        field = resolve1(i)
        name, value = field.get('T'), field.get('V')
        print('{0}: {1}'.format(name, value))
    '''
    return 0
#
def get_reference_center(center):
    textcenter = center
    if center == 3:
        textcenter = 'CHU Angers'
    return textcenter
#
def get_full_sex(sex):
    dict = { 'F' : 'female',
             'M' : 'male',
             'O' : 'other',
             'U' : 'unknown'
           }

    return dict[sex]
#
def get_phf_age(age):
    if age <= 1:
        age = '<1'
    return age
#
def get_dob(sample_id):
    nom, prenom, year, month, day = sample_id.split('-')
    dob = str(year) +"-"+ str(month) +"-"+ str(day)
    return dob
#
def subset_info_for_phenopdf(json_frame):
    ''' Subsets json info to only info needed for the phenopdf.
        In: json_frame
        Out: info dict
    '''
    #
    clinical = json_frame['Clinical']
    sample = json_frame['Sample']
    analysis = json_frame['Analysis']
    #
    reference_center = sample['laboratory_of_reference']
    collector_email = sample['data_handler_email']
    patient_id = clinical['patient_id']
    patient_name = clinical['name']
    sex = clinical['sex']
    age_of_onset = clinical['age_of_onset']
    cosanguinity = clinical['cosanguinity']
    age_at_sampling = sample['age_at_sampling']
    date_of_birth = get_dob(clinical['patient_id'])
    #
    sample_id = sample['sample_id_in_lab']
    tissue = sample['tissue']
    analysis_date = analysis['analysis_date']
    date_of_sampling = sample['date_of_sampling']
    haplogroup = sample['haplogroup']
    #
    technique = analysis['technique']
    library = '2 long range PCR'
    sequencer = ''
    mapper_caller_pipeline = 'TMAP/TSVC GATK VarScan SNVer LoFreq Platypus/niourk 1.7'
    if technique == 3 :
        sequencer = 'Ion Proton System'
    elif technique == 4 :
        sequencer = 'Ion S5 XL System'

    #
    info = { 'reference_center' : reference_center,
             'collector_email'  : collector_email,
             'patient_id'       : patient_id,
             'patient_name'     : patient_name,
             'sex'              : sex,
             'age_of_onset'     : age_of_onset,
             'cosanguinity'     : cosanguinity,
             'age_at_sampling'  : age_at_sampling,
             'sample_id'        : sample_id,
             'analysis_date'    : analysis_date,
             'date_of_sampling' : date_of_sampling,
             'haplogroup'       : haplogroup,
             'tissue'           : tissue,
             'library'          : library,
             'sequencer'        : sequencer,
             'mapper_caller_pipeline' : mapper_caller_pipeline,
             'date_of_birth'    : date_of_birth
    }
    return info
#
########
# main #
########
if __name__ == "__main__":
    print("Processing start.")
    #
    if not args.jsonfile:
        json_list = get_retrofisher_json_list()
        for json_file in json_list:
            print("Building pheno pdf for: ", json_file)
            build_phenopdf(json_file)
    else:
        json_file = args.jsonfile
        build_phenopdf(json_file)
