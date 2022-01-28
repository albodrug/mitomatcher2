#!/usr/bin/python3
# -*- coding: utf-8 -*-
#01/2022
#abodrug

# This script translates diverse file formats containing information relevant for MitoMatcherDB
# into a json format, to be able to store the raw data in a comprehensive and re-usable format
# and simplyfy and unify the insertion into the database step.

#
import sys, glob, os
import argparse as ap
import getpass
import xlrd
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
def build_json():
    ''' This is the function that builds the final json.
        Takes in:
        Returns nothing but prints a json per patient.
    '''
    if args.type == "stic":
        build_stic_json()
#
def build_stic_json():
    ''' Builds json file using stic data files. (Bannwarth et al. 2013)
        No input. Path to the files can be found in the config.py
        Returns nothing but prints a json per patient.
    '''
    # creates individual dataframe example
    dataframe_mitochip = build_dataframe()
    #dataframe_surveyor = build_dataframe()
    # Retrieving clinical data from the xlsx, for all patient, each in a separate dataframe
    all_df_clinic_mitochip = utilitary.getsticclinic(dataframe_mitochip)
    #all_df_clinic_surveyor = utilitary.getsticclinic(dataframe_surveyor)
    # Retrieving mitochip data from the xlsx
    #all_df_clinicogenetic_mitochip = utilitary.completewithmitochip(all_df_clinic_mitochip)
    # Retrieving surveyor data from the xlsx
    #all_df_clinicogenetic_surveyor = utilitary.completewithsurveyor(all_df_clinic_mitochip)
    # For each patient writes a json_mitochip and a json_surveyor to the input data.
    '''
    for each sample in all_df_clinicogenetic_mitochip:
        print(sample)
    for each sample in all_df_clinicogenetic_surveyor:
        print(sample)
    '''
#
def build_dataframe():
    ''' Builds MitoMatcher compatible .json object.
        Input is ?
        Output is a json format to be filled out.
    '''
    none = 'nechevo'
    nonr = -10
    nonarray = []
    data = {
        'Clinical' : {
            'name' : none,
            'patient_id' : none,
            'sex' : none,
            'age_at_sampling' : nonr,
            'age_of_onset' : nonr,
            'cosanguinity' : none
        },
        'Sample' : {
            'sample_id_in_lab' : none,
            'laboratory_of_sampling' : none,
            'laboratory_of_reference' : none,
            'data_of_sampling' : none,
            'tissue' : none,
            'type' : none,
            'data_handler' : none,
            'data_handler_phone' : none,
            'data_handler_email' : none
        },
        'Sequencing' : {
            'library' : none,
            'sequencer' : none,
            'mapper' : none,
            'caller' : none,
            'pipeline_version' : none
        },
        'Ontology' : {
            'hpo' : nonarray,
            'orpha' : nonarray,
            'ordo' : nonarray
        },
        'Variant' : {
            'halogroup' : none,
            'catalog' : nonarray
        }
    }
    print(data)
    return data

########
# main #
########
if __name__ == "__main__":
    print("Processing start.")
    print("You requested building a MitoMatcherDB compatible .json from the:\n\tBannwarth et al. 2012 dataset.")
    build_json()
