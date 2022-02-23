#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 01/2022
# abodrug

# This script translates diverse file formats containing information relevant for MitoMatcherDB
# into a json format, to be able to store the raw data in a comprehensive and re-usable format
# and simplyfy and unify the insertion into the database step.

# The input of this script is MMDB_RAWDATA, i.e. files obtainained from diverse genetical studies
# in formats that can be informatically exploitable. The output of this script is 'input' i.e. the
# input of the MitoMatcher database.

#
import sys, glob, os, re
import argparse as ap
import getpass
import xlrd
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
def build_json():
    ''' This is the function that builds the final json.
        Takes in: nothing
        Returns nothing but buils_json_stic write a json per sample-analyses.
    '''
    if args.type == "stic":
        build_stic_json()
    if args.type == "mdenis":
        build_mdenis_json()
#
def build_stic_json():
    ''' Builds json file using stic data files. (Bannwarth et al. 2013)
        No input. Path to the files can be found in the config.py
        Returns nothing but prints a json per sample-analysis, containing clinical, technical info.
    '''
    ################
    # Get and format identifiers accross clinical, surveyor and mitochip data
    clinical_identifiers = utilitary.get_id(config.STICCLINICxls, 'stic-clinic')
    surveyor_identifiers = utilitary.get_id(config.STICSURVEYORxls, 'stic-surveyor')
    mitochip_identifiers = utilitary.get_id(config.STICMITOCHIPxls, 'stic-mitochip')

    ################
    # Get some stats
    if args.verbose:
        get_stats(clinical_identifiers, surveyor_identifiers, mitochip_identifiers)

    ################
    # Build jsons
    for cid in clinical_identifiers:
        # Build a json if patient was squenced with mitochip: 772 mitochip patients with clinical data
        if cid in mitochip_identifiers:
            clinical_info = build_clinical_json(cid, args.type) # functional, uses clinic data
            sample_info = build_sample_json(cid, args.type) # functinal, uses clinic data
            sequencing_info = build_sequencing_json(cid, args.type+'-mitochip') # to code, use mitochip xls
            mitochip_data = {**clinical_info, **sample_info, **sequencing_info}
            with open(config.PATIENTINPUTsurveyormitochip+cid+"_mitochip.json", 'w', encoding='utf-8') as f:
                json.dump(mitochip_data, f, ensure_ascii=False, indent=4)
                if args.verbose:
                    print("Wrote mitochip data to:", config.PATIENTINPUTsurveyormitochip+"mitochip_"+cid+".json")
        # Build a json if patient was sequenced with mitochip: 741 surveyor patients with clinical data
        if cid in surveyor_identifiers:
            clinical_info = build_clinical_json(cid, args.type)
            sample_info = build_sample_json(cid, args.type)
            sequencing_info = build_sequencing_json(cid, args.type+'-surveyor')
            surveyor_data = {**clinical_info, **sample_info, **sequencing_info}
            with open(config.PATIENTINPUTsurveyormitochip+cid+"_surveyor.json", 'w', encoding='utf-8') as f:
                json.dump(surveyor_data, f, ensure_ascii=False, indent=4)
                if args.verbose:
                    print("Wrote surveyor data to:", config.PATIENTINPUTsurveyormitochip+"surveyor_"+cid+".json")
        # NB: There are 699 patients with surveyor and mitochip and clinical data.
        # There are 772 mitochip/clinical pairs, 741 surveyor/clinical, 72 only mitochip/clinical, 42 only syrveyor/clinical
    return 0
#
def get_stats(clinical_identifiers, surveyor_identifiers, mitochip_identifiers):
    ''' Run stats on stic identifiers
        Input three arrays of identifiers (clinical, surveyor, mitochip)
        Returns nothing
    '''
    print("###STATS###")
    print("Clinical:", len(clinical_identifiers))
    print("Surveyor:", len(surveyor_identifiers))
    print("Mitochip:", len(mitochip_identifiers))
    #
    clinical_as_set = set(clinical_identifiers)
    surveyor_in_clinical = clinical_as_set.intersection(surveyor_identifiers)
    print("Suveyor in clinical:", len(surveyor_in_clinical))
    mitochip_in_clinical = clinical_as_set.intersection(mitochip_identifiers)
    print("Mitochip in clinical:", len(mitochip_in_clinical))
    #
    surveyor_as_set = set(surveyor_identifiers)
    mitochip_in_surveyor = surveyor_as_set.intersection(mitochip_identifiers)
    print("Mitochip in Surveyor:", len(mitochip_in_surveyor))
    #
    n=0; m=0; l=0
    for cid in clinical_identifiers:
        if cid in surveyor_identifiers and cid in mitochip_identifiers:
            #print(cid, ": Clinical, Surveyor, Mitochip", n)
            n=n+1
        elif cid in surveyor_identifiers and cid not in mitochip_identifiers:
            #print(cid, ": Clinical, Surveyor", m)
            m=m+1
        elif cid not in surveyor_identifiers and cid in mitochip_identifiers:
            #print(cid, ": Clinical, Mitochip", l)
            l=l+1
    print("Clinical & Suveyor & Mitochip :", n)
    print("Clinical & Suveyor :", m)
    print("Clinical & Mitochip :", l)
    return 0
#
def build_clinical_json(patid, source):
    ''' Builds MitoMatcher compatible Clinical .json
        Input is patien id and data type (stic, mdenis).
        Output is a json formatted hash.
    '''
    # All needed info
    name = ''
    sex = ''
    age_of_onset = ''
    cosanguinity = ''
    hpoterm = ''
    # Parsing info file
    if source == 'stic':
        # Opening xls
        file = config.STICCLINICxls
        book = xlrd.open_workbook(file)
        sheet = book.sheet_by_index(0)
        # Patient information
        for row_index in range(1,sheet.nrows):
            id = sheet.cell(rowx=row_index,colx=0).value
            if patid == id:
                name = id[5:]
                patient_id = "STIC-"+patid
                sex = sheet.cell(rowx=row_index,colx=1).value
                if sex == 'WOMAN':
                    sex='F'
                elif sex == 'MAN':
                    sex='M'
                age_of_onset = sheet.cell(rowx=row_index,colx=2).value
                cosanguinity = sheet.cell(rowx=row_index,colx=3).value
                # Phenotype information
                hpoterms = {}
                for column_index in range(4, sheet.ncols):
                    annot = str(sheet.cell(colx=column_index, rowx=row_index).value)
                    if (annot == 'Abnormal' or annot == 'Normal' or annot=='X'):
                        if annot == 'Abnormal' :
                            annot = 'Present'
                        elif annot == 'Normal':
                            annot = 'Absent'
                        elif annot == 'X':
                            annot = 'Present'
                        # type of term should be an HPO and contain HP
                        hpoterm = str(sheet.cell(colx=column_index, rowx=0).value)
                        if (re.findall('HP', hpoterm)):
                            nb_hpo = len(re.findall('HP', hpoterm))
                            if ( nb_hpo == 1 ):
                                hpoterms[hpoterm] = annot
                            if (nb_hpo > 1):
                                hpos = hpoterm.split(' – ')
                                for h in hpos:
                                    hpoterms[h] = annot
    # fill in frame
    clinical = {
        'Clinical' : {
            'name' : name,
            'patient_id' : patient_id,
            'sex' : sex,
            'age_of_onset' : age_of_onset,
            'cosanguinity' : cosanguinity
        },
        'Ontology' : {
            'hpo' : hpoterms,
            'orpha' : {},
            'ordo' : {}
        }
    }
    return clinical
#
def build_sample_json(patid, source):
    ''' Builds MitoMatcher compatible Sample info .json
        Input is patient id and data type (stic, mdenis).
        Output is a json formatted hash.
    '''
    # Needed info
    sample_id_in_lab = ''
    laboratory_of_sampling = ''
    laboratory_of_reference = ''
    date_of_sampling = ''
    age_at_sampling = ''
    tissue = ''
    type = ''
    data_handler = ''
    data_handler_phone = ''
    data_handler_email = ''
    haplogroup = ''
    # Parsing files
    if source == 'stic':
        sample_id_in_lab = "STIC-"+patid
        laboratory_of_sampling = int(patid[0:2])
        laboratory_of_reference = int(patid[0:2])
        date_of_sampling = '2012-4-11'
        age_at_sampling = 'unknown'
        tissue = 'blood urine muscle'
        type = 'index-stic'
        data_handler = 'Sylvie Bannwarth'
        data_handler_email = 'sylvie.bannwarth@unice.fr'
        haplogroup = utilitary.get_stic_haplogroup(patid)
    # Filling in frame
    sampleinfo = {
        'Sample' : {
            'sample_id_in_lab' : sample_id_in_lab,
            'laboratory_of_sampling' : laboratory_of_sampling,
            'laboratory_of_reference' : laboratory_of_reference,
            'date_of_sampling' : date_of_sampling,
            'age_at_sampling' : age_at_sampling,
            'tissue' : tissue,
            'type' : type,
            'data_handler' : data_handler,
            'data_handler_phone' : data_handler_phone,
            'data_handler_email' : data_handler_email,
            'haplogroup' : haplogroup
            }
    }

    return sampleinfo
#
def build_sequencing_json(patid, source):
    ''' Builds MitoMatcher compatible Sequencing/Analysis info .json
        Input is patient id and data type (stic-surveyor, mdenis).
        Output is a json formatted hash.
    '''
    # Needed info
    technique = ''
    library = ''
    sequencer = ''
    mapper = ''
    caller = ''
    pipeline_version = ''
    analysis_date = ''
    catalog = []
    # Parsing files
    if source == 'stic-surveyor':
        sequencer = 'surveyor'
        analysis_date = '2012-4-11'
        technique = 2
        file = config.STICSURVEYORxls
        book = xlrd.open_workbook(file)
        for sheet_index in range(book.nsheets):
            sheet = book.sheet_by_index(sheet_index)
            for row in range(5,sheet.nrows):
                if patid == str(sheet.cell(rowx=row,colx=0).value).replace(" ",""):
                    for col in range(1,sheet.ncols):
                        call = str(sheet.cell(rowx=row,colx=col).value) # this detects if in the xls cell there's a he or ho
                        if call != '' :
                            # Data frame of a variant
                            variant = {
                                'chr' : 'chrM',
                                'pos' : -1,
                                'ref' : -1,
                                'alt' : -1,
                                'heteroplasmy_rate' : '',
                                'heteroplasmy_status' : '',
                                'depth' : '',
                                'quality' : ''
                            }
                            if 'ho' in call or 'HO' in call or 'hO' in call:
                                heteroplasmy_status = 'HOM'
                            elif 'he' in call or 'HE' in call or 'hE' in call or 'He' in call:
                                heteroplasmy_status = 'HET'
                            elif 'dec' in call or 'DEC' in call:
                                heteroplasmy_status = 'DEC'
                            else:
                                heteroplasmy_satus = call
                                print("Warning:", call, "call in", patid )
                            variant['heteroplasmy_status'] = heteroplasmy_status
                            # row 4 holds the variant info in the surveyor xls
                            variant['pos'] = int(str(sheet.cell(rowx=3,colx=col).value).split(" ")[0])
                            variant['ref'] = str(sheet.cell(rowx=3,colx=col).value).split(" ")[1].split(">")[0].upper()
                            variant['alt'] = str(sheet.cell(rowx=3,colx=col).value).split(" ")[1].split(">")[1].upper()
                            catalog.append(variant)

    elif source == 'stic-mitochip':
        sequencer = 'mitochip'
        analysis_date = '2012-4-11'
        technique = 1
        laboratory_nr = int(patid[0:2])
        sampling_nr = int(patid[2:5])
        mitochip_file = config.STICMITOCHIPxls+"_c"+str(laboratory_nr)+"_2012-04-11.xls"
        book = xlrd.open_workbook(mitochip_file)
        for sheet_index in range(book.nsheets):
            sheet = book.sheet_by_index(sheet_index)
            for row in range(1,sheet.nrows):
                centre = int(sheet.cell(rowx=row,colx=0).value)
                sample = int(sheet.cell(rowx=row,colx=1).value)
                if laboratory_nr == centre and sampling_nr == sample:
                    # Data frame of a variant
                    variant = {
                        'chr' : 'chrM',
                        'pos' : -1,
                        'ref' : -1,
                        'alt' : -1,
                        'heteroplasmy_rate' : '',
                        'heteroplasmy_status' : '',
                        'depth' : '',
                        'quality' : ''
                    }
                    variant['pos'] = int(sheet.cell(rowx=row,colx=5).value)
                    variant['ref'] = str(sheet.cell(rowx=row,colx=6).value).upper()
                    # First sheet carries homoplasmic variants
                    if sheet_index == 0:
                        variant['alt'] = str(sheet.cell(rowx=row,colx=7).value).upper()
                        variant['heteroplasmy_rate'] = 1
                        variant['heteroplasmy_status'] = 'HOM'
                    # Second sheet carries heteroplasmic variants
                    elif sheet_index == 1:
                        variant['heteroplasmy_status'] = 'HET'
                        major_allele = str(sheet.cell(rowx=row,colx=7).value).upper()
                        major_af = float(sheet.cell(rowx=row,colx=8).value)
                        minor_allele = str(sheet.cell(rowx=row,colx=9).value).upper()
                        minor_af = float(sheet.cell(rowx=row,colx=10).value)
                        if variant['ref'] == minor_allele:
                            variant['alt'] = major_allele
                            variant['heteroplasmy_rate'] = float(major_af/100)
                        elif variant['ref'] == major_allele:
                            variant['alt'] = minor_allele
                            variant['heteroplasmy_rate'] = float(minor_af/100)
                        else:
                            print('Warning: issue with alleles in heteroplasmic variants: PATID REF MAJAL MINAL', \
                            str(patid), str(ref), str(major_allele), str(minor_allele) )
                    catalog.append(variant)

    # Fill in frame
    sequencinginfo = {
        'Analysis' : {
            'technique' : technique,
            'library' : library,
            'sequencer' : sequencer,
            'mapper' : mapper,
            'caller' : caller,
            'pipeline_version' : pipeline_version,
            'analysis_date' : analysis_date
        },
        'Catalog' : catalog
    }

    return sequencinginfo

########
# main #
########
if __name__ == "__main__":
    print("Processing start.")
    if args.type == 'stic':
        print("\t###You requested building a MitoMatcherDB compatible .json from the:\n\t\tBannwarth et al. 2012 dataset.")
        print("\t###Good luck soldier, these treachearous file formats will be the end of us. Brace.")
        build_json()
    if args.type == 'mdenis':
        print("\t###You requested building a MitoMatcherDB compatible .json from the:\n\t\tDenis et al. 2022 dataset.")
        print("\t###Good luck and don't forget to take off your party hat before boarding the file format rollercoaster.")
        build_json()
    print("Processing ended.")
