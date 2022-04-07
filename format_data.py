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
import inspect
import xlrd
import json
#
import config
sys.path.append(config.SOURCE)
import utilitary
#
import multiprocessing as mp
from time import time

####################
# help description #
####################
if len(sys.argv) == 1:
    sys.argv.append("--help")
if sys.argv[1] in ["-h", "--help", "-help", "getopt", "usage"]:
    sys.exit('''
    -t  --type  :   Type of data to format. Each dataset has its own formats and has to be parsed separately.
                    choices "[stic, mdenis, genbank, retrofisher]"
    -v  --verbose : Make script verbose.
    -t  --threads : Number of threads to use (for retro fisher data)
    ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-t", "--type", required=True, choices=['stic','mdenis','genbank', "retrofisher"])
p.add_argument("-v", "--verbose", required=False, default=False, action='store_true')
p.add_argument("-n", "--threads", required=False, type=int)
p.add_argument("-r", "--run", required=False, type=int, default=0) # run index from which to compute
args = p.parse_args()

#############
# functions #
#############
#
# STIC DATA
def build_json():
    ''' This is the function that builds the final json.
        Takes in: nothing
        Returns nothing but buils_json_stic write a json per sample-analyses.
    '''
    if args.type == "stic":
        build_stic_json()
    if args.type == "mdenis":
        build_mdenis_json()
    if args.type == "retrofisher":
        build_retrofisher_json()
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
                json.dump(mitochip_data, f, ensure_ascii=False, indent=4, default=str)
                if args.verbose:
                    print("Wrote mitochip data to:", config.PATIENTINPUTsurveyormitochip+"mitochip_"+cid+".json")
        # Build a json if patient was sequenced with mitochip: 741 surveyor patients with clinical data
        if cid in surveyor_identifiers:
            clinical_info = build_clinical_json(cid, args.type)
            sample_info = build_sample_json(cid, args.type)
            sequencing_info = build_sequencing_json(cid, args.type+'-surveyor')
            surveyor_data = {**clinical_info, **sample_info, **sequencing_info}
            with open(config.PATIENTINPUTsurveyormitochip+cid+"_surveyor.json", 'w', encoding='utf-8') as f:
                json.dump(surveyor_data, f, ensure_ascii=False, indent=4, default=str)
                if args.verbose:
                    print("Wrote surveyor data to: ", config.PATIENTINPUTsurveyormitochip+"surveyor_"+cid+".json")
        # NB: There are 699 patients with surveyor and mitochip and clinical data.
        # There are 772 mitochip/clinical pairs, 741 surveyor/clinical, 72 only mitochip/clinical, 42 only syrveyor/clinical
    return 0
#
# RETROFISHER DATA
def build_retrofisher_json():
    ''' This function parses the RUN folder of Ion Proton and Ion 5XL Angers Hospital
        retroadata.
        No input. Path to the files can be found in the config.py
        Returns nothing but prints a json per sample-analysis, containing clinical, technical info.
    '''
    ##################
    # Get and format identifiers: samples is an array of dicts containing ids,
    # tissue and haplorype information
    runfolder = config.IONTHERMO
    run_list = glob.glob(runfolder+"MITO_*/*")
    # sort list
    run_list = sorted(run_list)
    # debugging typos and new key words
    run_list = run_list #[30:31]
    for i in range(args.run, len(run_list)):
        run = run_list[i]
        print("\n\n\n################ Processing RUN: ", run, "(",i,")")
        #####################
        # Finding recap file
        recap_file = utilitary.get_recap_file(run)
        #######################################
        # Obtaining sample ids from recap file
        samples = utilitary.get_ionthermo_id(recap_file, "retrofisher-recap")
        #############################
        # Obtain entire patient data
        # Multithreading -> within run
        if args.threads:
            pool = mp.Pool(args.threads)
            pool.starmap(build_retrofisher_jsons_in_a_run, [(s, run) for s in samples])
            pool.close()
        else:
           for s in samples:
               build_retrofisher_jsons_in_a_run(s, run)
#
def build_retrofisher_jsons_in_a_run(s, run):
    ''' Function to be parallalized
    '''
    sample_id = str(s['sample_id'])
    sample_file = glob.glob(run+"/**/*"+sample_id+"*.xls")
    #
    if len(sample_file) == 1:
        pass
    elif len(sample_file) == 0:
        print(inspect.stack()[0][3],": No files with appropriate sample id found in run : ", sample_id, run)
        exit()
    else:
        print(inspect.stack()[0][3],": More than one file with the sample id found in run: ", sample_id, run)
    sample_file = sample_file[0]
    if args.verbose:
        print("\t\t\t#### Processing SAMPLE: ", sample_file)

    ###############
    # Build jsons
    try:
        clinical_info = build_clinical_json(sample_id, args.type+":"+run)
    except:
        e = sys.exc_info()
        print("Failed to build clinical json.")
        print(e)
        exit()
    try:
        sample_info = build_sample_json(sample_id, args.type+":"+run)
    except:
        print("Failed to build sample json.")
        exit()
    try:
        sequencing_info = build_sequencing_json(sample_id, args.type+":"+run)
    except:
        print("Failed to build sequencing json.")
        exit()

    retrofisher_data = {**clinical_info, **sample_info, **sequencing_info}

    ###############
    # Output jsons
    runterm = str(os.path.basename(os.path.normpath(run))).replace(" ", "") # get name of the run
    #
    # Create directory
    dirpath = config.PATIENTINPUTretrofisher+runterm+"/"
    isExist = os.path.exists(dirpath)
    if not isExist:
        os.makedirs(dirpath)
    #
    # Write JSONS
    with open(dirpath+str(sample_id)+"_"+runterm+".json", 'w', encoding='utf-8') as f:
        json.dump(retrofisher_data, f, ensure_ascii=False, indent=4, default=str)
        if args.verbose:
            print("\t\t\t#### Wrote retrofisher data to: ", config.PATIENTINPUTretrofisher+str(sample_id)+"_"+runterm+".json")
#
# COMMON METHODS
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
    patient_id = ''
    sex = ''
    age_of_onset = ''
    date_of_birth = ''
    cosanguinity = ''
    hpoterms = {}
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
    elif "retrofisher" in source:
        # In the RUN files in G:/, the only clinical information
        # one can find is the name and surname, and tissue
        sample_id = patid
        #
        runfolder = source.split(':')[1] # the run folder is appended to the source
        # this is a workaround in order to get the run number as several runs can
        # have the same sample_ids
        #
        file = glob.glob(runfolder + "/**/*"+sample_id+"*.xls", recursive = True)
        #
        if len(file) == 1:
            pass
        elif len(file) == 0:
            print(inspect.stack()[0][3],": No file found with appropriate sample id : ", sample_id)
            exit()
        else:
            print(inspect.stack()[0][3],": Too many samples found with sample id : ", sample_id)
            exit()
        file = file[0]
        try:
            book = xlrd.open_workbook(file)
        except xlrd.XLRDError as e:
            print("Could not open spreadsheet file: ", file)
            print("Error message: ", e)
            exit()
        sheet = book.sheet_by_index(0) # sheet containing patient info
        # name and surname is retrieved from the GLIMS file
        #nom = sheet.cell(rowx=0,colx=1).value
        #prenom = sheet.cell(rowx=1,colx=1).value
        #try:
        #    nom = nom.upper()
        #    prenom = prenom.upper()
        #except:
        #    print("Warning: name issue, can't uppercase ", sample_id)
        # optional, to later check that the G: xls name and surname
        # are the same as the glims extraction
        info = utilitary.retrieve_glims_info(sample_id, 'all')
        name = utilitary.format_patient_name(info['nom'], info['prenom'])
        # patient id is like surname-name-dob,
        patient_id = utilitary.format_patient_id(sample_id)
        sex = info['sex']
        #
        # Retrieve data from clinical pdf file of retro fisher samples
        #age_of_onset = pdfparsing.get_age_of_onset_from_phenopdf(sample_id)
        #cosanguinity = pdfparsing.get_cosanguinity_from_phenopdf(sample_id)
        #hpoterms, orpha, ordo = pdfparsing.get_ontologies_from_phenopdf(sample_id)

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
        centre = str(patid[:2])
        if centre == "01":
            data_handler = 'Sylvie Bannwarth'
            data_handler_email = 'sylvie.bannwarth@unice.fr'
        elif centre == "02":
            data_handler = 'ljonard'
            data_handler_email = 'laurence.jonard@nck.aphp.fr'
        elif centre == "03":
            data_handler = 'vprocaccio'
            data_handler_email = 'vincent.procaccio@univ-angers.fr'
        elif centre == "04":
            data_handler = 'brucheton'
            data_handler_email = 'benoit.rucheton@aphp.fr'
        elif centre == "05":
            data_handler = 'mlmartinnegrier'
            data_handler_email = 'marie-laure.martin-negrier@u-bordeaux.fr'
        elif centre == "06":
            data_handler = 'ghardy'
            data_handler_email = 'ghardy@chu-grenoble.fr'
        elif centre == "07":
            data_handler = 'pgaignard'
            data_handler_email = 'pauline.gaignard@aphp.fr'
        elif centre == "08":
            data_handler = 'adevos '
            data_handler_email = 'aurore.devos@chu-lille.fr'
        elif centre == "09":
            data_handler = 'cpagan'
            data_handler_email = 'cecile.pagan@chu-lyon.fr'
        elif centre == "10":
            data_handler = 'usertrousseau'
            data_handler_email = 'ND'
        elif centre == "11":
            data_handler = 'aslebre'
            data_handler_email = 'aslebre@chu-reims.fr'
        elif centre == "12":
            data_handler = 'abodrug'
            data_handler_email = 'alexandrina.bodrug@chu-angers.fr'
        elif centre == "13":
            data_handler = 'sallouche'
            data_handler_email = 'allouche-s@chu-caen.fr'
        else:
            print("User issue:", centre, patid)

        haplogroup = utilitary.get_stic_haplogroup(patid)
    elif 'retrofisher' in source:
        sample_id = patid
        #
        runfolder = source.split(':')[1] # the run folder is appended to the source
        # this is a workaround in order to get the run number as several runs can
        # have the same sample_ids
        #
        file = glob.glob(runfolder + "/**/*"+sample_id+"*.xls", recursive = True)
        #
        if len(file) == 1:
            pass
        elif len(file) == 0:
            print(inspect.stack()[0][3],": No file found with appropriate sample id: ", sample_id)
            exit()
        else:
            print(inspect.stack()[0][3],": Too many samples found with sample id: ", sample_id)
            exit()
        file = file[0]
        book = xlrd.open_workbook(file)
        sheet = book.sheet_by_name('Résumé  analyse')
        sample_id_in_lab = sample_id
        laboratory_of_sampling = 3
        laboratory_of_reference = 3
        # actually date of analysis, but it's roughly within two months of sampling
        try:
            info = utilitary.retrieve_glims_info(sample_id, 'all') #str(sheet.cell(rowx=36,colx=2).value).split()[0]
        except:
            print("Could not retrieve GLIMS info (2).")
            exit()
        date_of_sampling = info['date_of_sampling']
        age_at_sampling = utilitary.get_age_at_sampling(date_of_sampling, info['date_of_birth']) # neither info or function exists yet
        tissue = (sheet.cell(rowx=3,colx=1).value).lower()
        tissue = utilitary.translate_tissue(tissue, sample_id)
        type = 'index-thermo'
        data_handler = 'abodrug'
        data_handler_email = 'alexandrina.bodrug@chu-angers.fr'
        if (sheet.cell(rowx=21,colx=0).value).lower() == 'haplogroupe':
            haplogroup = sheet.cell(rowx=21,colx=1).value # sometimes patients don't have a haplogroup
            if haplogroup != "":
                haplogroup = haplogroup.replace("Haplogroup = ", "")
                haplogroup = haplogroup.split()[0]
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
        analysis_date = '2012-4-12' # surveyor is 2014-4-12, mitochip is 2012-4-11
        # for database unicity's sake: an analysis is uniq(id_sample, analysis_date, id_tech)
        # BUT in the script, i only check for uniq(id_sample, analysis_date)
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
        analysis_date = '2012-4-11' # surveyor is 2014-4-12, mitochip is 2012-4-11
        # for database unicity's sake: an analysis is uniq(id_sample, analysis_date, id_tech)
        # BUT in the script, i only check for uniq(id_sample, analysis_date)
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
    elif 'retrofisher' in source:
        # S5 XL or Proton
        if "S5" in source:
            technique = 4
        elif "Proton" in source:
            technique = 3
        #
        sample_id = patid
        #
        runfolder = source.split(':')[1] # the run folder is appended to the source
        # this is a workaround in order to get the run number as several runs can
        # have the same sample_ids
        #
        file = glob.glob(runfolder + "/**/*"+sample_id+"*.xls", recursive = True)
        #
        if len(file) == 1:
            pass
        elif len(file) == 0:
            print(inspect.stack()[0][3],": No file found with appropriate sample id: ", sample_id)
            exit()
        else:
            print(inspect.stack()[0][3],": Too many samples found with sample id: ", sample_id)
            exit()
        file = file[0]
        book = xlrd.open_workbook(file)
        try:
            sheet = book.sheet_by_name('Résumé  analyse')
        except ValueError:
            print("Issue with sheet retrieval, 'Résumé analyse'.")
            exit()
        date = utilitary.get_date_recapfile(sheet) # str(sheet.cell(rowx=36,colx=2).value).split()[0]
        analysis_date = utilitary.format_datetime(date, sample_id, 'euro')
        catalog = utilitary.get_retrofisher_catalog(file)

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
#

########
# main #
########
if __name__ == "__main__":
    print("Processing start.")
    print("Number of processors: ", mp.cpu_count())
    nbproc = 1
    if args.threads:
        nbproc = args.threads
    print("Using ", nbproc, " processors.")
    if args.type == 'stic':
        print("\t###You requested building a MitoMatcherDB compatible .json from the:\n\t\tBannwarth et al. 2012 dataset.")
        print("\t###Good luck soldier, these treachearous file formats will be the end of us. Brace.")
        build_json()
    if args.type == 'mdenis':
        print("\t###You requested building a MitoMatcherDB compatible .json from the:\n\t\tDenis et al. 2022 dataset.")
        print("\t###Good luck and don't forget to take off your party hat before boarding the file format rollercoaster.")
        build_json()
    if args.type == 'retrofisher':
        print("\t###You requested building a MitoMatcherDB compatible .json from the:\n\t\tThermo Fisher retrospective datasets.")
        print("\t###Good luck. You'll need it. It's a file format and folder organization nightmare.")
        build_json()
    print("Processing ended.")
