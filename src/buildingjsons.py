#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 01/26/2022
# author: abodrug

# This script contains functions that are broadly used by main MitoMatcher scripts.
# Not intended to run as stand alone
# Contains functions building different postions of the data json to be insterted
# into MitoMatcher
import os
import re
import sys
import glob
import pymysql
import xlrd
import datetime
import inspect
#
import config
import utilitary
#

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
    if 'stic' in source :
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
    if 'stic' in source:
        sample_id_in_lab = "STIC-"+patid
        laboratory_of_sampling = int(patid[0:2])
        laboratory_of_reference = int(patid[0:2])
        date_of_sampling = datetime.date(2012, 4, 11)
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
            print(inspect.stack()[0][3],utilitary.bcolors.FAIL + ": No file found with appropriate sample id: ",
            sample_id +  utilitary.bcolors.ENDC)
            exit()
        else:
            print(inspect.stack()[0][3],utilitary.bcolors.FAIL + ": Too many samples found with sample id: ",
            sample_id +  utilitary.bcolors.ENDC)
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
        for r in range(19, 35):
            if (sheet.cell(rowx=r,colx=0).value).lower() == 'haplogroupe':
                try:
                    haplogroup = sheet.cell(rowx=r,colx=1).value # sometimes patients don't have a haplogroup
                except:
                    print(utilitary.bcolors.WARNING + "No haplogroup found for: " + utilitary.bcolors.ENDC, sample_id_in_lab)
                if haplogroup != "" and haplogroup != ".":
                    haplogroup = haplogroup.replace("Haplogroup = ", "")
                    haplogroup = haplogroup.split()[0]
                    break
                else:
                    haplogroup = ''
    # Filling in frame
    try:
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
    except:
        print(utilitary.bcolors.FAIL + "Could not build sample info : " + utilitary.bcolors.ENDC, sample_id_in_lab)

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
        analysis_date = datetime.date(2012, 4, 12) # surveyor is 2014-4-12, mitochip is 2012-4-11
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
                            heteroplasmy_status = ''
                            heteroplasmy_rate = ''
                            if 'ho' in call or 'HO' in call or 'hO' in call:
                                heteroplasmy_status = 'HOM'
                                heteroplasmy_rate = '100'
                            elif 'he' in call or 'HE' in call or 'hE' in call or 'He' in call:
                                heteroplasmy_status = 'HET'
                            elif 'dec' in call or 'DEC' in call:
                                heteroplasmy_status = 'HET'
                            else:
                                heteroplasmy_status = call
                                print("Warning:", call, "call in", patid )
                            variant['heteroplasmy_status'] = heteroplasmy_status
                            variant['heteroplasmy_rate'] = heteroplasmy_rate
                            # row 4 holds the variant info in the surveyor xls
                            variant['pos'] = int(str(sheet.cell(rowx=3,colx=col).value).split(" ")[0])
                            variant['ref'] = str(sheet.cell(rowx=3,colx=col).value).split(" ")[1].split(">")[0].upper()
                            variant['alt'] = str(sheet.cell(rowx=3,colx=col).value).split(" ")[1].split(">")[1].upper()
                            catalog.append(variant)

    elif source == 'stic-mitochip':
        sequencer = 'mitochip'
        analysis_date = datetime.date(2012, 4, 12) # surveyor is 2014-4-12, mitochip is 2012-4-11
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
                        variant['heteroplasmy_rate'] = 100
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
                            variant['heteroplasmy_rate'] = round(major_af, 2)
                        elif variant['ref'] == major_allele:
                            variant['alt'] = minor_allele
                            variant['heteroplasmy_rate'] = round(minor_af, 2)
                        else:
                            print('Warning: issue with alleles in heteroplasmic variants: PATID REF MAJAL MINAL', \
                            str(patid), str(ref), str(major_allele), str(minor_allele) )
                    catalog.append(variant)
    elif 'retrofisher' in source:
        sample_id = patid
        #
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
        # S5 XL or Proton
        if "S5" in file:
            technique = 4
        elif "Proton" in file:
            technique = 3
        #
        book = xlrd.open_workbook(file)
        try:
            sheet = book.sheet_by_name('Résumé  analyse')
        except ValueError:
            print("Issue with sheet retrieval, 'Résumé analyse'.")
            exit()
        try:
            date = utilitary.get_date_recapfile(sheet) # str(sheet.cell(rowx=36,colx=2).value).split()[0]
            #print("Date extracted.")
        except:
            print("Could not get date from recap file.")
            exit()
        try:
            analysis_date = utilitary.format_datetime(date, sample_id, 'euro')
            #print("Date formatted.")
        except:
            print("Could format date.")
            exit()
        try:
            catalog = utilitary.get_retrofisher_catalog(file)
            #print("Catalog retrieved.")
        except:
            print("Could not retrieve variant catalog.")
            exit()

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
