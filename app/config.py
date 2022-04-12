#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 26/01/2022
# author: abodrug, achoury

# Thus script contains paths and global variable for the MitoMatcher script suite

import os

# custom
dir = os.getcwd()
dir = os.path.dirname(dir)

# passwordS AND USERS
USERADMIN = 'mito_admin'
PWDADMIN = ''
USER = 'mito_user'
PWD = ''
DATABASE = 'mitomatcher2'

# path to mitomatcher folder
USERPATH = dir + "/"

# script source
SOURCE = USERPATH + 'src/'

# raw data stic
STICDATA = USERPATH + 'MMDB_RAWDATA/'
STICCLINICxls = USERPATH + 'MMDB_RAWDATA/stic/CLINIC_BDD_PSTIC_120219.xls'
STICSURVEYORxls = USERPATH + 'MMDB_RAWDATA/stic/SURVEYOR-Final.xls'
STICMITOCHIPxls = USERPATH + 'MMDB_RAWDATA/stic/MITOCHIP'

# raw data retro Ion Proton and Ion 5S XL Systems
IONTHERMO = USERPATH + 'MMDB_RAWDATA/thermofisher_runs/'
GLIMSINFOfile = USERPATH + 'MMDB_RAWDATA/thermofisher_runs/GLIMS/GLIMS_extraction-01012008-30122021.xls'
# raw data retro Ion Proton and Ion S5 XL Systems, phenotype pdfs
PHENOPDFMODEL = USERPATH + 'MMDB_RAWDATA/thermofisher_runs_clinical_pdfs/Mitomatcher-Phenotypic-Data-Collection_V4.pdf'
PHENOPDFDIR = USERPATH + 'MMDB_RAWDATA/thermofisher_runs_clinical_pdfs/prefilled_autogenerated/'
PHENOPDFDIRcomplete = USERPATH + 'MMDB_RAWDATA/thermofisher_runs_clinical_pdfs/filledin_ontologies/'

# MitoMatcherDB input
METADATA = USERPATH + 'input/metadata/'
PATIENTINPUTsurveyormitochip = USERPATH + 'input/json_surveyormitochip/'
PATIENTINPUTretrofisher = USERPATH + 'input/json_retrofisher/'

# Encryption
FERNETKEY = USERPATH + 'MMDB_RAWDATA/mmdb2_fernet.key'
