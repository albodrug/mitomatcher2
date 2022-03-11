#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 26/01/2022
# author: abodrug, achoury

# Thus script contains paths and global variable for the MitoMatcher script suite

# passwordS AND USERS
USERADMIN = 'mito_admin'
PWDADMIN = ''
USER = 'mito_user'
PWD = ''
DATABASE = 'mitomatcher2'

# script source
SOURCE='/home/abodrug/mitomatcher2_dev/src/'

# raw data stic
STICDATA='/home/abodrug/mitomatcher2_dev/MMDB_RAWDATA/'
STICCLINICxls='/home/abodrug/mitomatcher2_dev/MMDB_RAWDATA/stic/CLINIC_BDD_PSTIC_120219.xls'
STICSURVEYORxls='/home/abodrug/mitomatcher2_dev/MMDB_RAWDATA/stic/SURVEYOR-Final.xls'
STICMITOCHIPxls='/home/abodrug/mitomatcher2_dev/MMDB_RAWDATA/stic/MITOCHIP'

# raw data retro Ion Proton and Ion 5S XL Systems
IONTHERMO='/home/abodrug/mitomatcher2_dev/MMDB_RAWDATA/thermofisher_runs/'

# MitoMatcherDB input
METADATA='/home/abodrug/mitomatcher2_dev/input/metadata/'
PATIENTINPUTsurveyormitochip='/home/abodrug/mitomatcher2_dev/input/json_surveyormitochip/'
PATIENTINPUTretrofisher='/home/abodrug/mitomatcher2_dev/input/json_retrofisher/'

# Encryption
FERNETKEY='/home/abodrug/mitomatcher2_dev/MMDB_RAWDATA/mmdb2_fernet.key'
