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
import buildingjsons
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
    -f  --xfile   : path to single xls file
    ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-t", "--type", required=True, choices=['stic','genbank', "retrofisher"])
p.add_argument("-v", "--verbose", required=False, default=False, action='store_true')
p.add_argument("-n", "--threads", required=False, type=int)
p.add_argument("-r", "--run", required=False, type=int, default=0) # run index from which to compute
p.add_argument("-d", "--debug", required=False, type=int, default=0) # debug sample ID
p.add_argument("-f", "--xfile", required=False) # path to xls file
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
# STIC DATA
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
            clinical_info = buildingjsons.build_clinical_json(cid, args.type) # functional, uses clinic data
            sample_info = buildingjsons.build_sample_json(cid, args.type) # functinal, uses clinic data
            sequencing_info = buildingjsons.build_sequencing_json(cid, args.type+'-mitochip') # to code, use mitochip xls
            mitochip_data = {**clinical_info, **sample_info, **sequencing_info}
            with open(config.PATIENTINPUTsurveyormitochip+cid+"_mitochip.json", 'w', encoding='utf-8') as f:
                json.dump(mitochip_data, f, ensure_ascii=False, indent=4, default=str)
                if args.verbose:
                    print("Wrote mitochip data to:", config.PATIENTINPUTsurveyormitochip+"mitochip_"+cid+".json")
        # Build a json if patient was sequenced with mitochip: 741 surveyor patients with clinical data
        if cid in surveyor_identifiers:
            clinical_info = buildingjsons.build_clinical_json(cid, args.type)
            sample_info = buildingjsons.build_sample_json(cid, args.type)
            sequencing_info = buildingjsons.build_sequencing_json(cid, args.type+'-surveyor')
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
    if args.xfile:
        runfolder = config.IONTHERMO
        sample_id = args.xfile
        sample_file = glob.glob(runfolder+"/**/*"+sample_id+"*.xls", recursive=True)
        for sf in sample_file:
            run = os.path.dirname(sf)
            while "_MITO" in run:
                newrun = os.path.dirname(run)
                if "_MITO" not in newrun:
                    break
                else:
                    run = newrun
            print("\n\n\n################ Processing RUN: ", run)
            recap_file = utilitary.get_recap_file(run)
            samples = utilitary.get_ionthermo_id(recap_file, "retrofisher-recap")
            for s in samples:
                if int(s['sample_id']) == int(args.xfile):
                    build_retrofisher_jsons_in_a_run(s, run)
    else:
        runfolder = config.IONTHERMO
        run_list = glob.glob(runfolder+"MITO_*/*")
        # sort list
        run_list = sorted(run_list)
        # debugging typos and new key words
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
    sample_file = glob.glob(run+"/**/*"+sample_id+"*.xls", recursive=True)
    #
    if len(sample_file) == 1:
        pass
    elif len(sample_file) == 0:
        print(inspect.stack()[0][3],utilitary.bcolors.FAIL +
                ": No files with appropriate sample id found in run : " +
                utilitary.bcolors.ENDC, sample_id, run)
        exit()
    else:
        print(inspect.stack()[0][3],": More than one file with the sample id found in run: ", sample_id, run)
    sample_file = sample_file[0]
    if args.verbose:
        print("\t\t\t#### Processing SAMPLE: ", sample_file)

    ###############
    # Build jsons
    try:
        clinical_info = buildingjsons.build_clinical_json(sample_id, args.type+":"+run)
    except:
        e = sys.exc_info()
        print(utilitary.bcolors.FAIL + "Failed to build clinical json." + utilitary.bcolors.ENDC)
        print(e)
        exit()
    try:
        sample_info = buildingjsons.build_sample_json(sample_id, args.type+":"+run)
    except:
        e = sys.exc_info()
        print(utilitary.bcolors.FAIL + "Failed to build sample json." + utilitary.bcolors.ENDC)
        print(e)
        exit()
    try:
        sequencing_info = buildingjsons.build_sequencing_json(sample_id, args.type+":"+run)
    except:
        e = sys.exc_info()
        print(utilitary.bcolors.FAIL + "Failed to build sequencing json." + utilitary.bcolors.ENDC)
        print(e)
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
