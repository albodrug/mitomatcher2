#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 01/2022
# authors: abodrug, achoury


# This script creates the MitoMatcherDB architecture and permits the
# import of some metadata (Users, Technique, Laboratory, Ontology)
# Ontologies are linked to files online (Phenotero): https://phenotero.github.io/data_json/

#
import sys, glob, os
import argparse as ap
import getpass
import json
import urllib.request
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
    -c  --createdb  :  Create the MitoMatcherDB architecure.
                        default "False", type bool
    -m  --addmeta  :   Add metadata in MitoMatcher, such as Lab, User and Technique information.
                        default "False", type bool.
    -u  --update  :    Updates metadata in MitoMatcher, such as Lab, User and Technique information.
                        default "False", type bool.
    -v  --verbose  :   Make the script verbose.
                        default "False", type bool.
    ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-c", "--createdb", required=False, default=False, action='store_true', help="create architecture")
p.add_argument("-m", "--addmeta", required=False, default=False, action='store_true', help="add metadata")
p.add_argument("-u", "--update", required=False, default=False, action='store_true', help="updates metadata")
p.add_argument("-v", "--verbose", required=False, default=False, action='store_true', help="verbose log")
args = p.parse_args()

#############
# functions #
#############
#
def create_db (database):
    ''' Creates the MitoMatcher architecture.
        Takes in: database
        Returns: database
    '''
    create_variant_tables(database)
    create_sample_tables(database)
    create_analysis_tables(database)
    return database
#
def add_dbmetadata(database):
    '''Adds database metadata.
       Takes in: database
       Returns: database
    '''
    cursor = database.cursor()
    # Add laboratories, users, technique
    file = open(config.METADATA+"LaboratoriesUsers.json")
    metadata = json.load(file)
    ######################################
    # Add Laboratories.
    laboratories = metadata['Laboratory']
    for labname in laboratories:
        # gather data in variables
        labindex = laboratories[labname]
        # build sql command
        insertion = (" INSERT INTO Laboratory \
        (id_labo, name) \
        VALUES ("+ \
        ",".join([str(labindex), '"'+str(labname)+'"']) \
        +");")
        # execute sql command
        utilitary.executesqlmetadatainsertion(insertion, cursor)
        # commit sql change, so it actually appears in the database
        database.commit()
    ######################################
    # Add Users
    users = metadata['User']
    for user in users:
        # gather data in variables
        username = user
        info = users[user]
        usermail = info[0]
        userhospital = info[1]
        # build sql command
        insertion = (" INSERT INTO User \
        (id_user, mail, id_labo) \
        VALUES ("+ \
        ",".join(['"'+str(username)+'"', '"'+str(usermail)+'"', str(laboratories[userhospital])]) \
        +");")
        # execute sql command
        utilitary.executesqlmetadatainsertion(insertion, cursor)
        # commit sql change, so it actually appears in the database
        database.commit()

    ######################################
    # Add Techniques
    techniques = metadata['Technique']
    for tech in techniques:
        # gather data in variables
        sequencer = tech
        info = techniques[tech]
        library = info['library']
        sequencer = info['sequencer']
        mapper = info['mapper']
        pipeline_version = info['pipeline_version']
        caller = info['caller']
        index = info['index']
        # build sql command
        insertion = (" INSERT INTO Technique \
        (id_tech, library, sequencer, mapper, caller, pipeline_version) \
        VALUES ("+ \
        ",".join(['"'+str(index)+'"', '"'+str(library)+'"', '"'+str(sequencer)+'"', '"'+str(mapper)+'"', '"'+str(caller)+'"', '"'+str(pipeline_version)+'"']) \
        +");")
        # execute sql command
        utilitary.executesqlmetadatainsertion(insertion,cursor)
        # commit sql change, so it actually appears in the database
        database.commit()

    ######################################
    # Add Ontologies
    ######
    # HPO
    file = open(config.METADATA+"mmdb_HPO.list")
    hpoarr = file.read().split('\n') # this is an array containing HPOs appearing in MitoMatcher
    hpourl = urllib.request.urlopen('https://phenotero.github.io/json/hp.obo.json')
    hpojson = json.loads(hpourl.read().decode())
    # gather data in variables
    for element in hpojson:
        if element['container-title'] in hpoarr:
            id_ontologyterm = element['container-title']
            name = element['title']
            type = "HPO "+element['type']
            definition = []
            author = element['author']
            for d in author:
                definition.append(d['family'])
            strdefinition = "; ".join(definition)
            # build sql command
            insertion = (" INSERT INTO Ontology \
            (id_ontologyterm, name, definition, type) \
            VALUES ("+ \
            ",".join(['"'+str(id_ontologyterm)+'"', '"'+str(name)+'"', '"'+str(strdefinition)+'"', '"'+str(type)+'"']) \
            +");")
            # execute command
            utilitary.executesqlmetadatainsertion(insertion,cursor)
            # commit sql change, so it actually appears in the database
            database.commit()
    ########
    # MONDO
    mondoarr = []
    with open(config.METADATA+"mmdb_MONDO.list", 'r') as f:
        for line in f:
            arr = line.strip().split() # two element array, first is ORPHA, second is MONDO
            mondoarr.append(arr[1]) # append MONDO terms
    mondourl = urllib.request.urlopen('https://phenotero.github.io/json/mondo.obo.json')
    mondojson = json.loads(mondourl.read().decode())
    # gather data in variables
    for element in mondojson:
        if element['container-title'] in mondoarr:
            id_ontologyterm = element['container-title']
            name = element['title']
            type = "MONDO "+element['type']
            definition = []
            author = element['author']
            for d in author:
                definition.append(d['family'])
            strdefinition = "; ".join(definition)
            # build sql command
            insertion = (" INSERT INTO Ontology \
            (id_ontologyterm, name, definition, type) \
            VALUES ("+ \
            ",".join(['"'+str(id_ontologyterm)+'"', '"'+str(name)+'"', '"'+str(strdefinition)+'"', '"'+str(type)+'"']) \
            +");")
            # execute command
            utilitary.executesqlmetadatainsertion(insertion,cursor)
            # commit sql change, so it actually appears in the database
            database.commit()

    return 0
#
def create_variant_tables(database):
    '''Creates variant related tables in MitoMatcherDB.
       Tables: [Variant], all variants per positions
       [Annotations], all annotations per variant
       [Gene], genes
       [Gene_Variant], association between variant and gene when a variant is within a gene
    '''
    cursor = database.cursor()
    # Variant Table, id_variant PK, no FK
    instruction = ("CREATE TABLE Variant \
    (id_variant INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
    chr VARCHAR(5), \
    pos INT, \
    ref VARCHAR(50), \
    alt VARCHAR(50), \
    UNIQUE(chr, pos, ref, alt) \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Gene Table, id_gene PK, no FK
    instruction =("CREATE TABLE Gene \
    (id_gene INT PRIMARY KEY NOT NULL, \
    name VARCHAR(25) \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Annotation Table, id_annotation PK, id_variant FK
    instruction = ("CREATE TABLE Annotation \
    (id_annotation INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
    score_apogee FLOAT, \
    score_mitotip FLOAT, \
    frequency_genbank FLOAT, \
    frequency_gnomAD FLOAT, \
    frequency_helix FLOAT, \
    helix_haplogroup_list VARCHAR(1000), \
    id_variant INT, \
    CONSTRAINT fk_id_variant_annotation FOREIGN KEY (id_variant) REFERENCES Variant(id_variant) ON UPDATE CASCADE \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Gene_Variant Table, id_gene_variant PK, id_gene FK, id_variant FK
    instruction = ("CREATE TABLE Gene_Variant (\
    id_gene_variant INT PRIMARY KEY NOT NULL AUTO_INCREMENT,\
    id_gene INT,\
    CONSTRAINT fk_id_gene_gene_variant FOREIGN KEY (id_gene) REFERENCES Gene(id_gene) ON UPDATE CASCADE,\
    id_variant INT,\
    CONSTRAINT fk_id_variant_gene_variant FOREIGN KEY (id_variant) REFERENCES Variant(id_variant) ON UPDATE CASCADE \
    )ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Tab Table
    #instruction = ("CREATE TABLE Tab () ENGINE=INNODB;")
    #utilitary.executesqlinstruction(instruction, cursor)
    return 0
#
def create_sample_tables(database):
    '''Creates Sample related tables.
    [Laboratory],
    [Sample],
    [Sample_Ontology],
    [Ontology],
    [Sample_Clinic],
    [Clinic],
    '''
    cursor = database.cursor()
    # Laboratory Table
    instruction = ("CREATE TABLE Laboratory (\
    id_labo INT PRIMARY KEY NOT NULL, \
    name VARCHAR(100), \
    UNIQUE(name) \
    )ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Sample Table, id_sample PK, id_labo FK
    instruction = ("CREATE TABLE Sample (\
    id_sample INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
    id_sample_in_lab VARCHAR(50) NOT NULL, \
    tissue VARCHAR(25) NOT NULL, \
    haplogroup VARCHAR(50), \
    id_labo INT, \
    sample_date DATE, \
    UNIQUE(id_sample_in_lab, tissue, sample_date), \
    CONSTRAINT fk_id_labo_laboratory FOREIGN KEY (id_labo) REFERENCES Laboratory(id_labo) ON UPDATE CASCADE, \
    type VARCHAR(25) \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Ontology Table, PK id_ontology
    instruction = ("CREATE TABLE Ontology ( \
    id_ontology INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
    id_ontologyterm VARCHAR(20) UNIQUE NOT NULL, \
    name VARCHAR(100) NOT NULL, \
    definition VARCHAR(1000), \
    comment VARCHAR(200), \
    type VARCHAR(30) NOT NULL \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Sample_Ontology Table, PK id_sample_ontology, FK id_ontology, FK id_sample
    instruction = ("CREATE TABLE Sample_Ontology ( \
    id_ontology_sample INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
    id_ontology INT NOT NULL, \
    CONSTRAINT fk_id_orpha_sample_ontology FOREIGN KEY (id_ontology) REFERENCES Ontology(id_ontology) ON UPDATE CASCADE, \
    id_sample INT NOT NULL, \
    UNIQUE(id_ontology, id_sample), \
    CONSTRAINT fk_id_sample_sample_ontology FOREIGN KEY (id_sample) REFERENCES Sample(id_sample) ON UPDATE CASCADE, \
    annot VARCHAR(50) NOT NULL \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Clinic Table
    instruction = ("CREATE TABLE Clinic ( \
    id_patient INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
    id_patient_in_lab VARCHAR(125) UNIQUE, \
    sex VARCHAR(10), \
    age INT, \
    age_of_onset VARCHAR(50), \
    cosanguinity VARCHAR(10) \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Sample_Clinic Table
    instruction = ("CREATE TABLE Sample_Clinic ( \
    id_patient_sample INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
    id_patient INT NOT NULL, \
    CONSTRAINT fk_id_patient_sample_clinic FOREIGN KEY (id_patient) REFERENCES Clinic(id_patient) ON UPDATE CASCADE, \
    id_sample INT NOT NULL, \
    UNIQUE(id_patient, id_sample), \
    CONSTRAINT fk_id_sample_sample_clinic FOREIGN KEY (id_sample) REFERENCES Sample(id_sample) ON UPDATE CASCADE \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    return 0
#
def create_analysis_tables(database):
    '''Creates Analysis related tables.
    [Analysis],
    [Technique],
    [Variant_Call],
    [User],
    [Laboratory],
    '''
    cursor = database.cursor()
    # Technique Table
    instruction = ("CREATE TABLE Technique ( \
    id_tech INT PRIMARY KEY NOT NULL, \
    library VARCHAR(25), \
    sequencer VARCHAR(25), \
    mapper VARCHAR(25), \
    caller VARCHAR(25), \
    pipeline_version VARCHAR(25) \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Analysis Table
    instruction = ("CREATE TABLE Analysis ( \
    id_analysis INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
    date_analysis DATE, \
    id_sample INT, \
    CONSTRAINT fk_id_sample FOREIGN KEY (id_sample) REFERENCES Sample(id_sample) ON UPDATE CASCADE, \
    id_tech INT, \
    CONSTRAINT fk_id_tech FOREIGN KEY (id_tech) REFERENCES Technique(id_tech) ON UPDATE CASCADE \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # User Table
    instruction = ("CREATE TABLE User ( \
    id_user VARCHAR(25) PRIMARY KEY NOT NULL, \
    id_labo INT, \
    mail VARCHAR(100) UNIQUE, \
    phone INT, \
    CONSTRAINT fk_user_labo FOREIGN KEY (id_labo) REFERENCES Laboratory(id_labo) ON UPDATE CASCADE \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)
    # Variant_Call Table
    instruction = ("CREATE TABLE Variant_Call ( \
    id_call INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
    quality FLOAT, \
    heteroplasmy_rate FLOAT, \
    heteroplasmy VARCHAR(3), \
    depth INT, \
    id_user VARCHAR(25), \
    CONSTRAINT fk_user FOREIGN KEY (id_user) REFERENCES User(id_user), \
    id_analysis INT, \
    CONSTRAINT fk_analysis FOREIGN KEY (id_analysis) REFERENCES Analysis(id_analysis) ON UPDATE CASCADE, \
    id_variant INT, \
    CONSTRAINT fk_variant FOREIGN KEY (id_variant) REFERENCES Variant(id_variant) ON UPDATE CASCADE \
    ) ENGINE=INNODB;")
    utilitary.executesqlinstruction(instruction, cursor)

    return 0

########
# main #
########
if __name__ == "__main__":
    if args.createdb :
        print("Database creation initiated. Put on your safety gear and brace.")
        password = 'Mimas' #getpass.getpass()
        database = utilitary.connect2databse(str(password))
        create_db(database)
    if args.addmeta :
        print("Add metadata (Users, Techniques, Laboratories, Ontologies). Get ready for the ride.")
        password = 'Mimas' #getpass.getpass()
        database = utilitary.connect2databse(str(password))
        add_dbmetadata(database)
