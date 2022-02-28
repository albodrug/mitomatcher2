#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 02/2022
# abodrug

# This script's makes queries in MitoMatcherDB

import sys, glob, os
import argparse as ap
#
import pymysql
import sqlalchemy as sqal
from sqlalchemy.ext.declarative import declarative_base

#
import config
sys.path.append(config.SOURCE)
import utilitary

####################
# help description #
####################
for el in sys.argv:
    if el in ["-h", "--help", "-help", "getopt", "usage"]:
        sys.exit('''
        -v  --verbose  :   Make the script verbose.
                        default "False", type bool.
        ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-v", "--verbose", required=False, default=False, action='store_true', help="verbose log")
args = p.parse_args()

###########
# Classes #
###########
#
Base = declarative_base()
database = config.DATABASE
#
class Variant(Base):

    __tablename__ = 'Variant'

    id_variant = sqal.Column(sqal.Integer, primary_key=True)
    chr = sqal.Column(sqal.String(5))
    pos = sqal.Column(sqal.Integer)
    ref = sqal.Column(sqal.String(50))
    alt = sqal.Column(sqal.String(50))
#
#############
# functions #
#############
#
def create_engine (user, password, database):
    ''' Creates sqlalchemy engine.
        Takes in user, password.
        Outputs engine.
    '''
    engine = sqal.create_engine(
        'mysql+pymysql://' +
        user + ':' + password + '@localhost:3306/' + database,
        echo=True,
        pool_size=10,
        connect_args={'connect_timeout': 300},
        execution_options={"timeout": 300,
                           "statement_timeout": 300,
                           "query_timeout": 300,
                           "execution_timeout": 300})
    return engine
#
def get_session(engine):
    ''' Get and return a sqlalchemy session.
    '''
    Session = sqal.orm.sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    return session
#
def query_variant(session, position, altallele, database):
    ''' Queries variants based on position
    '''
    # Does the variant exist?

    # How many samples carry this variant?

    # What are the haplogroups of the samples carrying this variant?

    # What are the HPOs of the samples carrying this variant?
    result = session.query(Variant.id_variant)
    result = result.add_columns(Variant.chr, Variant.pos, Variant.ref, Variant.alt)
    for record in result:
        print(record.pos, record.ref, record.alt)
    return 0
#
########
# main #
########
if __name__ == "__main__":
    # Creating engine and session
    if args.verbose:
        print("Processing start.")
    engine = create_engine(config.USERADMIN, config.PWDADMIN, config.DATABASE)
    session = get_session(engine)
    if args.verbose:
        print("Engine created. Session engaged.")
        print("Query starts.")
    # Querying
    pos=299
    alt='T'
    query_variant(session, pos, alt, config.DATABASE)
