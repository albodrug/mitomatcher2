#!/usr/bin/python3
# -*- coding: utf-8 -*-
#02/2022
#abodrug

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
        -a  --argument  :   This is the description of the argument.
                            default "", choices "[]"
        ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-pos", "--position", nargs="+", required=False, default=["d1", "d2"])
args = p.parse_args()

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
def get_session(engine):
    ''' Get and return a sqlalchemy session.
    '''
def query_variant(seance, position, altallele):
    ''' Queries variants based on position
    '''
    from .database import seance
    from .models import Variant
    variants = seance.query(Variant).filter_by(pos=position, alt=altallele).first()
########
# main #
########
if __name__ == "__main__":
    print("Processing start.")
    engine = create_engine(config.USERADMIN, config.PWDADMIN, config.DATABASE)
    session = sqal.orm.sessionmaker()
    session.configure(bind=engine)
    seance = session()
    pos=299
    alt='T'
    query_variant(seance, pos, alt)
