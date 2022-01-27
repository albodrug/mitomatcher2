#!/usr/bin/python3
# -*- coding: utf-8 -*-
#00/00/2022
#abodrug

# This script's general description

import sys, glob, os
import argparse as ap

####################
# help description #
####################
if len(sys.argv) == 1:
    sys.argv.append("--help")
if sys.argv[1] in ["-h", "--help", "-help", "getopt", "usage"]:
    sys.exit('''
    -a  --argument  :   This is the description of the argument.
                        default "", choices "[]"
    ''')

###################
# argument parser #
###################
p = ap.ArgumentParser()
p.add_argument("-a", "--argument", nargs="+", required=False, default=["d1", "d2"])
args = p.parse_args()

#############
# functions #
#############
#
def function1 (something, args):
    ''' This is the description of the function.
        Takes in:
        Returns:
    '''
    return 0

########
# main #
########
if __name__ == "__main__":
    print("Processing start.")
