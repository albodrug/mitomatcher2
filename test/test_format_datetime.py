#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 12/04/2022
# abodrug

# This script tests the function utilitary.format_datetime
#
import sys
import datetime
#
sys.path.append('../app')
import config
sys.path.append(config.SOURCE)
import utilitary
#
import unittest

class TestDateformat(unittest.TestCase):
    def setUp(self):
        return 0
    #
    def test_type(self):
        ''' Format is datetime.date
        '''
        actual = type(utilitary.format_datetime(datestring="12/07/2012", sample_id=0, style="euro"))
        expected = type(datetime.date(2012, 7, 12))
        self.assertEqual(actual, expected)
    #
    def test_style(self):
        ''' Style is euro or us
        '''
        actual_euro = utilitary.format_datetime(datestring="02/07/2012", sample_id=0, style="euro")
        actual_us = utilitary.format_datetime(datestring="07/02/2012", sample_id=0, style="us")
        expected = datetime.date(2012, 7, 2)
        self.assertEqual(actual_euro, expected)
        self.assertEqual(actual_us, expected)
    #
    def test_inputs(self):
        ''' Inputs can be different, wildly different, because extracted from manually filled xls
        '''
        days = ["01", "1", "12", "17", "03", "3"]
        months = ["01", "1", "12", "9", "09"]
        years = ["2021", "12", "22", "08"]
        for d in days:
            for m in months:
                for y in years:
                    input = d+"/"+m+"/"+y
                    actual = utilitary.format_datetime(datestring=input, sample_id=0, style="euro")
                    self.assertIs(type(actual), type(datetime.date(2012,1,1)))
    #
    def tearDown(self):
        return 0
