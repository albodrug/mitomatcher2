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
    #
    def setUp(self):
        super(TestDateformat, self).setUp()
        # odd are us, even are euro
        self.datadate = [
                        {'dstr' : '12/07/12',
                         'ddate' : datetime.date(2012, 7, 12),
                         'dstyle' : 'euro'},
                        {'dstr' : '07/12/12',
                         'ddate' : datetime.date(2012, 7, 12),
                         'dstyle' : 'us'},
                        {'dstr' : '02/07/12',
                         'ddate' : datetime.date(2012, 7, 2),
                         'dstyle' : 'euro'},
                        {'dstr' : '07/02/12',
                         'ddate' : datetime.date(2012, 7, 2),
                         'dstyle' : 'us'}
                         ]
    #
    def test_type(self):
        ''' Format is datetime.date
        '''
        for el in self.datadate:
            actual = type(utilitary.format_datetime(datestring=el['dstr'], sample_id=0, style=el['dstyle']))
            expected = type(el['ddate'])
            self.assertEqual(actual, expected)
    #
    def test_style(self):
        ''' Style is euro or us, in datadate even is euro, uneven is us
        '''
        for i in range(len(self.datadate))[::2]:
            el_eu = self.datadate[i]
            el_us = self.datadate[i+1]
            actual_euro = utilitary.format_datetime(datestring=el_eu['dstr'], sample_id=0, style=el_eu['dstyle'])
            actual_us = utilitary.format_datetime(datestring=el_us['dstr'], sample_id=0, style=el_us['dstyle'])
            expected_euro = el_eu['ddate']
            expected_us = el_us['ddate']
            self.assertEqual(expected_euro, expected_us)
            self.assertEqual(actual_euro, expected_us)
            self.assertEqual(actual_us, expected_euro)

    #
    def test_inputs(self):
        ''' Inputs can be different, because extracted from manually filled xls
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
        super(TestDateformat, self).tearDown()
        self.datadate = []
