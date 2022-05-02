#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 12/04/2022
# abodrug

# This script is the runner test
#
import unittest
#
import test_format_datetime
import test_build_clinical_json

# initialize the test suite
loader = unittest.TestLoader()
suite  = unittest.TestSuite()

# add tests to the test suite
suite.addTests(loader.loadTestsFromModule(test_format_datetime))
suite.addTests(loader.loadTestsFromModule(test_build_clinical_json))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)
