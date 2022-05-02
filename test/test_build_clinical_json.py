#!/usr/bin/python3
# -*- coding: utf-8 -*-
# 12/04/2022
# abodrug

# This script tests the function utilitary.build_clinical_json
#
import sys
import json
import datetime
#
sys.path.append('../app')
import config
sys.path.append(config.SOURCE)
import buildingjsons
#
import unittest

#
class TestClinicalJson(unittest.TestCase):
    #
    def setUp(self):
        super(TestClinicalJson, self).setUp()
        self.data = [
                {'patid' : '01001VET',
                 'source': 'stic-mitochip' },
                {'patid' : '01001VET',
                 'source': 'stic-surveyor' },
                {'patid' : '170065804',
                 'source' : 'retrofisher:'+config.IONTHERMO}
                ]
    #
    def test_build_clinical(self):
        ''' Building of clinical json is successful and yields expected data.
        '''
        for el in self.data:
            clinical_json = buildingjsons.build_clinical_json(el['patid'], el['source'])
            ##########
            # Clinical
            clinical = clinical_json['Clinical']
            # name of the patient should be contained in the patient_id
            # patient_id is surname, name, date of birth
            self.assertRegexpMatches(clinical['patient_id'], clinical['name'])
            # patient_id is made of two elements for stic data, and four elements
            # for retrofisher data (name, surname, year, month and day of birth)
            id_elements = clinical['patient_id'].split('-')
            if el['source'] == 'stic':
                self.assertEqual(2, len(id_elements))
            elif el['source'] == 'retrofisher':
                self.assertEqual(4, len(id_elements))
            # other elements can be null
            self.assertTrue(clinical['sex'] in ['M' , 'F', 'I', ''])
            self.assertTrue(clinical['age_of_onset'] in ['perinatal' ,'neonatal',  'congenital',
                'infantile', 'child', 'juvenile', 'teenager', 'adult', 'elderly', 'unknown', ''])
            self.assertTrue(clinical['cosanguinity'] in ['YES' , 'NO', 'unknown', ''])
            ##########
            # Ontology
            ontology = clinical_json['Ontology']
            ontologies = ['hpo', 'orpha', 'ordo']
            for onto in ontologies:
                dict = ontology[onto]
                for element in dict:
                    self.assertTrue(dict[element] in ['Present', 'Absent'])
                    ontotype, codification = element.split(':')
                    icodification = int(codification)
                    self.assertTrue(ontotype in ['HP', 'ORDO', 'MONDO'])
                    self.assertIsInstance(icodification, int)

    #
    def tearDown(self):
        super(TestClinicalJson, self).tearDown()
        self.data = []
#
class TestSampleJson(unittest.TestCase):
    #
    def setUp(self):
        super(TestSampleJson, self).setUp()
        self.data = [
                {'patid' : '01001VET',
                 'source': 'stic-mitochip' },
                {'patid' : '01001VET',
                 'source': 'stic-surveyor' },
                {'patid' : '170065804',
                 'source' : 'retrofisher:'+config.IONTHERMO}
                ]
    #
    def test_build_sample(self):
        ''' Building of sample json is succesful and yields expected data
        '''
        for el in self.data:
            sample_json = buildingjsons.build_sample_json(el['patid'], el['source'])
            #########
            # Sample
            sample = sample_json['Sample']
            self.assertTrue(sample['sample_id_in_lab']) # can't be empty
            self.assertTrue(sample['laboratory_of_sampling']) # can't be empty
            self.assertTrue(sample['date_of_sampling']) # can't be empty
            self.assertIsInstance(sample['date_of_sampling'], type(datetime.date.today())) # is datetime type
            self.assertTrue(sample['age_at_sampling'] in range(0,100) or sample['age_at_sampling'] == 'unknown')
            haplogroups = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","P","R","S","T","U","V","W","X", ""]
            try:
                h = sample['haplogroup'][0]
            except:
                h = sample['haplogroup']
            self.assertIn(h, haplogroups)
    #
    def tearDown(self):
        super(TestSampleJson, self).tearDown()
        self.data = []
#
class TestSequencingJson(unittest.TestCase):
    #
    def setUp(self):
        super(TestSequencingJson, self).setUp()
        self.data = [
                {'patid' : '01001VET',
                 'source': 'stic-mitochip' },
                {'patid' : '01001VET',
                 'source': 'stic-surveyor' },
                {'patid' : '03049DAF',
                 'source': 'stic-mitochip' },
                {'patid' : '03049DAF',
                 'source': 'stic-surveyor' },
                {'patid' : '170065804',
                 'source' : 'retrofisher:'+config.IONTHERMO}
                ]
    #
    def test_build_sequencing(self):
        ''' Test itegrity of the sequencing part of the json.
        '''
        for el in self.data:
            sequencing_json = buildingjsons.build_sequencing_json(el['patid'], el['source'])
            #print("\n\n", json.dumps(sequencing_json, indent=4, default=str), "\n\n")
            catalog = sequencing_json['Catalog']
            for variant in catalog:
                # extract info
                chr = variant['chr']
                pos = variant['pos']
                ref = variant['ref']
                alt = variant['alt']
                try:
                    heteroplasmy_rate = float(variant['heteroplasmy_rate'])
                except:
                    heteroplasmy_rate = variant['heteroplasmy_rate']
                #print(heteroplasmy_rate, type(heteroplasmy_rate))
                heteroplasmy_status = variant['heteroplasmy_status']
                depth = variant['depth']
                quality = variant['quality']
                # test info
                self.assertTrue(pos in range(0,16570))
                self.assertTrue([True for r in ref if r in ['A', 'C', 'G', 'T']])
                self.assertTrue([True for a in alt if a in ['A', 'C', 'G', 'T']])
                self.assertTrue(heteroplasmy_status in ['HOM', 'HET', ''])
                if type(heteroplasmy_rate) == str:
                    self.assertTrue(heteroplasmy_rate == '')
                if type(heteroplasmy_rate) == float:
                    self.assertTrue(heteroplasmy_rate >1 and heteroplasmy_rate <= 100)
                    # rate should be between 0 and 100, not between 1 and 0. In practice
                    # a heteroplasmy below 1 is neither detectable nor reported


    #
    def tearDown(self):
        super(TestSequencingJson, self).tearDown()
        self.data = []
    #
