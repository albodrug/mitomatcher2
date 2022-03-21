17/03/2022, abodrug

## mitomatcher2 synopsis

These script compilation will enable you to recreate the MitoMatcherDB. It comes with four main scripts:

-	create_mmdb : creates the MitoMatcherDB architecture and adds metadata.
-	format_data : provides a certain number of methods to transform excel, pdf or vcf files into MitoMatcher compatible .json files
-	insert_data : inserts the standardized json files into MitoMatcher
-	query_data : provides several methods to query and vizualize data

The config file enables you to provide paths to your data folders.

Inside the src you will find most functions used by several scripts.

NB: format_data is multiprocessed
## Getting started

Before running the python scripts, you should install mysql (and a LAMP server, optionally).
You should have created mitomatcher2 and the admin user interacting with it.

mysql> create database mitomatcher2;

mysql> grant all privileges on mitomatcher2.* to mito_admin@localhost;

Now you can run:

python create_mmdb.py --createdb --verbose

python create_mmdb.py --addmeta --verbose

--createdb will create all the tables in MitoMatcher.v2 with the appropriate constraints.

--addmeta will add metadata, i.e. fill in the following columns using metadata jsons, vcfs and other files

-	Ontology
-	Laboratory
-	Technique
-	User
-	Gene
-	Annotation

Once you've obtained sample jsons using format_data.py or other means, you can insert it with insert_data.py.

The data will be inserted in the following tables in the following order:
-	Sample
-	Sample_Ontology
-	Clinic and Sample_Clinic
-	Variant and Gene_Variant
-	Variant_Call and Analysis

## .json description and how it relates to the database structure
The .json is a file PER ANALYSIS. It indicates patient data (Clinical, Ontology) and sample data (Sample).

Analysis data is indicated in the Sequencing and Catalog section.

A clinic_id can have several sample_id through Sample_Clinic.

A sample_id can have only a single clinic_id through Sample_Clinic and several ontology_id through Sample_Ontology.
