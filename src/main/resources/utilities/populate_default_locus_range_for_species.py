#!/usr/bin/env python3

# Copyright 2019 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import click
import configparser
import os
import re
import subprocess
import urllib.parse

from pymongo import MongoClient
from __init__ import *

DEFAULT_LOCUS_RANGE_COLLECTION_NAME = "defaultLocusRange"


pick_locus_range_query = [
        {
            "$group": {
                "_id": {
                    "chr": "$chr",
                    "start_1M_multiple": {"$trunc": {"$divide": ["$start", 1e6]}}
                },
                "COUNT(*)": {
                    "$sum": 1
                }
            }
        },
        {
            "$project": {
                "chr": "$_id.chr",
                "start_1M_multiple": "$_id.start_1M_multiple",
                "num_entries": "$COUNT(*)",
                "_id": 0
            }
        },
        {
            "$sort": {"num_entries": -1}
        },
        {
            "$group": {
                "_id": None,
                "topChrStartPick": {"$first": "$$ROOT"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "topChrStartPick": 1
            }
        }
    ]


def get_best_locus_range_for_species (species_db_handle):
    result = species_db_handle[DEFAULT_LOCUS_RANGE_COLLECTION_NAME].aggregate(pipeline=pick_locus_range_query,
                                                                              allowDiskUse=True)
    return list(result) if result else []


def get_args_from_release_properties_file(pipeline_properties_file):
    parser = configparser.ConfigParser()
    parser.optionxform = str

    with open(pipeline_properties_file, "r") as properties_file_handle:
        # Dummy section is needed because
        # ConfigParser is not clever enough to read config files without section headers
        properties_section_name = "pipeline_properties"
        properties_string = '[{0}]\n{1}'.format(properties_section_name, properties_file_handle.read())
        parser.read_string(properties_string)
        config = dict(parser.items(section=properties_section_name))
        return config


def default_locus_range_collection_exists (species_db_handle):
    if DEFAULT_LOCUS_RANGE_COLLECTION_NAME in species_db_handle.list_collection_names():
        default_locus_range_collection_handle = species_db_handle[DEFAULT_LOCUS_RANGE_COLLECTION_NAME]
        return True if default_locus_range_collection_handle.find_one() else False
    return False


def populate_default_locus_range (pipeline_properties_file, species_db_name):
    properties_file_args = get_args_from_release_properties_file(pipeline_properties_file)
    mongo_host = properties_file_args["spring.data.mongodb.host"]
    mongo_port = properties_file_args["spring.data.mongodb.port"]
    mongo_user = properties_file_args["spring.data.mongodb.username"]
    mongo_pass = properties_file_args["spring.data.mongodb.password"]
    mongo_auth = properties_file_args["spring.data.mongodb.authentication-database"]

    with MongoClient("mongodb://{0}:{1}@{2}:{3}/{4}".format(mongo_user, urllib.parse.quote_plus(mongo_pass),
                                                            mongo_host, mongo_port, mongo_auth)) as client:
        species_db_handle = client[species_db_name]
        if default_locus_range_collection_exists(species_db_handle):
            logger.warn("Species {0} already has the {1} collection"
                        .format(species_db_name, DEFAULT_LOCUS_RANGE_COLLECTION_NAME))
            return
        get_best_locus_range_for_species (species_db_handle)
        default_locus_range_collection = species_db_handle["defaultLocusRange"]
        unique_release_rs_ids_file = generate_unique_rs_ids_file()
        missing_rs_ids_file = diff_release_ids_against_mongo_rs_ids(unique_release_rs_ids_file, mongo_unique_ids_file)

        get_missing_ids_attributions(missing_rs_ids_file)

@click.option("-p", "--pipeline-properties-file", required=True)
@click.option("-s", "--species-db-name", required=True)
@click.command()
def main(pipeline_properties_file, species_db_name):
    populate_default_locus_range(pipeline_properties_file, species_db_name)


if __name__ == '__main__':
    main()
