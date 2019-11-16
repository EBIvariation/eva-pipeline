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
import urllib.parse

from pymongo import MongoClient
from __init__ import *

DEFAULT_LOCUS_RANGE_COLLECTION_NAME = "defaultLocusRange"
VARIANT_COLLECTION_NAME = "variants_2_0"
VARIANT_COLLECTION_NAME_ALT = "variants_1_2"

# This is basically the MongoDB pipeline version of
# select top 1 from
#  (select chr, start/1e6, count(*) as num_entries from variants_2_0 group by 1,2) counts order by num_entries desc
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


def get_best_locus_range_for_species(species_db_handle):
    result = list(species_db_handle[VARIANT_COLLECTION_NAME].aggregate(pipeline=pick_locus_range_query,
                                                                       allowDiskUse=True))
    result = result or list(species_db_handle[VARIANT_COLLECTION_NAME_ALT].aggregate(pipeline=pick_locus_range_query,
                                                                                     allowDiskUse=True))
    if result:
        locus_range_attributes = result[0]["topChrStartPick"]
        chromosome, start = locus_range_attributes["chr"], int(locus_range_attributes["start_1M_multiple"] * 1e6 + 1)
        end = int(start + 1e6 - 1)
        return {"chr": chromosome, "start": start, "end": end}
    return {}


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


def default_locus_range_collection_exists(species_db_handle):
    if DEFAULT_LOCUS_RANGE_COLLECTION_NAME in species_db_handle.list_collection_names():
        default_locus_range_collection_handle = species_db_handle[DEFAULT_LOCUS_RANGE_COLLECTION_NAME]
        return True if default_locus_range_collection_handle.find_one() else False
    return False


def populate_default_locus_range(pipeline_properties_file, species_db_name):
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
        best_locus_range_for_species = get_best_locus_range_for_species(species_db_handle)
        if best_locus_range_for_species:
            species_db_handle[DEFAULT_LOCUS_RANGE_COLLECTION_NAME].insert_one(best_locus_range_for_species)
        else:
            logger.error("Could not find default locus range for species {0}".format(species_db_name))


@click.option("-p", "--pipeline-properties-file", required=True)
@click.argument("species-db-names", nargs=-1, required=True)
@click.command()
def main(pipeline_properties_file, species_db_names):
    for species_db_name in species_db_names:
        populate_default_locus_range(pipeline_properties_file, species_db_name)


if __name__ == '__main__':
    main()
