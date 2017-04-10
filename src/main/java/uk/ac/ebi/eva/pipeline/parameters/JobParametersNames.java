/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.parameters;

import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager;

public class JobParametersNames {

    /*
     * Input
     */

    public static final String INPUT_VCF = "input.vcf";

    public static final String INPUT_VCF_ID = "input.vcf.id";

    public static final String INPUT_VCF_AGGREGATION = "input.vcf.aggregation";

    public static final String INPUT_STUDY_NAME = "input.study.name";

    public static final String INPUT_STUDY_ID = "input.study.id";

    public static final String INPUT_STUDY_TYPE = "input.study.type";

    public static final String INPUT_PEDIGREE = "input.pedigree";

    public static final String INPUT_GTF = "input.gtf";

    public static final String INPUT_FASTA = "input.fasta";

    public static final String INPUT_VCF_AGGREGATION_MAPPING_PATH = "input.vcf.aggregation.mapping-path";

    /*
     * Output
     */

    public static final String OUTPUT_DIR = "output.dir";

    public static final String OUTPUT_DIR_ANNOTATION = "output.dir.annotation";

    public static final String OUTPUT_DIR_STATISTICS = "output.dir.statistics";


    /*
     * Database infrastructure (Spring Data)
     */

    public static final String CONFIG_DB_HOSTS = "spring.data.mongodb.host";

    public static final String CONFIG_DB_AUTHENTICATIONDB = "spring.data.mongodb.authentication-database";

    public static final String CONFIG_DB_USER = "spring.data.mongodb.username";

    public static final String CONFIG_DB_PASSWORD = "spring.data.mongodb.password";

    public static final String CONFIG_DB_READPREFERENCE = "config.db.read-preference";


    /*
     * Database and collections
     */

    public static final String DB_NAME = "spring.data.mongodb.database";

    public static final String DB_COLLECTIONS_VARIANTS_NAME = "db.collections.variants.name";

    public static final String DB_COLLECTIONS_FILES_NAME = "db.collections.files.name";

    public static final String DB_COLLECTIONS_FEATURES_NAME = "db.collections.features.name";

    public static final String DB_COLLECTIONS_STATISTICS_NAME = "db.collections.stats.name";

    public static final String DB_COLLECTIONS_ANNOTATION_METADATA_NAME = "db.collections.annotation.metadata.name";

    public static final String DB_COLLECTIONS_ANNOTATIONS_NAME = "db.collections.annotations.name";


    /*
     * Skip and overwrite steps
     */

    public static final String ANNOTATION_SKIP = "annotation.skip";

    public static final String STATISTICS_SKIP = "statistics.skip";

    public static final String STATISTICS_OVERWRITE = "statistics.overwrite";


    /*
     * OpenCGA (parameters read from OpenCGA "conf" folder)
     */

    public static final String APP_OPENCGA_PATH = "app.opencga.path";

    public static final String OPENCGA_DB_NAME = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME;

    public static final String OPENCGA_DB_HOSTS = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOSTS;

    public static final String OPENCGA_DB_AUTHENTICATIONDB = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_AUTHENTICATION_DB;

    public static final String OPENCGA_DB_USER = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER;

    public static final String OPENCGA_DB_PASSWORD = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS;

    public static final String OPENCGA_DB_COLLECTIONS_VARIANTS_NAME = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS;

    public static final String OPENCGA_DB_COLLECTIONS_FILES_NAME = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES;


    /*
     * Variant Effect Predictor (VEP)
     */

    public static final String APP_VEP_PATH = "app.vep.path";

    public static final String APP_VEP_VERSION = "app.vep.version";

    public static final String APP_VEP_CACHE_PATH = "app.vep.cache.path";

    public static final String APP_VEP_CACHE_VERSION = "app.vep.cache.version";

    public static final String APP_VEP_CACHE_SPECIES = "app.vep.cache.species";

    public static final String APP_VEP_NUMFORKS = "app.vep.num-forks";

    public static final String APP_VEP_TIMEOUT = "app.vep.timeout";


    /*
     * Other configuration
     */

    public static final String CONFIG_RESTARTABILITY_ALLOW = "config.restartability.allow";

    public static final String CONFIG_CHUNK_SIZE = "config.chunk.size";


    public static final String PROPERTY_FILE_PROPERTY = "parameters.path";

    public static final String RESTART_PROPERTY = "force.restart";

}
