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
package uk.ac.ebi.eva.pipeline.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.util.Properties;

/**
 * Common configuration used to test different job scenario
 * Test must change the property based on the job configuration and the step that it's running
 */
@Configuration
public class CommonConfiguration {
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Bean
    private static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        PropertySourcesPlaceholderConfigurer configurer = new PropertySourcesPlaceholderConfigurer();

        Properties properties = new Properties();
        properties.put(JobParametersNames.INPUT_VCF, "");
        properties.put(JobParametersNames.INPUT_VCF_ID, "1");
        properties.put(JobParametersNames.INPUT_VCF_AGGREGATION, "NONE");
        properties.put(JobParametersNames.INPUT_STUDY_TYPE, "COLLECTION");
        properties.put(JobParametersNames.INPUT_STUDY_NAME, JobParametersNames.INPUT_STUDY_NAME);
        properties.put(JobParametersNames.INPUT_STUDY_ID, "1");
        properties.put(JobParametersNames.INPUT_PEDIGREE, "");
        properties.put(JobParametersNames.INPUT_GTF, "");
        properties.put(JobParametersNames.INPUT_FASTA, "");

        properties.put(JobParametersNames.OUTPUT_DIR, "/tmp");
        properties.put(JobParametersNames.OUTPUT_DIR_ANNOTATION, "");
        properties.put(JobParametersNames.OUTPUT_DIR_STATISTICS, "/tmp");

        properties.put(JobParametersNames.STATISTICS_OVERWRITE, "false");

        properties.put(JobParametersNames.CONFIG_DB_HOSTS, "localhost:27017");
//        properties.put("dbName", null);
        properties.put(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, "variants");
        properties.put(JobParametersNames.DB_COLLECTIONS_FILES_NAME, "files");
        properties.put(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME, "features");
        properties.put(JobParametersNames.DB_COLLECTIONS_STATISTICS_NAME, "populationStatistics");
        properties.put(JobParametersNames.CONFIG_DB_READPREFERENCE, "primary");

        properties.put(JobParametersNames.APP_OPENCGA_PATH, opencgaHome);
        properties.put(JobParametersNames.APP_VEP_PATH, "");
        properties.put(JobParametersNames.APP_VEP_CACHE_PATH, "");
        properties.put(JobParametersNames.APP_VEP_CACHE_VERSION, "");
        properties.put(JobParametersNames.APP_VEP_CACHE_SPECIES, "");
        properties.put(JobParametersNames.APP_VEP_NUMFORKS, "3");

        properties.put(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, false);

        configurer.setProperties(properties);

        return configurer;
    }
}
