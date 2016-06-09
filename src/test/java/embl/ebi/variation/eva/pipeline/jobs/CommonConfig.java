/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
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
package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.pipeline.steps.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import java.util.Properties;

/**
 * @author Diego Poggioli
 *
 * Common configuration used to test different job scenario
 * Test must change the property based on the job configuration and the step that it's running
 */
@Configuration
public class CommonConfig {
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Bean
    static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        PropertySourcesPlaceholderConfigurer configurer = new PropertySourcesPlaceholderConfigurer();

        Properties properties = new Properties();
        properties.put("input", "");
        properties.put("overwriteStats", "false");
        properties.put("calculateStats", "false");
        properties.put("outputDir", "/tmp");
        properties.put("dbName", "");
        properties.put("compressExtension", ".gz");
        properties.put("compressGenotypes", "true");
        properties.put("includeSrc", "FIRST_8_COLUMNS");
        properties.put("pedigree", "FIRST_8_COLUMNS");
        properties.put("annotate", "false");
        properties.put("includeSamples", "false");
        properties.put("includeStats", "false");
        properties.put("aggregated", "NONE");
        properties.put("studyType", "COLLECTION");
        properties.put("studyName", "studyName");
        properties.put("studyId", "1");
        properties.put("fileId", "1");
        properties.put("opencga.app.home", opencgaHome);
        properties.put(VariantsLoad.SKIP_LOAD, "true");
        properties.put(VariantsStatsCreate.SKIP_STATS_CREATE, "true");
        properties.put(VariantsStatsLoad.SKIP_STATS_LOAD, "true");
        properties.put(VariantsAnnotGenerateInput.SKIP_ANNOT_GENERATE_INPUT, "true");
        properties.put(VariantsAnnotCreate.SKIP_ANNOT_CREATE, "true");
        properties.put(VariantsAnnotLoad.SKIP_ANNOT_LOAD, "true");
        properties.put("vepInput", "");
        properties.put("vepOutput", "");
        properties.put("vepPath", "");
        properties.put("vepCacheDirectory", "");
        properties.put("vepCacheVersion", "");
        properties.put("vepSpecies", "");
        properties.put("vepFasta", "");
        properties.put("vepNumForks", "3");

        configurer.setProperties(properties);

        return configurer;
    }
}
