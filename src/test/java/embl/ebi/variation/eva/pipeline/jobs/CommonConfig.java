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
package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.pipeline.steps.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import java.util.Properties;

/**
 * @author Diego Poggioli
 * @author Cristina Yenyxe Gonzalez
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
        properties.put("input.vcf", "");
        properties.put("input.vcf.id", "1");
        properties.put("input.vcf.aggregation", "NONE");
        properties.put("input.study.type", "COLLECTION");
        properties.put("input.study.name", "input.study.name");
        properties.put("input.study.id", "1");
        properties.put("input.pedigree", "");
        properties.put("input.fasta", "");
        
        properties.put("output.dir", "/tmp");
        properties.put("output.dir.annotation", "");
        
        properties.put("statistics.overwrite", "false");
        
        properties.put("dbHosts", "localhost:27017");
        properties.put("dbName", "");
        properties.put("dbCollectionVariantsName", "variants");
        properties.put("dbCollectionFilesName", "files");
        properties.put("config.db.read-preference", "primary");
        
        properties.put("app.opencga.path", opencgaHome);
        properties.put("app.vep.path", "");
        properties.put("app.vep.cache.path", "");
        properties.put("app.vep.cache.version", "");
        properties.put("app.vep.cache.species", "");
        properties.put("app.vep.num-forks", "3");
        
        properties.put(VariantsLoad.SKIP_LOAD, "true");
        properties.put(VariantsStatsCreate.SKIP_STATS_CREATE, "true");
        properties.put(VariantsStatsLoad.SKIP_STATS_LOAD, "true");
        properties.put(VariantsAnnotCreate.SKIP_ANNOT_CREATE, "true");
        properties.put("config.restartability.allow", false);

        configurer.setProperties(properties);

        return configurer;
    }
}
