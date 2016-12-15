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

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoOperations;
import uk.ac.ebi.eva.pipeline.io.writers.GeneWriter;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.net.UnknownHostException;

@Configuration
public class GeneWriterConfiguration {

    @Bean
    @StepScope
    public ItemWriter<FeatureCoordinates> geneWriter(JobOptions jobOptions) throws UnknownHostException {
        MongoOperations mongoOperations = MongoDBHelper
                .getMongoOperations(jobOptions.getDbName(), jobOptions.getMongoConnection());
        return new GeneWriter(mongoOperations, jobOptions.getDbCollectionsFeaturesName());
    }

}