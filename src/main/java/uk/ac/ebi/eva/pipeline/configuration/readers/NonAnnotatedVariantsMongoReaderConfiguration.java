/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.readers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.io.readers.NonAnnotatedVariantsMongoReader;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import java.net.UnknownHostException;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.NON_ANNOTATED_VARIANTS_READER;

/**
 * Configuration to inject a NonannotatedVariants bean that reads from a mongo database in the pipeline
 */
@Configuration
@Import({ MongoConfiguration.class })
public class NonAnnotatedVariantsMongoReaderConfiguration {

    @Autowired
    private MongoConfiguration mongoConfiguration;

    @Bean(NON_ANNOTATED_VARIANTS_READER)
    @StepScope
    public NonAnnotatedVariantsMongoReader nonAnnotatedVariantsMongoReader(JobOptions jobOptions)
            throws UnknownHostException {
        MongoOperations mongoOperations = mongoConfiguration.getMongoOperations(
                jobOptions.getDbName(), jobOptions.getMongoConnection());
        return new NonAnnotatedVariantsMongoReader(mongoOperations, jobOptions.getDbCollectionsVariantsName());
    }

}
