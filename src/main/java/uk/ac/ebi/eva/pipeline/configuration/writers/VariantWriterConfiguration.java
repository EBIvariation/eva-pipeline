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
package uk.ac.ebi.eva.pipeline.configuration.writers;

import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.MongoOperations;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.io.writers.VariantMongoWriter;
import uk.ac.ebi.eva.pipeline.model.converters.data.VariantToMongoDbObjectConverter;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_WRITER;

@Configuration
public class VariantWriterConfiguration {

    @Bean(VARIANT_WRITER)
    @StepScope
    @Profile(Application.VARIANT_WRITER_MONGO_PROFILE)
    public ItemWriter<Variant> variantMongoWriter(JobOptions jobOptions, DatabaseParameters databaseParameters) throws
            Exception {
        MongoOperations mongoOperations = databaseParameters.getMongoOperations();
        return new VariantMongoWriter(databaseParameters.getCollectionVariantsName(),
                mongoOperations,
                variantToMongoDbObjectConverter(jobOptions));
    }

    @Bean
    @StepScope
    public VariantToMongoDbObjectConverter variantToMongoDbObjectConverter(JobOptions jobOptions) throws Exception {
        return new VariantToMongoDbObjectConverter(
                jobOptions.getVariantOptions().getBoolean(VariantStorageManager.INCLUDE_STATS),
                jobOptions.getVariantOptions().getBoolean(VariantStorageManager.CALCULATE_STATS),
                jobOptions.getVariantOptions().getBoolean(VariantStorageManager.INCLUDE_SAMPLES),
                (VariantStorageManager.IncludeSrc) jobOptions.getVariantOptions()
                        .get(VariantStorageManager.INCLUDE_SRC));
    }

}
