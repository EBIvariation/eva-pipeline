/*
 * Copyright 2024 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.io.writers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.pipeline.io.writers.VariantStatsWriter;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_STATS_WRITER;

@Configuration
public class VariantStatsWriterConfiguration {

    @Bean(VARIANT_STATS_WRITER)
    @StepScope
    public ItemWriter<VariantDocument> variantStatsWriter(DatabaseParameters databaseParameters, MongoTemplate mongoTemplate) {
        return new VariantStatsWriter(databaseParameters, mongoTemplate);
    }
}
