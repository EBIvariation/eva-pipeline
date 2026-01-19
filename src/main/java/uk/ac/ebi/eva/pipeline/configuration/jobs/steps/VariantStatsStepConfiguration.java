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
package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.readers.VariantStatsReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.VariantStatsWriterConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.processors.VariantStatsProcessorConfiguration;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_STATS_PROCESSOR;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_STATS_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_STATS_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_STATS_WRITER;


@Configuration
@EnableBatchProcessing
@Import({VariantStatsReaderConfiguration.class, VariantStatsWriterConfiguration.class,
        VariantStatsProcessorConfiguration.class, ChunkSizeCompletionPolicyConfiguration.class})
public class VariantStatsStepConfiguration {

    @Bean(VARIANT_STATS_STEP)
    public Step variantStatsStep(
            @Qualifier(VARIANT_STATS_READER) ItemStreamReader<VariantMongo> variantStatsReader,
            @Qualifier(VARIANT_STATS_PROCESSOR) ItemProcessor<VariantMongo, VariantMongo> variantStatsProcessor,
            @Qualifier(VARIANT_STATS_WRITER) ItemWriter<VariantMongo> variantStatsWriter,
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(VARIANT_STATS_STEP)
                .<VariantMongo, VariantMongo>chunk(chunkSizeCompletionPolicy)
                .reader(variantStatsReader)
                .processor(variantStatsProcessor)
                .writer(variantStatsWriter)
                .build();
        return step;
    }
}
