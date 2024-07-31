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
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.readers.StatsVariantsReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.StatsVariantsWriterConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.processors.StatsVariantsProcessorConfiguration;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_STEP_NEW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.STATS_VARIANTS_PROCESSOR;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.STATS_VARIANTS_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.STATS_VARIANTS_WRITER;


@Configuration
@EnableBatchProcessing
@Import({StatsVariantsReaderConfiguration.class, StatsVariantsWriterConfiguration.class,
        StatsVariantsProcessorConfiguration.class, ChunkSizeCompletionPolicyConfiguration.class})
public class CalculateStatisticsStepConfigurationNew {

    @Bean(CALCULATE_STATISTICS_STEP_NEW)
    public Step calculateStatisticsStep(
            @Qualifier(STATS_VARIANTS_READER) ItemStreamReader<VariantDocument> variantReader,
            @Qualifier(STATS_VARIANTS_PROCESSOR) ItemProcessor<VariantDocument, VariantDocument> variantProcessor,
            @Qualifier(STATS_VARIANTS_WRITER) ItemWriter<VariantDocument> variantWriter,
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(CALCULATE_STATISTICS_STEP_NEW)
                .<VariantDocument, VariantDocument>chunk(chunkSizeCompletionPolicy)
                .reader(variantReader)
                .processor(variantProcessor)
                .writer(variantWriter)
                .build();
        return step;
    }
}
