/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.readers.VcfReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.VariantWriterConfiguration;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.VariantNoAlternateFilterProcessor;
import uk.ac.ebi.eva.pipeline.listeners.SkippedItemListener;
import uk.ac.ebi.eva.pipeline.listeners.StepProgressListener;
import uk.ac.ebi.eva.pipeline.listeners.VariantLoaderStepStatisticsListener;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_VARIANTS_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_WRITER;

/**
 * Step that normalizes variants during the reading and loads them into MongoDB
 * <p>
 * Input: VCF file
 * Output: variants loaded into mongodb
 */
@Configuration
@EnableBatchProcessing
@Import({VcfReaderConfiguration.class, VariantWriterConfiguration.class, ChunkSizeCompletionPolicyConfiguration.class})
public class LoadVariantsStepConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(LoadVariantsStepConfiguration.class);

    @Autowired
    @Qualifier(VARIANT_READER)
    private ItemStreamReader<Variant> reader;

    @Autowired
    @Qualifier(VARIANT_WRITER)
    private ItemWriter<Variant> variantWriter;

    @Bean(LOAD_VARIANTS_STEP)
    public Step loadVariantsStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions,
                                 SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        logger.debug("Building '" + LOAD_VARIANTS_STEP + "'");

        return stepBuilderFactory.get(LOAD_VARIANTS_STEP)
                .<Variant, Variant>chunk(chunkSizeCompletionPolicy)
                .reader(reader)
                .processor(new VariantNoAlternateFilterProcessor())
                .writer(variantWriter)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .listener(new SkippedItemListener())
                .listener(new StepProgressListener())
                .listener(new VariantLoaderStepStatisticsListener())
                .build();
    }

}
