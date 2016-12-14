/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.configuration.VariantWriterConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.VcfReaderConfiguration;
import uk.ac.ebi.eva.pipeline.listeners.SkippedItemListener;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

/**
 * Step that normalizes variants during the reading and loads them into MongoDB
 * <p>
 * Input: VCF file
 * Output: variants loaded into mongodb
 */
@Configuration
@EnableBatchProcessing
@Import({VariantWriterConfiguration.class, JobOptions.class, VcfReaderConfiguration.class})
public class VariantLoaderStep {
    private static final Logger logger = LoggerFactory.getLogger(VariantLoaderStep.class);

    public static final String NAME_LOAD_VARIANTS_STEP = "load-variants-step";

    @Autowired
    private JobOptions jobOptions;

    @Autowired
    private ItemStreamReader<Variant> reader;

    @Autowired
    private ItemWriter<Variant> variantWriter;

    @Bean(NAME_LOAD_VARIANTS_STEP)
    public Step loadVariantsStep(StepBuilderFactory stepBuilderFactory) throws Exception {
        logger.debug("Building '" + NAME_LOAD_VARIANTS_STEP + "'");

        return stepBuilderFactory.get(NAME_LOAD_VARIANTS_STEP)
                .<Variant, Variant>chunk(jobOptions.getPipelineOptions().getInt(JobParametersNames.CONFIG_CHUNK_SIZE))
                .reader(reader)
                .writer(variantWriter)
                .faultTolerant().skipLimit(50).skip(FlatFileParseException.class)
                .listener(new SkippedItemListener())
                .build();
    }

}
