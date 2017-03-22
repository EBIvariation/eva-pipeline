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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.readers.NonAnnotatedVariantsMongoReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.writers.VepInputFlatFileWriterConfiguration;
import uk.ac.ebi.eva.pipeline.io.writers.VepInputFlatFileWriter;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENERATE_VEP_INPUT_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.NON_ANNOTATED_VARIANTS_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_INPUT_WRITER;

/**
 * This step dumps a list of variants without annotations from mongo.
 * <p>
 * input: mongo collection with the variants.
 * output: file with the list of variants, in a format to feed VEP ({@link VepInputFlatFileWriter})
 * <p>
 * TODO:
 * - Handle the overwrite
 * - The variant list should be compressed. It is not possible to write into a zipped file with FlatFile item writer
 * see jmmut comment at https://github.com/EBIvariation/eva-v2/pull/22
 * We can create an extra step to convert the file and remove the nonp-zipped one
 * https://www.mkyong.com/java/how-to-compress-a-file-in-gzip-format/
 * https://examples.javacodegeeks.com/core-java/io/fileinputstream/compress-a-file-in-gzip-format-in-java/
 * http://www.journaldev.com/966/java-gzip-example-compress-and-decompress-file-in-gzip-format-in-java
 */
@Configuration
@EnableBatchProcessing
@Import({NonAnnotatedVariantsMongoReaderConfiguration.class, VepInputFlatFileWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class})
public class VepInputGeneratorStep {

    private static final Logger logger = LoggerFactory.getLogger(VepInputGeneratorStep.class);

    @Autowired
    @Qualifier(NON_ANNOTATED_VARIANTS_READER)
    private ItemStreamReader<VariantWrapper> reader;

    @Autowired
    @Qualifier(VEP_INPUT_WRITER)
    private ItemStreamWriter<VariantWrapper> writer;

    @Bean(GENERATE_VEP_INPUT_STEP)
    public Step generateVepInputStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions,
                                     SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        logger.debug("Building '" + GENERATE_VEP_INPUT_STEP + "'");

        return stepBuilderFactory.get(GENERATE_VEP_INPUT_STEP)
                .<VariantWrapper, VariantWrapper>chunk(chunkSizeCompletionPolicy)
                .reader(reader)
                .writer(writer)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .build();
    }
}
