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

import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.readers.VariantsMongoReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.VepAnnotationFileWriterConfiguration;
import uk.ac.ebi.eva.pipeline.io.readers.AnnotationFlatFileReader;
import uk.ac.ebi.eva.pipeline.listeners.StepProgressListener;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENERATE_VEP_ANNOTATION_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANTS_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_WRITER;

/**
 * This step creates a file with variant annotations.
 * <p>
 * Input: mongo collection with the variants. Only non-annotated variants will be retrieved.
 * <p>
 * Output: file with the list of annotated variants, in a format written by VEP, readable with
 * {@link AnnotationFlatFileReader}
 */
@Configuration
@EnableBatchProcessing
@Import({VariantsMongoReaderConfiguration.class, VepAnnotationFileWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class})
public class GenerateVepAnnotationStepConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(GenerateVepAnnotationStepConfiguration.class);

    @Autowired
    @Qualifier(VARIANTS_READER)
    private ItemStreamReader<EnsemblVariant> nonAnnotatedVariantsReader;

    @Autowired
    @Qualifier(VEP_ANNOTATION_WRITER)
    private ItemWriter<EnsemblVariant> vepAnnotationWriter;

    @Bean(GENERATE_VEP_ANNOTATION_STEP)
    public Step generateVepAnnotationStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions,
            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        logger.debug("Building '" + GENERATE_VEP_ANNOTATION_STEP + "'");

        return stepBuilderFactory.get(GENERATE_VEP_ANNOTATION_STEP)
                .<EnsemblVariant, EnsemblVariant>chunk(chunkSizeCompletionPolicy)
                .reader(nonAnnotatedVariantsReader)
                .writer(vepAnnotationWriter)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .listener(new StepProgressListener())
                .build();
    }
}
