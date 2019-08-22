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
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.readers.VariantsMongoReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.AnnotationCompositeWriterConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.AnnotationWriterConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.processors.AnnotationCompositeProcessorConfiguration;
import uk.ac.ebi.eva.pipeline.io.readers.AnnotationFlatFileReader;
import uk.ac.ebi.eva.pipeline.listeners.StepProgressListener;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import java.util.List;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATION_COMPOSITE_PROCESSOR;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATION_WRITER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.COMPOSITE_ANNOTATION_VARIANT_WRITER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENERATE_VEP_ANNOTATION_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANTS_READER;

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
@Import({VariantsMongoReaderConfiguration.class, AnnotationCompositeProcessorConfiguration.class,
        AnnotationCompositeWriterConfiguration.class, ChunkSizeCompletionPolicyConfiguration.class})
public class GenerateVepAnnotationStepConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(GenerateVepAnnotationStepConfiguration.class);

    @Autowired
    @Qualifier(VARIANTS_READER)
    private ItemStreamReader<List<EnsemblVariant>> nonAnnotatedVariantsReader;

    @Autowired
    @Qualifier(ANNOTATION_COMPOSITE_PROCESSOR)
    private ItemProcessor<List<EnsemblVariant>, List<AnnotationMongo>> annotationCompositeProcessor;

    @Autowired
    @Qualifier(COMPOSITE_ANNOTATION_VARIANT_WRITER)
    private ItemWriter<List<AnnotationMongo>> annotationWriter;

    @Bean(GENERATE_VEP_ANNOTATION_STEP)
    public Step generateVepAnnotationStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions) {
        logger.debug("Building '" + GENERATE_VEP_ANNOTATION_STEP + "'");

        return stepBuilderFactory.get(GENERATE_VEP_ANNOTATION_STEP)
                .<List<EnsemblVariant>, List<AnnotationMongo>>chunk(1)
                .reader(nonAnnotatedVariantsReader)
                .processor(annotationCompositeProcessor)
                .writer(annotationWriter)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .listener(new StepProgressListener())
                .build();
    }
}
