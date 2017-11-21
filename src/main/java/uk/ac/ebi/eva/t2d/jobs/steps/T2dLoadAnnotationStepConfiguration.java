/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.t2d.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.listeners.SkippedItemListener;
import uk.ac.ebi.eva.pipeline.listeners.StepProgressListener;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.t2d.configuration.readers.T2dVariantAnnotationReaderConfiguration;
import uk.ac.ebi.eva.t2d.configuration.writers.T2dAnnotationLoadWriterConfiguration;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_VARIANT_ANNOTATION_READER;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_ANNOTATION_LOAD_WRITER;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_LOAD_ANNOTATION_STEP;

/**
 * Step to load a annotation from a VEP output
 */
@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
@Import({T2dVariantAnnotationReaderConfiguration.class, T2dAnnotationLoadWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class})
public class T2dLoadAnnotationStepConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(T2dLoadAnnotationStepConfiguration.class);

    @Bean(T2D_LOAD_ANNOTATION_STEP)
    public Step t2dLoadAnnotationStep(
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy,
            JobOptions jobOptions,
            @Qualifier(T2D_VARIANT_ANNOTATION_READER) ItemStreamReader<T2dAnnotation> annotationReader,
            @Qualifier(T2D_ANNOTATION_LOAD_WRITER) ItemWriter<T2dAnnotation> t2dAnnotationLoadWriter) {
        logger.debug("Building '" + T2D_LOAD_ANNOTATION_STEP + "'");

        return stepBuilderFactory.get(T2D_LOAD_ANNOTATION_STEP)
                .<T2dAnnotation, T2dAnnotation>chunk(chunkSizeCompletionPolicy)
                .reader(annotationReader)
                .writer(t2dAnnotationLoadWriter)
                .faultTolerant().skipLimit(0).skip(FlatFileParseException.class)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .listener(new SkippedItemListener())
                .listener(new StepProgressListener())
                .build();
    }

}
