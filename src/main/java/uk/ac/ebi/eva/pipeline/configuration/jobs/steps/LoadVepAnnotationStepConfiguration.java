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
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.readers.AnnotationReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.AnnotationCompositeWriterConfiguration;
import uk.ac.ebi.eva.pipeline.io.readers.AnnotationFlatFileReader;
import uk.ac.ebi.eva.pipeline.io.writers.AnnotationInVariantMongoWriter;
import uk.ac.ebi.eva.pipeline.io.writers.AnnotationMongoWriter;
import uk.ac.ebi.eva.pipeline.listeners.AnnotationLoaderStepStatisticsListener;
import uk.ac.ebi.eva.pipeline.listeners.SkippedItemListener;
import uk.ac.ebi.eva.pipeline.listeners.StepProgressListener;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.COMPOSITE_ANNOTATION_VARIANT_WRITER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_VEP_ANNOTATION_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_ANNOTATION_READER;

/**
 * This step loads annotations into MongoDB.
 * <p>
 * input: file written by VEP listing annotated variants
 * output: write the annotations into a given variant MongoDB collection.
 * <p>
 * Example file content:
 * 20_60343_G/A	20:60343	A	-	-	-	intergenic_variant	-	-	-	-	-	-
 * 20_60419_A/G	20:60419	G	-	-	-	intergenic_variant	-	-	-	-	-	-
 * 20_60479_C/T	20:60479	T	-	-	-	intergenic_variant	-	-	-	-	-	rs149529999	GMAF=T:0.0018;AFR_MAF=T:0.01;AMR_MAF=T:0.0028
 * <p>
 * each line of the file is loaded with {@link AnnotationFlatFileReader} into a {@link Annotation} and then sent
 * to mongo with {@link AnnotationMongoWriter} and {@link AnnotationInVariantMongoWriter}.
 */

@Configuration
@EnableBatchProcessing
@Import({AnnotationReaderConfiguration.class, AnnotationCompositeWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class})
public class LoadVepAnnotationStepConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(LoadVepAnnotationStepConfiguration.class);

    @Autowired
    @Qualifier(VARIANT_ANNOTATION_READER)
    private ItemStreamReader<Annotation> annotationReader;

    @Autowired
    @Qualifier(COMPOSITE_ANNOTATION_VARIANT_WRITER)
    private ItemWriter<Annotation> compositeAnnotationVariantItemWriter;

    @Bean(LOAD_VEP_ANNOTATION_STEP)
    public Step loadVepAnnotationStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions,
                                      SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        logger.debug("Building '" + LOAD_VEP_ANNOTATION_STEP + "'");

        return stepBuilderFactory.get(LOAD_VEP_ANNOTATION_STEP)
                .<Annotation, Annotation>chunk(chunkSizeCompletionPolicy)
                .reader(annotationReader)
                .writer(compositeAnnotationVariantItemWriter)
                .faultTolerant().skipLimit(50).skip(FlatFileParseException.class)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .listener(new SkippedItemListener())
                .listener(new StepProgressListener())
                .listener(new AnnotationLoaderStepStatisticsListener())
                .build();
    }

}
