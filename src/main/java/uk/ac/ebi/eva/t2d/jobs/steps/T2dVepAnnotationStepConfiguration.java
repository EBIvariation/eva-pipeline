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
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.readers.VcfReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.VepAnnotationFileWriterConfiguration;
import uk.ac.ebi.eva.pipeline.listeners.StepProgressListener;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.t2d.configuration.processors.ExistingVariantFilterConfiguration;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_WRITER;
import static uk.ac.ebi.eva.t2d.BeanNames.EXISTING_VARIANT_FILTER;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_VEP_ANNOTATION_STEP;

/**
 * Step to generate vep annotation.
 */
@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
@Import({VcfReaderConfiguration.class, ExistingVariantFilterConfiguration.class,
        VepAnnotationFileWriterConfiguration.class, ChunkSizeCompletionPolicyConfiguration.class})
public class T2dVepAnnotationStepConfiguration {


    private static final Logger logger = LoggerFactory.getLogger(T2dLoadAnnotationStepConfiguration.class);

    @Bean(T2D_VEP_ANNOTATION_STEP)
    public Step t2dLoadAnnotationStep(
            StepBuilderFactory stepBuilderFactory,
            JobOptions jobOptions,
            SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Qualifier(VARIANT_READER) ItemStreamReader<Variant> variantsReader,
            @Qualifier(EXISTING_VARIANT_FILTER) ItemProcessor<Variant, EnsemblVariant> existingVariantFilter,
            @Qualifier(VEP_ANNOTATION_WRITER) ItemWriter<EnsemblVariant> vepAnnotationWriter) {
        logger.debug("Building '" + T2D_VEP_ANNOTATION_STEP + "'");

        return stepBuilderFactory.get(T2D_VEP_ANNOTATION_STEP)
                .<Variant, EnsemblVariant>chunk(chunkSizeCompletionPolicy)
                    .reader(variantsReader)
                .processor(existingVariantFilter)
                .writer(vepAnnotationWriter)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .listener(new StepProgressListener())
                .build();
    }


}
