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
package uk.ac.ebi.eva.t2d.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.steps.T2dLoadAnnotationStepConfiguration;
import uk.ac.ebi.eva.t2d.jobs.steps.T2dVepAnnotationStepConfiguration;
import uk.ac.ebi.eva.t2d.parameters.validation.job.T2dLoadVariantsParametersValidator;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_LOAD_ANNOTATION_STEP;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_ANNOTATE_AND_LOAD_VCF_JOB;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_VEP_ANNOTATION_STEP;

/**
 * Job to Annotate and load a VCF file
 */
@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
@Import({T2dVepAnnotationStepConfiguration.class, T2dLoadAnnotationStepConfiguration.class})
public class AnnotateAndLoadVcfJob {

    private static Logger logger = LoggerFactory.getLogger(LoadSamplesDataJob.class);

    @Bean(T2D_ANNOTATE_AND_LOAD_VCF_JOB)
    public Job loadSampleDataJob(JobBuilderFactory jobBuilderFactory,
                                 @Qualifier(T2D_VEP_ANNOTATION_STEP) Step vepAnnotation,
                                 @Qualifier(T2D_LOAD_ANNOTATION_STEP) Step loadAnnotation) {
        logger.debug("Building '" + T2D_ANNOTATE_AND_LOAD_VCF_JOB +"'");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(T2D_ANNOTATE_AND_LOAD_VCF_JOB)
                .incrementer(new RunIdIncrementer())
                .validator(new T2dLoadVariantsParametersValidator());

        return jobBuilder.flow(vepAnnotation)
                .next(loadAnnotation)
                .end()
                .build();
    }

}
