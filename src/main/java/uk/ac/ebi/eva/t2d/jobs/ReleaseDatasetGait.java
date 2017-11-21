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
import uk.ac.ebi.eva.t2d.jobs.steps.ReleaseDatasetGaitStep;
import uk.ac.ebi.eva.t2d.parameters.validation.job.ReleaseDatasetJobValidator;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_RELEASE_DATASET_GAIT_JOB;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_RELEASE_DATASET_GAIT_STEP;

/**
 * Release a dataset for GAIT in the T2D database
 */
@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
@Import({ReleaseDatasetGaitStep.class})
public class ReleaseDatasetGait {

    private static Logger logger = LoggerFactory.getLogger(ReleaseDatasetGaitStep.class);

    @Bean(T2D_RELEASE_DATASET_GAIT_JOB)
    public Job loadSampleDataJob(JobBuilderFactory jobBuilderFactory,
                                 @Qualifier(T2D_RELEASE_DATASET_GAIT_STEP) Step releaseDatasetStep) {
        logger.debug("Building '" + T2D_RELEASE_DATASET_GAIT_JOB + "'");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(T2D_RELEASE_DATASET_GAIT_JOB)
                .incrementer(new RunIdIncrementer())
                .validator(new ReleaseDatasetJobValidator());
        return jobBuilder.start(releaseDatasetStep)
                .build();
    }


}
