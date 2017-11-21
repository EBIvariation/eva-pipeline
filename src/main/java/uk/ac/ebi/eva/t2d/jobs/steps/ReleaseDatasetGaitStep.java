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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.tasklet.ReleaseDatasetGaitTasklet;
import uk.ac.ebi.eva.t2d.jobs.tasklet.ReleaseDatasetTasklet;
import uk.ac.ebi.eva.t2d.parameters.T2dMetadataParameters;
import uk.ac.ebi.eva.t2d.services.T2dService;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_RELEASE_DATASET_GAIT_STEP;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_RELEASE_DATASET_STEP;

/**
 * Step to release the dataset to be used in GAIT
 */
@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
public class ReleaseDatasetGaitStep {

    private static final Logger logger = LoggerFactory.getLogger(ReleaseDatasetGaitStep.class);

    @Bean
    ReleaseDatasetGaitTasklet releaseDatasetGaitTasklet(T2dService service, T2dMetadataParameters metadataParameters) {
        return new ReleaseDatasetGaitTasklet(service, metadataParameters);
    }

    @Bean(T2D_RELEASE_DATASET_GAIT_STEP)
    public Step releaseDatasetStep(StepBuilderFactory stepBuilderFactory,
                                   ReleaseDatasetGaitTasklet releaseDatasetGaitTasklet) {
        logger.debug("Building '" + T2D_RELEASE_DATASET_GAIT_STEP + "'");
        return stepBuilderFactory.get(T2D_RELEASE_DATASET_GAIT_STEP)
                .tasklet(releaseDatasetGaitTasklet)
                .build();
    }

}
