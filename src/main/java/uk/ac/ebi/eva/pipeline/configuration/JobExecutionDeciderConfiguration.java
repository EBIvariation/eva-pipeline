/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.configuration;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATION_SKIP_STEP_DECIDER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.STATISTICS_SKIP_STEP_DECIDER;

/**
 * This class defines the beans for the deciders to skip annotation and statistics step.
 */
@Configuration
@EnableBatchProcessing
public class JobExecutionDeciderConfiguration {

    @Bean(ANNOTATION_SKIP_STEP_DECIDER)
    public JobExecutionDecider annotationSkipStepDecider() {
        return new SkipStepDecider(JobParametersNames.ANNOTATION_SKIP);
    }

    @Bean(STATISTICS_SKIP_STEP_DECIDER)
    public JobExecutionDecider statisticsSkipStepDecider() {
        return new SkipStepDecider(JobParametersNames.STATISTICS_SKIP);
    }

}
