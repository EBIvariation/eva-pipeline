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
package uk.ac.ebi.eva.pipeline.configuration.jobs.flows;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.pipeline.configuration.JobExecutionDeciderConfiguration;
import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_OPTIONAL_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.STATISTICS_SKIP_STEP_DECIDER;

/**
 * Configuration that defines a calculate statistics flow that can be skipped depending on property 'statistics.skip'
 * In the case that the property is set to false, then the process executes the flow at {@link PopulationStatisticsFlowConfiguration}
 */
@Configuration
@EnableBatchProcessing
@Import({PopulationStatisticsFlowConfiguration.class, JobExecutionDeciderConfiguration.class})
public class PopulationStatisticsOptionalFlowConfiguration {

    @Bean(CALCULATE_STATISTICS_OPTIONAL_FLOW)
    public Flow calculateStatisticsOptionalFlow(@Qualifier(CALCULATE_STATISTICS_FLOW) Flow calculateStatisticsflow,
                                                @Qualifier(STATISTICS_SKIP_STEP_DECIDER) JobExecutionDecider decider) {
        return new FlowBuilder<Flow>(CALCULATE_STATISTICS_OPTIONAL_FLOW)
                .start(decider).on(SkipStepDecider.DO_STEP)
                .to(calculateStatisticsflow)
                .from(decider).on(SkipStepDecider.SKIP_STEP)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

}
