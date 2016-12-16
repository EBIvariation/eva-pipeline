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
package uk.ac.ebi.eva.pipeline.jobs.flows;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_OPTIONAL_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.STATISTICS_SKIP_STEP_DECIDER;

/**
 * Configuration that defines a calculate statistics flow that can be skipped depending on property 'statistics.skip'
 * In the case that the property is set to false, then the process executes the flow at {@link PopulationStatisticsFlow}
 */
@Configuration
@EnableBatchProcessing
@Import({PopulationStatisticsFlow.class})
public class PopulationStatisticsOptionalFlow {

    @Autowired
    @Qualifier(CALCULATE_STATISTICS_FLOW)
    private Flow calculateStatisticsflow;

    @Autowired
    @Qualifier(STATISTICS_SKIP_STEP_DECIDER)
    private SkipStepDecider skipStepDecider;

    @Bean(CALCULATE_STATISTICS_OPTIONAL_FLOW)
    public Flow calculateStatisticsOptionalFlow() {
        return new FlowBuilder<Flow>(CALCULATE_STATISTICS_OPTIONAL_FLOW)
                .start(skipStepDecider).on(SkipStepDecider.DO_STEP)
                .to(calculateStatisticsflow)
                .from(skipStepDecider).on(SkipStepDecider.SKIP_STEP)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

    @Bean(STATISTICS_SKIP_STEP_DECIDER)
    @Scope("prototype")
    SkipStepDecider skipStepDecider(JobOptions jobOptions) {
        return new SkipStepDecider(jobOptions.getPipelineOptions(), JobParametersNames.STATISTICS_SKIP);
    }

}