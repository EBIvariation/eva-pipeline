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

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.CalculateStatisticsStepConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadStatisticsStepConfiguration;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_STATISTICS_STEP;

/**
 * Configurations that defines the calcule statistics process. First calculate the statistics then load them to
 * the knowledge base.
 */
@Configuration
@EnableBatchProcessing
@Import({CalculateStatisticsStepConfiguration.class, LoadStatisticsStepConfiguration.class})
public class PopulationStatisticsFlowConfiguration {

    @Autowired
    @Qualifier(CALCULATE_STATISTICS_STEP)
    private Step calculateStatisticsStep;

    @Autowired
    @Qualifier(LOAD_STATISTICS_STEP)
    private Step loadStatisticsStep;

    @Bean(CALCULATE_STATISTICS_FLOW)
    public Flow calculateStatisticsOptionalFlow() {
        return new FlowBuilder<Flow>(CALCULATE_STATISTICS_FLOW)
                .start(calculateStatisticsStep).next(loadStatisticsStep).build();
    }

}
