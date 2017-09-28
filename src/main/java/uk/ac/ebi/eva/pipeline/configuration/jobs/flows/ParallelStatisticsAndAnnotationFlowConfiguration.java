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

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_OPTIONAL_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.PARALLEL_STATISTICS_AND_ANNOTATION;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_OPTIONAL_FLOW;

/**
 * Configuration class that defines a flow that executes in parallel the annotation and the statistics flows.
 */
@Configuration
@EnableBatchProcessing
@Import({AnnotationFlowOptionalConfiguration.class, PopulationStatisticsOptionalFlowConfiguration.class})
public class ParallelStatisticsAndAnnotationFlowConfiguration {

    @Autowired
    @Qualifier(VEP_ANNOTATION_OPTIONAL_FLOW)
    private Flow annotationFlowOptional;

    @Autowired
    @Qualifier(CALCULATE_STATISTICS_OPTIONAL_FLOW)
    private Flow optionalStatisticsFlow;

    @Bean(PARALLEL_STATISTICS_AND_ANNOTATION)
    public Flow parallelStatisticsAndAnnotation() {
        return new FlowBuilder<Flow>(PARALLEL_STATISTICS_AND_ANNOTATION)
                .split(new SimpleAsyncTaskExecutor())
                .add(optionalStatisticsFlow, annotationFlowOptional)
                .build();
    }

}
