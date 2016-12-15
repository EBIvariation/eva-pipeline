/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_OPTIONAL_FLOW;

/**
 * Configuration to run a full Statistics job: variantStatsFlow: statsCreate --> statsLoad
 */
@Configuration
@EnableBatchProcessing
@Import({PopulationStatisticsFlow.class})
public class PopulationStatisticsJob {

    private static final Logger logger = LoggerFactory.getLogger(PopulationStatisticsJob.class);

    @Autowired
    @Qualifier(CALCULATE_STATISTICS_OPTIONAL_FLOW)
    private Flow optionalStatisticsFlow;

    @Bean(CALCULATE_STATISTICS_JOB)
    @Scope("prototype")
    public Job calculateStatisticsJob(JobBuilderFactory jobBuilderFactory) {
        logger.debug("Building '" + CALCULATE_STATISTICS_JOB + "'");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(CALCULATE_STATISTICS_JOB)
                .incrementer(new RunIdIncrementer());

        return jobBuilder
                .start(optionalStatisticsFlow)
                .build().build();
    }

}
