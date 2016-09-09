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
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.pipeline.steps.decider.OptionalDecider;
import uk.ac.ebi.eva.pipeline.steps.tasklet.VariantsStatsCreate;
import uk.ac.ebi.eva.pipeline.steps.tasklet.VariantsStatsLoad;

@Configuration
@EnableBatchProcessing
@Import({VariantsStatsCreate.class, VariantsStatsLoad.class})
public class VariantStatsConfiguration extends CommonJobStepInitialization{

    private static final Logger logger = LoggerFactory.getLogger(VariantStatsConfiguration.class);
    private static final String jobName = "calculate-statistics";
    public static final String SKIP_STATS = "statistics.skip";
    public static final String CALCULATE_STATISTICS = "Calculate statistics";
    public static final String LOAD_STATISTICS = "Load statistics";
    private static final String STATS_FLOW = "statsFlow";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private VariantsStatsCreate variantsStatsCreate;

    @Autowired
    private VariantsStatsLoad variantsStatsLoad;

    @Bean
    public Job variantStatsJob() {
        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        return jobBuilder
                .start(variantStatsFlow())
                .build().build();
    }

    @Bean
    public Flow variantStatsFlow(){
        OptionalDecider statisticsOptionalDecider = new OptionalDecider(getPipelineOptions(), SKIP_STATS);

        return new FlowBuilder<Flow>(STATS_FLOW)
                .start(statisticsOptionalDecider).on(OptionalDecider.DO_STEP)
                .to(statsCreate())
                .next(statsLoad())
                .from(statisticsOptionalDecider).on(OptionalDecider.SKIP_STEP).end(BatchStatus.COMPLETED.toString())
                .build();
    }

    private Step statsCreate() {
        return generateStep(CALCULATE_STATISTICS,variantsStatsCreate);
    }

    private Step statsLoad() {
        return generateStep(LOAD_STATISTICS, variantsStatsLoad);
    }
}
