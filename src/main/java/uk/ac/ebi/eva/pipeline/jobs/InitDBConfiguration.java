/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.pipeline.steps.GenesLoad;
import uk.ac.ebi.eva.pipeline.steps.tasklet.IndexesCreate;

@Configuration
@EnableBatchProcessing
@Import({IndexesCreate.class, GenesLoad.class})
public class InitDBConfiguration extends CommonJobStepInitialization {

    private static final Logger logger = LoggerFactory.getLogger(InitDBConfiguration.class);
    public static final String jobName = "initialize-database";
    public static final String CREATE_DATABASE_INDEXES = "Create database indexes";

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Qualifier("genesLoadStep")
    @Autowired
    private Step genesLoadStep;

    @Autowired
    private IndexesCreate indexesCreate;

    @Bean
    @Qualifier("initDBJob")
    public Job initDBJob() {
        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        return jobBuilder
                .start(indexesCreate())
                .next(genesLoadStep)
                .build();
    }

    public Step indexesCreate() {
        return generateStep(CREATE_DATABASE_INDEXES, indexesCreate);
    }
}
