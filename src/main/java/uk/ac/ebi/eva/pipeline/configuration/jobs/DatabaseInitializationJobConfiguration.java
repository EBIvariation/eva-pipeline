/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.CreateDatabaseIndexesStepConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadGenesStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.NewJobIncrementer;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CREATE_DATABASE_INDEXES_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.INIT_DATABASE_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_GENES_STEP;

/**
 * Job to initialize the databases that will be used in later jobs.
 * <p>
 * 1. create the needed indexes in the DBs
 * 2. load genomic features for the species
 *
 * TODO add a new DatabaseInitializationJobParametersValidator
 */
@Configuration
@EnableBatchProcessing
@Import({LoadGenesStepConfiguration.class, CreateDatabaseIndexesStepConfiguration.class})
public class DatabaseInitializationJobConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseInitializationJobConfiguration.class);

    @Autowired
    @Qualifier(LOAD_GENES_STEP)
    private Step genesLoadStep;

    @Autowired
    @Qualifier(CREATE_DATABASE_INDEXES_STEP)
    private Step createDatabaseIndexesStep;

    @Bean(INIT_DATABASE_JOB)
    @Scope("prototype")
    public Job initDatabaseJob(JobBuilderFactory jobBuilderFactory) {
        logger.debug("Building '" + INIT_DATABASE_JOB + "'");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(INIT_DATABASE_JOB)
                .incrementer(new NewJobIncrementer());

        return jobBuilder
                .start(createDatabaseIndexesStep)
                .next(genesLoadStep)
                .build();
    }

}
