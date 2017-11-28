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
package uk.ac.ebi.eva.t2d.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.steps.GeneratePedStepConfiguration;
import uk.ac.ebi.eva.t2d.jobs.steps.ReadSampleDefinitionStepConfiguration;
import uk.ac.ebi.eva.t2d.parameters.validation.job.GeneratePedValidator;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_GENERATE_PED_JOB;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_GENERATE_PED_STEP;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_READ_SAMPLE_DEFINITION;

/**
 * Job to generate a PED file from the samples TSV file and the processor definition.
 */
@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
@Import({GeneratePedStepConfiguration.class, ReadSampleDefinitionStepConfiguration.class})
public class GeneratePedJob {

    private static Logger logger = LoggerFactory.getLogger(GeneratePedJob.class);

    @Bean(T2D_GENERATE_PED_JOB)
    public Job generatePedJob(JobBuilderFactory jobBuilderFactory,
                              @Qualifier(T2D_READ_SAMPLE_DEFINITION) Step readSampleDefinition,
                              @Qualifier(T2D_GENERATE_PED_STEP) Step generatePed) {
        logger.debug("Building '" + T2D_GENERATE_PED_JOB + "'");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(T2D_GENERATE_PED_JOB)
                .incrementer(new RunIdIncrementer())
                .validator(new GeneratePedValidator());

        return jobBuilder
                .flow(readSampleDefinition)
                .next(generatePed)
                .end()
                .build();
    }
}
