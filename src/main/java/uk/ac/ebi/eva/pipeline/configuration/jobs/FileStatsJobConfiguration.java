/*
 * Copyright 2024 EMBL - European Bioinformatics Institute
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.FileStatsStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.NewJobIncrementer;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.FILE_STATS_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.FILE_STATS_STEP;

@Configuration
@EnableBatchProcessing
@Import({FileStatsStepConfiguration.class})
public class FileStatsJobConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(FileStatsJobConfiguration.class);

    @Autowired
    @Qualifier(FILE_STATS_STEP)
    private Step fileStatsStep;

    @Bean(FILE_STATS_JOB)
    @Scope("prototype")
    public Job fileStatsJob(JobBuilderFactory jobBuilderFactory) {
        logger.debug("Building '" + FILE_STATS_JOB + "'");

        return jobBuilderFactory
                .get(FILE_STATS_JOB)
                .incrementer(new NewJobIncrementer())
                .start(fileStatsStep)
                .build();
    }

}
