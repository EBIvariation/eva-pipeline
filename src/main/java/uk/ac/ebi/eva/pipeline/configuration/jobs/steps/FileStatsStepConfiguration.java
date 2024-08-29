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
package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.FileStatsTasklet;
import uk.ac.ebi.eva.pipeline.parameters.ChunkSizeParameters;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;
import uk.ac.ebi.eva.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.TaskletUtils;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.FILE_STATS_STEP;


@Configuration
@EnableBatchProcessing
public class FileStatsStepConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(FileStatsStepConfiguration.class);

    @Bean
    @StepScope
    public FileStatsTasklet fileStatsTasklet(DatabaseParameters databaseParameters,
                                             MongoTemplate mongoTemplate,
                                             InputParameters inputParameters,
                                             ChunkSizeParameters chunkSizeParameters) {
        return new FileStatsTasklet(databaseParameters, mongoTemplate, inputParameters.getStudyId(),
                chunkSizeParameters.getChunkSize());
    }

    @Bean(FILE_STATS_STEP)
    public TaskletStep fileStatsStep(DatabaseParameters databaseParameters,
                                     MongoTemplate mongoTemplate,
                                     InputParameters inputParameters,
                                     ChunkSizeParameters chunkSizeParameters,
                                     StepBuilderFactory stepBuilderFactory,
                                     JobOptions jobOptions) {
        logger.debug("Building '" + FILE_STATS_STEP + "'");

        return TaskletUtils.generateStep(stepBuilderFactory, FILE_STATS_STEP,
                fileStatsTasklet(databaseParameters, mongoTemplate, inputParameters, chunkSizeParameters),
                jobOptions.isAllowStartIfComplete());
    }
}
