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
package uk.ac.ebi.eva.t2d.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.tasklet.ReadSampleDefinitionTasklet;
import uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_READ_SAMPLE_DEFINITION;

@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
public class ReadSampleDefinitionStepConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ReadSampleDefinitionStepConfiguration.class);

    @Bean
    ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener promotionListener = new ExecutionContextPromotionListener();
        promotionListener.setKeys(new String[]{T2dJobParametersNames.CONTEXT_TSV_DEFINITION});
        return promotionListener;
    }

    @Bean
    ReadSampleDefinitionTasklet readSampleDefinitionTasklet(T2dTsvParameters tsvParameters){
        return new ReadSampleDefinitionTasklet() {

            @Override
            protected String getTableName() {
                return "no-name-needed";
            }

            @Override
            protected String getTableDefinitionFilePath() {
                return tsvParameters.getSamplesDefinitionFile();
            }

        };
    }

    @Bean(T2D_READ_SAMPLE_DEFINITION)
    public Step prepareDatabaseT2d(StepBuilderFactory stepBuilderFactory,
                                   ReadSampleDefinitionTasklet readSampleDefinitionTasklet) {
        logger.debug("Building '" + T2D_READ_SAMPLE_DEFINITION + "'");
        return stepBuilderFactory.get(T2D_READ_SAMPLE_DEFINITION)
                .tasklet(readSampleDefinitionTasklet)
                .listener(promotionListener())
                .build();
    }

}
