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
import uk.ac.ebi.eva.t2d.entity.Phenotype;
import uk.ac.ebi.eva.t2d.jobs.tasklet.PrepareDatabaseTasklet;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;
import uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames;
import uk.ac.ebi.eva.t2d.parameters.T2dMetadataParameters;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;
import uk.ac.ebi.eva.t2d.services.T2dService;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_PREPARE_DATABASE_SUMMARY_STATISTICS_STEP;

/**
 * Step that creates the table for the summary statistics for a TSV file
 */
@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
public class PrepareDatabaseSummaryStatisticsStep {

    private static final Logger logger = LoggerFactory.getLogger(PrepareDatabaseSummaryStatisticsStep.class);

    @Bean
    ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener promotionListener = new ExecutionContextPromotionListener();
        promotionListener.setKeys(new String[]{T2dJobParametersNames.CONTEXT_TSV_DEFINITION});
        return promotionListener;
    }

    @Bean
    PrepareDatabaseTasklet prepareDatabaseStatisticsTasklet(T2dService service,
                                                            T2dMetadataParameters metadataParameters,
                                                            T2dTsvParameters tsvParameters) {
        return new PrepareDatabaseTasklet(service, metadataParameters, tsvParameters) {
            @Override
            protected void insertProperties(T2dService service, String datasetId, T2DTableStructure dataStructure, T2dTsvParameters tsvParameters) {
                service.insertSampleProperties(datasetId, dataStructure, tsvParameters.getSummaryStatisticsPhenotype());
            }

            @Override
            protected String getDatasetId(T2dMetadataParameters metadataParameters) {
                return metadataParameters.getDatasetMetadata().getId();
            }

            @Override
            protected String getTableDefinitionFilePath(T2dTsvParameters tsvParameters) {
                return tsvParameters.getSummaryStatisticsDefinitionFile();
            }

            @Override
            protected String getTableName(T2dMetadataParameters metadataParameters, T2dTsvParameters tsvParameters) {
                Phenotype phenotype = tsvParameters.getSummaryStatisticsPhenotype();
                if (phenotype == null) {
                    return metadataParameters.getDatasetMetadata().getTableName();
                } else {
                    return metadataParameters.getDatasetMetadata().getTableName() + "__" + phenotype.getId();
                }
            }
        };
    }

    @Bean(T2D_PREPARE_DATABASE_SUMMARY_STATISTICS_STEP)
    public Step prepareDatabaseT2d(StepBuilderFactory stepBuilderFactory,
                                   PrepareDatabaseTasklet prepareDatabaseStatisticsTasklet) {
        logger.debug("Building '" + T2D_PREPARE_DATABASE_SUMMARY_STATISTICS_STEP + "'");
        return stepBuilderFactory.get(T2D_PREPARE_DATABASE_SUMMARY_STATISTICS_STEP)
                .tasklet(prepareDatabaseStatisticsTasklet)
                .listener(promotionListener())
                .build();
    }

}
