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
import uk.ac.ebi.eva.t2d.jobs.tasklet.PrepareDatabaseTasklet;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;
import uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames;
import uk.ac.ebi.eva.t2d.parameters.T2dMetadataParameters;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;
import uk.ac.ebi.eva.t2d.services.T2dService;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_PREPARE_DATABASE_SAMPLES_STEP;

@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
public class PrepareDatabaseSamplesStep {

    private static final Logger logger = LoggerFactory.getLogger(PrepareDatabaseSamplesStep.class);

    @Bean
    ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener promotionListener = new ExecutionContextPromotionListener();
        promotionListener.setKeys(new String[]{T2dJobParametersNames.CONTEXT_TSV_DEFINITION});
        return promotionListener;
    }

    @Bean
    PrepareDatabaseTasklet prepareDatabaseSamplesTasklet(T2dService service,
                                                         T2dMetadataParameters metadataParameters,
                                                         T2dTsvParameters tsvParameters) {
        return new PrepareDatabaseTasklet(service, metadataParameters, tsvParameters) {

            @Override
            protected void insertProperties(T2dService service, String datasetId, T2DTableStructure dataStructure, T2dTsvParameters tsvParameters) {
                service.insertSampleProperties(datasetId, dataStructure);
            }

            @Override
            protected String getDatasetId(T2dMetadataParameters metadataParameters) {
                return metadataParameters.getSamplesMetadata().getId();
            }

            @Override
            protected String getTableDefinitionFilePath(T2dTsvParameters tsvParameters) {
                return tsvParameters.getSamplesDefinitionFile();
            }

            @Override
            protected String getTableName(T2dMetadataParameters metadataParameters, T2dTsvParameters tsvParameters) {
                return metadataParameters.getSamplesMetadata().getTableName();
            }
        };
    }

    @Bean(T2D_PREPARE_DATABASE_SAMPLES_STEP)
    public Step prepareDatabaseT2d(StepBuilderFactory stepBuilderFactory,
                                   PrepareDatabaseTasklet prepareDatabaseSamplesTasklet) {
        logger.debug("Building '" + T2D_PREPARE_DATABASE_SAMPLES_STEP + "'");
        return stepBuilderFactory.get(T2D_PREPARE_DATABASE_SAMPLES_STEP)
                .tasklet(prepareDatabaseSamplesTasklet)
                .listener(promotionListener())
                .build();
    }

}
