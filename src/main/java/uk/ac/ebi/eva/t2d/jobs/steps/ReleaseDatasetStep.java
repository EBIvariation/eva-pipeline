package uk.ac.ebi.eva.t2d.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.tasklet.ReleaseDatasetTasklet;
import uk.ac.ebi.eva.t2d.parameters.T2dMetadataParameters;
import uk.ac.ebi.eva.t2d.services.T2dService;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_PREPARE_DATABASE_SUMMARY_STATISTICS_STEP;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_RELEASE_DATASET_STEP;

@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
public class ReleaseDatasetStep {

    private static final Logger logger = LoggerFactory.getLogger(ReleaseDatasetStep.class);

    @Bean
    ReleaseDatasetTasklet releaseDatasetTasklet(T2dService service, T2dMetadataParameters metadataParameters) {
        return new ReleaseDatasetTasklet(service, metadataParameters);
    }

    @Bean(T2D_RELEASE_DATASET_STEP)
    public Step releaseDatasetStep(StepBuilderFactory stepBuilderFactory,
                                   ReleaseDatasetTasklet releaseDatasetTasklet) {
        logger.debug("Building '" + T2D_RELEASE_DATASET_STEP + "'");
        return stepBuilderFactory.get(T2D_RELEASE_DATASET_STEP)
                .tasklet(releaseDatasetTasklet)
                .build();
    }

}
