package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.PopulationStatisticsLoaderStep;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.TaskletUtils;

import static uk.ac.ebi.eva.pipeline.jobs.steps.CreateDatabaseIndexesStep.NAME_CREATE_DATABASE_INDEXES_STEP;

@Configuration
@EnableBatchProcessing
@Import(JobOptions.class)
public class LoadStatisticsStep {

    private static final Logger logger = LoggerFactory.getLogger(LoadStatisticsStep.class);
    public static final String NAME_LOAD_STATISTICS_STEP = "load-statistics-step";

    @Bean
    @StepScope
    PopulationStatisticsLoaderStep populationStatisticsLoaderStep() {
        return new PopulationStatisticsLoaderStep();
    }

    @Bean(NAME_LOAD_STATISTICS_STEP)
    public TaskletStep loadStatisticsStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions) {
        logger.debug("Building '" + NAME_LOAD_STATISTICS_STEP + "'");
        return TaskletUtils.generateStep(stepBuilderFactory, NAME_LOAD_STATISTICS_STEP,
                populationStatisticsLoaderStep(), jobOptions);
    }

}
