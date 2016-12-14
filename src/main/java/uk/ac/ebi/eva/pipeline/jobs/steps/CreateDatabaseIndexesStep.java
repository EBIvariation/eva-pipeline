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
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.IndexesGeneratorStep;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.TaskletUtils;

@Configuration
@EnableBatchProcessing
@Import({JobOptions.class})
public class CreateDatabaseIndexesStep {

    private static final Logger logger = LoggerFactory.getLogger(CreateDatabaseIndexesStep.class);
    public static final String NAME_CREATE_DATABASE_INDEXES_STEP = "create-database-indexes-step";

    @Bean
    @StepScope
    IndexesGeneratorStep indexesGeneratorStep() {
        return new IndexesGeneratorStep();
    }

    @Bean(NAME_CREATE_DATABASE_INDEXES_STEP)
    public TaskletStep createDatabaseIndexesStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions) {
        logger.debug("Building '" + NAME_CREATE_DATABASE_INDEXES_STEP + "'");
        return TaskletUtils.generateStep(stepBuilderFactory, NAME_CREATE_DATABASE_INDEXES_STEP, indexesGeneratorStep(),
                jobOptions);
    }

}
