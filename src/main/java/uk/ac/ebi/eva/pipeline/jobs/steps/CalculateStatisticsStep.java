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
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.PopulationStatisticsGeneratorStep;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.TaskletUtils;

@Configuration
@EnableBatchProcessing
public class CalculateStatisticsStep {

    private static final Logger logger = LoggerFactory.getLogger(CalculateStatisticsStep.class);
    public static final String NAME_CALCULATE_STATISTICS_STEP = "calculate-statistics-step";

    @Bean
    @StepScope
    PopulationStatisticsGeneratorStep populationStatisticsGeneratorStep() {
        return new PopulationStatisticsGeneratorStep();
    }

    @Bean(NAME_CALCULATE_STATISTICS_STEP)
    public TaskletStep calculateStatisticsStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions) {
        logger.debug("Building '" + NAME_CALCULATE_STATISTICS_STEP + "'");
        return TaskletUtils.generateStep(stepBuilderFactory, NAME_CALCULATE_STATISTICS_STEP,
                populationStatisticsGeneratorStep(), jobOptions);
    }

}
