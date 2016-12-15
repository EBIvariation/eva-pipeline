package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.PopulationStatisticsGeneratorStep;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.TaskletUtils;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_STEP;

@Configuration
@EnableBatchProcessing
public class CalculateStatisticsStep {

    private static final Logger logger = LoggerFactory.getLogger(CalculateStatisticsStep.class);

    @Bean
    @StepScope
    PopulationStatisticsGeneratorStep populationStatisticsGeneratorStep() {
        return new PopulationStatisticsGeneratorStep();
    }

    @Bean(CALCULATE_STATISTICS_STEP)
    public TaskletStep calculateStatisticsStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions) {
        logger.debug("Building '" + CALCULATE_STATISTICS_STEP + "'");
        return TaskletUtils.generateStep(stepBuilderFactory, CALCULATE_STATISTICS_STEP,
                populationStatisticsGeneratorStep(), jobOptions);
    }

}
