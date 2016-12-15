package uk.ac.ebi.eva.pipeline.jobs.flows;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;
import uk.ac.ebi.eva.pipeline.jobs.steps.CalculateStatisticsStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.LoadStatisticsStep;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_OPTIONAL_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_STATISTICS_STEP;

@Configuration
@EnableBatchProcessing
@Import({CalculateStatisticsStep.class, LoadStatisticsStep.class})
public class PopulationStatisticsFlow {

    @Autowired
    @Qualifier(CALCULATE_STATISTICS_STEP)
    private Step calculateStatisticsStep;

    @Autowired
    @Qualifier(LOAD_STATISTICS_STEP)
    private Step loadStatisticsStep;

    @Bean(CALCULATE_STATISTICS_OPTIONAL_FLOW)
    public Flow calculateStatisticsOptionalFlow(JobOptions jobOptions) {
        SkipStepDecider statisticsSkipStepDecider = new SkipStepDecider(jobOptions.getPipelineOptions(),
                JobParametersNames.STATISTICS_SKIP);

        return new FlowBuilder<Flow>(CALCULATE_STATISTICS_OPTIONAL_FLOW)
                .start(statisticsSkipStepDecider).on(SkipStepDecider.DO_STEP)
                .to(calculateStatisticsStep)
                .next(loadStatisticsStep)
                .from(statisticsSkipStepDecider).on(SkipStepDecider.SKIP_STEP).end(BatchStatus.COMPLETED.toString())
                .build();
    }

}
