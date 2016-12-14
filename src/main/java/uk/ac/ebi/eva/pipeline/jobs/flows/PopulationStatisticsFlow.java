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

@Configuration
@EnableBatchProcessing
@Import({CalculateStatisticsStep.class, LoadStatisticsStep.class})
public class PopulationStatisticsFlow {

    public static final String NAME_CALCULATE_STATISTICS_OPTIONAL_FLOW = "calculate-statistics-optional-flow";

    @Autowired
    @Qualifier(CalculateStatisticsStep.NAME_CALCULATE_STATISTICS_STEP)
    private Step calculateStatisticsStep;

    @Autowired
    @Qualifier(LoadStatisticsStep.NAME_LOAD_STATISTICS_STEP)
    private Step loadStatisticsStep;

    @Autowired
    private JobOptions jobOptions;

    @Bean(NAME_CALCULATE_STATISTICS_OPTIONAL_FLOW)
    public Flow calculateStatisticsOptionalFlow() {
        SkipStepDecider statisticsSkipStepDecider = new SkipStepDecider(jobOptions.getPipelineOptions(),
                JobParametersNames.STATISTICS_SKIP);

        return new FlowBuilder<Flow>(NAME_CALCULATE_STATISTICS_OPTIONAL_FLOW)
                .start(statisticsSkipStepDecider).on(SkipStepDecider.DO_STEP)
                .to(calculateStatisticsStep)
                .next(loadStatisticsStep)
                .from(statisticsSkipStepDecider).on(SkipStepDecider.SKIP_STEP).end(BatchStatus.COMPLETED.toString())
                .build();
    }

}
