package uk.ac.ebi.eva.pipeline.jobs.flows;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.pipeline.jobs.CommonJobStepInitialization;
import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.PopulationStatisticsGeneratorStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.PopulationStatisticsLoaderStep;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

@Configuration
@EnableBatchProcessing
@Import({PopulationStatisticsGeneratorStep.class, PopulationStatisticsLoaderStep.class})
public class PopulationStatisticsFlow extends CommonJobStepInitialization {

    public static final String NAME_CALCULATE_STATISTICS_OPTIONAL_FLOW = "calculate-statistics-optional-flow";
    public static final String CALCULATE_STATISTICS = "Calculate statistics";
    public static final String LOAD_STATISTICS = "Load statistics";


    @Autowired
    private PopulationStatisticsGeneratorStep populationStatisticsGeneratorStep;

    @Autowired
    private PopulationStatisticsLoaderStep populationStatisticsLoaderStep;

    @Bean(NAME_CALCULATE_STATISTICS_OPTIONAL_FLOW)
    public Flow calculateStatisticsOptionalFlow() {
        SkipStepDecider statisticsSkipStepDecider = new SkipStepDecider(getPipelineOptions(), JobParametersNames.STATISTICS_SKIP);

        return new FlowBuilder<Flow>(NAME_CALCULATE_STATISTICS_OPTIONAL_FLOW)
                .start(statisticsSkipStepDecider).on(SkipStepDecider.DO_STEP)
                .to(statsCreate())
                .next(statsLoad())
                .from(statisticsSkipStepDecider).on(SkipStepDecider.SKIP_STEP).end(BatchStatus.COMPLETED.toString())
                .build();
    }

    private Step statsCreate() {
        return generateStep(CALCULATE_STATISTICS, populationStatisticsGeneratorStep);
    }

    private Step statsLoad() {
        return generateStep(LOAD_STATISTICS, populationStatisticsLoaderStep);
    }
}
