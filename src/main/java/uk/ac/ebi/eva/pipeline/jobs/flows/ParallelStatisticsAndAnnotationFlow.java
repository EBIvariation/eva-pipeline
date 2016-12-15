package uk.ac.ebi.eva.pipeline.jobs.flows;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_OPTIONAL_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_OPTIONAL_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.PARALLEL_STATISTICS_AND_ANNOTATION;

@Configuration
@EnableBatchProcessing
@Import({AnnotationFlowOptional.class, PopulationStatisticsFlow.class})
public class ParallelStatisticsAndAnnotationFlow {

    @Autowired
    @Qualifier(VEP_ANNOTATION_OPTIONAL_FLOW)
    private Flow annotationFlowOptional;

    @Autowired
    @Qualifier(CALCULATE_STATISTICS_OPTIONAL_FLOW)
    private Flow optionalStatisticsFlow;

    @Bean(PARALLEL_STATISTICS_AND_ANNOTATION)
    public Flow parallelStatisticsAndAnnotation() {
        return new FlowBuilder<Flow>(PARALLEL_STATISTICS_AND_ANNOTATION)
                .split(new SimpleAsyncTaskExecutor())
                .add(optionalStatisticsFlow, annotationFlowOptional)
                .build();
    }

}
