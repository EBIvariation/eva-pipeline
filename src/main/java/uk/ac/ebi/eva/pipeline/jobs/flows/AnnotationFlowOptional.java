package uk.ac.ebi.eva.pipeline.jobs.flows;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_OPTIONAL_FLOW;

@Configuration
@EnableBatchProcessing
@Import({AnnotationFlow.class})
public class AnnotationFlowOptional {

    @Bean(VEP_ANNOTATION_OPTIONAL_FLOW)
    Flow vepAnnotationOptionalFlow(@Qualifier(VEP_ANNOTATION_FLOW) Flow vepAnnotationFlow,
                                   SkipStepDecider skipStepDecider) {
        return new FlowBuilder<Flow>(VEP_ANNOTATION_OPTIONAL_FLOW)
                .start(skipStepDecider).on(SkipStepDecider.DO_STEP)
                .to(vepAnnotationFlow)
                .from(skipStepDecider).on(SkipStepDecider.SKIP_STEP)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

    @Bean
    @Scope("prototype")
    SkipStepDecider skipStepDecider(JobOptions jobOptions) {
        return new SkipStepDecider(jobOptions.getPipelineOptions(),
                JobParametersNames.ANNOTATION_SKIP);
    }

}
