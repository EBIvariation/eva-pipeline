package uk.ac.ebi.eva.pipeline.jobs.flows;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

@Configuration
@EnableBatchProcessing
@Import({JobOptions.class, AnnotationFlow.class})
public class AnnotationFlowOptional {

    public static final String NAME_VEP_ANNOTATION_OPTIONAL_FLOW = "VEP annotation optional flow";

    @Autowired
    private JobOptions jobOptions;

    @Bean(NAME_VEP_ANNOTATION_OPTIONAL_FLOW)
    Flow vepAnnotationOptionalFlow(@Qualifier(AnnotationFlow.NAME_VEP_ANNOTATION_FLOW) Flow vepAnnotationFlow) {
        SkipStepDecider annotationSkipStepDecider = new SkipStepDecider(jobOptions.getPipelineOptions(),
                JobParametersNames.ANNOTATION_SKIP);

        return new FlowBuilder<Flow>(NAME_VEP_ANNOTATION_OPTIONAL_FLOW)
                .start(annotationSkipStepDecider).on(SkipStepDecider.DO_STEP)
                .to(vepAnnotationFlow)
                .from(annotationSkipStepDecider).on(SkipStepDecider.SKIP_STEP)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

}
