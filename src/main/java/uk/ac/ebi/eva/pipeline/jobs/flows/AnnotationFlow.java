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
import uk.ac.ebi.eva.pipeline.jobs.deciders.EmptyFileDecider;
import uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.GenerateVepAnnotationStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.VepAnnotationGeneratorStep;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

@Configuration
@EnableBatchProcessing
@Import({VepAnnotationGeneratorStep.class, VepInputGeneratorStep.class, AnnotationLoaderStep.class,
        GenerateVepAnnotationStep.class})
public class AnnotationFlow {

    public static final String NAME_VEP_ANNOTATION_FLOW = "VEP annotation flow";

    @Autowired
    @Qualifier(VepInputGeneratorStep.NAME_GENERATE_VEP_INPUT_STEP)
    public Step generateVepInputStep;

    @Autowired
    @Qualifier(AnnotationLoaderStep.NAME_LOAD_VEP_ANNOTATION_STEP)
    private Step annotationLoadStep;

    @Autowired
    @Qualifier(GenerateVepAnnotationStep.NAME_GENERATE_VEP_ANNOTATION_STEP)
    private Step generateVepAnnotationStep;

    @Bean(NAME_VEP_ANNOTATION_FLOW)
    public Flow vepAnnotationFlow(JobOptions jobOptions) {
        EmptyFileDecider emptyFileDecider = new EmptyFileDecider(jobOptions.getPipelineOptions().getString(JobOptions
                .VEP_INPUT));

        return new FlowBuilder<Flow>(NAME_VEP_ANNOTATION_FLOW)
                .start(generateVepInputStep)
                .next(emptyFileDecider).on(EmptyFileDecider.CONTINUE_FLOW)
                .to(generateVepAnnotationStep)
                .next(annotationLoadStep)
                .from(emptyFileDecider).on(EmptyFileDecider.STOP_FLOW)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

}
