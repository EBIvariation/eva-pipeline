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

import uk.ac.ebi.eva.pipeline.jobs.CommonJobStepInitialization;
import uk.ac.ebi.eva.pipeline.jobs.deciders.EmptyFileDecider;
import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;
import uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep;

@Configuration
@EnableBatchProcessing
@Import({VepAnnotationGeneratorStep.class, VepInputGeneratorStep.class, AnnotationLoaderStep.class})
public class AnnotationFlow extends CommonJobStepInitialization {

    public static final String SKIP_ANNOT = "annotation.skip";
    public static final String GENERATE_VEP_ANNOTATION = "Generate VEP annotation";
    private static final String OPTIONAL_VARIANT_VEP_ANNOTATION_FLOW = "Optional variant VEP annotation flow";
    private static final String VARIANT_VEP_ANNOTATION_FLOW = "Variant VEP annotation flow";

    @Qualifier("vepInputGeneratorStep")
    @Autowired
    public Step variantsAnnotGenerateInputBatchStep;

    @Qualifier("annotationLoad")
    @Autowired
    private Step annotationLoadBatchStep;

    @Autowired
    private VepAnnotationGeneratorStep vepAnnotationGeneratorStep;

    @Bean
    Flow annotationFlowOptional() {
        SkipStepDecider annotationSkipStepDecider = new SkipStepDecider(getPipelineOptions(), SKIP_ANNOT);

        return new FlowBuilder<Flow>(OPTIONAL_VARIANT_VEP_ANNOTATION_FLOW)
                .start(annotationSkipStepDecider).on(SkipStepDecider.DO_STEP)
                .to(annotationFlowBasic())
                .from(annotationSkipStepDecider).on(SkipStepDecider.SKIP_STEP)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

    @Bean
    Flow annotationFlowBasic() {
        EmptyFileDecider emptyFileDecider = new EmptyFileDecider(getPipelineOptions().getString("vep.input"));

        return new FlowBuilder<Flow>(VARIANT_VEP_ANNOTATION_FLOW)
                .start(variantsAnnotGenerateInputBatchStep)
                .next(emptyFileDecider).on(EmptyFileDecider.CONTINUE_FLOW)
                .to(annotationCreate())
                .next(annotationLoadBatchStep)
                .from(emptyFileDecider).on(EmptyFileDecider.STOP_FLOW)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

    private Step annotationCreate() {
        return generateStep(GENERATE_VEP_ANNOTATION, vepAnnotationGeneratorStep);
    }

}
