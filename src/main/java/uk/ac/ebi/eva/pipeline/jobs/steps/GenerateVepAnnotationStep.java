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
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.VepAnnotationGeneratorStep;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.TaskletUtils;

@Configuration
@EnableBatchProcessing
public class GenerateVepAnnotationStep {

    private static final Logger logger = LoggerFactory.getLogger(GenerateVepAnnotationStep.class);
    public static final String NAME_GENERATE_VEP_ANNOTATION_STEP = "generate-vep-annotation";

    @Bean
    @StepScope
    VepAnnotationGeneratorStep vepAnnotationGeneratorStep() {
        return new VepAnnotationGeneratorStep();
    }

    @Bean(NAME_GENERATE_VEP_ANNOTATION_STEP)
    public TaskletStep generateVepAnnotationStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions) {
        logger.debug("Building '" + NAME_GENERATE_VEP_ANNOTATION_STEP + "'");
        return TaskletUtils.generateStep(stepBuilderFactory, NAME_GENERATE_VEP_ANNOTATION_STEP,
                vepAnnotationGeneratorStep(), jobOptions);
    }

}
