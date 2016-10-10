/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ebi.eva.pipeline.jobs;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.pipeline.jobs.deciders.EmptyFileDecider;
import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep;

/**
 * @author Diego Poggioli
 *
 * Batch class to wire together:
 * 1) variantsAnnotGenerateInputBatchStep - Dump a list of variants without annotations to be used as input for VEP
 * 2) annotationCreate - run VEP
 * 3) annotationLoadBatchStep - Load VEP annotations into mongo
 *
 * Optional flow: variantsAnnotGenerateInput --> (annotationCreate --> annotationLoad)
 * annotationCreate and annotationLoad steps are only executed if variantsAnnotGenerateInput is generating a
 * non-empty VEP input file
 *
 */

@Configuration
@EnableBatchProcessing
@Import({VepAnnotationGeneratorStep.class, VepInputGeneratorStep.class, AnnotationLoaderStep.class})
public class AnnotationJob extends CommonJobStepInitialization{
    public static final String jobName = "annotate-variants";
    public static final String SKIP_ANNOT = "annotation.skip";
    public static final String GENERATE_VEP_ANNOTATION = "Generate VEP annotation";
    private static final String OPTIONAL_VARIANT_VEP_ANNOTATION_FLOW = "Optional variant VEP annotation flow";
    private static final String VARIANT_VEP_ANNOTATION_FLOW = "Variant VEP annotation flow";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Qualifier("vepInputGeneratorStep")
    @Autowired
    public Step variantsAnnotGenerateInputBatchStep;

    @Qualifier("annotationLoad")
    @Autowired
    private Step annotationLoadBatchStep;

    @Autowired
    private VepAnnotationGeneratorStep vepAnnotationGeneratorStep;

    @Bean
    public Job variantAnnotationBatchJob(){
        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        return jobBuilder.start(optionalAnnotationFlow()).build().build();
    }

    @Bean
    public Flow annotationFlow(){
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

    @Bean
    public Flow optionalAnnotationFlow(){
        SkipStepDecider annotationSkipStepDecider = new SkipStepDecider(getPipelineOptions(), SKIP_ANNOT);

        return new FlowBuilder<Flow>(OPTIONAL_VARIANT_VEP_ANNOTATION_FLOW)
                .start(annotationSkipStepDecider).on(SkipStepDecider.DO_STEP)
                .to(annotationFlow())
                .from(annotationSkipStepDecider).on(SkipStepDecider.SKIP_STEP)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

    private Step annotationCreate() {
        return generateStep(GENERATE_VEP_ANNOTATION, vepAnnotationGeneratorStep);
    }

}
