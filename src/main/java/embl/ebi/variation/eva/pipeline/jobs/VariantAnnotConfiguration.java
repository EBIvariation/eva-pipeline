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

package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.pipeline.steps.VariantsAnnotGenerateInput;
import embl.ebi.variation.eva.pipeline.steps.VariantsAnnotLoad;
import embl.ebi.variation.eva.pipeline.steps.decider.OptionalDecider;
import embl.ebi.variation.eva.pipeline.steps.tasklet.VariantsAnnotCreate;
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

/**
 * @author Diego Poggioli
 *
 * Batch class to wire together:
 * 1) variantsAnnotGenerateInputBatchStep - Dump a list of variants without annotations to be used as input for VEP
 * 2) annotationCreate - run VEP
 * 3) variantAnnotLoadBatchStep - Load VEP annotations into mongo
 *
 * At the moment is no longer possible to skip a single step like skipAnnotGenerateInput=true in the property
 * To solve this we can implement the dynamic-workflow (https://github.com/EBIvariation/examples/tree/master/spring-batch-dynamic-workflow)
 * or we can create a new Job class for each possible scenario
 *
 */

@Configuration
@EnableBatchProcessing
@Import({VariantsAnnotCreate.class, VariantsAnnotGenerateInput.class, VariantsAnnotLoad.class})
public class VariantAnnotConfiguration extends CommonJobStepInitialization{
    public static final String jobName = "annotate-variants";
    public static final String SKIP_ANNOT = "annotation.skip";
    private static final String GENERATE_VEP_ANNOTATION = "Generate VEP annotation";
    private static final String VARIANT_VEP_ANNOTATION_FLOW = "Variant VEP annotation flow";
    private static final String COMPLETED = "COMPLETED";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Qualifier("variantsAnnotGenerateInput")
    @Autowired
    public Step variantsAnnotGenerateInputBatchStep;

    @Qualifier("variantAnnotLoad")
    @Autowired
    private Step variantAnnotLoadBatchStep;

    @Autowired
    private VariantsAnnotCreate variantsAnnotCreate;

    @Bean
    public Job variantAnnotationBatchJob(){
        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        return jobBuilder.start(variantAnnotationFlow()).build().build();
    }

    @Bean
    public Flow variantAnnotationFlow(){
        OptionalDecider annotationOptionalDecider = new OptionalDecider(getPipelineOptions(), SKIP_ANNOT);

        return new FlowBuilder<Flow>(VARIANT_VEP_ANNOTATION_FLOW)
                .start(annotationOptionalDecider).on(OptionalDecider.DO_STEP)
                .to(variantsAnnotGenerateInputBatchStep)
                .next(annotationCreate())
                .next(variantAnnotLoadBatchStep)
                .from(annotationOptionalDecider).on(OptionalDecider.SKIP_STEP).end(COMPLETED)
                .build();

    }

    private Step annotationCreate() {
        return generateStep(GENERATE_VEP_ANNOTATION, variantsAnnotCreate);
    }

}
