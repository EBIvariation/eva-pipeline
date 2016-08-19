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
import embl.ebi.variation.eva.pipeline.steps.VariantsAnnotCreate;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
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
 * TODO:
 * - move variantsAnnotCreate and annotationCreate into a separate class to reuse across different jobs
 */

@Configuration
@EnableBatchProcessing
@Import({VariantsAnnotGenerateInput.class, VariantsAnnotLoad.class, VariantJobArgsConfig.class})
public class VariantAnnotConfiguration {
    public static final String jobName = "annotate-variants";

    @Autowired private JobBuilderFactory jobBuilderFactory;

    @Autowired private StepBuilderFactory stepBuilderFactory;

    @Autowired private ObjectMap pipelineOptions;

    @Qualifier("variantsAnnotGenerateInput")
    @Autowired public Step variantsAnnotGenerateInputBatchStep;

    @Qualifier("variantAnnotLoad")
    @Autowired private Step variantAnnotLoadBatchStep;

    @Qualifier("annotationCreate")
    @Autowired private Step annotationCreate;

    @Bean
    public Job variantAnnotationBatchJob(){
        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        return jobBuilder.start(variantAnnotationFlow()).build().build();
    }

    @Bean
    public Flow variantAnnotationFlow(){
        Flow annotationFlow = new FlowBuilder<Flow>("Variant VEP annotation flow")
                .start(variantsAnnotGenerateInputBatchStep)
                .next(annotationCreate)
                .next(variantAnnotLoadBatchStep)
                .build();
        return annotationFlow;
    }

    @Bean
    public VariantsAnnotCreate variantsAnnotCreate(){
        return new VariantsAnnotCreate();
    }

    @Bean
    @Qualifier("annotationCreate")
    public Step annotationCreate() {
        StepBuilder step1 = stepBuilderFactory.get("Generate VEP annotation");
        TaskletStepBuilder tasklet = step1.tasklet(variantsAnnotCreate());
        initStep(tasklet);
        return tasklet.build();
    }

    /**
     * Initialize a Step with common configuration
     * @param tasklet to be initialized with common configuration
     */
    private void initStep(TaskletStepBuilder tasklet) {

        boolean allowStartIfComplete  = pipelineOptions.getBoolean("config.restartability.allow");

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false(default): if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(allowStartIfComplete);
    }

}
