/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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

import embl.ebi.variation.eva.pipeline.steps.VariantAnnotLoadBatch;
import embl.ebi.variation.eva.pipeline.steps.VariantsAnnotCreate;
import embl.ebi.variation.eva.pipeline.steps.VariantsAnnotGenerateInputBatch;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
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
@Import({VariantsAnnotGenerateInputBatch.class, VariantAnnotLoadBatch.class, VariantJobArgsConfig.class})
public class VariantAnnotConfigurationBatch {
    public static final String jobName = "variantAnnotBatchJob";

    @Autowired private JobBuilderFactory jobBuilderFactory;

    @Autowired private StepBuilderFactory stepBuilderFactory;

    @Qualifier("variantsAnnotGenerateInputBatchStep")
    @Autowired public Step variantsAnnotGenerateInputBatchStep;

    @Qualifier("variantAnnotLoadBatchStep")
    @Autowired private Step variantAnnotLoadBatchStep;

    @Qualifier("annotationCreate")
    @Autowired private Step annotationCreate;

    @Bean
    public Job variantAnnotBatchJob() {
        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        return jobBuilder
                .start(variantsAnnotGenerateInputBatchStep)
                .next(annotationCreate)
                .next(variantAnnotLoadBatchStep)
                .build();
    }

    @Bean
    public VariantsAnnotCreate variantsAnnotCreate(){
        return new VariantsAnnotCreate();
    }

    @Bean
    @Qualifier("annotationCreate")
    public Step annotationCreate() {
        StepBuilder step1 = stepBuilderFactory.get("annotationCreate");
        TaskletStepBuilder tasklet = step1.tasklet(variantsAnnotCreate());

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);
        return tasklet.build();
    }
}
