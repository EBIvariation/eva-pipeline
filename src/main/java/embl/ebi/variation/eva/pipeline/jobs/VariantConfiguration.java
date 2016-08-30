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

import embl.ebi.variation.eva.pipeline.steps.*;
import org.opencb.datastore.core.ObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 *  Complete pipeline workflow:
 *
 *                       |--> (variantStatsFlow: statsCreate --> statsLoad)
 *  transform ---> load -+
 *                       |--> (variantAnnotationFlow: variantsAnnotGenerateInput --> annotationCreate --> variantAnnotLoad)
 *
 *  Steps in () are optional
 */
@Configuration
@EnableBatchProcessing
@Import({VariantJobArgsConfig.class, VariantAnnotConfiguration.class, VariantStatsConfiguration.class, VariantsLoad.class, VariantsTransform.class})
public class VariantConfiguration extends CommonJobStepInitialization{

    private static final Logger logger = LoggerFactory.getLogger(VariantConfiguration.class);
    public static final String jobName = "load-genotyped-vcf";
    private static final String NORMALIZE_VARIANTS = "Normalize variants";
    private static final String LOAD_VARIANTS = "Load variants";
    private static final String PARALLEL_STATISTICS_AND_ANNOTATION = "Parallel statistics and annotation";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private Flow variantAnnotationFlow;
    @Autowired
    private Flow variantStatsFlow;

    @Autowired
    private VariantsLoad variantsLoad;
    @Autowired
    private VariantsTransform variantsTransform;

    @Bean
    @Qualifier("variantJob")
    public Job variantJob() {
        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        Flow parallelStatsAndAnnotation = new FlowBuilder<Flow>(PARALLEL_STATISTICS_AND_ANNOTATION)
                .split(new SimpleAsyncTaskExecutor())
                .add(variantStatsFlow, variantAnnotationFlow)
                .build();

        FlowJobBuilder builder = jobBuilder
                .flow(transform())
                .next(load())
                .next(parallelStatsAndAnnotation)
                .end();

        return builder.build();
    }

    private Step transform() {
        return generateStep(NORMALIZE_VARIANTS,variantsTransform);
    }

    private Step load() {
        return generateStep(LOAD_VARIANTS, variantsLoad);
    }

}
