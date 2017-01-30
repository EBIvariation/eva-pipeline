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

import uk.ac.ebi.eva.pipeline.jobs.deciders.EmptyVepInputDecider;
import uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.GenerateVepAnnotationStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENERATE_VEP_ANNOTATION_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENERATE_VEP_INPUT_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_VEP_ANNOTATION_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_FLOW;

/**
 * Configuration class that describes flow process in the annotation process.
 * <p>
 * This flow generates a vep input file, then if this file contains results then it starts the annotation process.
 * In the case that the file is empty this flow process ends.
 */
@Configuration
@EnableBatchProcessing
@Import({VepInputGeneratorStep.class, AnnotationLoaderStep.class, GenerateVepAnnotationStep.class})
public class AnnotationFlow {

    @Autowired
    @Qualifier(GENERATE_VEP_INPUT_STEP)
    public Step generateVepInputStep;

    @Autowired
    @Qualifier(LOAD_VEP_ANNOTATION_STEP)
    private Step annotationLoadStep;

    @Autowired
    @Qualifier(GENERATE_VEP_ANNOTATION_STEP)
    private Step generateVepAnnotationStep;

    @Bean(VEP_ANNOTATION_FLOW)
    public Flow vepAnnotationFlow() {
        EmptyVepInputDecider emptyVepInputDecider = new EmptyVepInputDecider();

        return new FlowBuilder<Flow>(VEP_ANNOTATION_FLOW)
                .start(generateVepInputStep)
                .next(emptyVepInputDecider).on(EmptyVepInputDecider.CONTINUE_FLOW)
                .to(generateVepAnnotationStep)
                .next(annotationLoadStep)
                .from(emptyVepInputDecider).on(EmptyVepInputDecider.STOP_FLOW)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

}
