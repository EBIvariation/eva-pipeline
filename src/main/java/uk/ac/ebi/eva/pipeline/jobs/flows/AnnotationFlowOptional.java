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
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.jobs.deciders.SkipStepDecider;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_FLOW;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_OPTIONAL_FLOW;

/**
 * Configuration class that defines an annotation process that can be skipped.
 * <p>
 * The flow uses the skipStepDecider to execute or not the pipeline depending 'annotation.skip' flag. In the case
 * that the annotation flag is enabled, then the annotation flow proceeds as described in {@link AnnotationFlow}
 */
@Configuration
@EnableBatchProcessing
@Import({AnnotationFlow.class})
public class AnnotationFlowOptional {

    @Bean(VEP_ANNOTATION_OPTIONAL_FLOW)
    Flow vepAnnotationOptionalFlow(@Qualifier(VEP_ANNOTATION_FLOW) Flow vepAnnotationFlow,
                                   SkipStepDecider skipStepDecider) {
        return new FlowBuilder<Flow>(VEP_ANNOTATION_OPTIONAL_FLOW)
                .start(skipStepDecider).on(SkipStepDecider.DO_STEP)
                .to(vepAnnotationFlow)
                .from(skipStepDecider).on(SkipStepDecider.SKIP_STEP)
                .end(BatchStatus.COMPLETED.toString())
                .build();
    }

    @Bean
    @Scope("prototype")
    SkipStepDecider skipStepDecider(JobOptions jobOptions) {
        return new SkipStepDecider(jobOptions.getPipelineOptions(), JobParametersNames.ANNOTATION_SKIP);
    }

}
