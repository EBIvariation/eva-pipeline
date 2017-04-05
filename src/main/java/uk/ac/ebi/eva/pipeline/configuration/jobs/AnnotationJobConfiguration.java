/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.configuration.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import uk.ac.ebi.eva.pipeline.configuration.jobs.flows.AnnotationFlowConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.NewJobIncrementer;
import uk.ac.ebi.eva.pipeline.parameters.validation.job.AnnotationJobParametersValidator;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATE_VARIANTS_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_FLOW;

/**
 * Batch class to wire together:
 * 1) generateVepInputStep - Dump a list of variants without annotations and run VEP with them
 * 3) annotationLoadBatchStep - Load VEP annotations into mongo
 * <p>
 * Optional flow: variantsAnnotGenerateInput --> (annotationLoad)
 * annotationLoad step is only executed if variantsAnnotGenerateInput is generating a
 * non-empty VEP input file
 *
 * TODO add a new AnnotationJobParametersValidator
 */

@Configuration
@EnableBatchProcessing
@Import({AnnotationFlowConfiguration.class})
public class AnnotationJobConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(AnnotationJobConfiguration.class);

    @Autowired
    @Qualifier(VEP_ANNOTATION_FLOW)
    private Flow annotation;

    @Bean(ANNOTATE_VARIANTS_JOB)
    @Scope("prototype")
    public Job annotateVariantsJob(JobBuilderFactory jobBuilderFactory) {
        logger.debug("Building '" + ANNOTATE_VARIANTS_JOB + "'");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(ANNOTATE_VARIANTS_JOB)
                .incrementer(new NewJobIncrementer())
                .validator(new AnnotationJobParametersValidator());
        return jobBuilder.start(annotation).build().build();
    }

}
