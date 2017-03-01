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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.jobs.flows.AnnotationFlow;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATE_VARIANTS_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_FLOW;

/**
 * Batch class to wire together:
 * 1) generateVepInputStep - Dump a list of variants without annotations to be used as input for VEP
 * 2) annotationCreate - run VEP
 * 3) annotationLoadBatchStep - Load VEP annotations into mongo
 * <p>
 * Optional flow: variantsAnnotGenerateInput --> (annotationCreate --> annotationLoad)
 * annotationCreate and annotationLoad steps are only executed if variantsAnnotGenerateInput is generating a
 * non-empty VEP input file
 *
 * TODO add a new AnnotationJobParametersValidator
 */

@Configuration
@EnableBatchProcessing
@Import({AnnotationFlow.class})
public class AnnotationJob {

    private static final Logger logger = LoggerFactory.getLogger(AnnotationJob.class);

    @Autowired
    @Qualifier(VEP_ANNOTATION_FLOW)
    private Flow annotation;

    @Bean(ANNOTATE_VARIANTS_JOB)
    @Scope("prototype")
    public Job annotateVariantsJob(JobBuilderFactory jobBuilderFactory) {
        logger.debug("Building '" + ANNOTATE_VARIANTS_JOB + "'");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(ANNOTATE_VARIANTS_JOB)
                .incrementer(new RunIdIncrementer());
        return jobBuilder.start(annotation).build().build();
    }

}
