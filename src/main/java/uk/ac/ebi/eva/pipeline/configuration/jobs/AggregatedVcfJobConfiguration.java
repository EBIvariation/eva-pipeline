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
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import uk.ac.ebi.eva.pipeline.configuration.jobs.flows.AnnotationFlowOptionalConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadFileStepConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadVariantsStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.NewJobIncrementer;
import uk.ac.ebi.eva.pipeline.parameters.validation.job.AggregatedVcfJobParametersValidator;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.AGGREGATED_VCF_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_FILE_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_VARIANTS_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_OPTIONAL_FLOW;

/**
 * Complete pipeline workflow for aggregated VCF. Aggregated statistics are provided in the VCF instead of the
 * genotypes.
 * <p>
 * load --> (optionalAnnotationFlow: variantsAnnotGenerateInput --> (annotationCreate --> annotationLoad))
 * <p>
 * Steps in () are optional
 */
@Configuration
@EnableBatchProcessing
@Import({LoadVariantsStepConfiguration.class, LoadFileStepConfiguration.class, AnnotationFlowOptionalConfiguration.class})
public class AggregatedVcfJobConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(AggregatedVcfJobConfiguration.class);

    @Autowired
    @Qualifier(VEP_ANNOTATION_OPTIONAL_FLOW)
    private Flow annotationFlowOptional;

    @Autowired
    @Qualifier(LOAD_VARIANTS_STEP)
    private Step variantLoaderStep;

    @Autowired
    @Qualifier(LOAD_FILE_STEP)
    private Step loadFileStep;

    @Bean(AGGREGATED_VCF_JOB)
    @Scope("prototype")
    public Job aggregatedVcfJob(JobBuilderFactory jobBuilderFactory) {
        logger.debug("Building '" + AGGREGATED_VCF_JOB + "'");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(AGGREGATED_VCF_JOB)
                .incrementer(new NewJobIncrementer())
                .validator(new AggregatedVcfJobParametersValidator());
        FlowJobBuilder builder = jobBuilder
                .flow(variantLoaderStep)
                .next(loadFileStep)
                .next(annotationFlowOptional)
                .end();

        return builder.build();
    }
}
