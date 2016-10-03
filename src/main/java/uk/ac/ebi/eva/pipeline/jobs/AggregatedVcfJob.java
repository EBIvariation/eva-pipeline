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
package uk.ac.ebi.eva.pipeline.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.jobs.steps.VariantsLoad;
import uk.ac.ebi.eva.pipeline.jobs.steps.VariantsTransform;

import javax.annotation.PostConstruct;

/**
 *  Complete pipeline workflow for aggregated VCF.
 *  Aggregated statistics are provided in the VCF instead of the genotypes.
 *
 *  transform ---> load --> (optionalVariantAnnotationFlow: variantsAnnotGenerateInput --> (annotationCreate --> variantAnnotLoad))
 *
 *  Steps in () are optional
 */
@Configuration
@EnableBatchProcessing
@Import({VariantLoaderStep.class, VariantNormalizerStep.class})
public class AggregatedVcfJob extends CommonJobStepInitialization{

    private static final Logger logger = LoggerFactory.getLogger(VariantAggregatedConfiguration.class);

    private static final String jobName = "load-aggregated-vcf";
    public static final String NORMALIZE_VARIANTS = "Normalize variants";
    public static final String LOAD_VARIANTS = "Load variants";

    //job default settings
    private static final boolean INCLUDE_SAMPLES = false;
    private static final boolean COMPRESS_GENOTYPES = false;
    private static final boolean CALCULATE_STATS = true;
    private static final boolean INCLUDE_STATS = true;

    @PostConstruct
    public void configureDefaultVariantOptions() {
        getVariantJobsArgs().configureGenotypesStorage(INCLUDE_SAMPLES, COMPRESS_GENOTYPES);
        getVariantJobsArgs().configureStatisticsStorage(CALCULATE_STATS, INCLUDE_STATS);
    }

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private Flow optionalVariantAnnotationFlow;
    @Autowired
    private VariantLoaderStep variantLoaderStep;

    @Bean
    @Qualifier("variantJobAggregated")
    public Job variantJob() {
        logger.debug("Building variant aggregated job");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        FlowJobBuilder builder = jobBuilder
                .flow(transform())
                .next(load())
                .next(optionalVariantAnnotationFlow)
                .end();

        return builder.build();
    }

    @Bean
    @Scope("prototype")
    protected Step transform() {
        return generateStep(NORMALIZE_VARIANTS, new VariantNormalizerStep(getVariantOptions(), getPipelineOptions()));
    }

    private Step load() {
        return generateStep(LOAD_VARIANTS, variantLoaderStep);
    }

}
