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
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.DropFilesByStudyStepConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.DropVariantsByStudyStepConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.PullFilesAndStatisticsByStudyStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.NewJobIncrementer;
import uk.ac.ebi.eva.pipeline.parameters.validation.job.DropStudyJobParametersValidator;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.DROP_FILES_BY_STUDY_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.DROP_VARIANTS_BY_STUDY_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.DROP_STUDY_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.PULL_FILES_AND_STATISTICS_BY_STUDY_STEP;

/**
 * Job that removes a study from the database. Given a study to remove:
 * <p>
 * remove variants in single study --> pull study entries from the rest of variants --> remove file entry in files collection
 */
@Configuration
@EnableBatchProcessing
@Import({DropVariantsByStudyStepConfiguration.class, PullFilesAndStatisticsByStudyStepConfiguration.class, DropFilesByStudyStepConfiguration.class})
public class DropStudyJobConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(DropStudyJobConfiguration.class);

    @Autowired
    @Qualifier(DROP_VARIANTS_BY_STUDY_STEP)
    private Step dropVariantsByStudyStep;

    @Autowired
    @Qualifier(PULL_FILES_AND_STATISTICS_BY_STUDY_STEP)
    private Step dropVariantsAndStatisticsByStudyStep;

    @Autowired
    @Qualifier(DROP_FILES_BY_STUDY_STEP)
    private Step dropFileStep;

    @Bean(DROP_STUDY_JOB)
    @Scope("prototype")
    public Job dropStudyJob(JobBuilderFactory jobBuilderFactory) {
        logger.debug("Building '" + DROP_STUDY_JOB + "'");

        JobBuilder jobBuilder = jobBuilderFactory
                .get(DROP_STUDY_JOB)
                .incrementer(new NewJobIncrementer())
                .validator(new DropStudyJobParametersValidator());

        SimpleJobBuilder builder = jobBuilder
                .start(dropVariantsByStudyStep)
                .next(dropVariantsAndStatisticsByStudyStep)
                .next(dropFileStep);

        return builder.build();
    }
}
