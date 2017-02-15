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
package uk.ac.ebi.eva.pipeline.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

/**
 * Modifies the JobOptions instance with job-specific configuration, where only the configuration for the
 * running job should be set. This avoids setting incompatible settings for jobs that won't even be executed.
 * This is achieved with a `beforeJob` listener, which only gets called when a job is going to be executed.
 */
public class VariantOptionsConfigurerListener implements JobExecutionListener {

    private static final Logger logger = LoggerFactory.getLogger(VariantOptionsConfigurerListener.class);
    private final boolean includeSamples;
    private final boolean calculateStats;
    private final boolean includeStats;

    @Autowired
    private JobOptions jobOptions;

    public VariantOptionsConfigurerListener(boolean includeSamples,
                                            boolean calculateStats,
                                            boolean includeStats) {
        this.includeSamples = includeSamples;
        this.calculateStats = calculateStats;
        this.includeStats = includeStats;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        logger.debug("Setting up job " + jobExecution.getJobInstance().getJobName());
        jobOptions.configureGenotypesStorage(includeSamples);
        jobOptions.configureStatisticsStorage(calculateStats, includeStats);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
    }
}
