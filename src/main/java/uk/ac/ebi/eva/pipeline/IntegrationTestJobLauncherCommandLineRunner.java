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
package uk.ac.ebi.eva.pipeline;

import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import uk.ac.ebi.eva.pipeline.parameters.ParametersFromProperties;

/**
 * Custom JobLauncherCommandLineRunner that retrieves all known jobParameters from the application context
 * and injects them in the running instance of the job. Used for integration tests.
 */
@Component
@Profile("integrationTest")
public class IntegrationTestJobLauncherCommandLineRunner extends JobLauncherCommandLineRunner {

    @Value("${spring.batch.job.names:#{null}}")
    private String springBatchJob;

    @Autowired
    private ParametersFromProperties parametersFromProperties;

    public IntegrationTestJobLauncherCommandLineRunner(JobLauncher jobLauncher, JobExplorer jobExplorer) {
        super(jobLauncher, jobExplorer);
    }

    public void run(String... args) throws JobExecutionException {
        if (springBatchJob != null) {
            setJobNames(springBatchJob);
        }
        launchJobFromProperties(parametersFromProperties.getProperties());
    }

}
