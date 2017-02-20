/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.test.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.StepRunner;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is a simplified version of the official JobLauncherTestUtils to avoid the injection of full jobs when we
 * want to test only steps. It offers the same exection functions for
 * {@link org.springframework.batch.core.step.AbstractStep}
 */
public class StepLauncherTestUtils {

    private static final long JOB_PARAMETER_MAXIMUM = 1000000;

    private static final Logger logger = LoggerFactory.getLogger(JobTestUtils.class);

    private JobLauncher jobLauncher;

    private Step step;

    private JobRepository jobRepository;

    private StepRunner stepRunner;

    /**
     * The Step instance that can be manipulated (e.g. launched) in this utility.
     *
     * @param step the {@link org.springframework.batch.core.step.AbstractStep} to use
     */
    @Autowired
    public void setStep(Step step) {
        this.step = step;
    }

    /**
     * @return the Step
     */
    public Step getStep() {
        return step;
    }

    /**
     * The {@link JobRepository} to use for creating new {@link JobExecution}
     * instances.
     *
     * @param jobRepository a {@link JobRepository}
     */
    @Autowired
    public void setJobRepository(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    /**
     * @return the job repository
     */
    public JobRepository getJobRepository() {
        return jobRepository;
    }

    /**
     * A {@link JobLauncher} instance that can be used to launch jobs.
     *
     * @param jobLauncher a job launcher
     */
    @Autowired
    public void setJobLauncher(JobLauncher jobLauncher) {
        this.jobLauncher = jobLauncher;
    }

    /**
     * @return the job launcher
     */
    public JobLauncher getJobLauncher() {
        return jobLauncher;
    }

    /**
     * @return a new JobParameters object containing only a parameter for the
     * current timestamp, to ensure that the job instance will be unique.
     */
    public JobParameters getUniqueJobParameters() {
        Map<String, JobParameter> parameters = new HashMap<String, JobParameter>();
        parameters.put("random", new JobParameter((long) (Math.random() * JOB_PARAMETER_MAXIMUM)));
        return new JobParameters(parameters);
    }

    /**
     * Convenient method for subclasses to grab a {@link StepRunner} for running
     * steps by name.
     *
     * @return a {@link StepRunner}
     */
    protected StepRunner getStepRunner() {
        if (this.stepRunner == null) {
            this.stepRunner = new StepRunner(getJobLauncher(), getJobRepository());
        }
        return this.stepRunner;
    }

    /**
     * Launch just the specified step in the job. A unique set of JobParameters
     * will automatically be generated. An IllegalStateException is thrown if
     * there is no Step with the given name.
     *
     * @param stepName The name of the step to launch
     * @return JobExecution
     */
    public JobExecution launchStep(String stepName) {
        return this.launchStep(stepName, this.getUniqueJobParameters(), null);
    }

    /**
     * Launch just the specified step in the job. A unique set of JobParameters
     * will automatically be generated. An IllegalStateException is thrown if
     * there is no Step with the given name.
     *
     * @param stepName            The name of the step to launch
     * @param jobExecutionContext An ExecutionContext whose values will be
     *                            loaded into the Job ExecutionContext prior to launching the step.
     * @return JobExecution
     */
    public JobExecution launchStep(String stepName, ExecutionContext jobExecutionContext) {
        return this.launchStep(stepName, this.getUniqueJobParameters(), jobExecutionContext);
    }

    /**
     * Launch just the specified step in the job. An IllegalStateException is
     * thrown if there is no Step with the given name.
     *
     * @param stepName      The name of the step to launch
     * @param jobParameters The JobParameters to use during the launch
     * @return JobExecution
     */
    public JobExecution launchStep(String stepName, JobParameters jobParameters) {
        return this.launchStep(stepName, jobParameters, null);
    }

    /**
     * Launch just the specified step in the job. An IllegalStateException is
     * thrown if there is no Step with the given name.
     *
     * @param stepName            The name of the step to launch
     * @param jobParameters       The JobParameters to use during the launch
     * @param jobExecutionContext An ExecutionContext whose values will be
     *                            loaded into the Job ExecutionContext prior to launching the step.
     * @return JobExecution
     */
    public JobExecution launchStep(String stepName, JobParameters jobParameters, ExecutionContext jobExecutionContext) {
        return getStepRunner().launchStep(step, jobParameters, jobExecutionContext);
    }
}
