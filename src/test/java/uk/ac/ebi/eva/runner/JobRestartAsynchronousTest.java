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
package uk.ac.ebi.eva.runner;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.test.configuration.AsynchronousBatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.AbstractJobRestartUtils;

/**
 * Test to check launcher behaviour in Asynchronous cases.
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {AsynchronousBatchTestConfiguration.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class JobRestartAsynchronousTest extends AbstractJobRestartUtils {

    // Wait until the job has been launched properly. The launch operation is not transactional, and other
    // instances of the same job with the same parameter can throw exceptions in this interval.
    public static final int INITIALIZE_JOB_SLEEP = 100;
    public static final int STEP_TIME_DURATION = 1000;
    public static final int WAIT_FOR_JOB_TO_END = 2000;

    @Autowired
    private JobOperator jobOperator;

    @Test(expected = JobExecutionAlreadyRunningException.class)
    public void runSameJobWhileExecutingThrowsException() throws Exception {
        JobLauncherTestUtils jobLauncherTestUtils = getJobLauncherTestUtils(getTestJob(
                getWaitingStep(false, STEP_TIME_DURATION)));
        launchJob(jobLauncherTestUtils);
        launchJob(jobLauncherTestUtils);
        Thread.sleep(WAIT_FOR_JOB_TO_END);
    }

    @Test(expected = JobExecutionAlreadyRunningException.class)
    public void cantRunSecondJobEvenIfFirstIsStopped() throws Exception {
        JobLauncherTestUtils jobLauncherTestUtils = getJobLauncherTestUtils(getTestJob(
                getWaitingStep(false, STEP_TIME_DURATION)));
        JobExecution jobExecution = launchJob(jobLauncherTestUtils);

        jobOperator.stop(jobExecution.getJobId());
        jobLauncherTestUtils.launchJob(new JobParameters());
        Thread.sleep(WAIT_FOR_JOB_TO_END);
    }

    @Test
    public void jobStoppedAndAbandonedCanBeStarted() throws Exception {
        JobLauncherTestUtils jobLauncherTestUtils = getJobLauncherTestUtils(getTestJob(
                getWaitingStep(false, STEP_TIME_DURATION)));
        JobExecution jobExecution = launchJob(jobLauncherTestUtils);
        jobOperator.stop(jobExecution.getJobId());
        jobOperator.abandon(jobExecution.getJobId());
        Thread.sleep(WAIT_FOR_JOB_TO_END);
        jobLauncherTestUtils.launchJob(new JobParameters());
        Thread.sleep(WAIT_FOR_JOB_TO_END);
    }

    @Test
    public void abandonedJobsRequireStepsMarkedAsRestartable() throws Exception {
        Job job = getTestJob(getQuickStep(false), getWaitingStep(false, STEP_TIME_DURATION));
        JobLauncherTestUtils jobLauncherTestUtils = getJobLauncherTestUtils(job);
        JobExecution jobExecution = launchJob(jobLauncherTestUtils);
        Thread.sleep(INITIALIZE_JOB_SLEEP);
        jobOperator.stop(jobExecution.getJobId());
        jobOperator.abandon(jobExecution.getJobId());
        Thread.sleep(WAIT_FOR_JOB_TO_END);
        jobExecution = launchJob(jobLauncherTestUtils);
        Thread.sleep(WAIT_FOR_JOB_TO_END);
        Assert.assertTrue(jobExecution.getStepExecutions().isEmpty());
    }

    @Test
    public void abandonJobsMarkedAsRestartableRestartAllSteps() throws Exception {
        Job job = getTestJob(getQuickStep(true), getWaitingStep(true, STEP_TIME_DURATION));
        JobLauncherTestUtils jobLauncherTestUtils = getJobLauncherTestUtils(job);
        JobExecution jobExecution = launchJob(jobLauncherTestUtils);
        Thread.sleep(INITIALIZE_JOB_SLEEP);
        jobOperator.stop(jobExecution.getJobId());
        jobOperator.abandon(jobExecution.getJobId());
        Thread.sleep(WAIT_FOR_JOB_TO_END);
        jobExecution = launchJob(jobLauncherTestUtils);
        Thread.sleep(WAIT_FOR_JOB_TO_END);
        Assert.assertFalse(jobExecution.getStepExecutions().isEmpty());
    }

}
