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
package uk.ac.ebi.eva.test.utils;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.DuplicateJobException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.StepRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.ReferenceJobFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;

import uk.ac.ebi.eva.runner.JobRestartAsynchronousTest;

import java.util.Arrays;
import java.util.UUID;

/**
 * Base class for the jobs to check the behaviour of spring boot. This class has all the utility methods to create
 * jobs and steps on demand in the tests without any need to generate or inject them from specific configuration classes
 */
public abstract class AbstractJobRestartUtils {

    @Autowired
    private StepRegistry stepRegistry;

    @Autowired
    private JobRegistry jobRegistry;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public StepBuilderFactory getStepBuilderFactory() {
        return stepBuilderFactory;
    }

    protected JobExecution launchJob(JobLauncherTestUtils jobLauncherTestUtils) throws Exception {
        JobExecution jobExecution;
        jobExecution = jobLauncherTestUtils.launchJob(new JobParameters());
        Thread.sleep(JobRestartAsynchronousTest.INITIALIZE_JOB_SLEEP);
        return jobExecution;
    }

    protected JobLauncherTestUtils getJobLauncherTestUtils(Job job) {
        JobLauncherTestUtils jobLauncherTestUtils = new JobLauncherTestUtils();
        jobLauncherTestUtils.setJobLauncher(jobLauncher);
        jobLauncherTestUtils.setJobRepository(jobRepository);
        jobLauncherTestUtils.setJob(job);
        return jobLauncherTestUtils;
    }

    protected Job getTestJob(Step step, Step... steps) throws DuplicateJobException {
        Job job;
        if (steps == null) {
            job = jobBuilderFactory.get(UUID.randomUUID().toString()).incrementer(new RunIdIncrementer()).start(step)
                    .build();
        } else {
            FlowBuilder<Flow> builder = new FlowBuilder<Flow>(UUID.randomUUID().toString()).start(step);
            for (Step arrayStep : steps) {
                builder = builder.next(arrayStep);
            }
            Flow flow = builder.build();
            job = jobBuilderFactory.get(UUID.randomUUID().toString()).incrementer(new RunIdIncrementer()).start(flow)
                    .build().build();
        }
        jobRegistry.register(new ReferenceJobFactory(job));
        stepRegistry.register(job.getName(), Arrays.asList(step));
        return job;
    }

    protected Step getQuickStep(boolean restartable) {
        return getWaitingStep(restartable, 0L);
    }

    protected Step getWaitingStep(boolean restartable, final long waitTime) {
        return stepBuilderFactory.get(UUID.randomUUID().toString()).tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                if (waitTime > 0) {
                    try {
                        Thread.sleep(waitTime);
                    } catch (Exception e) {
                        //Do nothing
                    }
                }
                return RepeatStatus.FINISHED;
            }
        }).allowStartIfComplete(restartable).build();
    }
}
