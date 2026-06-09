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
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.DuplicateJobException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.StepRegistry;
import org.springframework.batch.core.configuration.support.ReferenceJobFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    private PlatformTransactionManager transactionManager;

    @Autowired
    private JobLauncher jobLauncher;

    protected JobLauncherTestUtils getJobLauncherTestUtils(Job job) {
        JobLauncherTestUtils jobLauncherTestUtils = new JobLauncherTestUtils();
        jobLauncherTestUtils.setJobLauncher(jobLauncher);
        jobLauncherTestUtils.setJobRepository(jobRepository);
        jobLauncherTestUtils.setJob(job);
        return jobLauncherTestUtils;
    }

    protected Job getTestJob(Step step, Step... steps) throws DuplicateJobException {
        String jobName = UUID.randomUUID().toString();
        Job job;
        if (steps == null || steps.length == 0) {
            job = new JobBuilder(jobName, jobRepository).incrementer(new RunIdIncrementer()).start(step)
                    .build();
        } else {
            FlowBuilder<Flow> builder = new FlowBuilder<Flow>(UUID.randomUUID().toString()).start(step);
            for (Step arrayStep : steps) {
                builder = builder.next(arrayStep);
            }
            Flow flow = builder.build();
            job = new JobBuilder(jobName, jobRepository).incrementer(new RunIdIncrementer()).start(flow)
                    .build().build();
        }
        jobRegistry.register(new ReferenceJobFactory(job));
        List<Step> registeredSteps = new ArrayList<>();
        registeredSteps.add(step);
        if (steps != null && steps.length > 0) {
            registeredSteps.addAll(Arrays.asList(steps));
        }
        stepRegistry.register(job.getName(), registeredSteps);
        return job;
    }

    protected Step getQuickStep(boolean restartable) {
        return getWaitingStep(restartable, 0L);
    }

    protected Step getWaitingStep(boolean restartable, final long waitTime) {
        return new StepBuilder(UUID.randomUUID().toString(), jobRepository).tasklet((contribution, chunkContext) -> {
            if (waitTime > 0) {
                try {
                    Thread.sleep(waitTime);
                } catch (Exception e) {
                    // Do nothing
                }
            }
            return RepeatStatus.FINISHED;
        }, transactionManager).allowStartIfComplete(restartable).build();
    }
}