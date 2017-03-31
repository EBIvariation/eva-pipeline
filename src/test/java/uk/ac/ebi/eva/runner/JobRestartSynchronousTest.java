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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.test.configuration.SynchronousBatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.AbstractJobRestartUtils;

import java.util.UUID;

/**
 * Test to check launcher behaviour in synchronous cases.
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {SynchronousBatchTestConfiguration.class})
public class JobRestartSynchronousTest extends AbstractJobRestartUtils {

    @Test
    public void runCompleteJobTwiceWithSameParameters() throws Exception {
        JobLauncherTestUtils jobLauncherTestUtils = getJobLauncherTestUtils(getTestJob(getQuickStep(false)));
        jobLauncherTestUtils.launchJob(new JobParameters());
        jobLauncherTestUtils.launchJob(new JobParameters());
    }

    @Test
    public void runFailedJobTwiceWithSameParameters() throws Exception {
        JobLauncherTestUtils jobLauncherTestUtils = getJobLauncherTestUtils(getTestJob(getTestExceptionStep()));
        jobLauncherTestUtils.launchJob(new JobParameters());
        jobLauncherTestUtils.launchJob(new JobParameters());
    }

    private Step getTestExceptionStep() {
        return getStepBuilderFactory().get(UUID.randomUUID().toString()).tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                throw new RuntimeException("THIS IS A TEST EXCEPTION");
            }
        }).build();
    }

}
