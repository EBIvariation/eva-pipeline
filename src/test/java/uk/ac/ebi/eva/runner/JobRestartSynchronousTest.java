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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.configuration.SynchronousBatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.AbstractJobRestartUtils;

import java.util.UUID;

@ExtendWith(SpringExtension.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@ContextConfiguration(classes = {SynchronousBatchTestConfiguration.class, BatchTestConfiguration.class})
public class JobRestartSynchronousTest extends AbstractJobRestartUtils {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

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
        return new StepBuilder(UUID.randomUUID().toString(), jobRepository).tasklet((contribution, chunkContext) -> {
            throw new RuntimeException("THIS IS A TEST EXCEPTION");
        }, transactionManager).build();
    }

}
