/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.parameters.ParametersFromProperties;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

/**
 * The purpose of this test is to imitate an execution made by an user through the CLI.
 * This is needed because all the other tests just instantiate what they need (just one step, or just one job) and
 * sometimes we have errors due to collisions instantiating several jobs. This test should instantiate everything
 * Spring instantiates in a real execution.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"integrationTest,test,mongo"})
@TestPropertySource({"classpath:test-mongo.properties"})
public class ApplicationTest {

    /**
     * Used for .getJobInstances(). It asks for a jobInstanceId and the count of job instances to return.
     */
    private static final int FIRST_JOB_INSTANCE = 0;

    private static final int EXPECTED_JOB_COUNT = 1;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private ParametersFromProperties parametersFromProperties;

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Test
    public void main() throws Exception {
        mongoRule.getTemporaryDatabase(parametersFromProperties.getDatabaseName());

        Assert.assertEquals(EXPECTED_JOB_COUNT, jobExplorer.getJobNames().size());
        Assert.assertEquals(BeanNames.GENOTYPED_VCF_JOB, jobExplorer.getJobNames().get(0));
        Assert.assertEquals(EXPECTED_JOB_COUNT, jobExplorer.getJobInstanceCount(BeanNames.GENOTYPED_VCF_JOB));

        JobInstance jobInstance = jobExplorer.getJobInstances(
                BeanNames.GENOTYPED_VCF_JOB, FIRST_JOB_INSTANCE, EXPECTED_JOB_COUNT).get(0);

        JobExecution jobExecution = jobExplorer.getJobExecution(jobInstance.getInstanceId());
        Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
    }
}
