/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.configuration.TemporaryRuleConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {DatabaseInitializationJobConfiguration.class, BatchTestConfiguration.class, TemporaryRuleConfiguration.class})
public class DatabaseInitializationJobTest {

    private static final String COLLECTION_FEATURES_NAME = "features";

    private static final String DATABASE_NAME = "databaseInitializationTestDb";

    private static final String INPUT_FILE = "/input-files/gtf/small_sample.gtf.gz";

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void testDatabaseInitializationJob() throws Exception {
        File inputFile = getResource(INPUT_FILE);
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(DATABASE_NAME)
                .inputGtf(inputFile.getAbsolutePath())
                .collectionFeaturesName(COLLECTION_FEATURES_NAME)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertCompleted(jobExecution);
        assertEquals(252L, mongoRule.getCollection(DATABASE_NAME, COLLECTION_FEATURES_NAME).count());
    }
}
