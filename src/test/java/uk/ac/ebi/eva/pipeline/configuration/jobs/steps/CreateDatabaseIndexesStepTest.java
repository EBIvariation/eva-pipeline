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
package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DuplicateKeyException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.DatabaseInitializationJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;


/**
 * Test {@link CreateDatabaseIndexesStepConfiguration}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {DatabaseInitializationJobConfiguration.class, BatchTestConfiguration.class})
public class CreateDatabaseIndexesStepTest {

    private static final String COLLECTION_FEATURES_NAME = "features";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void testIndexesAreCreated() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(databaseName)
                .collectionFeaturesName(COLLECTION_FEATURES_NAME)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.CREATE_DATABASE_INDEXES_STEP,
                jobParameters);

        assertCompleted(jobExecution);

        DBCollection genesCollection = mongoRule.getCollection(databaseName, COLLECTION_FEATURES_NAME);
        assertEquals("[{ \"v\" : 1 , \"key\" : { \"_id\" : 1} , \"name\" : \"_id_\" , \"ns\" : \"" +
                        databaseName + "." + COLLECTION_FEATURES_NAME +
                        "\"}, { \"v\" : 1 , \"key\" : { \"name\" : 1} , \"name\" : \"name_1\" , \"ns\" : \"" +
                        databaseName + "." + COLLECTION_FEATURES_NAME + "\" , \"sparse\" : true , \"background\" : true}]",
                genesCollection.getIndexInfo().toString());
    }

    @Test(expected = DuplicateKeyException.class)
    public void testNoDuplicatesCanBeInserted() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(databaseName)
                .collectionFeaturesName(COLLECTION_FEATURES_NAME)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.CREATE_DATABASE_INDEXES_STEP,
                jobParameters);

        assertCompleted(jobExecution);

        DBCollection genesCollection = mongoRule.getCollection(databaseName, COLLECTION_FEATURES_NAME);
        genesCollection.insert(new BasicDBObject("_id", "example_id"));
        genesCollection.insert(new BasicDBObject("_id", "example_id"));
    }
}
