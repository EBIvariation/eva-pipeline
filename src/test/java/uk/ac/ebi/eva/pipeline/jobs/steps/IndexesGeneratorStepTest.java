/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.DatabaseInitializationConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.DatabaseInitializationJob;
import uk.ac.ebi.eva.test.rules.TemporalMongoRule;

import static org.junit.Assert.assertEquals;


/**
 * Test {@link IndexesGeneratorStep}
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {DatabaseInitializationJob.class, DatabaseInitializationConfiguration.class, JobLauncherTestUtils.class})
public class IndexesGeneratorStepTest {

    private static final String DATABASE_NAME = IndexesGeneratorStepTest.class.getSimpleName();

    @Rule
    public TemporalMongoRule mongoRule = new TemporalMongoRule();

    @Autowired
    public JobOptions jobOptions;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
        jobOptions.setDbName(DATABASE_NAME);
    }

    @Test
    public void testIndexesAreCreated() throws Exception {
        mongoRule.getTemporalDatabase(DATABASE_NAME);
        String dbCollectionGenesName = jobOptions.getDbCollectionsFeaturesName();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DatabaseInitializationJob.CREATE_DATABASE_INDEXES);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        MongoClient mongoClient = new MongoClient();
        DBCollection genesCollection = mongoClient.getDB(DATABASE_NAME).getCollection(dbCollectionGenesName);
        assertEquals("[{ \"v\" : 1 , \"key\" : { \"_id\" : 1} , \"name\" : \"_id_\" , \"ns\" : \"" + DATABASE_NAME +
                        "." + dbCollectionGenesName +
                        "\"}, { \"v\" : 1 , \"key\" : { \"name\" : 1} , \"name\" : \"name_1\" , \"ns\" : \"" +
                        DATABASE_NAME + "." + dbCollectionGenesName + "\" , \"sparse\" : true , \"background\" : true}]",
                genesCollection.getIndexInfo().toString());
    }

    @Test(expected = DuplicateKeyException.class)
    public void testNoDuplicatesCanBeInserted() throws Exception {
        mongoRule.getTemporalDatabase(DATABASE_NAME);
        String dbCollectionGenesName = jobOptions.getDbCollectionsFeaturesName();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DatabaseInitializationJob.CREATE_DATABASE_INDEXES);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());


        MongoClient mongoClient = new MongoClient();
        DBCollection genesCollection = mongoClient.getDB(DATABASE_NAME).getCollection(dbCollectionGenesName);
        genesCollection.insert(new BasicDBObject("_id", "example_id"));
        genesCollection.insert(new BasicDBObject("_id", "example_id"));
    }
}
