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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.InitDBConfig;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.jobs.InitDBConfiguration;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import static junit.framework.TestCase.assertEquals;

/**
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Test {@link IndexesCreate}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { InitDBConfiguration.class, InitDBConfig.class, JobLauncherTestUtils.class})
public class IndexesCreateTest {

    @Autowired
    public VariantJobsArgs variantJobsArgs;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private String dbName;

    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
        dbName = variantJobsArgs.getDbName();
        JobTestUtils.cleanDBs(dbName);
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
    }

    @Test
    public void testIndexesAreCreated() throws Exception {
        String dbCollectionGenesName = variantJobsArgs.getDbCollectionsFeaturesName();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(InitDBConfiguration.CREATE_DATABASE_INDEXES);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        MongoClient mongoClient = new MongoClient();
        DBCollection genesCollection = mongoClient.getDB(dbName).getCollection(dbCollectionGenesName);
        assertEquals("[{ \"v\" : 1 , \"key\" : { \"_id\" : 1} , \"name\" : \"_id_\" , \"ns\" : \"" + dbName +
                        "." + dbCollectionGenesName +
                        "\"}, { \"v\" : 1 , \"key\" : { \"name\" : 1} , \"name\" : \"name_1\" , \"ns\" : \"" +
                        dbName + "." + dbCollectionGenesName + "\" , \"sparse\" : true , \"background\" : true}]",
                genesCollection.getIndexInfo().toString());
    }

    @Test(expected = DuplicateKeyException.class)
    public void testNoDuplicatesCanBeInserted() throws Exception {
        String dbCollectionGenesName = variantJobsArgs.getDbCollectionsFeaturesName();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(InitDBConfiguration.CREATE_DATABASE_INDEXES);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());


        MongoClient mongoClient = new MongoClient();
        DBCollection genesCollection = mongoClient.getDB(dbName).getCollection(dbCollectionGenesName);
        genesCollection.insert(new BasicDBObject("_id", "example_id"));
        genesCollection.insert(new BasicDBObject("_id", "example_id"));
    }
}
