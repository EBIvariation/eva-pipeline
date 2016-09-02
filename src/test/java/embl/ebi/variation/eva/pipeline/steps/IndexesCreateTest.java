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
package embl.ebi.variation.eva.pipeline.steps;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import embl.ebi.variation.eva.VariantJobsArgs;
import embl.ebi.variation.eva.pipeline.jobs.InitDBConfiguration;
import embl.ebi.variation.eva.pipeline.config.InitDBConfig;
import embl.ebi.variation.eva.pipeline.jobs.JobTestUtils;
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
import embl.ebi.variation.eva.pipeline.steps.tasklet.IndexesCreate;

import static junit.framework.TestCase.assertEquals;

/**
 * Created by jmmut on 2016-08-26.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
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
        dbName = variantJobsArgs.getPipelineOptions().getString("db.name");
        JobTestUtils.cleanDBs(dbName);
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
    }

    @Test
    public void testIndexesAreCreated() throws Exception {
        String dbCollectionGenesName = variantJobsArgs.getPipelineOptions().getString("db.collections.features.name");
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

    @Test(expected = MongoException.DuplicateKey.class)
    public void testNoDuplicatesCanBeInserted() throws Exception {
        String dbCollectionGenesName = variantJobsArgs.getPipelineOptions().getString("db.collections.features.name");
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(InitDBConfiguration.CREATE_DATABASE_INDEXES);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());


        MongoClient mongoClient = new MongoClient();
        DBCollection genesCollection = mongoClient.getDB(dbName).getCollection(dbCollectionGenesName);
        genesCollection.insert(new BasicDBObject("_id", "example_id"));
        genesCollection.insert(new BasicDBObject("_id", "example_id"));
    }
}
