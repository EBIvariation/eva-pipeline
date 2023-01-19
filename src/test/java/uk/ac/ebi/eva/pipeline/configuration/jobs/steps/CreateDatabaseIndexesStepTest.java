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

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
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
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.DatabaseInitializationJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.configuration.TemporaryRuleConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;


/**
 * Test {@link CreateDatabaseIndexesStepConfiguration}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {DatabaseInitializationJobConfiguration.class, BatchTestConfiguration.class, TemporaryRuleConfiguration.class})
public class CreateDatabaseIndexesStepTest {

    private static final String COLLECTION_FEATURES_NAME = "features";

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

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

        MongoCollection<Document> genesCollection = mongoRule.getCollection(databaseName, COLLECTION_FEATURES_NAME);

        Document idIndex = new Document("v", "2").append("key", new Document("_id", 1)).append("name", "_id_")
                .append("ns", databaseName + "." + COLLECTION_FEATURES_NAME);
        Document nameIndex = new Document("v", "2").append("key", new Document("name", 1)).append("name", "name_1")
                .append("ns", databaseName + "." + COLLECTION_FEATURES_NAME).append("background", true)
                .append("sparse", true);
        String expectedIndexes = Stream.of(idIndex, nameIndex).map(Object::toString).collect(Collectors.joining());

        String actualIndexes = genesCollection.listIndexes().into(new ArrayList<>()).stream().map(Object::toString)
                .collect(Collectors.joining());
        assertEquals(expectedIndexes, actualIndexes);
    }

    @Test(expected = MongoWriteException.class)
    public void testNoDuplicatesCanBeInserted() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(databaseName)
                .collectionFeaturesName(COLLECTION_FEATURES_NAME)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.CREATE_DATABASE_INDEXES_STEP,
                jobParameters);

        assertCompleted(jobExecution);

        MongoCollection<Document> genesCollection = mongoRule.getCollection(databaseName, COLLECTION_FEATURES_NAME);
        genesCollection.insertOne(new Document("_id", "example_id"));
        genesCollection.insertOne(new Document("_id", "example_id"));
    }
}
