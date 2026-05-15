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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.DatabaseInitializationJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.ArrayList;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_INIT_DATABASE_JOB;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;


/**
 * Test {@link CreateDatabaseIndexesStepConfiguration}
 */
@ExtendWith(SpringExtension.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@ContextConfiguration(classes = {DatabaseInitializationJobConfiguration.class, BatchTestConfiguration.class})
public class CreateDatabaseIndexesStepTest extends MongoTestContainerHelper {
    private static final String DB_NAME = "create-db-index-test-db";

    private static final String COLLECTION_FEATURES_NAME = "features";

    @Autowired
    @Qualifier(JOB_INIT_DATABASE_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    private BatchTestConfiguration batchTestConfiguration;

    private MongoTemplate mongoTemplate;

    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate = batchTestConfiguration.getMongoTemplate(DB_NAME, mongoMappingContext);
        mongoTemplate.getDb().drop();
    }

    @AfterEach
    void cleanDb() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void testIndexesAreCreated() {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(DB_NAME)
                .collectionFeaturesName(COLLECTION_FEATURES_NAME)
                .addString("run.id", UUID.randomUUID().toString())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.CREATE_DATABASE_INDEXES_STEP,
                jobParameters);

        assertCompleted(jobExecution);

        MongoCollection<Document> genesCollection = mongoTemplate.getDb().getCollection(COLLECTION_FEATURES_NAME);

        Document idIndex = new Document("v", "2").append("key", new Document("_id", 1)).append("name", "_id_");
        Document nameIndex = new Document("v", "2").append("key", new Document("name", 1)).append("name", "name_1")
                .append("background", true)
                .append("sparse", true);
        String expectedIndexes = Stream.of(idIndex, nameIndex).map(Object::toString).collect(Collectors.joining());

        String actualIndexes = genesCollection.listIndexes().into(new ArrayList<>()).stream()
                .peek(d -> d.remove("ns"))
                .map(Object::toString)
                .collect(Collectors.joining());
        assertEquals(expectedIndexes, actualIndexes);
    }

    @Test
    public void testNoDuplicatesCanBeInserted() {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(DB_NAME)
                .collectionFeaturesName(COLLECTION_FEATURES_NAME)
                .addString("run.id", UUID.randomUUID().toString())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.CREATE_DATABASE_INDEXES_STEP,
                jobParameters);

        assertCompleted(jobExecution);

        MongoCollection<Document> genesCollection = mongoTemplate.getDb().getCollection(COLLECTION_FEATURES_NAME);
        genesCollection.insertOne(new Document("_id", "example_id"));
        assertThrows(MongoWriteException.class, () -> genesCollection.insertOne(new Document("_id", "example_id")));
    }
}
