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
import uk.ac.ebi.eva.pipeline.configuration.jobs.DropStudyJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_DROP_STUDY_JOB;
import static uk.ac.ebi.eva.test.utils.DropStudyJobTestUtils.assertDropFiles;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

/**
 * Test for {@link DropFilesByStudyStepConfiguration}
 */
@ExtendWith(SpringExtension.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@ContextConfiguration(classes = {DropStudyJobConfiguration.class, BatchTestConfiguration.class})
public class DropFilesByStudyStepTest extends MongoTestContainerHelper {

    private static final String COLLECTION_FILES_NAME = "files";

    private static final long EXPECTED_FILES_AFTER_DROP_STUDY = 1;

    private static final String STUDY_ID_TO_DROP = "studyToDrop";

    private static final String OTHER_STUDY_ID = "otherStudy";

    private static final Document FILES_DOCUMENT = JobTestUtils.buildFilesDocument(STUDY_ID_TO_DROP, "fileOne");

    private static final Document OTHER_FILES_DOCUMENT = JobTestUtils.buildFilesDocument(STUDY_ID_TO_DROP,
            "fileTwo");

    private static final Document OTHER_STUDY_FILES_DOCUMENT = JobTestUtils.buildFilesDocument(OTHER_STUDY_ID,
            "fileThree");

    private static final String DB_NAME = "drop-files-by-study-test-db";

    @Autowired
    @Qualifier(JOB_DROP_STUDY_JOB)
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
    public void testNoFilesToDrop() {
        mongoTemplate.getDb().getCollection(COLLECTION_FILES_NAME).insertMany(
                Collections.singletonList(OTHER_STUDY_FILES_DOCUMENT));

        checkDrop(EXPECTED_FILES_AFTER_DROP_STUDY);
    }

    @Test
    public void testOneFileToDrop() {
        mongoTemplate.getDb().getCollection(COLLECTION_FILES_NAME).insertMany(
                Arrays.asList(FILES_DOCUMENT, OTHER_STUDY_FILES_DOCUMENT));

        checkDrop(EXPECTED_FILES_AFTER_DROP_STUDY);
    }

    @Test
    public void testSeveralFilesToDrop() {
        mongoTemplate.getDb().getCollection(COLLECTION_FILES_NAME).insertMany(
                Arrays.asList(FILES_DOCUMENT, OTHER_FILES_DOCUMENT, OTHER_STUDY_FILES_DOCUMENT));

        checkDrop(EXPECTED_FILES_AFTER_DROP_STUDY);
    }

    private void checkDrop(long expectedFilesAfterDropStudy) {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .databaseName(DB_NAME)
                .inputStudyId(STUDY_ID_TO_DROP)
                .addString("run.id", UUID.randomUUID().toString())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.DROP_FILES_BY_STUDY_STEP,
                jobParameters);

        assertCompleted(jobExecution);

        MongoCollection<Document> filesCollection = mongoTemplate.getDb().getCollection(COLLECTION_FILES_NAME);
        assertDropFiles(filesCollection, STUDY_ID_TO_DROP, expectedFilesAfterDropStudy);
    }

}
