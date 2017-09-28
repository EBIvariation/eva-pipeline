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

import com.mongodb.DBCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.DropStudyJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.DropStudyJobTestUtils.assertDropFiles;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

/**
 * Test for {@link DropFilesByStudyStepConfiguration}
 */
@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {DropStudyJobConfiguration.class, BatchTestConfiguration.class})
public class DropFilesByStudyStepTest {

    private static final String COLLECTION_FILES_NAME = "files";

    private static final long EXPECTED_FILES_AFTER_DROP_STUDY = 1;

    private static final String STUDY_ID_TO_DROP = "studyToDrop";

    private static final String OTHER_STUDY_ID = "otherStudy";

    private static final String FILES_DOCUMENT = JobTestUtils.buildFilesDocumentString(STUDY_ID_TO_DROP, "fileOne");

    private static final String OTHER_FILES_DOCUMENT = JobTestUtils.buildFilesDocumentString(STUDY_ID_TO_DROP,
            "fileTwo");

    private static final String OTHER_STUDY_FILES_DOCUMENT = JobTestUtils.buildFilesDocumentString(OTHER_STUDY_ID,
            "fileThree");

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void testNoFilesToDrop() throws Exception {
        String databaseName = mongoRule.createDBAndInsertDocuments(COLLECTION_FILES_NAME,
                Collections.singletonList(OTHER_STUDY_FILES_DOCUMENT));

        checkDrop(databaseName, EXPECTED_FILES_AFTER_DROP_STUDY);
    }

    @Test
    public void testOneFileToDrop() throws Exception {
        String databaseName = mongoRule.createDBAndInsertDocuments(COLLECTION_FILES_NAME,
                Arrays.asList(FILES_DOCUMENT, OTHER_STUDY_FILES_DOCUMENT));

        checkDrop(databaseName, EXPECTED_FILES_AFTER_DROP_STUDY);
    }

    @Test
    public void testSeveralFilesToDrop() throws Exception {
        String databaseName = mongoRule.createDBAndInsertDocuments(COLLECTION_FILES_NAME,
                Arrays.asList(FILES_DOCUMENT, OTHER_FILES_DOCUMENT, OTHER_STUDY_FILES_DOCUMENT));

        checkDrop(databaseName, EXPECTED_FILES_AFTER_DROP_STUDY);
    }

    private void checkDrop(String databaseName, long expectedFilesAfterDropStudy) {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .databaseName(databaseName)
                .inputStudyId(STUDY_ID_TO_DROP)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.DROP_FILES_BY_STUDY_STEP,
                jobParameters);

        assertCompleted(jobExecution);

        DBCollection filesCollection = mongoRule.getCollection(databaseName, COLLECTION_FILES_NAME);
        assertDropFiles(filesCollection, STUDY_ID_TO_DROP, expectedFilesAfterDropStudy);
    }

}
