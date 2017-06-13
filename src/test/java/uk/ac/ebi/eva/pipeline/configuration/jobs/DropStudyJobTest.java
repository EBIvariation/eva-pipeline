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

package uk.ac.ebi.eva.pipeline.configuration.jobs;

import com.mongodb.DBCollection;
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
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.DropStudyJobTestUtils.assertDropFiles;
import static uk.ac.ebi.eva.test.utils.DropStudyJobTestUtils.assertDropVariantsByStudy;
import static uk.ac.ebi.eva.test.utils.DropStudyJobTestUtils.assertPullStudy;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

/**
 * Test for {@link DropStudyJobConfiguration}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {DropStudyJobConfiguration.class, BatchTestConfiguration.class})
public class DropStudyJobTest {

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String COLLECTION_FILES_NAME = "files";

    private static final long EXPECTED_FILES_AFTER_DROP_STUDY = 1;

    private static final long EXPECTED_VARIANTS_AFTER_DROP_STUDY = 2;

    private static final long EXPECTED_FILE_COUNT = 0;

    private static final long EXPECTED_STATS_COUNT = 0;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private static final String STUDY_ID_TO_DROP = "studyIdToDrop";

    @Test
    public void fullDropStudyJob() throws Exception {
        String dbName = mongoRule.createDBAndInsertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithOneStudyToDrop(),
                VariantData.getOtherVariantWithOneStudyToDrop(),
                VariantData.getVariantWithOneStudy(),
                VariantData.getVariantWithTwoStudies()));

        mongoRule.insertDocuments(dbName, COLLECTION_FILES_NAME, Arrays.asList(
                JobTestUtils.buildFilesDocumentString(STUDY_ID_TO_DROP, "fileIdOne"),
                JobTestUtils.buildFilesDocumentString(STUDY_ID_TO_DROP, "fileIdTwo"),
                JobTestUtils.buildFilesDocumentString("otherStudyId", "fileIdThree")));

        DBCollection variantsCollection = mongoRule.getCollection(dbName, COLLECTION_VARIANTS_NAME);
        DBCollection filesCollection = mongoRule.getCollection(dbName, COLLECTION_FILES_NAME);

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(dbName)
                .inputStudyId(STUDY_ID_TO_DROP)
                .timestamp()
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertCompleted(jobExecution);

        assertDropVariantsByStudy(variantsCollection, STUDY_ID_TO_DROP, EXPECTED_VARIANTS_AFTER_DROP_STUDY);
        assertPullStudy(variantsCollection, STUDY_ID_TO_DROP, EXPECTED_FILE_COUNT, EXPECTED_STATS_COUNT);
        assertDropFiles(filesCollection, STUDY_ID_TO_DROP, EXPECTED_FILES_AFTER_DROP_STUDY);
    }

}
