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
package uk.ac.ebi.eva.pipeline.jobs;

import com.mongodb.BasicDBObject;
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
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter.FILES_FIELD;
import static uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter.STATS_FIELD;
import static uk.ac.ebi.eva.commons.models.data.VariantSourceEntity.STUDYID_FIELD;
import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.COLLECTION_FILES_NAME;
import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.COLLECTION_VARIANTS_NAME;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link PopulationStatisticsJob}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {DropStudyJob.class, BatchTestConfiguration.class})
public class DropStudyJobTest {

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private static final long EXPECTED_FILES_AFTER_DROP_STUDY = 1;

    private static final long EXPECTED_VARIANTS_AFTER_DROP_STUDY = 2;

    private static final long EXPECTED_FILE_COUNT = 0;

    private static final long EXPECTED_STATS_COUNT = 0;

    private static final String FILES_STUDY_ID_FIELD = String.format("%s.%s", FILES_FIELD, STUDYID_FIELD);

    private static final String STATS_STUDY_ID_FIELD = String.format("%s.%s", STATS_FIELD, STUDYID_FIELD);

    @Test
    public void fullDropStudyJob() throws Exception {
        //Given a valid VCF input file
        String dbName = mongoRule.insertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithOneStudyToDrop(),
                VariantData.getOtherVariantWithOneStudyToDrop(),
                VariantData.getVariantWithOneStudy(),
                VariantData.getVariantWithTwoStudies()));

        String studyId = "studyIdToDrop";
        mongoRule.insert(dbName, COLLECTION_FILES_NAME, JobTestUtils.buildFilesDocumentString(studyId, "fileIdOne"));
        mongoRule.insert(dbName, COLLECTION_FILES_NAME, JobTestUtils.buildFilesDocumentString(studyId, "fileIdTwo"));
        mongoRule.insert(dbName, COLLECTION_FILES_NAME,
                JobTestUtils.buildFilesDocumentString("otherStudyId", "fileIdThree"));

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName("files")
                .collectionVariantsName("variants")
                .databaseName(dbName)
                .inputStudyId(studyId)
                .timestamp()
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());


        checkDropSingleStudy(dbName, studyId, EXPECTED_VARIANTS_AFTER_DROP_STUDY);
        checkPullStudy(dbName, studyId, EXPECTED_FILE_COUNT, EXPECTED_STATS_COUNT);
        checkDropFiles(dbName, studyId, EXPECTED_FILES_AFTER_DROP_STUDY);
    }

    private void checkDropSingleStudy(String dbName, String studyId, long expectedVariantsAfterDropStudy) {
        DBCollection variantsCollection = mongoRule.getCollection(dbName, COLLECTION_VARIANTS_NAME);
        assertEquals(expectedVariantsAfterDropStudy, variantsCollection.count());

        BasicDBObject singleStudyVariants = new BasicDBObject(FILES_STUDY_ID_FIELD, studyId)
                .append(FILES_FIELD, new BasicDBObject("$size", 1));
        assertEquals(0, variantsCollection.count(singleStudyVariants));
    }

    private void checkPullStudy(String dbName, String studyId, long expectedFileCount, long expectedStatsCount) {
        DBCollection variantsCollection = mongoRule.getCollection(dbName, COLLECTION_VARIANTS_NAME);

        BasicDBObject variantFiles = new BasicDBObject(FILES_STUDY_ID_FIELD, studyId);
        BasicDBObject variantStats = new BasicDBObject(STATS_STUDY_ID_FIELD, studyId);

        assertEquals(expectedFileCount, variantsCollection.count(variantFiles));
        assertEquals(expectedStatsCount, variantsCollection.count(variantStats));
    }

    private void checkDropFiles(String dbName, String studyId, long expectedFilesAfterDropStudy) {
        DBCollection filesCollection = mongoRule.getCollection(dbName, COLLECTION_FILES_NAME);
        assertEquals(expectedFilesAfterDropStudy, filesCollection.count());

        BasicDBObject remainingFilesThatShouldHaveBeenDropped = new BasicDBObject(STUDYID_FIELD, studyId);
        assertEquals(0, filesCollection.count(remainingFilesThatShouldHaveBeenDropped));
    }

}
