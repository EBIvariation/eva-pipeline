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
package uk.ac.ebi.eva.pipeline.jobs.steps;

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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.jobs.DropStudyJob;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.commons.models.converters.data.VariantSourceEntryToDBObjectConverter.STUDYID_FIELD;
import static uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter.FILES_FIELD;
import static uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter.STATS_FIELD;

/**
 * Test for {@link DropVariantsAndStatisticsByStudyStep}
 */
@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {DropStudyJob.class, BatchTestConfiguration.class})
public class DropVariantsAndStatisticsByStudyStepTest {

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String STUDY_ID_TO_DROP = "studyIdToDrop";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void testNothingToPull() throws IOException {
        final int expectedFilesBefore = 0;
        final int expectedFilesAfter = 0;
        final int expectedStatsBefore = 0;
        final int expectedStatsAfter = 0;

        String databaseName = mongoRule.insertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithOneStudy()));

        check(databaseName, expectedFilesBefore, expectedStatsBefore);
        executeStep(databaseName);
        check(databaseName, expectedFilesAfter, expectedStatsAfter);
    }

    @Test
    public void testFileToPull() throws IOException {
        final int expectedFilesBefore = 1;
        final int expectedFilesAfter = 0;
        final int expectedStatsBefore = 0;
        final int expectedStatsAfter = 0;

        String databaseName = mongoRule.insertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithOneStudy(),
                VariantData.getVariantWithTwoStudies()));

        check(databaseName, expectedFilesBefore, expectedStatsBefore);
        executeStep(databaseName);
        check(databaseName, expectedFilesAfter, expectedStatsAfter);
    }

    @Test
    public void testFileAndStatsToPull() throws IOException {
        final int expectedFilesBefore = 2;
        final int expectedFilesAfter = 0;
        final int expectedStatsBefore = 1;
        final int expectedStatsAfter = 0;

        String databaseName = mongoRule.insertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithOneStudy(),
                VariantData.getVariantWithTwoStudies(),
                VariantData.getVariantWithOneStudyToDrop()));

        check(databaseName, expectedFilesBefore, expectedStatsBefore);
        executeStep(databaseName);
        check(databaseName, expectedFilesAfter, expectedStatsAfter);
    }

    @Test
    public void testFileAndStatsToPullWithAllTestFiles() throws IOException {
        final int expectedFilesBefore = 3;
        final int expectedFilesAfter = 0;
        final int expectedStatsBefore = 2;
        final int expectedStatsAfter = 0;

        String databaseName = mongoRule.insertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithOneStudy(),
                VariantData.getVariantWithTwoStudies(),
                VariantData.getVariantWithOneStudyToDrop(),
                VariantData.getOtherVariantWithOneStudyToDrop()));

        check(databaseName, expectedFilesBefore, expectedStatsBefore);
        executeStep(databaseName);
        check(databaseName, expectedFilesAfter, expectedStatsAfter);
    }

    private void executeStep(String databaseName) {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputStudyId(STUDY_ID_TO_DROP)
                .toJobParameters();

        // When the execute method in variantsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.DROP_VARIANTS_AND_STATISTICS_BY_STUDY_STEP,
                jobParameters);

        //Then variantsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    private void check(String databaseName, int expectedFileCount, int expectedStatsCount) {
        String filesStudyIdField = String.format("%s.%s", FILES_FIELD, STUDYID_FIELD);
        String statsStudyIdField = String.format("%s.%s", STATS_FIELD, STUDYID_FIELD);
        // And the documents in the DB should not contain the study removed
        DBCollection variantsCollection = mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME);
        BasicDBObject variantFiles = new BasicDBObject(filesStudyIdField, STUDY_ID_TO_DROP);
        BasicDBObject variantStats = new BasicDBObject(statsStudyIdField, STUDY_ID_TO_DROP);

        System.out.println(variantsCollection.count(variantFiles));
        System.out.println(variantsCollection.count(variantStats));

        assertEquals(expectedFileCount, variantsCollection.count(variantFiles));
        assertEquals(expectedStatsCount, variantsCollection.count(variantStats));
    }

}
