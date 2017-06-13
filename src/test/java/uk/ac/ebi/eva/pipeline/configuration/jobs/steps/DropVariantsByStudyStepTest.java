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
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.Arrays;

import static uk.ac.ebi.eva.test.utils.DropStudyJobTestUtils.assertDropVariantsByStudy;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

/**
 * Test for {@link DropVariantsByStudyStepConfiguration}
 */
@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {DropStudyJobConfiguration.class, BatchTestConfiguration.class})
public class DropVariantsByStudyStepTest {

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final long EXPECTED_VARIANTS_AFTER_DROP_STUDY = 2;

    private static final String STUDY_ID_TO_DROP = "studyIdToDrop";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void testNoVariantsToDrop() throws Exception {
        String databaseName = mongoRule.createDBAndInsertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithOneStudy(),
                VariantData.getVariantWithTwoStudies()));

        checkDrop(databaseName, EXPECTED_VARIANTS_AFTER_DROP_STUDY);
    }

    @Test
    public void testOneVariantToDrop() throws Exception {
        String databaseName = mongoRule.createDBAndInsertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithOneStudyToDrop(),
                VariantData.getVariantWithOneStudy(),
                VariantData.getVariantWithTwoStudies()));

        checkDrop(databaseName, EXPECTED_VARIANTS_AFTER_DROP_STUDY);
    }

    @Test
    public void testSeveralVariantsToDrop() throws Exception {
        String databaseName = mongoRule.createDBAndInsertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithOneStudyToDrop(),
                VariantData.getOtherVariantWithOneStudyToDrop(),
                VariantData.getVariantWithOneStudy(),
                VariantData.getVariantWithTwoStudies()));

        checkDrop(databaseName, EXPECTED_VARIANTS_AFTER_DROP_STUDY);
    }

    private void checkDrop(String databaseName, long expectedVariantsAfterDropStudy) {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputStudyId(STUDY_ID_TO_DROP)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.DROP_VARIANTS_BY_STUDY_STEP,
                jobParameters);

        assertCompleted(jobExecution);

        DBCollection variantsCollection = mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME);
        assertDropVariantsByStudy(variantsCollection, STUDY_ID_TO_DROP, expectedVariantsAfterDropStudy);
    }

}
