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
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.DropStudyJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.UUID;

import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_DROP_STUDY_JOB;
import static uk.ac.ebi.eva.test.data.VariantData.OTHER_VARIANT_WITH_ONE_STUDY_TO_DROP_PATH;
import static uk.ac.ebi.eva.test.data.VariantData.VARIANT_WITH_ONE_STUDY_PATH;
import static uk.ac.ebi.eva.test.data.VariantData.VARIANT_WITH_ONE_STUDY_TO_DROP_PATH;
import static uk.ac.ebi.eva.test.data.VariantData.VARIANT_WITH_TWO_STUDIES_PATH;
import static uk.ac.ebi.eva.test.utils.DropStudyJobTestUtils.assertDropVariantsByStudy;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

/**
 * Test for {@link DropVariantsByStudyStepConfiguration}
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {DropStudyJobConfiguration.class, BatchTestConfiguration.class})
public class DropVariantsByStudyStepTest extends MongoTestContainerHelper {

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final long EXPECTED_VARIANTS_AFTER_DROP_STUDY = 2;

    private static final String STUDY_ID_TO_DROP = "studyIdToDrop";

    private static final String DB_NAME = "drop-variants-by-study-test-db";


    @Autowired
    @Qualifier(JOB_DROP_STUDY_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private ResourceLoader resourceLoader;

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
    public void testNoVariantsToDrop() {
        MongoTestDataLoader mongoTestDataLoader = new MongoTestDataLoader(mongoTemplate, resourceLoader);
        mongoTestDataLoader.load(VARIANT_WITH_ONE_STUDY_PATH, COLLECTION_VARIANTS_NAME);
        mongoTestDataLoader.load(VARIANT_WITH_TWO_STUDIES_PATH, COLLECTION_VARIANTS_NAME);

        checkDrop(EXPECTED_VARIANTS_AFTER_DROP_STUDY);
    }

    @Test
    public void testOneVariantToDrop() {
        MongoTestDataLoader mongoTestDataLoader = new MongoTestDataLoader(mongoTemplate, resourceLoader);
        mongoTestDataLoader.load(VARIANT_WITH_ONE_STUDY_TO_DROP_PATH, COLLECTION_VARIANTS_NAME);
        mongoTestDataLoader.load(VARIANT_WITH_ONE_STUDY_PATH, COLLECTION_VARIANTS_NAME);
        mongoTestDataLoader.load(VARIANT_WITH_TWO_STUDIES_PATH, COLLECTION_VARIANTS_NAME);

        checkDrop(EXPECTED_VARIANTS_AFTER_DROP_STUDY);
    }

    @Test
    public void testSeveralVariantsToDrop() {
        MongoTestDataLoader mongoTestDataLoader = new MongoTestDataLoader(mongoTemplate, resourceLoader);
        mongoTestDataLoader.load(VARIANT_WITH_ONE_STUDY_TO_DROP_PATH, COLLECTION_VARIANTS_NAME);
        mongoTestDataLoader.load(OTHER_VARIANT_WITH_ONE_STUDY_TO_DROP_PATH, COLLECTION_VARIANTS_NAME);
        mongoTestDataLoader.load(VARIANT_WITH_ONE_STUDY_PATH, COLLECTION_VARIANTS_NAME);
        mongoTestDataLoader.load(VARIANT_WITH_TWO_STUDIES_PATH, COLLECTION_VARIANTS_NAME);

        checkDrop(EXPECTED_VARIANTS_AFTER_DROP_STUDY);
    }

    private void checkDrop(long expectedVariantsAfterDropStudy) {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
                .inputStudyId(STUDY_ID_TO_DROP)
                .addString("run.id", UUID.randomUUID().toString())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.DROP_VARIANTS_BY_STUDY_STEP,
                jobParameters);

        assertCompleted(jobExecution);

        MongoCollection<Document> variantsCollection = mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME);
        assertDropVariantsByStudy(variantsCollection, STUDY_ID_TO_DROP, expectedVariantsAfterDropStudy);
    }

}
