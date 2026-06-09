/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.DatabaseInitializationJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_INIT_DATABASE_JOB;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {DatabaseInitializationJobConfiguration.class, BatchTestConfiguration.class})
public class LoadFeatureCoordinatesStepTest extends MongoTestContainerHelper {

    private static final String COLLECTION_FEATURES_NAME = "features";

    private static final String INPUT_FILE = "/input-files/gtf/small_sample.gtf.gz";

    private static final String DB_NAME = "load-feature-coordinates-test-db";

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
    public void testLoadFeatureCoordinates() {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(DB_NAME)
                .inputGtf(getResource(INPUT_FILE).getAbsolutePath())
                .collectionFeaturesName(COLLECTION_FEATURES_NAME)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_FEATURE_COORDINATES_STEP,
                jobParameters);
        assertCompleted(jobExecution);
        assertEquals(252L, mongoTemplate.getDb().getCollection(COLLECTION_FEATURES_NAME).countDocuments());
    }
}
