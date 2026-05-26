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

package uk.ac.ebi.eva.pipeline.configuration.jobs;

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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_INIT_DATABASE_JOB;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

@ExtendWith(SpringExtension.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@ContextConfiguration(classes = {DatabaseInitializationJobConfiguration.class, BatchTestConfiguration.class})
public class DatabaseInitializationJobTest extends MongoTestContainerHelper {

    private static final String COLLECTION_FEATURES_NAME = "features";

    private static final String INPUT_FILE = "/input-files/gtf/small_sample.gtf.gz";

    private static final String DB_NAME = "db-init-test-db";

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @Autowired
    @Qualifier(JOB_INIT_DATABASE_JOB)
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
    public void testDatabaseInitializationJob() throws Exception {
        File inputFile = resourceLoader.getResource(INPUT_FILE).getFile();
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(DB_NAME)
                .inputGtf(inputFile.getAbsolutePath())
                .collectionFeaturesName(COLLECTION_FEATURES_NAME)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertCompleted(jobExecution);

        assertEquals(252L, mongoTemplate.getDb().getCollection(COLLECTION_FEATURES_NAME).countDocuments());
    }
}
