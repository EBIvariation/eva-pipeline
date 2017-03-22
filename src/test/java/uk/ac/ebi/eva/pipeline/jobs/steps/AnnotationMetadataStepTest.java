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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.commons.models.metadata.AnnotationMetadata;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.jobs.AnnotationJob;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnection;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * TODO jmmut remove import AnnotationJob when we add the stepLauncherTestUtils
 */
@RunWith(SpringRunner.class)
@ActiveProfiles(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {AnnotationJob.class, BatchTestConfiguration.class})
public class AnnotationMetadataStepTest {
    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoConnection mongoConnection;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Test
    public void shouldWriteVersions() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(databaseName, mongoConnection,
                mongoMappingContext);
        String vepCacheVersion = "87";
        String vepVersion = "88";

        assertStepCompletes(databaseName, vepCacheVersion, vepVersion);

        //check that the document was written in mongo
        List<AnnotationMetadata> annotationMetadatas = mongoOperations.findAll(AnnotationMetadata.class);

        assertEquals(1, annotationMetadatas.size());
        assertEquals(vepCacheVersion, annotationMetadatas.get(0).getCacheVersion());
        assertEquals(vepVersion, annotationMetadatas.get(0).getVepVersion());
    }

    private void assertStepCompletes(String databaseName, String vepCacheVersion, String vepVersion) {
        String collectionAnnotationMetadataName = "annotationMetadata";
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionAnnotationMetadataName(collectionAnnotationMetadataName)
                .databaseName(databaseName)
                .vepCacheVersion(vepCacheVersion)
                .vepVersion(vepVersion)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_ANNOTATION_METADATA_STEP, jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void shouldNotAddRedundantVersions() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(databaseName, mongoConnection,
                mongoMappingContext);
        mongoOperations.save(new AnnotationMetadata("70", "72"));

        String vepCacheVersion = "87";
        String vepVersion = "88";

        assertStepCompletes(databaseName, vepCacheVersion, vepVersion);

        //check that the document was written in mongo
        List<AnnotationMetadata> annotationMetadatas = mongoOperations.findAll(AnnotationMetadata.class);

        assertEquals(2, annotationMetadatas.size());
    }

    @Test
    public void shouldKeepOtherVersions() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(databaseName, mongoConnection,
                mongoMappingContext);
        String vepCacheVersion = "87";
        String vepVersion = "88";
        mongoOperations.save(new AnnotationMetadata(vepVersion, vepCacheVersion));

        assertStepCompletes(databaseName, vepCacheVersion, vepVersion);

        //check that the document was written in mongo
        List<AnnotationMetadata> annotationMetadatas = mongoOperations.findAll(AnnotationMetadata.class);

        assertEquals(1, annotationMetadatas.size());
    }
}
