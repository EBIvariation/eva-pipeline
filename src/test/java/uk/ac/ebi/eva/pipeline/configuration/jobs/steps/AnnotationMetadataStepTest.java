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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMetadataMongo;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.AnnotationJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_ANNOTATE_VARIANTS_JOB;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

/**
 * Test for {@link AnnotationMetadataStepConfiguration}
 */
@ExtendWith(SpringExtension.class)
@TestPropertySource({"classpath:application.properties"})
@ContextConfiguration(classes = {AnnotationJobConfiguration.class, BatchTestConfiguration.class,
        MongoCollectionNameConfiguration.class, MongoConfiguration.class})
public class AnnotationMetadataStepTest extends MongoTestContainerHelper {

    private static final String DB_NAME = "annotation-metadata-test-db";

    @Autowired
    @Qualifier(JOB_ANNOTATE_VARIANTS_JOB)
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
    public void cleanUp() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void shouldWriteVersions() {
        String vepCacheVersion = "87";
        String vepVersion = "88";

        assertStepIsComplete(vepCacheVersion, vepVersion);

        //check that the document was written in mongo
        List<AnnotationMetadataMongo> annotationMetadataList = mongoTemplate.findAll(AnnotationMetadataMongo.class);

        assertEquals(1, annotationMetadataList.size());
        assertEquals(vepCacheVersion, annotationMetadataList.get(0).getCacheVersion());
        assertEquals(vepVersion, annotationMetadataList.get(0).getVepVersion());
        assertTrue(annotationMetadataList.get(0).isDefaultVersion());
    }

    private void assertStepIsComplete(String vepCacheVersion, String vepVersion) {
        String collectionAnnotationMetadataName = "annotationMetadata";
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionAnnotationMetadataName(collectionAnnotationMetadataName)
                .databaseName(DB_NAME)
                .vepCacheVersion(vepCacheVersion)
                .vepVersion(vepVersion)
                .addString("run.id", UUID.randomUUID().toString())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_ANNOTATION_METADATA_STEP, jobParameters);

        assertCompleted(jobExecution);
    }

    @Test
    public void shouldKeepOtherVersions() {
        AnnotationMetadataMongo defaultMetadata = new AnnotationMetadataMongo("70", "72");
        defaultMetadata.setDefaultVersion(true);
        mongoTemplate.save(defaultMetadata);

        String vepCacheVersion = "87";
        String vepVersion = "88";

        assertStepIsComplete(vepCacheVersion, vepVersion);

        //check that the document was written in mongo
        List<AnnotationMetadataMongo> annotationMetadataList = mongoTemplate.findAll(AnnotationMetadataMongo.class);

        assertEquals(2, annotationMetadataList.size());
        for (AnnotationMetadataMongo metadata : annotationMetadataList) {
            if (metadata.getVepVersion().equals(defaultMetadata.getVepVersion())
                    && metadata.getCacheVersion().equals(defaultMetadata.getCacheVersion())) {
                assertTrue(metadata.isDefaultVersion());
            } else {
                assertFalse(metadata.isDefaultVersion());
            }
        }
    }

    @Test
    public void shouldNotAddRedundantVersions() {
        String vepCacheVersion = "87";
        String vepVersion = "88";
        AnnotationMetadataMongo annotationMetadataMongo = new AnnotationMetadataMongo(vepVersion, vepCacheVersion);
        annotationMetadataMongo.setDefaultVersion(true);
        mongoTemplate.save(annotationMetadataMongo);

        assertStepIsComplete(vepCacheVersion, vepVersion);

        //check that the document was written in mongo
        List<AnnotationMetadataMongo> annotationMetadataList = mongoTemplate.findAll(AnnotationMetadataMongo.class);

        assertEquals(1, annotationMetadataList.size());
        assertTrue(annotationMetadataList.get(0).isDefaultVersion());
    }
}
