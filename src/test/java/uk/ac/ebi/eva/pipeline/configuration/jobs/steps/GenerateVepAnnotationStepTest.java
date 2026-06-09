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

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.AnnotationJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.AnnotationIndexMongo.POLYPHEN_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.AnnotationIndexMongo.SIFT_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.AnnotationIndexMongo.SO_ACCESSION_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.AnnotationIndexMongo.XREFS_FIELD;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_ANNOTATE_VARIANTS_JOB;
import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.COLLECTION_ANNOTATIONS_NAME;
import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.COLLECTION_VARIANTS_NAME;
import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.checkLoadedAnnotation;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertFailed;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link GenerateVepAnnotationStepConfiguration}
 */
@ExtendWith(SpringExtension.class)
@TestPropertySource({"classpath:application.properties"})
@ContextConfiguration(classes = {AnnotationJobConfiguration.class, BatchTestConfiguration.class,
        MongoCollectionNameConfiguration.class})
public class GenerateVepAnnotationStepTest extends MongoTestContainerHelper {
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    private static final String MOCKVEP = "/mockvep.pl";

    private static final String FAILING_MOCKVEP = "/mockvep_writeToFile_error.pl";

    private static final String STUDY_ID = "1";

    private static final String FILE_ID = "1";

    private static final int EXTRA_ANNOTATIONS = 1;

    private static final String DB_NAME = "generate-vep-annotation-test-db";

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @Autowired
    @Qualifier(JOB_ANNOTATE_VARIANTS_JOB)
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
    public void shouldGenerateVepAnnotations() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).restoreDumpFromFolder(MONGO_DUMP);
        String outputDirAnnot = temporaryFolderUtil.newFolder().getAbsolutePath();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
                .inputFasta("")
                .inputStudyId(STUDY_ID)
                .inputVcfId(FILE_ID)
                .outputDirAnnotation(outputDirAnnot)
                .annotationOverwrite("false")
                .vepCachePath("")
                .vepCacheSpecies("")
                .vepCacheVersion("80")
                .vepNumForks("4")
                .vepPath(getResource(MOCKVEP).getPath())
                .vepVersion("80")
                .vepTimeout("60")
                .toJobParameters();

        // When the execute method in variantsAnnotCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils
                .launchStep(BeanNames.GENERATE_VEP_ANNOTATION_STEP, jobParameters);

        //Then variantsAnnotCreate step should complete correctly
        assertCompleted(jobExecution);

        //check that the annotation collection has been populated properly
        checkLoadedAnnotation(mongoTemplate);

        //check that the annotation fields are present in the variant
        MongoCursor<Document> variantCursor = mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME).find()
                .iterator();
        while (variantCursor.hasNext()) {
            Document variant = variantCursor.next();
            if (variant.get("_id").equals("20_68363_A_T")) {
                Document annotationField = ((List<Document>) variant.get(
                        VariantMongo.ANNOTATION_FIELD)).get(0);
                assertNotNull(annotationField.get(SIFT_FIELD));
                assertNotNull(annotationField.get(SO_ACCESSION_FIELD));
                assertNotNull(annotationField.get(POLYPHEN_FIELD));
                assertNotNull(annotationField.get(XREFS_FIELD));
            }
        }
        variantCursor.close();
    }

    @Test
    public void shouldResumeJob() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).restoreDumpFromFolder(MONGO_DUMP);
        String outputDirAnnot = temporaryFolderUtil.newFolder().getAbsolutePath();

        File vepOutput = new File(URLHelper.resolveVepOutput(outputDirAnnot, STUDY_ID, FILE_ID));
        int chunkSize = 100;

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .chunkSize(Integer.toString(chunkSize))
                .databaseName(DB_NAME)
                .inputFasta("")
                .inputStudyId(STUDY_ID)
                .inputVcfId(FILE_ID)
                .outputDirAnnotation(outputDirAnnot)
                .vepCachePath("")
                .vepCacheSpecies("")
                .vepCacheVersion("80")
                .vepNumForks("4")
                .vepVersion("80")
                .vepPath(getResource(FAILING_MOCKVEP).getPath())
                .vepTimeout("10").toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils
                .launchStep(BeanNames.GENERATE_VEP_ANNOTATION_STEP, jobParameters);

        assertFailed(jobExecution);
        assertVepErrorFilesExist(outputDirAnnot);
        assertAnnotationsCount(chunkSize);

        simulateFix(COLLECTION_VARIANTS_NAME);

        JobExecution secondJobExecution = jobLauncherTestUtils
                .launchStep(BeanNames.GENERATE_VEP_ANNOTATION_STEP, jobParameters);

        assertCompleted(secondJobExecution);

        int chunks = 3;
        int expectedTotalAnnotations = chunkSize * chunks;
        assertAnnotationsCount(expectedTotalAnnotations);
    }

    /**
     * mockvep_writeToFile_error.pl returns 1 immediately if it finds a variant on chromosome 20 and position 65900
     */
    private void simulateFix(String collectionVariantsName) {
        MongoCollection<Document> collection = mongoTemplate.getDb().getCollection(collectionVariantsName);
        int startThatProvokesError = 65900;
        Document query = new Document("start", startThatProvokesError);

        assertEquals(1, collection.countDocuments(query));
        MongoCursor<Document> documents = collection.find(query).iterator();
        Document variant = documents.next();

        int fixedStart = startThatProvokesError - 1;
        variant.put("start", fixedStart);
        variant.put("_id", "20_65899_G_A");
        variant.put("end", fixedStart);
        collection.insertOne(variant);

        collection.deleteMany(query);

    }

    private void assertAnnotationsCount(int expectedCount) {
        BasicDBObject queryNonAnnotated = new BasicDBObject("annot", new BasicDBObject("$exists", true));
        long count = mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME).countDocuments(queryNonAnnotated);
        assertEquals(expectedCount, count);

        long cursorAnnotations = mongoTemplate.getDb().getCollection(COLLECTION_ANNOTATIONS_NAME).countDocuments();
        assertEquals(expectedCount, cursorAnnotations);
    }

    private void assertVepErrorFilesExist(String outputDirAnnot) throws IOException {
        List<Path> files = Files.list(Paths.get(outputDirAnnot))
                .filter(path -> path.getFileName().toString().contains("error"))
                .collect(Collectors.toList());
        assertTrue(files.size() > 0);
    }

}
