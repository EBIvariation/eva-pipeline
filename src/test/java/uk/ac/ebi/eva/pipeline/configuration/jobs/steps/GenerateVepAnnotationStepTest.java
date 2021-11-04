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
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.AnnotationJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.POLYPHEN_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.SIFT_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.SO_ACCESSION_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.XREFS_FIELD;
import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.COLLECTION_ANNOTATIONS_NAME;
import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.COLLECTION_VARIANTS_NAME;
import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.checkLoadedAnnotation;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertFailed;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link GenerateVepAnnotationStepConfiguration}
 */
@RunWith(SpringRunner.class)
@ActiveProfiles(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {AnnotationJobConfiguration.class, BatchTestConfiguration.class})
public class GenerateVepAnnotationStepTest {
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    private static final String MOCKVEP = "/mockvep.pl";

    private static final String FAILING_MOCKVEP = "/mockvep_writeToFile_error.pl";

    private static final String STUDY_ID = "1";

    private static final String FILE_ID = "1";

    private static final int EXTRA_ANNOTATIONS = 1;

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void shouldGenerateVepAnnotations() throws Exception {
        String databaseName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String outputDirAnnot = temporaryFolderRule.getRoot().getAbsolutePath();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
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
        checkLoadedAnnotation(mongoRule, databaseName);

        //check that the annotation fields are present in the variant
        MongoCursor<Document> variantCursor = mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).find()
                .iterator();
        while (variantCursor.hasNext()) {
            Document variant = variantCursor.next();
            if (variant.get("_id").equals("20_68363_A_T")) {
                Document annotationField = ((List<Document>) variant.get(
                        VariantDocument.ANNOTATION_FIELD)).get(0);
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
        String databaseName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String outputDirAnnot = temporaryFolderRule.getRoot().getAbsolutePath();
        File vepOutput = new File(URLHelper.resolveVepOutput(outputDirAnnot, STUDY_ID, FILE_ID));
        int chunkSize = 100;

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .chunkSize(Integer.toString(chunkSize))
                .databaseName(databaseName)
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
        assertAnnotationsCount(databaseName, chunkSize);

        simulateFix(databaseName, COLLECTION_VARIANTS_NAME);

        JobExecution secondJobExecution = jobLauncherTestUtils
                .launchStep(BeanNames.GENERATE_VEP_ANNOTATION_STEP, jobParameters);

        assertCompleted(secondJobExecution);

        int chunks = 3;
        int expectedTotalAnnotations = chunkSize * chunks;
        assertAnnotationsCount(databaseName, expectedTotalAnnotations);
    }

    /**
     * mockvep_writeToFile_error.pl returns 1 immediately if it finds a variant on chromosome 20 and position 65900
     */
    private void simulateFix(String databaseName, String collectionVariantsName) {
        MongoCollection<Document> collection = mongoRule.getCollection(databaseName, collectionVariantsName);
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

    private void assertAnnotationsCount(String databaseName, int expectedCount) {
        BasicDBObject queryNonAnnotated = new BasicDBObject("annot", new BasicDBObject("$exists", true));
        long count = mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).count(queryNonAnnotated);
        assertEquals(expectedCount, count);

        long cursorAnnotations = mongoRule.getCollection(databaseName, COLLECTION_ANNOTATIONS_NAME).count();
        assertEquals(expectedCount, cursorAnnotations);
    }

    private void assertVepErrorFilesExist(String outputDirAnnot) throws IOException {
        List<Path> files = Files.list(Paths.get(outputDirAnnot))
                                .filter(path -> path.getFileName().toString().contains("error"))
                                .collect(Collectors.toList());
        assertTrue(files.size() > 0);
    }

}
