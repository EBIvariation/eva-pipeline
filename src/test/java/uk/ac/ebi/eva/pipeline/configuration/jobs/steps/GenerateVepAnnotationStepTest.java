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
import uk.ac.ebi.eva.pipeline.configuration.jobs.AnnotationJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

    private static final String MOCKVEP = "/mockvep_writeToFile.pl";

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
        File vepOutput = new File(URLHelper.resolveVepOutput(outputDirAnnot, STUDY_ID, FILE_ID));

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName("variants")
                .databaseName(databaseName)
                .inputFasta("")
                .inputStudyId(STUDY_ID)
                .inputVcfId(FILE_ID)
                .outputDirAnnotation(outputDirAnnot)
                .annotationOverwrite("false")
                .vepCachePath("")
                .vepCacheSpecies("")
                .vepCacheVersion("")
                .vepNumForks("4")
                .vepPath(getResource(MOCKVEP).getPath())
                .vepTimeout("60")
                .toJobParameters();

        // When the execute method in variantsAnnotCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils
                .launchStep(BeanNames.GENERATE_VEP_ANNOTATION_STEP, jobParameters);

        //Then variantsAnnotCreate step should complete correctly
        assertCompleted(jobExecution);

        // And VEP output should exist and annotations should be in the file
        assertTrue(vepOutput.exists());
        assertEquals(300 + EXTRA_ANNOTATIONS,
                JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(vepOutput))));
    }

    @Test
    public void shouldResumeJob() throws Exception {
        String databaseName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String collectionVariantsName = "variants";
        String outputDirAnnot = temporaryFolderRule.getRoot().getAbsolutePath();
        File vepOutput = new File(URLHelper.resolveVepOutput(outputDirAnnot, STUDY_ID, FILE_ID));
        int chunkSize = 100;

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionVariantsName(collectionVariantsName)
                .chunkSize(Integer.toString(chunkSize))
                .databaseName(databaseName)
                .inputFasta("")
                .inputStudyId(STUDY_ID)
                .inputVcfId(FILE_ID)
                .outputDirAnnotation(outputDirAnnot)
                .vepCachePath("")
                .vepCacheSpecies("")
                .vepCacheVersion("")
                .vepNumForks("4")
                .vepPath(getResource(FAILING_MOCKVEP).getPath())
                .vepTimeout("10").toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils
                .launchStep(BeanNames.GENERATE_VEP_ANNOTATION_STEP, jobParameters);

        assertFailed(jobExecution);

        assertTrue(!vepOutput.exists());
        List<Path> files = Files.list(Paths.get(outputDirAnnot))
                .filter(path -> !path.getFileName().toString().contains("error"))
                .collect(Collectors.toList());
        assertEquals(1, files.size());
        assertEquals(chunkSize + EXTRA_ANNOTATIONS,
                JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(files.get(0).toFile()))));

        simulateFix(databaseName, collectionVariantsName);

        JobExecution secondJobExecution = jobLauncherTestUtils
                .launchStep(BeanNames.GENERATE_VEP_ANNOTATION_STEP, jobParameters);

        assertCompleted(secondJobExecution);

        assertTrue(vepOutput.exists());
        int chunks = 3;
        int expectedTotalAnnotations = (chunkSize + EXTRA_ANNOTATIONS) * chunks;
        assertEquals(expectedTotalAnnotations,
                JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(vepOutput))));
    }

    /**
     * mockvep_writeToFile_error.pl returns 1 immediately if it finds a variant on chromosome 20 and position 65900
     */
    private void simulateFix(String databaseName, String collectionVariantsName) {
        DBCollection collection = mongoRule.getCollection(databaseName, collectionVariantsName);
        int startThatProvokesError = 65900;
        BasicDBObject query = new BasicDBObject("start", startThatProvokesError);

        int fixedStart = startThatProvokesError - 1;
        BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("start", fixedStart));

        collection.update(query, update);
    }

}
