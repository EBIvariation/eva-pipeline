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

package uk.ac.ebi.eva.pipeline.configuration.jobs;

import com.mongodb.BasicDBList;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.ac.ebi.eva.commons.models.mongo.entity.Annotation.CONSEQUENCE_TYPE_FIELD;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link AnnotationJobConfiguration}
 * <p>
 * TODO The test should fail when we will integrate the JobParameter validation since there are empty parameters for VEP
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {AnnotationJobConfiguration.class, BatchTestConfiguration.class})
public class AnnotationJobTest {
    private static final String MOCK_VEP = "/mockvep.pl";
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";
    private static final String INPUT_STUDY_ID = "1";
    private static final String INPUT_VCF_ID = "1";
    private static final String COLLECTION_ANNOTATIONS_NAME = "annotations";
    private static final String COLLECTION_ANNOTATION_METADATA_NAME = "annotationMetadata";
    private static final String COLLECTION_VARIANTS_NAME = "variants";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void allAnnotationStepsShouldBeExecuted() throws Exception {
        String dbName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String outputDirAnnot = temporaryFolderRule.getRoot().getAbsolutePath();

        File vepOutput = new File(URLHelper.resolveVepOutput(outputDirAnnot, INPUT_STUDY_ID, INPUT_VCF_ID));
        String vepOutputName = vepOutput.getName();
        temporaryFolderRule.newFile(vepOutputName);
        File fasta = temporaryFolderRule.newFile();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionAnnotationMetadataName(COLLECTION_ANNOTATION_METADATA_NAME)
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(dbName)
                .inputFasta(fasta.getAbsolutePath())
                .inputStudyId(INPUT_STUDY_ID)
                .inputVcfId(INPUT_VCF_ID)
                .outputDirAnnotation(outputDirAnnot)
                .vepCachePath("")
                .vepCacheSpecies("human")
                .vepCacheVersion("80")
                .vepNumForks("4")
                .vepPath(getResource(MOCK_VEP).getPath())
                .vepTimeout("60")
                .vepVersion("80")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertCompleted(jobExecution);

        assertEquals(3, jobExecution.getStepExecutions().size());
        List<StepExecution> steps = new ArrayList<>(jobExecution.getStepExecutions());
        StepExecution generateVepAnnotationsStep = steps.get(0);
        StepExecution loadVepAnnotationsStep = steps.get(1);
        StepExecution loadAnnotationMetadataStep = steps.get(2);

        assertEquals(BeanNames.GENERATE_VEP_ANNOTATION_STEP, generateVepAnnotationsStep.getStepName());
        assertEquals(BeanNames.LOAD_VEP_ANNOTATION_STEP, loadVepAnnotationsStep.getStepName());
        assertEquals(BeanNames.LOAD_ANNOTATION_METADATA_STEP, loadAnnotationMetadataStep.getStepName());

        //check that documents have the annotation
        DBCursor cursor = mongoRule.getCollection(dbName, COLLECTION_ANNOTATIONS_NAME).find();

        int annotationCount = 0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            annotationCount++;
            DBObject annotation = cursor.next();
            BasicDBList consequenceTypes = (BasicDBList) annotation.get(CONSEQUENCE_TYPE_FIELD);
            assertNotNull(consequenceTypes);
            consequenceTypeCount += consequenceTypes.size();
        }
        cursor.close();

        assertEquals(299, annotationCount);
        assertEquals(536, consequenceTypeCount);

        //check that one line is skipped because malformed
        List<StepExecution> annotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals(BeanNames.LOAD_VEP_ANNOTATION_STEP))
                .collect(Collectors.toList());
        assertEquals(1, annotationLoadStepExecution.get(0).getReadSkipCount());
    }

    @Test
    public void noVariantsToAnnotateOnlyGenerateAnnotationStepShouldRun() throws Exception {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        String outputDirAnnot = temporaryFolderRule.getRoot().getAbsolutePath();
        File fasta = temporaryFolderRule.newFile();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionAnnotationMetadataName(COLLECTION_ANNOTATION_METADATA_NAME)
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(dbName)
                .inputFasta(fasta.getAbsolutePath())
                .inputStudyId(INPUT_STUDY_ID)
                .inputVcfId(INPUT_VCF_ID)
                .outputDirAnnotation(outputDirAnnot)
                .vepCachePath("")
                .vepCacheSpecies("Human")
                .vepCacheVersion("80")
                .vepNumForks("4")
                .vepPath(getResource(MOCK_VEP).getPath())
                .vepTimeout("60")
                .vepVersion("80")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertCompleted(jobExecution);

        assertEquals(1, jobExecution.getStepExecutions().size());
        StepExecution findVariantsToAnnotateStep = new ArrayList<>(jobExecution.getStepExecutions()).get(0);

        assertEquals(BeanNames.GENERATE_VEP_ANNOTATION_STEP, findVariantsToAnnotateStep.getStepName());
    }

}
