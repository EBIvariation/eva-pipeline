/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.jobs;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
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
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;

/**
 * Test for {@link AnnotationJob}
 *
 * TODO The test should fail when we will integrate the JobParameter validation since there are empty parameters for VEP
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource({"classpath:annotation-job.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {AnnotationJob.class, BatchTestConfiguration.class})
public class AnnotationJobTest {
    private static final String MOCK_VEP = "/mockvep.pl";
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";
    private static final String INPUT_STUDY_ID = "annotation-job";
    private static final String INPUT_VCF_ID = "1";
    private static final String COLLECTION_VARIANTS_NAME = "variants";
    //TODO check later to substitute files for temporary ones / pay attention to vep Input file

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private DBObjectToVariantAnnotationConverter converter;

    @Test
    public void allAnnotationStepsShouldBeExecuted() throws Exception {
        String dbName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String outputDirAnnot = temporaryFolderRule.getRoot().getAbsolutePath();

        File vepInput = new File(URLHelper.resolveVepInput(outputDirAnnot, INPUT_STUDY_ID, INPUT_VCF_ID));
        String vepInputName = vepInput.getName();
        temporaryFolderRule.newFile(vepInputName);

        File vepOutput = new File(URLHelper.resolveVepOutput(outputDirAnnot, INPUT_STUDY_ID, INPUT_VCF_ID));
        String vepOutputName = vepOutput.getName();
        temporaryFolderRule.newFile(vepOutputName);

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(dbName)
                .inputFasta("")
                .inputStudyId(INPUT_STUDY_ID)
                .inputVcfId(INPUT_VCF_ID)
                .outputDirAnnotation(outputDirAnnot)
                .vepCachePath("")
                .vepCacheSpecies("")
                .vepCacheVersion("")
                .vepNumForks("")
                .vepPath(getResource(MOCK_VEP).getPath())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(3, jobExecution.getStepExecutions().size());
        List<StepExecution> steps = new ArrayList<>(jobExecution.getStepExecutions());
        StepExecution findVariantsToAnnotateStep = steps.get(0);
        StepExecution generateVepAnnotationsStep = steps.get(1);
        StepExecution loadVepAnnotationsStep = steps.get(2);

        assertEquals(BeanNames.GENERATE_VEP_INPUT_STEP, findVariantsToAnnotateStep.getStepName());
        assertEquals(BeanNames.GENERATE_VEP_ANNOTATION_STEP, generateVepAnnotationsStep.getStepName());
        assertEquals(BeanNames.LOAD_VEP_ANNOTATION_STEP, loadVepAnnotationsStep.getStepName());

        //check list of variants without annotation output file
        assertTrue(vepInput.exists());
        assertEquals("20\t60343\t60343\tG/A\t+", JobTestUtils.readFirstLine(vepInput));

        //check that documents have the annotation
        DBCursor cursor = mongoRule.getCollection(dbName, COLLECTION_VARIANTS_NAME).find();

        int cnt = 0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            cnt++;
            DBObject dbObject = (DBObject) cursor.next().get("annot");
            if (dbObject != null) {
                VariantAnnotation annot = converter.convertToDataModelType(dbObject);
                assertNotNull(annot.getConsequenceTypes());
                consequenceTypeCount += annot.getConsequenceTypes().size();
            }
        }

        assertEquals(300, cnt);
        assertEquals(536, consequenceTypeCount);

        //check that one line is skipped because malformed
        List<StepExecution> variantAnnotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals(BeanNames.LOAD_VEP_ANNOTATION_STEP))
                .collect(Collectors.toList());
        assertEquals(1, variantAnnotationLoadStepExecution.get(0).getReadSkipCount());
    }

    @Test
    public void noVariantsToAnnotateOnlyFindVariantsToAnnotateStepShouldRun() throws Exception {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        String outputDirAnnot = temporaryFolderRule.getRoot().getAbsolutePath();

        File vepInput = new File(URLHelper.resolveVepInput(outputDirAnnot, INPUT_STUDY_ID, INPUT_VCF_ID));
        String vepInputName = vepInput.getName();
        temporaryFolderRule.newFile(vepInputName);

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(dbName)
                .inputFasta("")
                .inputStudyId(INPUT_STUDY_ID)
                .inputVcfId(INPUT_VCF_ID)
                .outputDirAnnotation(outputDirAnnot)
                .vepCachePath("")
                .vepCacheSpecies("")
                .vepCacheVersion("")
                .vepNumForks("")
                .vepPath(getResource(MOCK_VEP).getPath())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(1, jobExecution.getStepExecutions().size());
        StepExecution findVariantsToAnnotateStep = new ArrayList<>(jobExecution.getStepExecutions()).get(0);

        assertEquals(BeanNames.GENERATE_VEP_INPUT_STEP, findVariantsToAnnotateStep.getStepName());

        assertTrue(vepInput.exists());
        assertTrue(Files.size(Paths.get(vepInput.toPath().toUri())) == 0);
    }

    @Before
    public void setUp() throws Exception {
        converter = new DBObjectToVariantAnnotationConverter();
    }

}
