/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.AnnotationJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.test.rules.TemporalMongoRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep.LOAD_VEP_ANNOTATION;
import static uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION;
import static uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;

/**
 * Test for {@link AnnotationJob}
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = {JobOptions.class, AnnotationJob.class, AnnotationJobConfiguration.class, JobLauncherTestUtils.class})
public class AnnotationJobTest {
    private static final String MOCK_VEP = "/mockvep.pl";
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";
    //TODO check later to substitute files for temporal ones / pay attention to vep Input file

    @Rule
    public TemporalMongoRule mongoRule = new TemporalMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    private File vepInputFile;
    private DBObjectToVariantAnnotationConverter converter;

    @Test
    public void allAnnotationStepsShouldBeExecuted() throws Exception {
        mongoRule.importDump(getResourceUrl(MONGO_DUMP), jobOptions.getDbName());

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(3, jobExecution.getStepExecutions().size());
        List<StepExecution> steps = new ArrayList<>(jobExecution.getStepExecutions());
        StepExecution findVariantsToAnnotateStep = steps.get(0);
        StepExecution generateVepAnnotationsStep = steps.get(1);
        StepExecution loadVepAnnotationsStep = steps.get(2);

        assertEquals(FIND_VARIANTS_TO_ANNOTATE, findVariantsToAnnotateStep.getStepName());
        assertEquals(GENERATE_VEP_ANNOTATION, generateVepAnnotationsStep.getStepName());
        assertEquals(LOAD_VEP_ANNOTATION, loadVepAnnotationsStep.getStepName());

        //check list of variants without annotation output file
        assertTrue(vepInputFile.exists());
        assertEquals("20\t60343\t60343\tG/A\t+", JobTestUtils.readFirstLine(vepInputFile));

        //check that documents have the annotation
        DBCursor cursor = mongoRule.getCollection(jobOptions.getDbName(), jobOptions.getDbCollectionsVariantsName())
                .find();

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
                .filter(stepExecution -> stepExecution.getStepName().equals(LOAD_VEP_ANNOTATION))
                .collect(Collectors.toList());
        assertEquals(1, variantAnnotationLoadStepExecution.get(0).getReadSkipCount());
    }

    @Test
    public void noVariantsToAnnotateOnlyFindVariantsToAnnotateStepShouldRun() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(1, jobExecution.getStepExecutions().size());
        StepExecution findVariantsToAnnotateStep = new ArrayList<>(jobExecution.getStepExecutions()).get(0);

        assertEquals(FIND_VARIANTS_TO_ANNOTATE, findVariantsToAnnotateStep.getStepName());

        assertTrue(vepInputFile.exists());
        assertTrue(Files.size(Paths.get(vepInputFile.toPath().toUri())) == 0);
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();

        vepInputFile = new File(jobOptions.getVepInput());
        jobOptions.setAppVepPath(getResource(MOCK_VEP));

        converter = new DBObjectToVariantAnnotationConverter();
    }

    /**
     * Release resources and delete the temporary output file
     */
    @After
    public void tearDown() throws Exception {
        vepInputFile.delete();
        new File(jobOptions.getVepOutput()).delete();
    }

}
