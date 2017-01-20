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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.jobs.AnnotationJob;
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.VepAnnotationGeneratorStep;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.VepUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

/**
 * Test for {@link VepAnnotationGeneratorStep}
 */
@RunWith(SpringRunner.class)
@ActiveProfiles(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
@TestPropertySource("classpath:annotation.properties")
@ContextConfiguration(classes = {AnnotationJob.class, BatchTestConfiguration.class})
public class VepAnnotationGeneratorStepTest {

    private static final String MOCKVEP = "/mockvep.pl";

    private static final String STUDY_ID = "7";

    private static final String FILE_ID = "5";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void shouldGenerateVepAnnotations() throws Exception {
        File vepOutputFolder = temporaryFolderRule.newFolder();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .inputFasta("")
                .inputStudyId(STUDY_ID)
                .inputVcfId(FILE_ID)
                .outputDirAnnotation(vepOutputFolder.getAbsolutePath())
                .vepCachePath("")
                .vepCacheSpecies("")
                .vepCacheVersion("")
                .vepNumForks("")
                .vepPath(getResource(MOCKVEP).getPath())
                .toJobParameters();

        // When the execute method in variantsAnnotCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils
                .launchStep(BeanNames.GENERATE_VEP_ANNOTATION_STEP, jobParameters);

        //Then variantsAnnotCreate step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And VEP output should exist and annotations should be in the file
        File vepOutputFile = new File(VepUtils.resolveVepOutput(vepOutputFolder.getAbsolutePath(), STUDY_ID, FILE_ID));

        assertTrue(vepOutputFile.exists());
        assertEquals(537, JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
    }

}
