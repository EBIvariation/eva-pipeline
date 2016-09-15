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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.AnnotationConfig;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.jobs.VariantAnnotConfiguration;
import uk.ac.ebi.eva.pipeline.jobs.VariantAnnotConfigurationTest;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import static junit.framework.TestCase.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.makeGzipFile;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantsAnnotCreate}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantJobsArgs.class, VariantAnnotConfiguration.class, AnnotationConfig.class, JobLauncherTestUtils.class})
public class VariantsAnnotCreateTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
        File vepPathFile = new File(VariantAnnotConfigurationTest.class.getResource("/mockvep.pl").getFile());
        variantJobsArgs.setAppVepPath(vepPathFile);
    }

    @Test
    public void shouldGenerateAnnotations() throws Exception {
        makeGzipFile("20\t60343\t60343\tG/A\t+", variantJobsArgs.getVepInput());

        File vepOutputFile = JobTestUtils.createTempFile();
        variantJobsArgs.setVepOutput(vepOutputFile.getAbsolutePath());

        vepOutputFile.delete();
        TestCase.assertFalse(vepOutputFile.exists());  // ensure the annot file doesn't exist from previous executions

        // When the execute method in variantsAnnotCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(VariantAnnotConfiguration.GENERATE_VEP_ANNOTATION);

        //Then variantsAnnotCreate step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And VEP output should exist and annotations should be in the file
        TestCase.assertTrue(vepOutputFile.exists());
        Assert.assertEquals(537, JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
        vepOutputFile.delete();
    }

}