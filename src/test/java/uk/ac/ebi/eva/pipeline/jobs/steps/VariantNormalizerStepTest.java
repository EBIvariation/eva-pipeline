/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.opencga.lib.common.Config;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.configuration.GenotypedVcfConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;
import uk.ac.ebi.eva.pipeline.jobs.GenotypedVcfJob;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getTransformedOutputPath;

/**
 * Test for {@link VariantNormalizerStep}
 * <p>
 * TODO:
 * FILE_WRONG_NO_ALT should be renamed because the alt allele is not missing but is the same as the reference
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {GenotypedVcfJob.class, JobOptions.class, GenotypedVcfConfiguration.class, JobLauncherTestUtils.class})
public class VariantNormalizerStepTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    private String input;
    private String outputDir;
    private String dbName;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void normalizerStepShouldTransformAllVariants() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        String inputFile = VariantNormalizerStepTest.class.getResource(input).getFile();
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, inputFile);

        String outputFilename = getTransformedOutputPath(Paths.get(input).getFileName(), ".gz", "/tmp");

        File file = new File(outputFilename);
        if (file.exists())
            file.delete();
        assertFalse(file.exists());

        // When the execute method in variantsTransform is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.NORMALIZE_VARIANTS);

        //Then variantsTransform should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And the transformed file should contain the same number of line of the Vcf input file
        Assert.assertEquals(300, getLines(new GZIPInputStream(new FileInputStream(outputFilename))));

        file.delete();
        new File(outputDir, "small20.vcf.gz.file.json.gz").delete();
    }

    /**
     * This test has to fail because the vcf FILE_WRONG_NO_ALT is malformed:
     * in a variant a reference and a alternate allele are the same
     */
    @Test
    public void normalizerStepShouldFailIfVariantsAreMalformed() {
        final String FILE_WRONG_NO_ALT = "/wrong_no_alt.vcf.gz";
        Config.setOpenCGAHome(opencgaHome);

        //Given a malformed VCF input file
        String inputFile = VariantNormalizerStepTest.class.getResource(FILE_WRONG_NO_ALT).getFile();
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, inputFile);

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_WRONG_NO_ALT).getFileName(), ".gz", "/tmp");

        File file = new File(outputFilename);
        file.delete();
        assertFalse(file.exists());

        //When the execute method in variantsTransform is invoked then a StorageManagerException is thrown
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.NORMALIZE_VARIANTS);
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();

        input = jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF);
        outputDir = jobOptions.getOutputDir();
        dbName = jobOptions.getPipelineOptions().getString(JobParametersNames.DB_NAME);
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
    }

}
