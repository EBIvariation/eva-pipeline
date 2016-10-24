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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getTransformedOutputPath;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.opencb.opencga.lib.common.Config;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import uk.ac.ebi.eva.pipeline.configuration.GenotypedVcfConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.CommonJobStepInitialization;
import uk.ac.ebi.eva.pipeline.jobs.GenotypedVcfJob;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

/**
 * @author Diego Poggioli
 * @author Cristina Yenyxe Gonzalez Garcia
 *
 * Test for {@link VariantNormalizerStep}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {GenotypedVcfJob.class, JobOptions.class, GenotypedVcfConfiguration.class, JobLauncherTestUtils.class})
public class VariantNormalizerStepTest extends CommonJobStepInitialization {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();
    
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void normalizerStepShouldTransformAllVariants() throws IOException {
        final String inputFilePath = "/normalization/correct.vcf.gz";
        String inputFile = VariantNormalizerStepTest.class.getResource(inputFilePath).getFile();
        
        Config.setOpenCGAHome(opencgaHome);

        // When the execute method in variantsTransform is invoked
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.NORMALIZE_VARIANTS, jobParameters);

        // Then variantsTransform should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And the transformed file should contain the same number of line of the VCF input file
        String outputFilename = JobTestUtils.getTransformedOutputPath(
                Paths.get(inputFile).getFileName(), jobOptions.getCompressExtension(), outputFolder.getRoot().getCanonicalPath());
        assertEquals(300, JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(outputFilename))));
    }

    /**
     * This test has to fail because the VCF file is malformed:
     * in a variant a reference and a alternate allele are the same
     */
    @Test
    public void normalizerStepShouldFailIfVariantsAreMalformed() throws IOException {
        final String inputFilePath = "/normalization/wrong_same_alleles.vcf.gz";
        String inputFile = VariantNormalizerStepTest.class.getResource(inputFilePath).getFile();
        
        Config.setOpenCGAHome(opencgaHome);

        // When the execute method in variantsTransform is invoked then a StorageManagerException is thrown
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.NORMALIZE_VARIANTS, jobParameters);
        
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
    }

}
