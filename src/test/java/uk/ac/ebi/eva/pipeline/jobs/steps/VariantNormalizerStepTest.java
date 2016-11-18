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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.opencga.lib.common.Config;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.GenotypedVcfConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;
import uk.ac.ebi.eva.pipeline.jobs.GenotypedVcfJob;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporalMongoRule;
import uk.ac.ebi.eva.utils.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getTransformedOutputPath;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link VariantNormalizerStep}
 * <p>
 * TODO:
 * FILE_WRONG_NO_ALT should be renamed because the alt allele is not missing but is the same as the reference
 */
@RunWith(SpringRunner.class)
@ActiveProfiles("variant-annotation-mongo")
@ContextConfiguration(classes = {GenotypedVcfJob.class, JobOptions.class, GenotypedVcfConfiguration.class, JobLauncherTestUtils.class})
public class VariantNormalizerStepTest {

    private static final String FILE_WRONG_NO_ALT = "/wrong_no_alt.vcf.gz";
    private static final String VCF_FILE = "/small20.vcf.gz";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporalMongoRule mongoRule = new TemporalMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;
    
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void normalizerStepShouldTransformAllVariants() throws Exception {
        Config.setOpenCGAHome(opencgaHome);
        jobOptions.getPipelineOptions().put(JobParametersNames.DB_NAME,mongoRule.getRandomTemporalDatabaseName());
        String temporaryFolder = temporaryFolderRule.getRoot().getAbsolutePath();
        jobOptions.getPipelineOptions().put(JobParametersNames.OUTPUT_DIR, temporaryFolder);

        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, getResource(VCF_FILE));

        String outputFilename = getTransformedOutputPath(Paths.get(VCF_FILE).getFileName(), ".gz", temporaryFolder);

        // When the execute method in variantsTransform is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.NORMALIZE_VARIANTS);

        //Then variantsTransform should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And the transformed file should contain the same number of line of the Vcf input file
        Assert.assertEquals(300, getLines(new GZIPInputStream(new FileInputStream(outputFilename))));
    }

    /**
     * This test has to fail because the vcf FILE_WRONG_NO_ALT is malformed:
     * in a variant a reference and a alternate allele are the same
     */
    @Test
    public void normalizerStepShouldFailIfVariantsAreMalformed() {
        Config.setOpenCGAHome(opencgaHome);
        jobOptions.getPipelineOptions().put(JobParametersNames.DB_NAME,mongoRule.getRandomTemporalDatabaseName());
        String temporaryFolder = temporaryFolderRule.getRoot().getAbsolutePath();
        jobOptions.getPipelineOptions().put(JobParametersNames.OUTPUT_DIR, temporaryFolder);

        //Given a malformed VCF input file
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, getResource(FILE_WRONG_NO_ALT));

        //When the execute method in variantsTransform is invoked then a StorageManagerException is thrown
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.NORMALIZE_VARIANTS);
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
    }

}
