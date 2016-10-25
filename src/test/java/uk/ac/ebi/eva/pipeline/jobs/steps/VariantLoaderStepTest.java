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
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import uk.ac.ebi.eva.pipeline.configuration.GenotypedVcfConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.GenotypedVcfJob;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantLoaderStep}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {GenotypedVcfJob.class, JobOptions.class, GenotypedVcfConfiguration.class, JobLauncherTestUtils.class})
public class VariantLoaderStepTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    private String dbName;

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();
    
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void loaderStepShouldLoadAllVariants() throws Exception {
        final String inputFilePath = "/loading/small20.vcf.gz";
        String inputFile = VariantNormalizerStepTest.class.getResource(inputFilePath).getFile();
        Config.setOpenCGAHome(opencgaHome);

        //and a variants transform step already executed
        File transformedVcfVariantsFile =
                new File(VariantLoaderStepTest.class.getResource("/loading/small20.vcf.gz.variants.json.gz").getFile());
        FileUtils.copyFileToDirectory(transformedVcfVariantsFile, outputFolder.getRoot());

        File transformedVariantsFile =
                new File(VariantLoaderStepTest.class.getResource("/loading/small20.vcf.gz.file.json.gz").getFile());
        FileUtils.copyFileToDirectory(transformedVariantsFile, outputFolder.getRoot());

        // When the execute method in variantsLoad is executed
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.LOAD_VARIANTS, jobParameters);

        //Then variantsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And the number of documents in db should be the same number of line of the vcf transformed file
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        long lines = getLines(new GZIPInputStream(new FileInputStream(transformedVcfVariantsFile)));

        assertEquals(count(iterator), lines);
    }

    @Test
    public void loaderStepShouldFailBecauseOpenCGAHomeIsWrong() throws JobExecutionException, IOException {
        final String inputFilePath = "/loading/small20.vcf.gz";
        String inputFile = VariantNormalizerStepTest.class.getResource(inputFilePath).getFile();

        Config.setOpenCGAHome("");

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.LOAD_VARIANTS, jobParameters);

        assertEquals(inputFile, jobParameters.getString("input.vcf"));
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
        dbName = jobOptions.getPipelineOptions().getString("db.name");
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
    }
}
