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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
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
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;

/**
 * Test for {@link VariantLoaderStep}
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {GenotypedVcfJob.class, JobOptions.class, GenotypedVcfConfiguration.class, JobLauncherTestUtils.class})
public class VariantLoaderStepTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    private String input;
    private String outputDir;
    private String dbName;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void loaderStepShouldLoadAllVariants() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        jobOptions.getVariantOptions().put(VariantStorageManager.DB_NAME, dbName);
        jobOptions.getVariantOptions().put(VARIANT_SOURCE, new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE));

        //and a variants transform step already executed
        File transformedVcfVariantsFile =
                new File(VariantLoaderStepTest.class.getResource("/small20.vcf.gz.variants.json.gz").getFile());
        File tmpTransformedVcfVariantsFile = new File(outputDir, transformedVcfVariantsFile.getName());
        FileUtils.copyFile(transformedVcfVariantsFile, tmpTransformedVcfVariantsFile);

        File transformedVariantsFile =
                new File(VariantLoaderStepTest.class.getResource("/small20.vcf.gz.file.json.gz").getFile());
        File tmpTransformedVariantsFile = new File(outputDir, transformedVariantsFile.getName());
        FileUtils.copyFile(transformedVariantsFile, tmpTransformedVariantsFile);

        // When the execute method in variantsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.LOAD_VARIANTS);

        //Then variantsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And the number of documents in db should be the same number of line of the vcf transformed file
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        long lines = getLines(new GZIPInputStream(new FileInputStream(transformedVcfVariantsFile)));

        assertEquals(count(iterator), lines);

        tmpTransformedVcfVariantsFile.delete();
        tmpTransformedVariantsFile.delete();
    }

    @Test
    public void loaderStepShouldFailBecauseOpenCGAHomeIsWrong() throws JobExecutionException {
        String inputFile = VariantLoaderStepTest.class.getResource(input).getFile();

        Config.setOpenCGAHome("");

        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, inputFile);
        jobOptions.getVariantOptions().put(VariantStorageManager.DB_NAME, dbName);

        VariantSource source = (VariantSource) jobOptions.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE);

        jobOptions.getVariantOptions().put(VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                input,
                source.getFileId(),
                source.getStudyId(),
                source.getStudyName(),
                source.getType(),
                source.getAggregation()));

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(GenotypedVcfJob.LOAD_VARIANTS);

        assertEquals(inputFile, jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF));
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
