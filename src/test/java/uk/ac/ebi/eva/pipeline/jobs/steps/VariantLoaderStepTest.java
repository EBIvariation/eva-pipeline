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
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.VariantConfig;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.jobs.VariantConfiguration;
import uk.ac.ebi.eva.pipeline.jobs.VariantConfigurationTest;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import static junit.framework.TestCase.assertEquals;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantsLoad}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {VariantConfiguration.class, VariantJobsArgs.class, VariantConfig.class})
public class VariantLoaderStepTest {
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private VariantJobsArgs variantJobsArgs;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobRepository jobRepository;

    @Autowired
    @Qualifier("variantJob")
    public Job job;

    private String input;
    private String outputDir;
    private String dbName;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void loaderStepShouldLoadAllVariants() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        variantJobsArgs.getVariantOptions().put(VariantStorageManager.DB_NAME, dbName);
        variantJobsArgs.getVariantOptions().put(VARIANT_SOURCE, new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE));

        //and a variants transform step already executed
        File transformedVcfVariantsFile =
                new File(VariantConfigurationTest.class.getResource("/small20.vcf.gz.variants.json.gz").getFile());
        File tmpTransformedVcfVariantsFile = new File(outputDir, transformedVcfVariantsFile.getName());
        FileUtils.copyFile(transformedVcfVariantsFile, tmpTransformedVcfVariantsFile);

        File transformedVariantsFile =
                new File(VariantConfigurationTest.class.getResource("/small20.vcf.gz.file.json.gz").getFile());
        File tmpTransformedVariantsFile = new File(outputDir, transformedVariantsFile.getName());
        FileUtils.copyFile(transformedVariantsFile, tmpTransformedVariantsFile);

        // When the execute method in variantsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(VariantConfiguration.LOAD_VARIANTS);

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
        String inputFile = VariantConfigurationTest.class.getResource(input).getFile();

        Config.setOpenCGAHome("");

        variantJobsArgs.getPipelineOptions().put("input.vcf", inputFile);
        variantJobsArgs.getVariantOptions().put(VariantStorageManager.DB_NAME, dbName);

        VariantSource source = (VariantSource) variantJobsArgs.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE);

        variantJobsArgs.getVariantOptions().put(VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                input,
                source.getFileId(),
                source.getStudyId(),
                source.getStudyName(),
                source.getType(),
                source.getAggregation()));

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(VariantConfiguration.LOAD_VARIANTS);

        assertEquals(inputFile, variantJobsArgs.getPipelineOptions().getString("input.vcf"));
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
        jobLauncherTestUtils = new JobLauncherTestUtils();
        jobLauncherTestUtils.setJob(job);
        jobLauncherTestUtils.setJobLauncher(jobLauncher);
        jobLauncherTestUtils.setJobRepository(jobRepository);

        input = variantJobsArgs.getPipelineOptions().getString("input.vcf");
        outputDir = variantJobsArgs.getPipelineOptions().getString("output.dir");
        dbName = variantJobsArgs.getPipelineOptions().getString("db.name");
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
    }
}
