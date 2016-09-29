/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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

import org.junit.*;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
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
import org.springframework.boot.test.IntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.VariantAggregatedConfig;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.cleanDBs;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getTransformedOutputPath;

/**
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Test for {@link VariantAggregatedConfiguration}
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {VariantJobsArgs.class, VariantAggregatedConfiguration.class, VariantAggregatedConfig.class})
public class VariantAggregatedConfigurationTest {

    @Autowired
    @Qualifier("variantJobAggregated")
    public Job job;
    @Autowired
    private VariantJobsArgs variantJobsArgs;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobRepository jobRepository;

    private JobLauncherTestUtils jobLauncherTestUtils;
    private String input;
    private String outputDir;
    private String compressExtension;
    private String dbName;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void aggregatedTransformAndLoadShouldBeExecuted() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        // transformedVcf file init
        String transformedVcf = outputDir + input + ".variants.json" + compressExtension;
        File transformedVcfFile = new File(transformedVcf);
        transformedVcfFile.delete();
        assertFalse(transformedVcfFile.exists());

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // check execution flow
        Assert.assertEquals(2, jobExecution.getStepExecutions().size());
        List<StepExecution> steps = new ArrayList<>(jobExecution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Assert.assertEquals(VariantAggregatedConfiguration.NORMALIZE_VARIANTS, transformStep.getStepName());
        Assert.assertEquals(VariantAggregatedConfiguration.LOAD_VARIANTS, loadStep.getStepName());

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));

        // check transformed file
        String outputFilename = getTransformedOutputPath(Paths.get(input).getFileName(), compressExtension, outputDir);

        long lines = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(outputFilename)));
        assertEquals(156, lines);

        // check ((documents in DB) == (lines in transformed file))
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        Assert.assertEquals(JobTestUtils.count(iterator), lines);

        // check that stats are loaded properly
        assertFalse(variantDBAdaptor.iterator(
                new QueryOptions()).next().getSourceEntries().values().iterator().next().getCohortStats().isEmpty());
    }

    @Test
    public void aggregationNoneOptionShouldNotLoadStats() throws Exception {
        VariantSource source =
                (VariantSource) variantJobsArgs.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE);
        variantJobsArgs.getVariantOptions().put(
                VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                input,
                source.getFileId(),
                source.getStudyId(),
                source.getStudyName(),
                source.getType(),
                VariantSource.Aggregation.NONE));

        Config.setOpenCGAHome(opencgaHome);

        // transformedVcf file init
        String transformedVcf = outputDir + input + ".variants.json" + compressExtension;
        File transformedVcfFile = new File(transformedVcf);
        transformedVcfFile.delete();
        assertFalse(transformedVcfFile.exists());

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // check transformed file
        String outputFilename = getTransformedOutputPath(Paths.get(input).getFileName(), compressExtension, outputDir);

        long lines = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(outputFilename)));
        assertEquals(156, lines);

        // check ((documents in DB) == (lines in transformed file))
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        Assert.assertEquals(JobTestUtils.count(iterator), lines);

        // check that stats are NOT loaded
        assertTrue(variantDBAdaptor.iterator(
                new QueryOptions()).next().getSourceEntries().values().iterator().next().getCohortStats().isEmpty());
    }

    @BeforeClass
    public static void beforeTests() throws UnknownHostException {
        cleanDBs();
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
        compressExtension = variantJobsArgs.getPipelineOptions().getString("compressExtension");
        dbName = variantJobsArgs.getPipelineOptions().getString("db.name");

        String inputFile = VariantConfigurationTest.class.getResource(input).getFile();
        variantJobsArgs.getPipelineOptions().put("input.vcf", inputFile);
    }

    @After
    public void tearDown() throws Exception {
        cleanDBs(dbName);
    }

}
