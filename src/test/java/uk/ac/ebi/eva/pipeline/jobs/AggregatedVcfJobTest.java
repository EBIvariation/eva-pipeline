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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.configuration.VariantAggregatedConfig;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

/**
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Test for {@link AggregatedVcfJob}
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JobOptions.class, AggregatedVcfJob.class, VariantAggregatedConfig.class, JobLauncherTestUtils.class})
public class AggregatedVcfJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    
    @Autowired
    private JobOptions jobOptions;

    private String dbName;

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();
    
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void aggregatedTransformAndLoadShouldBeExecuted() throws Exception {
        final String inputFilePath = "/job-aggregated/aggregated.vcf.gz";
        String inputFile = AggregatedVcfJobTest.class.getResource(inputFilePath).getFile();
        Config.setOpenCGAHome(opencgaHome);

        jobOptions.getPipelineOptions().put("output.dir", outputFolder.getRoot().getCanonicalPath());
        jobOptions.getPipelineOptions().put("output.dir.statistics", outputFolder.getRoot().getCanonicalPath());
        
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // check execution flow
        Assert.assertEquals(2, jobExecution.getStepExecutions().size());
        List<StepExecution> steps = new ArrayList<>(jobExecution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Assert.assertEquals(AggregatedVcfJob.NORMALIZE_VARIANTS, transformStep.getStepName());
        Assert.assertEquals(AggregatedVcfJob.LOAD_VARIANTS, loadStep.getStepName());

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));

        // check transformed file
        String outputFilename = JobTestUtils.getTransformedOutputPath(
                Paths.get(inputFile).getFileName(), jobOptions.getCompressExtension(), outputFolder.getRoot().getCanonicalPath());

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

    @BeforeClass
    public static void beforeTests() throws UnknownHostException {
        JobTestUtils.cleanDBs();
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
