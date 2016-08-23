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
package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.VariantJobsArgs;
import embl.ebi.variation.eva.pipeline.steps.*;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Paths;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.restoreMongoDbFromDump;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;

/**
 * Created by jmmut on 2015-10-14.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Test for {@link VariantStatsConfiguration}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {VariantStatsConfiguration.class, CommonConfig.class, JobLauncherTestUtils.class})
public class VariantStatsConfigurationTest {

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";

    private static final String STATS_DB = "VariantStatsConfigurationTest_vl"; //this name should be the same of the dump DB in /dump

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer;

    @Autowired
    public VariantJobsArgs variantJobsArgs;

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;

    private File statsFile;
    private File statsFileToLoad;
    private File sourceFileToLoad;
    private File vcfFileToLoad;

    @Test
    public void statsCreateStepShouldCalculateStats() throws IOException, InterruptedException {
        //and a valid variants load step already completed
        String dump = VariantStatsConfigurationTest.class.getResource("/dump/").getFile();
        restoreMongoDbFromDump(dump);

        //Given a valid VCF input file
        String input = SMALL_VCF_FILE;

        pipelineOptions.put("input.vcf", input);
        variantOptions.put(VariantStorageManager.DB_NAME, STATS_DB);
        pipelineOptions.put(VariantsStatsCreate.SKIP_STATS_CREATE, false);

        VariantSource source = new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        variantOptions.put(VARIANT_SOURCE, source);

        statsFile = new File(Paths.get(pipelineOptions.getString("output.dir")).resolve(VariantStorageManager.buildFilename(source))
                + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        // When the execute method in variantsStatsCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("statsCreate");

        //Then variantsStatsCreate step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //and the file containing statistics should exist
        assertTrue(statsFile.exists());

        //delete created files
        statsFile.delete();
        new File(Paths.get(pipelineOptions.getString("output.dir")).resolve(VariantStorageManager.buildFilename(source))
                + ".source.stats.json.gz").delete();

    }

    /**
     * This test has to fail because it will try to extract variants from a non-existent DB.
     * Variants not loaded.. so nothing to query!
     */
    @Test
    public void statsCreateStepShouldFailIfVariantLoadStepIsNotCompleted() throws Exception {
        //Given a valid VCF input file
        String input = SMALL_VCF_FILE;

        pipelineOptions.put("input.vcf", input);
        variantOptions.put(VariantStorageManager.DB_NAME, STATS_DB);
        pipelineOptions.put(VariantsStatsCreate.SKIP_STATS_CREATE, false);

        VariantSource source = new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        variantOptions.put(VARIANT_SOURCE, source);

        statsFile = new File(Paths.get(pipelineOptions.getString("output.dir")).resolve(VariantStorageManager.buildFilename(source))
                + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        // When the execute method in variantsStatsCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("statsCreate");
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Test
    public void statsLoadStepShouldLoadStatsIntoDb() throws StorageManagerException, IllegalAccessException,
            ClassNotFoundException, InstantiationException, IOException, InterruptedException {
        //Given a valid VCF input file
        String input = VariantStatsConfigurationTest.class.getResource(SMALL_VCF_FILE).getFile();
        VariantSource source = new VariantSource(input, "1", "1", "studyName");

        String dbName = STATS_DB;

        pipelineOptions.put("input.vcf", input);
        pipelineOptions.put(VariantsStatsLoad.SKIP_STATS_LOAD, "false");
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        initStatsLoadStepFiles();

        // When the execute method in variantsStatsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("statsLoad");

        // Then variantsStatsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // The DB docs should have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

        statsFileToLoad.delete();
        sourceFileToLoad.delete();
        vcfFileToLoad.delete();
    }

    private void initStatsLoadStepFiles() throws IOException, InterruptedException {
        //and a valid variants load and stats create steps already completed
        String dump = VariantStatsConfigurationTest.class.getResource("/dump/").getFile();
        restoreMongoDbFromDump(dump);

        String outputDir = pipelineOptions.getString("output.dir");

        // copy stat file to load
        String variantsFileName = "/1_1.variants.stats.json.gz";
        statsFileToLoad = new File(outputDir, variantsFileName);
        File variantStatsFile = new File(VariantStatsConfigurationTest.class.getResource(variantsFileName).getFile());
        FileUtils.copyFile(variantStatsFile, statsFileToLoad);

        // copy source file to load
        String sourceFileName = "/1_1.source.stats.json.gz";
        sourceFileToLoad = new File(outputDir, sourceFileName);
        File sourceStatsFile = new File(VariantStatsConfigurationTest.class.getResource(sourceFileName).getFile());
        FileUtils.copyFile(sourceStatsFile, sourceFileToLoad);

        // copy transformed vcf
        String vcfFileName = "/small20.vcf.gz.variants.json.gz";
        vcfFileToLoad = new File(outputDir, vcfFileName);
        File vcfFile = new File(VariantStatsConfigurationTest.class.getResource(vcfFileName).getFile());
        FileUtils.copyFile(vcfFile, vcfFileToLoad);
    }

    /**
     *  This test should fail because the variants.stats file is missing
     */
    @Test
    public void invalidLoadStats() throws JobExecutionException {
        String input = VariantStatsConfigurationTest.class.getResource(SMALL_VCF_FILE).getFile();
        VariantSource source = new VariantSource(input, "4", "1", "studyName");

        pipelineOptions.put("input.vcf", input);
        pipelineOptions.put(VariantsStatsLoad.SKIP_STATS_LOAD, false);
        variantOptions.put(VariantStorageManager.DB_NAME, STATS_DB);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        JobExecution jobExecution = jobLauncherTestUtils.launchStep("statsLoad");

        assertEquals(input, pipelineOptions.getString("input.vcf"));
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Test
    public void fullStatsJob() throws Exception {
        //Given a valid VCF input file
        String input = SMALL_VCF_FILE;

        pipelineOptions.put("input.vcf", input);
        variantOptions.put(VariantStorageManager.DB_NAME, STATS_DB);
        pipelineOptions.put(VariantsStatsCreate.SKIP_STATS_CREATE, false);
        pipelineOptions.put(VariantsStatsLoad.SKIP_STATS_LOAD, false);

        VariantSource source = new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        variantOptions.put(VARIANT_SOURCE, source);

        statsFile = new File(Paths.get(pipelineOptions.getString("output.dir")).resolve(VariantStorageManager.buildFilename(source))
                + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        initStatsLoadStepFiles();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //and the file containing statistics should exist
        assertTrue(statsFile.exists());

        //delete created files
        statsFile.delete();
        new File(Paths.get(pipelineOptions.getString("output.dir")).resolve(VariantStorageManager.buildFilename(source))
                + ".source.stats.json.gz").delete();

        // The DB docs should have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(STATS_DB, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

        statsFileToLoad.delete();
        sourceFileToLoad.delete();
        vcfFileToLoad.delete();
    }

    @Before
    public void setUp() throws Exception {
        //re-initialize common config before each test
        variantJobsArgs.loadArgs();
        pipelineOptions = variantJobsArgs.getPipelineOptions();
        variantOptions = variantJobsArgs.getVariantOptions();
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(STATS_DB);
    }

}
