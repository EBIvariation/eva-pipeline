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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.CommonConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.restoreMongoDbFromDump;

/**
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Test for {@link PopulationStatisticsJob}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JobOptions.class, PopulationStatisticsJob.class, CommonConfiguration.class, JobLauncherTestUtils.class})
public class PopulationStatisticsJobTest {

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private JobOptions jobOptions;

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;

    private File statsFile;
    private File statsFileToLoad;
    private File sourceFileToLoad;
    private File vcfFileToLoad;

    @Test
    public void fullPopulationStatisticsJob() throws Exception {
        String dump = PopulationStatisticsJobTest.class.getResource("/dump/VariantStatsConfigurationTest_vl").getFile();
        JobTestUtils.restoreMongoDbFromDump(dump, jobOptions.getDbName());
        
        //Given a valid VCF input file
        String input = SMALL_VCF_FILE;

        pipelineOptions.put("input.vcf", input);

        VariantSource source = new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        variantOptions.put(VARIANT_SOURCE, source);

        statsFile = new File(Paths.get(pipelineOptions.getString("output.dir.statistics"))
                .resolve(VariantStorageManager.buildFilename(source)) + ".variants.stats.json.gz");
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
        new File(Paths.get(pipelineOptions.getString("output.dir.statistics")).resolve(VariantStorageManager.buildFilename(source))
                + ".source.stats.json.gz").delete();

        // The DB docs should have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(jobOptions.getDbName(), null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

        statsFileToLoad.delete();
        sourceFileToLoad.delete();
        vcfFileToLoad.delete();
    }

    private void initStatsLoadStepFiles() throws IOException, InterruptedException {
        //and a valid variants load and stats create steps already completed
        String dump = PopulationStatisticsJobTest.class.getResource("/dump/").getFile();
        restoreMongoDbFromDump(dump, jobOptions.getDbName());

        String outputDir = pipelineOptions.getString("output.dir.statistics");

        // copy stat file to load
        String variantsFileName = "/1_1.variants.stats.json.gz";
        statsFileToLoad = new File(outputDir, variantsFileName);
        File variantStatsFile = new File(PopulationStatisticsJobTest.class.getResource(variantsFileName).getFile());
        FileUtils.copyFile(variantStatsFile, statsFileToLoad);

        // copy source file to load
        String sourceFileName = "/1_1.source.stats.json.gz";
        sourceFileToLoad = new File(outputDir, sourceFileName);
        File sourceStatsFile = new File(PopulationStatisticsJobTest.class.getResource(sourceFileName).getFile());
        FileUtils.copyFile(sourceStatsFile, sourceFileToLoad);

        // copy transformed vcf
        String vcfFileName = "/small20.vcf.gz.variants.json.gz";
        vcfFileToLoad = new File(outputDir, vcfFileName);
        File vcfFile = new File(PopulationStatisticsJobTest.class.getResource(vcfFileName).getFile());
        FileUtils.copyFile(vcfFile, vcfFileToLoad);
    }

    @Before
    public void setUp() throws Exception {
        //re-initialize common config before each test
        jobOptions.loadArgs();
        jobOptions.setDbName(getClass().getSimpleName());
        pipelineOptions = jobOptions.getPipelineOptions();
        variantOptions = jobOptions.getVariantOptions();
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(jobOptions.getDbName());
    }

}
