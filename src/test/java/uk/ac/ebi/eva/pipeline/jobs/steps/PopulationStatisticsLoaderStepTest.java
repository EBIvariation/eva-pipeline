package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerException;
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
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.CommonConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.PopulationStatisticsJob;
import uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.restoreMongoDbFromDump;

/**
 * @author Diego Poggioli
 *
 * Test for {@link PopulationStatisticsLoaderStep}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JobOptions.class, PopulationStatisticsJob.class, CommonConfiguration.class, JobLauncherTestUtils.class})
public class PopulationStatisticsLoaderStepTest {

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";
    private static final String STATS_DB = "VariantStatsConfigurationTest_vl"; //this name should be the same of the dump DB in /dump

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private JobOptions jobOptions;

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;

    private File statsFileToLoad;
    private File sourceFileToLoad;
    private File vcfFileToLoad;

    @Test
    public void statisticsLoaderStepShouldLoadStatsIntoDb() throws StorageManagerException, IllegalAccessException,
            ClassNotFoundException, InstantiationException, IOException, InterruptedException {
        //Given a valid VCF input file
        String input = PopulationStatisticsLoaderStepTest.class.getResource(SMALL_VCF_FILE).getFile();
        VariantSource source = new VariantSource(input, "1", "1", "studyName");

        String dbName = STATS_DB;

        pipelineOptions.put("input.vcf", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        //and a valid variants load and stats create steps already completed
        String dump = PopulationStatisticsLoaderStepTest.class.getResource("/dump/").getFile();
        restoreMongoDbFromDump(dump);

        String outputDir = pipelineOptions.getString("output.dir.statistics");

        // copy stat file to load
        String variantsFileName = "/1_1.variants.stats.json.gz";
        statsFileToLoad = new File(outputDir, variantsFileName);
        File variantStatsFile = new File(PopulationStatisticsLoaderStepTest.class.getResource(variantsFileName).getFile());
        FileUtils.copyFile(variantStatsFile, statsFileToLoad);

        // copy source file to load
        String sourceFileName = "/1_1.source.stats.json.gz";
        sourceFileToLoad = new File(outputDir, sourceFileName);
        File sourceStatsFile = new File(PopulationStatisticsLoaderStepTest.class.getResource(sourceFileName).getFile());
        FileUtils.copyFile(sourceStatsFile, sourceFileToLoad);

        // copy transformed vcf
        String vcfFileName = "/small20.vcf.gz.variants.json.gz";
        vcfFileToLoad = new File(outputDir, vcfFileName);
        File vcfFile = new File(PopulationStatisticsLoaderStepTest.class.getResource(vcfFileName).getFile());
        FileUtils.copyFile(vcfFile, vcfFileToLoad);

        // When the execute method in variantsStatsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.LOAD_STATISTICS);

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

    @Test
    public void statisticsLoaderStepShouldFaildBecauseVariantStatsFileIsMissing() throws JobExecutionException {
        String input = PopulationStatisticsLoaderStepTest.class.getResource(SMALL_VCF_FILE).getFile();
        VariantSource source = new VariantSource(input, "4", "1", "studyName");

        pipelineOptions.put("input.vcf", input);
        variantOptions.put(VariantStorageManager.DB_NAME, STATS_DB);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.LOAD_STATISTICS);

        assertEquals(input, pipelineOptions.getString("input.vcf"));
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
        pipelineOptions = jobOptions.getPipelineOptions();
        variantOptions = jobOptions.getVariantOptions();
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(STATS_DB);
    }

}
