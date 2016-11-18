package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.junit.Before;
import org.junit.Rule;
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
import org.springframework.boot.test.rule.OutputCapture;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.CommonConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;
import uk.ac.ebi.eva.pipeline.jobs.PopulationStatisticsJob;
import uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporalMongoRule;
import uk.ac.ebi.eva.utils.FileUtils;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;
import static uk.ac.ebi.eva.utils.FileUtils.getResourceUrl;

/**
 * Test for {@link PopulationStatisticsLoaderStep}
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {JobOptions.class, PopulationStatisticsJob.class, CommonConfiguration.class, JobLauncherTestUtils.class})
public class PopulationStatisticsLoaderStepTest {

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";
    private static final String SOURCE_FILE_NAME = "/1_1.source.stats.json.gz";
    private static final String VARIANTS_FILE_NAME = "/1_1.variants.stats.json.gz";
    private static final String VCF_FILE_NAME = "/small20.vcf.gz.variants.json.gz";
    private static final String FILE_NOT_FOUND_EXCEPTION = "java.io.FileNotFoundException:";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporalMongoRule mongoRule = new TemporalMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private JobOptions jobOptions;

    //Capture error output
    @Rule
    public OutputCapture capture = new OutputCapture();

    @Test
    public void statisticsLoaderStepShouldLoadStatsIntoDb() throws StorageManagerException, IllegalAccessException,
            ClassNotFoundException, InstantiationException, IOException, InterruptedException {
        //Given a valid VCF input file
        String input = getResource(SMALL_VCF_FILE).getAbsolutePath();
        VariantSource source = new VariantSource(input, "1", "1", "studyName");

        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, input);
        jobOptions.getVariantOptions().put(VariantStorageManager.VARIANT_SOURCE, source);

        //and a valid variants load and stats create steps already completed
        jobOptions.setDbName(mongoRule.importDumpInTemporalDatabase(getResourceUrl(MONGO_DUMP)));

        copyFilesToOutpurDir(createTempDirectoryForStatistics());

        // When the execute method in variantsStatsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.LOAD_STATISTICS);

        // Then variantsStatsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // The DB docs should have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(jobOptions.getDbName(), null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());
    }

    private void copyFilesToOutpurDir(String outputDir) throws IOException {
        // copy stat file to load
        FileUtils.copyResource(VARIANTS_FILE_NAME, outputDir);
        // copy source file to load
        FileUtils.copyResource(SOURCE_FILE_NAME, outputDir);
        // copy transformed vcf
        FileUtils.copyResource(VCF_FILE_NAME, outputDir);
    }

    private String createTempDirectoryForStatistics() {
        File temporaryFolder = temporaryFolderRule.getRoot();
        jobOptions.getPipelineOptions().put(JobParametersNames.OUTPUT_DIR_STATISTICS, temporaryFolder);
        String outputDir = temporaryFolder.getAbsolutePath();
        return outputDir;
    }

    @Test
    public void statisticsLoaderStepShouldFaildBecauseVariantStatsFileIsMissing() throws JobExecutionException {
        String input = getResource(SMALL_VCF_FILE).getAbsolutePath();
        VariantSource source = new VariantSource(input, "4", "1", "studyName");

        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, input);
        jobOptions.getVariantOptions().put(VariantStorageManager.DB_NAME, mongoRule.getRandomTemporalDatabaseName());
        jobOptions.getVariantOptions().put(VariantStorageManager.VARIANT_SOURCE, source);

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.LOAD_STATISTICS);
        assertThat(capture.toString(), containsString(FILE_NOT_FOUND_EXCEPTION));

        assertEquals(input, jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF));
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
    }

}
