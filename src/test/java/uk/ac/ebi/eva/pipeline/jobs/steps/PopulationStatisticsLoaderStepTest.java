package uk.ac.ebi.eva.pipeline.jobs.steps;

import com.mongodb.DBCursor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantStatsConverter;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.rule.OutputCapture;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.jobs.PopulationStatisticsJob;
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.PopulationStatisticsLoaderStep;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.copyResource;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;

/**
 * Test for {@link PopulationStatisticsLoaderStep}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {PopulationStatisticsJob.class, BatchTestConfiguration.class})
public class PopulationStatisticsLoaderStepTest {

    private static final String SMALL_VCF_FILE = "/input-files/vcf/genotyped.vcf.gz";
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";
    private static final String SOURCE_FILE_NAME = "/input-files/statistics/1_1.source.stats.json.gz";
    private static final String VARIANTS_FILE_NAME = "/input-files/statistics/1_1.variants.stats.json.gz";
    private static final String FILE_NOT_FOUND_EXCEPTION = "java.io.FileNotFoundException:";

    private static final String COLLECTION_FILES_NAME = "files";
    private static final String COLLECTION_VARIANTS_NAME = "variants";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    //Capture error output
    @Rule
    public OutputCapture capture = new OutputCapture();

    @Test
    public void statisticsLoaderStepShouldLoadStatsIntoDb() throws StorageManagerException, IllegalAccessException,
            ClassNotFoundException, InstantiationException, IOException, InterruptedException {
        //Given a valid VCF input file
        String input = getResource(SMALL_VCF_FILE).getAbsolutePath();
        String fileId = "1";
        String studyId = "1";
        String dbName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String statsDir = temporaryFolderRule.newFolder().getAbsolutePath();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(dbName)
                .inputStudyId(studyId)
                .inputVcf(input)
                .inputVcfId(fileId)
                .outputDirStats(statsDir)
                .toJobParameters();

        //and a valid variants load and stats create steps already completed
        copyFilesToOutpurDir(statsDir);

        // When the execute method in variantsStatsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_STATISTICS_STEP, jobParameters);

        // Then variantsStatsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // The DB docs should have the field "st"
        DBCursor cursor = mongoRule.getCollection(dbName, COLLECTION_VARIANTS_NAME).find();
        assertEquals(1, getCohortStatsFromFirstVariant(cursor).size());
    }

    private Map<String, VariantStats> getCohortStatsFromFirstVariant(DBCursor cursor) {
        DBObjectToVariantConverter variantConverter = getVariantConverter();
        Variant variant = variantConverter.convertToDataModelType(cursor.iterator().next());
        return variant.getSourceEntries().values().iterator().next().getCohortStats();
    }

    private DBObjectToVariantConverter getVariantConverter() {
        return new DBObjectToVariantConverter(
                new DBObjectToVariantSourceEntryConverter(VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS),
                new DBObjectToVariantStatsConverter());
    }

    private void copyFilesToOutpurDir(String outputDir) throws IOException {
        // copy stat file to load
        copyResource(VARIANTS_FILE_NAME, outputDir);
        // copy source file to load
        copyResource(SOURCE_FILE_NAME, outputDir);
    }

    @Test
    public void statisticsLoaderStepShouldFaildBecauseVariantStatsFileIsMissing()
            throws JobExecutionException, IOException, InterruptedException {
        String input = getResource(SMALL_VCF_FILE).getAbsolutePath();
        String fileId = "1";
        String studyId = "1";
        String dbName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String statsDir = temporaryFolderRule.newFolder().getAbsolutePath();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName("files")
                .collectionVariantsName("variants")
                .databaseName(dbName)
                .inputStudyId(studyId)
                .inputVcf(input)
                .inputVcfId(fileId)
                .outputDirStats(statsDir)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_STATISTICS_STEP, jobParameters);
        assertThat(capture.toString(), containsString(FILE_NOT_FOUND_EXCEPTION));

        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

}
