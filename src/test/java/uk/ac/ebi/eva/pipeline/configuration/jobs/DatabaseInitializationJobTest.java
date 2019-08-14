package uk.ac.ebi.eva.pipeline.configuration.jobs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {DatabaseInitializationJobConfiguration.class, BatchTestConfiguration.class})
public class DatabaseInitializationJobTest {

    private static final String COLLECTION_FEATURES_NAME = "features";

    private static final String DATABASE_NAME = "databaseInitializationTestDb";
    private static final String INPUT_FILE = "/input-files/gtf/small_sample.gtf.gz";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void testDatabaseInitializationJob() throws Exception {
        File inputFile = getResource(INPUT_FILE);
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(DATABASE_NAME)
                .inputGtf(inputFile.getAbsolutePath())
                .collectionFeaturesName(COLLECTION_FEATURES_NAME)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertCompleted(jobExecution);
        assertEquals(252L, mongoRule.getCollection(DATABASE_NAME, "features").count());
    }
}
