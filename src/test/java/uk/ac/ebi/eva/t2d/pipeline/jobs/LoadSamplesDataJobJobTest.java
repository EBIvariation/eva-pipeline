package uk.ac.ebi.eva.t2d.pipeline.jobs;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.test.t2d.configuration.BatchJobExecutorInMemory;
import uk.ac.ebi.eva.t2d.configuration.T2dDataSourceConfiguration;
import uk.ac.ebi.eva.test.t2d.configuration.TestJpaConfiguration;
import uk.ac.ebi.eva.t2d.jobs.LoadSamplesDataJob;
import uk.ac.ebi.eva.t2d.repository.SamplePropertyRepository;
import uk.ac.ebi.eva.t2d.repository.SamplePropertyToDatasetRepository;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

@RunWith(SpringRunner.class)
@ActiveProfiles({Application.T2D_PROFILE})
@ContextConfiguration(classes = {T2dDataSourceConfiguration.class, TestJpaConfiguration.class,
        BatchJobExecutorInMemory.class, LoadSamplesDataJob.class})
@TestPropertySource({"classpath:application-t2d.properties"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class LoadSamplesDataJobJobTest {

    private static final String INPUT_SAMPLES_FILE = "/t2d/short.tsv";
    private static final String INPUT_SAMPLES_DEFINITION_FILE = "/t2d/short.definition.tsv";
    private static final String QUERY_CHECK_SAMPLES_TABLE_EXISTS =
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='SAMPLES_GWAS_OXBB_MDV1'";
    private static final String QUERY_COUNT_SAMPLES = "SELECT COUNT(*) FROM SAMPLES_GWAS_OXBB_MDV1";

    @Autowired//    @Bean
//    public ResourcelessTransactionManager transactionManager() {
//        return new ResourcelessTransactionManager();
//    }
//
//    @Bean
//    public JobRepository jobRepository(ResourcelessTransactionManager transactionManager) throws Exception {
//        MapJobRepositoryFactoryBean mapJobRepositoryFactoryBean = new MapJobRepositoryFactoryBean(transactionManager);
//        mapJobRepositoryFactoryBean.setTransactionManager(transactionManager);
//        return mapJobRepositoryFactoryBean.getObject();
//    }
//
//    @Bean
//    public SimpleJobLauncher jobLauncher(JobRepository jobRepository) {
//        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
//        simpleJobLauncher.setJobRepository(jobRepository);
//        return simpleJobLauncher;
//    }
//
//    @Bean
//    public JobLauncherTestUtils jobLauncherTestUtils(){
//        return new JobLauncherTestUtils();
//    }
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private DataSource datasource;

    @Autowired
    private SamplePropertyRepository samplePropertyRepository;

    @Autowired
    private SamplePropertyToDatasetRepository samplePropertyToDatasetRepository;

    @Test(expected = JobParametersInvalidException.class)
    public void testLoadSamplesDataStepMissingParameter() throws Exception {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputStudyType("GWAS")
                .t2dInputStudyGenerator("OxBB")
                .t2dInputStudyVersion(1)
                .t2dInputStudyRelease(1)
                .t2dInputSamplesDefinition(getResource(INPUT_SAMPLES_DEFINITION_FILE).getPath())
                .toJobParameters();

        jobLauncherTestUtils.launchJob(jobParameters);
    }

    @Test
    public void testLoadSamplesDataStep() throws Exception {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputStudyType("GWAS")
                .t2dInputStudyGenerator("OxBB")
                .t2dInputStudyVersion(1)
                .t2dInputStudyRelease(1)
                .t2dInputSamples(getResource(INPUT_SAMPLES_FILE).getPath())
                .t2dInputSamplesDefinition(getResource(INPUT_SAMPLES_DEFINITION_FILE).getPath())
                .toJobParameters();

        assertCompleted(jobLauncherTestUtils.launchJob(jobParameters));

        assertPrepareDatabase();
        assertNineSamplesStored();

    }

    public void assertNineSamplesStored() throws SQLException {
        ResultSet resultSet = datasource.getConnection()
                .prepareStatement(QUERY_COUNT_SAMPLES)
                .executeQuery();
        assertTrue(resultSet.next());
        assertEquals(9, resultSet.getInt(1));
    }

    public void assertPrepareDatabase() throws SQLException {
        assertEquals(31, samplePropertyRepository.count());
        assertEquals(31, samplePropertyToDatasetRepository.count());
        assertEquals("BOOLEAN", samplePropertyRepository.findOne("T2D").getType());
        assertEquals("SAMPLES_GWAS_OxBB_mdv1",
                samplePropertyToDatasetRepository.findAll().iterator().next().getDatasetId());

        ResultSet resultTableExists = datasource.getConnection()
                .prepareStatement(QUERY_CHECK_SAMPLES_TABLE_EXISTS)
                .executeQuery();
        assertTrue(resultTableExists.next());
    }

}
