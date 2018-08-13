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
import uk.ac.ebi.eva.t2d.configuration.T2dDataSourceConfiguration;
import uk.ac.ebi.eva.t2d.entity.DatasetIdToPhenotype;
import uk.ac.ebi.eva.t2d.entity.DatasetPhenotypeToTable;
import uk.ac.ebi.eva.t2d.jobs.LoadSummaryStatisticsJob;
import uk.ac.ebi.eva.t2d.repository.DatasetIdToPhenotypeRepository;
import uk.ac.ebi.eva.t2d.repository.DatasetPhenotypeToTableRepository;
import uk.ac.ebi.eva.t2d.repository.PhenotypeRepository;
import uk.ac.ebi.eva.t2d.repository.PropertyRepository;
import uk.ac.ebi.eva.t2d.repository.PropertyToDatasetAndPhenotypeRepository;
import uk.ac.ebi.eva.t2d.repository.PropertyToDatasetRepository;
import uk.ac.ebi.eva.test.t2d.configuration.BatchJobExecutorInMemory;
import uk.ac.ebi.eva.test.t2d.configuration.TestJpaConfiguration;
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
        BatchJobExecutorInMemory.class, LoadSummaryStatisticsJob.class})
@TestPropertySource({"classpath:application-t2d.properties"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class LoadSummaryStatisticsJobTest {

    private static final String INPUT_STATISTICS_FILE = "/t2d/stats.tsv";
    private static final String INPUT_STATISTICS_DEFINITION_FILE = "/t2d/stats.definition.tsv";
    private static final String QUERY_CHECK_TABLE_EXISTS =
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='GWAS_OXBB_MDV1'";
    private static final String QUERY_CHECK_TABLE_EXISTS_PHENOTYPE =
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='GWAS_OXBB_MDV1__BMI'";
    private static final String QUERY_COUNT_STATS = "SELECT COUNT(*) FROM GWAS_OXBB_MDV1";
    private static final String QUERY_COUNT_STATS_PHENOTYPE = "SELECT COUNT(*) FROM GWAS_OXBB_MDV1__BMI";
    private static final String PHENOTYPE = "BMI";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private DataSource datasource;

    @Autowired
    private PropertyRepository propertyRepository;

    @Autowired
    private PropertyToDatasetRepository propertyToDatasetRepository;

    @Autowired
    private DatasetPhenotypeToTableRepository datasetPhenotypeToTableRepository;

    @Autowired
    private DatasetIdToPhenotypeRepository datasetIdToPhenotypeRepository;

    @Autowired
    private PhenotypeRepository phenotypeRepository;

    @Autowired
    private PropertyToDatasetAndPhenotypeRepository propertyToDatasetAndPhenotypeRepository;

    @Test(expected = JobParametersInvalidException.class)
    public void testLoadSummaryStatisticsMissingParameter() throws Exception {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputStudyType("GWAS")
                .t2dInputStudyGenerator("OxBB")
                .t2dInputStudyVersion(1)
                .t2dInputStatistics(getResource(INPUT_STATISTICS_FILE).getPath())
                .t2dInputStatisticsDefinition(getResource(INPUT_STATISTICS_DEFINITION_FILE).getPath())
                .toJobParameters();

        jobLauncherTestUtils.launchJob(jobParameters);
    }

    @Test
    public void testLoadSummaryStatistics() throws Exception {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputStudyType("GWAS")
                .t2dInputStudyGenerator("OxBB")
                .t2dInputStudyVersion(1)
                .t2dInputStudyRelease(1)
                .t2dInputStatistics(getResource(INPUT_STATISTICS_FILE).getPath())
                .t2dInputStatisticsDefinition(getResource(INPUT_STATISTICS_DEFINITION_FILE).getPath())
                .toJobParameters();

        assertCompleted(jobLauncherTestUtils.launchJob(jobParameters));

        assertPrepareDatabasePhenotypeNull();
        assertNineVariantStatisticsStoredPhenotypeNull();
    }

    private void assertPrepareDatabasePhenotypeNull() throws SQLException {
        assertEquals(4, propertyRepository.count());
        assertEquals(0, phenotypeRepository.count());
        assertEquals(0, datasetIdToPhenotypeRepository.count());
        assertEquals(0, datasetPhenotypeToTableRepository.count());
        assertEquals(4, propertyToDatasetRepository.count());
        assertEquals(0, propertyToDatasetAndPhenotypeRepository.count());
        assertEquals("STRING", propertyRepository.findOne("VAR_ID").getType());
        assertEquals("GWAS_OxBB_mdv1",
                propertyToDatasetRepository.findAll().iterator().next().getDatasetId());

        ResultSet resultTableExists = datasource.getConnection()
                .prepareStatement(QUERY_CHECK_TABLE_EXISTS)
                .executeQuery();
        assertTrue(resultTableExists.next());
    }

    private void assertNineVariantStatisticsStoredPhenotypeNull() throws SQLException {
        ResultSet resultSet = datasource.getConnection()
                .prepareStatement(QUERY_COUNT_STATS)
                .executeQuery();
        assertTrue(resultSet.next());
        assertEquals(9, resultSet.getInt(1));
    }

    @Test
    public void testLoadSummaryStatisticsPhenotype() throws Exception {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputStudyType("GWAS")
                .t2dInputStudyGenerator("OxBB")
                .t2dInputStudyVersion(1)
                .t2dInputStudyRelease(1)
                .t2dInputStatistics(getResource(INPUT_STATISTICS_FILE).getPath())
                .t2dInputStatisticsDefinition(getResource(INPUT_STATISTICS_DEFINITION_FILE).getPath())
                .t2dInputStatisticsPhenotype(PHENOTYPE)
                .toJobParameters();

        assertCompleted(jobLauncherTestUtils.launchJob(jobParameters));

        assertPrepareDatabasePhenotype();
        assertNineVariantStatisticsStoredPhenotype();
    }

    private void assertPrepareDatabasePhenotype() throws SQLException {
        assertEquals(4, propertyRepository.count());
        assertEquals(1, phenotypeRepository.count());
        assertEquals(1, datasetIdToPhenotypeRepository.count());
        assertEquals(1, datasetPhenotypeToTableRepository.count());
        assertEquals(0, propertyToDatasetRepository.count());
        assertEquals(4, propertyToDatasetAndPhenotypeRepository.count());
        assertEquals("STRING", propertyRepository.findOne("VAR_ID").getType());
        assertEquals("GWAS_OxBB_mdv1",
                propertyToDatasetAndPhenotypeRepository.findAll().iterator().next().getDatasetId());
        assertEquals(new DatasetPhenotypeToTable("GWAS_OxBB_mdv1", PHENOTYPE).toString(),
                datasetPhenotypeToTableRepository.findAll().iterator().next().toString());
        assertEquals(new DatasetIdToPhenotype("GWAS_OxBB_mdv1", PHENOTYPE).toString(),
                datasetIdToPhenotypeRepository.findAll().iterator().next().toString());

        ResultSet resultTableExists = datasource.getConnection()
                .prepareStatement(QUERY_CHECK_TABLE_EXISTS_PHENOTYPE)
                .executeQuery();
        assertTrue(resultTableExists.next());
    }

    private void assertNineVariantStatisticsStoredPhenotype() throws SQLException {
        ResultSet resultSet = datasource.getConnection()
                .prepareStatement(QUERY_COUNT_STATS_PHENOTYPE)
                .executeQuery();
        assertTrue(resultSet.next());
        assertEquals(9, resultSet.getInt(1));
    }

}
