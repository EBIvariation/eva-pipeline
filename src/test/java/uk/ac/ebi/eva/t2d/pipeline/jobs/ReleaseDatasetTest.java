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
import uk.ac.ebi.eva.t2d.entity.DatasetVersionMetadata;
import uk.ac.ebi.eva.t2d.repository.DatasetVersionMetadataRepository;
import uk.ac.ebi.eva.test.t2d.configuration.BatchJobExecutorInMemory;
import uk.ac.ebi.eva.t2d.configuration.T2dDataSourceConfiguration;
import uk.ac.ebi.eva.test.t2d.configuration.TestJpaConfiguration;
import uk.ac.ebi.eva.t2d.entity.DatasetMetadata;
import uk.ac.ebi.eva.t2d.entity.SamplesDatasetMetadata;
import uk.ac.ebi.eva.t2d.jobs.ReleaseDataset;
import uk.ac.ebi.eva.t2d.repository.DatasetMetadataRepository;
import uk.ac.ebi.eva.t2d.repository.SamplesDatasetMetadataRepository;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

@RunWith(SpringRunner.class)
@ActiveProfiles({Application.T2D_PROFILE})
@ContextConfiguration(classes = {T2dDataSourceConfiguration.class, TestJpaConfiguration.class,
        BatchJobExecutorInMemory.class, ReleaseDataset.class})
@TestPropertySource({"classpath:application-t2d.properties"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class ReleaseDatasetTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private DatasetMetadataRepository datasetMetadataRepository;

    @Autowired
    private DatasetVersionMetadataRepository datasetVersionMetadataRepository;

    @Autowired
    private SamplesDatasetMetadataRepository samplesDatasetMetadataRepository;

    @Test(expected = JobParametersInvalidException.class)
    public void testReleaseDatasetMissingParameter() throws Exception {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputStudyType("GWAS")
                .t2dInputStudyVersion(1)
                .t2dInputStudyRelease(2)
                .toJobParameters();

        jobLauncherTestUtils.launchJob(jobParameters);
    }

    @Test
    public void testReleaseDataset() throws Exception {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputStudyType("GWAS")
                .t2dInputStudyGenerator("OxBB")
                .t2dInputStudyVersion(1)
                .t2dInputStudyRelease(2)
                .toJobParameters();

        assertCompleted(jobLauncherTestUtils.launchJob(jobParameters));

        assertEquals(1, datasetMetadataRepository.count());
        assertEquals(1,datasetVersionMetadataRepository.count());
        assertEquals(1, samplesDatasetMetadataRepository.count());

        final DatasetMetadata datasetMetadata = datasetMetadataRepository.findAll().iterator().next();
        final DatasetVersionMetadata  datasetVersionMetadata = datasetVersionMetadataRepository.findAll().iterator().next();
        final SamplesDatasetMetadata samplesDatasetMetadata = samplesDatasetMetadataRepository.findAll().iterator()
                .next();
        assertEquals("GWAS_OxBB_mdv1", datasetMetadata.getDatasetId());
        assertEquals("GWAS_OXBB_MDV1", datasetMetadata.getTableName());
        assertEquals("GWAS_OxBB_mdv1", datasetVersionMetadata.getId());
        assertEquals("GWAS_OxBB_mdv1", datasetVersionMetadata.getDatasetName());
        assertEquals("mdv2", datasetVersionMetadata.getVer());
        assertEquals("SAMPLES_GWAS_OxBB_mdv1", samplesDatasetMetadata.getId());
        assertEquals("SAMPLES_GWAS_OXBB_MDV1", samplesDatasetMetadata.getTableName());
        assertEquals("mdv2", samplesDatasetMetadata.getVer());
    }

}
