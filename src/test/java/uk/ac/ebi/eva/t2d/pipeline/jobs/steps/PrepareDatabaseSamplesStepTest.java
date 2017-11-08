package uk.ac.ebi.eva.t2d.pipeline.jobs.steps;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.test.t2d.configuration.BatchJobExecutorInMemory;
import uk.ac.ebi.eva.test.t2d.configuration.T2dDataSourceConfiguration;
import uk.ac.ebi.eva.test.t2d.configuration.TestJpaConfiguration;
import uk.ac.ebi.eva.t2d.jobs.LoadSamplesDataJob;
import uk.ac.ebi.eva.t2d.repository.SamplePropertyRepository;
import uk.ac.ebi.eva.t2d.repository.SamplePropertyToDatasetRepository;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_PREPARE_DATABASE_SAMPLES_STEP;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

@RunWith(SpringRunner.class)
@ActiveProfiles({Application.T2D_PROFILE})
@ContextConfiguration(classes = {T2dDataSourceConfiguration.class, TestJpaConfiguration.class,
        BatchJobExecutorInMemory.class, LoadSamplesDataJob.class})
@TestPropertySource({"classpath:application-t2d.properties"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class PrepareDatabaseSamplesStepTest {

    private static final String INPUT_SAMPLES_FILE = "/t2d/short.tsv";
    private static final String INPUT_SAMPLES_DEFINITION_FILE = "/t2d/short.definition.tsv";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private SamplePropertyRepository samplePropertyRepository;

    @Autowired
    private SamplePropertyToDatasetRepository samplePropertyToDatasetRepository;

    @Test
    public void testPrepareDatabaseSamplesStep() {
        assertEquals(0, samplePropertyRepository.count());
        assertEquals(0, samplePropertyToDatasetRepository.count());

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputStudyType("GWAS")
                .t2dInputStudyGenerator("OxBB")
                .t2dInputStudyVersion(1)
                .t2dInputStudyRelease(1)
                .t2dInputSamples(getResource(INPUT_SAMPLES_FILE).getPath())
                .t2dInputSamplesDefinition(getResource(INPUT_SAMPLES_DEFINITION_FILE).getPath())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(T2D_PREPARE_DATABASE_SAMPLES_STEP, jobParameters);
        //Then variantsAnnotCreate step should complete correctly
        assertCompleted(jobExecution);

        assertEquals(31, samplePropertyRepository.count());
        assertEquals(31, samplePropertyToDatasetRepository.count());

        assertEquals("BOOLEAN", samplePropertyRepository.findOne("T2D").getType());
    }

}
