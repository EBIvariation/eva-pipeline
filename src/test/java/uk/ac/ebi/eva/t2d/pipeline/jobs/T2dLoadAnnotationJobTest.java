package uk.ac.ebi.eva.t2d.pipeline.jobs;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import uk.ac.ebi.eva.t2d.jobs.T2dLoadAnnotationJob;
import uk.ac.ebi.eva.t2d.repository.VariantInfoRepository;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;

import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

@RunWith(SpringRunner.class)
@ActiveProfiles({Application.T2D_PROFILE})
@ContextConfiguration(classes = {T2dDataSourceConfiguration.class, TestJpaConfiguration.class,
        BatchJobExecutorInMemory.class, T2dLoadAnnotationJob.class})
@TestPropertySource({"classpath:application-t2d.properties"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class T2dLoadAnnotationJobTest {

    private static final String INPUT_FILE = "/input-files/vep/t2d.vep.vfc";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private VariantInfoRepository variantInfoRepository;

    @Test
    public void testLoadVariants() throws Exception {
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        File mockVep = GenotypedVcfJobTestUtils.getMockVep();
        File fasta = temporaryFolderRule.newFile();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputStudyType("GWAS")
                .t2dInputStudyVersion(1)
                .t2dInputStudyRelease(2)
                .inputVcf(inputFile.getAbsolutePath())
                .inputFasta(fasta.getAbsolutePath())
                .manualVepFile(getResource(INPUT_FILE))
                .vepCachePath("")
                .vepCacheSpecies("human")
                .vepCacheVersion("1")
                .vepNumForks("1")
                .vepPath(mockVep.getPath())
                .vepTimeout("60")
                .outputDirAnnotation(outputDirAnnotation)
                .toJobParameters();

        assertCompleted(jobLauncherTestUtils.launchJob(jobParameters));

        Assert.assertEquals(16, variantInfoRepository.count());
    }

}
