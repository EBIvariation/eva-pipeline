/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.runner;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.rule.OutputCapture;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.utils.EvaCommandLineBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENOTYPED_VCF_JOB;
import static uk.ac.ebi.eva.pipeline.runner.EvaPipelineJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS;
import static uk.ac.ebi.eva.pipeline.runner.EvaPipelineJobLauncherCommandLineRunner.SPRING_BATCH_JOB_NAME_PROPERTY;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * This suit of tests checks the behaviour of the {@link EvaPipelineJobLauncherCommandLineRunner} and launches a full execution of the
 * genotype vcf test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest()
@ActiveProfiles({"test,mongo"})
@TestPropertySource(value = {"classpath:test-mongo.properties"}, properties = "debug=true")
public class EvaPipelineJobLauncherCommandLineRunnerTest {

    private static final String GENOTYPED_PROPERTIES_FILE = "/genotype-test.properties";

    private static final String NO_JOB_NAME_HAS_BEEN_PROVIDED = "No job name has been provided";

    private static final String NO_JOB_PARAMETERS_HAVE_BEEN_PROVIDED = "No job parameters have been provided";

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private EvaPipelineJobLauncherCommandLineRunner evaPipelineJobLauncherCommandLineRunner;

    @Rule
    public OutputCapture capture = new OutputCapture();

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Before
    public void setUp() throws Exception {
        Config.setOpenCGAHome(GenotypedVcfJobTestUtils.getDefaultOpencgaHome());
    }

    @Test
    public void noJobParametersHaveBeenProvided() throws JobExecutionException {
        evaPipelineJobLauncherCommandLineRunner.run();
        assertThat(capture.toString(), containsString(NO_JOB_PARAMETERS_HAVE_BEEN_PROVIDED));
    }

    @Test
    public void jobProvidedButNoParameters() throws JobExecutionException {
        evaPipelineJobLauncherCommandLineRunner.setJobNames(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run("--" + SPRING_BATCH_JOB_NAME_PROPERTY + "=" + GENOTYPED_VCF_JOB);
        assertThat(capture.toString(), containsString(NO_JOB_PARAMETERS_HAVE_BEEN_PROVIDED));
    }

    @Test
    public void noJobNameProvidedAndAParameter() throws JobExecutionException {
        evaPipelineJobLauncherCommandLineRunner.run("--dummy=true");
        assertThat(capture.toString(), containsString(NO_JOB_NAME_HAS_BEEN_PROVIDED));
    }

    @Test
    public void genotypedVcfJobTest() throws JobExecutionException, IOException, URISyntaxException,
            ClassNotFoundException, StorageManagerException, InstantiationException, IllegalAccessException {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        String outputDirStats = temporaryFolderRule.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();

        File variantsStatsFile = GenotypedVcfJobTestUtils.getVariantsStatsFile(outputDirStats);
        File sourceStatsFile = GenotypedVcfJobTestUtils.getSourceStatsFile(outputDirStats);

        File vepOutputFile = GenotypedVcfJobTestUtils.getVepOutputFile(outputDirAnnotation);

        File fasta = temporaryFolderRule.newFile();

        evaPipelineJobLauncherCommandLineRunner.setJobNames(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run(new EvaCommandLineBuilder()
                .annotationOverwrite("false")
                .appVepPath(GenotypedVcfJobTestUtils.getMockVep().getPath())
                .appVepTimeout("60")
                .configDbReadPreference("secondary")
                .databaseName(databaseName)
                .dbCollectionsAnnotationMetadataName("annotationMetadata")
                .dbCollectionsAnnotationsName(GenotypedVcfJobTestUtils.COLLECTION_ANNOTATIONS_NAME)
                .dbCollectionsFeaturesName("features")
                .dbCollectionsFilesName("files")
                .dbCollectionsStatisticsName("populationStatistics")
                .dbCollectionsVariantsName("variants")
                .inputFasta(fasta.getAbsolutePath())
                .inputStudyId(GenotypedVcfJobTestUtils.INPUT_STUDY_ID)
                .inputStudyName("small vcf")
                .inputStudyType("COLLECTION")
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId(GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .outputDirAnnotation(outputDirAnnotation)
                .outputDirStatistics(outputDirStats)
                .vepCachePath("")
                .vepCacheSpecies("human")
                .vepCacheVersion("1")
                .vepNumForks("1")
                .vepVersion("1")
                .build()
        );

        assertEquals(EXIT_WITHOUT_ERRORS, evaPipelineJobLauncherCommandLineRunner.getExitCode());

        JobExecution jobExecution = getLastJobExecution(GENOTYPED_VCF_JOB);
        assertCompleted(jobExecution);

        GenotypedVcfJobTestUtils.checkLoadStep(mongoRule, databaseName);

        GenotypedVcfJobTestUtils.checkCreateStatsStep(variantsStatsFile, sourceStatsFile);

        GenotypedVcfJobTestUtils.checkLoadStatsStep(mongoRule, databaseName);

        GenotypedVcfJobTestUtils.checkAnnotationCreateStep(vepOutputFile);
        GenotypedVcfJobTestUtils.checkOutputFileLength(vepOutputFile);

        GenotypedVcfJobTestUtils.checkLoadedAnnotation(mongoRule, databaseName);

        GenotypedVcfJobTestUtils.checkSkippedOneMalformedLine(jobExecution);
    }

    private JobExecution getLastJobExecution(String jobName) {
        List<JobInstance> jobInstances = jobExplorer.getJobInstances(jobName, 0, 1);
        assertFalse(jobInstances.isEmpty());
        return jobExplorer.getJobExecution(jobInstances.get(0).getInstanceId());
    }

    @Test
    public void doNotReuseParametersFromPreviousCompletedJobs() throws JobExecutionException, IOException, URISyntaxException,
            ClassNotFoundException, StorageManagerException, InstantiationException, IllegalAccessException {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();

        evaPipelineJobLauncherCommandLineRunner.setJobNames(GENOTYPED_VCF_JOB);
        EvaCommandLineBuilder evaCommandLineBuilderWithoutChunksize = new EvaCommandLineBuilder()
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputStudyName("small vcf")
                .inputStudyId(GenotypedVcfJobTestUtils.INPUT_STUDY_ID)
                .inputStudyType("COLLECTION")
                .databaseName(databaseName)
                .configDbReadPreference("secondary")
                .dbCollectionsVariantsName("variants")
                .dbCollectionsFilesName("files")
                .dbCollectionsFeaturesName("features")
                .annotationSkip(true)
                .statisticsSkip(true);

        String[] commandLineWithoutChunksize = evaCommandLineBuilderWithoutChunksize
                .inputVcfId("without_chunksize_" + GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .build();

        String[] completeCommandLine = evaCommandLineBuilderWithoutChunksize
                .inputVcfId(GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .chunksize("100")
                .build();

        evaPipelineJobLauncherCommandLineRunner.run(completeCommandLine);
        assertEquals(EXIT_WITHOUT_ERRORS, evaPipelineJobLauncherCommandLineRunner.getExitCode());

        JobExecution firstJobExecution = getLastJobExecution(GENOTYPED_VCF_JOB);
        assertCompleted(firstJobExecution);

        assertNotNull(firstJobExecution.getJobParameters().getString(JobParametersNames.DB_NAME));
        assertNotNull(firstJobExecution.getJobParameters().getString(JobParametersNames.CONFIG_CHUNK_SIZE));

        // check second run doesn't include the chunksize parameter
        evaPipelineJobLauncherCommandLineRunner.run(commandLineWithoutChunksize);
        assertEquals(EXIT_WITHOUT_ERRORS, evaPipelineJobLauncherCommandLineRunner.getExitCode());

        JobExecution secondJobExecution = getLastJobExecution(GENOTYPED_VCF_JOB);
        assertCompleted(secondJobExecution);

        assertNotNull(secondJobExecution.getJobParameters().getString(JobParametersNames.DB_NAME));
        assertNull(secondJobExecution.getJobParameters().getString(JobParametersNames.CONFIG_CHUNK_SIZE));
    }

    @Test
    public void genotypedVcfJobTestWithParametersFileAndCommandLineParameters() throws JobExecutionException,
            IOException, URISyntaxException, ClassNotFoundException, StorageManagerException, InstantiationException,
            IllegalAccessException {

        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        String outputDirStats = temporaryFolderRule.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();

        File variantsStatsFile = GenotypedVcfJobTestUtils.getVariantsStatsFile(outputDirStats);
        File sourceStatsFile = GenotypedVcfJobTestUtils.getSourceStatsFile(outputDirStats);

        File vepOutputFile = GenotypedVcfJobTestUtils.getVepOutputFile(outputDirAnnotation);

        File fasta = temporaryFolderRule.newFile();

        //Set properties file to read
        evaPipelineJobLauncherCommandLineRunner.setPropertyFilePath(
                getResource(GENOTYPED_PROPERTIES_FILE).getAbsolutePath());

        evaPipelineJobLauncherCommandLineRunner.run(new EvaCommandLineBuilder()
                .annotationOverwrite("false")
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfId(GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .inputStudyId(GenotypedVcfJobTestUtils.INPUT_STUDY_ID)
                .outputDirAnnotation(outputDirAnnotation)
                .outputDirStatistics(outputDirStats)
                .databaseName(databaseName)
                .dbCollectionsAnnotationsName(GenotypedVcfJobTestUtils.COLLECTION_ANNOTATIONS_NAME)
                .appVepPath(GenotypedVcfJobTestUtils.getMockVep().getPath())
                .appVepTimeout("60")
                .inputFasta(fasta.getAbsolutePath())
                .build()
        );

        assertEquals(EXIT_WITHOUT_ERRORS, evaPipelineJobLauncherCommandLineRunner.getExitCode());

        JobExecution jobExecution = getLastJobExecution(GENOTYPED_VCF_JOB);
        assertCompleted(jobExecution);

        GenotypedVcfJobTestUtils.checkLoadStep(mongoRule, databaseName);

        GenotypedVcfJobTestUtils.checkCreateStatsStep(variantsStatsFile, sourceStatsFile);

        GenotypedVcfJobTestUtils.checkLoadStatsStep(mongoRule, databaseName);

        GenotypedVcfJobTestUtils.checkAnnotationCreateStep(vepOutputFile);
        GenotypedVcfJobTestUtils.checkOutputFileLength(vepOutputFile);

        GenotypedVcfJobTestUtils.checkLoadedAnnotation(mongoRule, databaseName);

        GenotypedVcfJobTestUtils.checkSkippedOneMalformedLine(jobExecution);
    }

    @Test
    public void onlyFileWithoutParametersFailsValidation() throws JobExecutionException, IOException,
            URISyntaxException,
            ClassNotFoundException, StorageManagerException, InstantiationException, IllegalAccessException {

        //Set properties file to read
        evaPipelineJobLauncherCommandLineRunner.setPropertyFilePath(
                getResource(GENOTYPED_PROPERTIES_FILE).getAbsolutePath());

        evaPipelineJobLauncherCommandLineRunner.setJobNames(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run(new EvaCommandLineBuilder().build());

        assertEquals(EvaPipelineJobLauncherCommandLineRunner.EXIT_WITH_ERRORS,
                evaPipelineJobLauncherCommandLineRunner.getExitCode());
    }
}
