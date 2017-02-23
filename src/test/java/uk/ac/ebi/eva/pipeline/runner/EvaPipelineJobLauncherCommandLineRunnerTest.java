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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.rule.OutputCapture;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.utils.EvaCommandLineBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static uk.ac.ebi.eva.pipeline.runner.EvaPipelineJobLauncherCommandLineRunner.SPRING_BATCH_JOB_NAME_PROPERTY;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENOTYPED_VCF_JOB;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

/**
 * This suit of tests checks the behaviour of the EvaPipelineJobLauncherCommandLineRunner and launches a full execution of the
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

    @Test
    public void noJobNameHasBeenProvidedStopsExecution() throws JobExecutionException {
        evaPipelineJobLauncherCommandLineRunner.setJobNames(null);
        evaPipelineJobLauncherCommandLineRunner.run();
        assertThat(capture.toString(), containsString(NO_JOB_NAME_HAS_BEEN_PROVIDED));
    }

    @Test
    public void jobProvidedButNoParameters() throws JobExecutionException {
        evaPipelineJobLauncherCommandLineRunner.setJobNames(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run("--" + SPRING_BATCH_JOB_NAME_PROPERTY + "=" + GENOTYPED_VCF_JOB);
        assertThat(capture.toString(), containsString(NO_JOB_PARAMETERS_HAVE_BEEN_PROVIDED));
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

        File vepInputFile = GenotypedVcfJobTestUtils.getVepInputFile(outputDirAnnotation);
        File vepOutputFile = GenotypedVcfJobTestUtils.getVepOutputFile(outputDirAnnotation);

        File fasta = temporaryFolderRule.newFile();

        evaPipelineJobLauncherCommandLineRunner.setJobNames(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run(new EvaCommandLineBuilder()
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfId(GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .inputVcfAggregation("NONE")
                .inputStudyName("small vcf")
                .inputStudyId(GenotypedVcfJobTestUtils.INPUT_STUDY_ID)
                .inputStudyType("COLLECTION")
                .outputDirAnnotation(outputDirAnnotation)
                .outputDirStatistics(outputDirStats)
                .databaseName(databaseName)
                .appVepPath(GenotypedVcfJobTestUtils.getMockVep().getPath())
                .vepCachePath("")
                .vepCacheSpecies("human")
                .vepCacheVersion("1")
                .vepNumForks("1")
                .inputFasta(fasta.getAbsolutePath())
                .configDbReadPreference("secondary")
                .dbCollectionsVariantsName("variants")
                .dbCollectionsFilesName("files")
                .dbCollectionsFeaturesName("features")
                .dbCollectionsStatisticsName("populationStatistics").build()
        );

        assertEquals(EvaPipelineJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS,
                evaPipelineJobLauncherCommandLineRunner.getExitCode());

        assertFalse(jobExplorer.getJobInstances(GENOTYPED_VCF_JOB, 0, 1).isEmpty());
        JobExecution jobExecution = jobExplorer.getJobExecution(jobExplorer.getJobInstances(GENOTYPED_VCF_JOB, 0, 1)
                .get(0).getInstanceId());

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        GenotypedVcfJobTestUtils.checkLoadStep(databaseName);

        GenotypedVcfJobTestUtils.checkCreateStatsStep(variantsStatsFile, sourceStatsFile);

        GenotypedVcfJobTestUtils.checkLoadStatsStep(databaseName);

        GenotypedVcfJobTestUtils.checkAnnotationInput(vepInputFile);

        GenotypedVcfJobTestUtils.checkAnnotationCreateStep(vepInputFile, vepOutputFile);

        GenotypedVcfJobTestUtils.checkOutputFileLength(vepOutputFile);

        GenotypedVcfJobTestUtils.checkLoadedAnnotation(databaseName);

        GenotypedVcfJobTestUtils.checkSkippedOneMalformedLine(jobExecution);
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

        File vepInputFile = GenotypedVcfJobTestUtils.getVepInputFile(outputDirAnnotation);
        File vepOutputFile = GenotypedVcfJobTestUtils.getVepOutputFile(outputDirAnnotation);

        File fasta = temporaryFolderRule.newFile();

        //Set properties file to read
        evaPipelineJobLauncherCommandLineRunner.setPropertyFilePath(
                getResource(GENOTYPED_PROPERTIES_FILE).getAbsolutePath());

        evaPipelineJobLauncherCommandLineRunner.setJobNames(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run(new EvaCommandLineBuilder()
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfId(GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .inputStudyId(GenotypedVcfJobTestUtils.INPUT_STUDY_ID)
                .outputDirAnnotation(outputDirAnnotation)
                .outputDirStatistics(outputDirStats)
                .databaseName(databaseName)
                .appVepPath(GenotypedVcfJobTestUtils.getMockVep().getPath())
                .inputFasta(fasta.getAbsolutePath())
                .build()
        );

        assertEquals(EvaPipelineJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS,
                evaPipelineJobLauncherCommandLineRunner.getExitCode());

        assertFalse(jobExplorer.getJobInstances(GENOTYPED_VCF_JOB, 0, 1).isEmpty());
        JobExecution jobExecution = jobExplorer.getJobExecution(jobExplorer.getJobInstances(GENOTYPED_VCF_JOB, 0, 1)
                .get(0).getInstanceId());

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        GenotypedVcfJobTestUtils.checkLoadStep(databaseName);

        GenotypedVcfJobTestUtils.checkCreateStatsStep(variantsStatsFile, sourceStatsFile);

        GenotypedVcfJobTestUtils.checkLoadStatsStep(databaseName);

        GenotypedVcfJobTestUtils.checkAnnotationInput(vepInputFile);

        GenotypedVcfJobTestUtils.checkAnnotationCreateStep(vepInputFile, vepOutputFile);

        GenotypedVcfJobTestUtils.checkOutputFileLength(vepOutputFile);

        GenotypedVcfJobTestUtils.checkLoadedAnnotation(databaseName);

        GenotypedVcfJobTestUtils.checkSkippedOneMalformedLine(jobExecution);
    }

    @Test
    public void onlyFileWithoutParametersFailsValidation() throws JobExecutionException, IOException,
            URISyntaxException,
            ClassNotFoundException, StorageManagerException, InstantiationException, IllegalAccessException {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        String outputDirStats = temporaryFolderRule.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();

        File variantsStatsFile = GenotypedVcfJobTestUtils.getVariantsStatsFile(outputDirStats);
        File sourceStatsFile = GenotypedVcfJobTestUtils.getSourceStatsFile(outputDirStats);

        File vepInputFile = GenotypedVcfJobTestUtils.getVepInputFile(outputDirAnnotation);
        File vepOutputFile = GenotypedVcfJobTestUtils.getVepOutputFile(outputDirAnnotation);

        File fasta = temporaryFolderRule.newFile();

        //Set properties file to read
        evaPipelineJobLauncherCommandLineRunner.setPropertyFilePath(
                getResource(GENOTYPED_PROPERTIES_FILE).getAbsolutePath());

        evaPipelineJobLauncherCommandLineRunner.setJobNames(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run(new EvaCommandLineBuilder()
                  .build()
        );

        assertEquals(EvaPipelineJobLauncherCommandLineRunner.EXIT_WITH_ERRORS,
                evaPipelineJobLauncherCommandLineRunner.getExitCode());
    }
}
