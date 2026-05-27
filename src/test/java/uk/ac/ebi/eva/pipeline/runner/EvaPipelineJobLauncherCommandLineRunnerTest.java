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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;
import uk.ac.ebi.eva.utils.EvaCommandLineBuilder;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENOTYPED_VCF_JOB;
import static uk.ac.ebi.eva.pipeline.runner.EvaPipelineJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS;
import static uk.ac.ebi.eva.pipeline.runner.EvaPipelineJobLauncherCommandLineRunner.SPRING_BATCH_JOB_NAME_PROPERTY;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * This suit of tests checks the behaviour of the {@link EvaPipelineJobLauncherCommandLineRunner} and launches a full execution of the
 * genotype vcf test.
 */
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@Import({MongoCollectionNameConfiguration.class, BatchTestConfiguration.class})
public class EvaPipelineJobLauncherCommandLineRunnerTest extends MongoTestContainerHelper {

    private static final String GENOTYPED_PROPERTIES_FILE = "/genotype-test.properties";

    private static final String NO_JOB_NAME_HAS_BEEN_PROVIDED = "No job name has been provided";

    private static final String NO_JOB_PARAMETERS_HAVE_BEEN_PROVIDED = "No job parameters have been provided";

    private static final String DB_NAME = "test-db";

    @Autowired
    private EvaPipelineJobLauncherCommandLineRunner evaPipelineJobLauncherCommandLineRunner;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    private BatchTestConfiguration batchTestConfiguration;

    private MongoTemplate mongoTemplate;

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate = batchTestConfiguration.getMongoTemplate(DB_NAME, mongoMappingContext);
        mongoTemplate.getDb().drop();
    }

    @AfterEach
    void cleanDb() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void noJobParametersHaveBeenProvided(CapturedOutput output) throws JobExecutionException {
        evaPipelineJobLauncherCommandLineRunner.run();
        assertThat(output.getOut(), containsString(NO_JOB_PARAMETERS_HAVE_BEEN_PROVIDED));
    }

    @Test
    public void jobProvidedButNoParameters(CapturedOutput output) throws JobExecutionException {
        evaPipelineJobLauncherCommandLineRunner.setJobName(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run("--" + SPRING_BATCH_JOB_NAME_PROPERTY + "=" + GENOTYPED_VCF_JOB);
        assertThat(output.getOut(), containsString(NO_JOB_PARAMETERS_HAVE_BEEN_PROVIDED));
    }

    @Test
    public void noJobNameProvidedAndAParameter(CapturedOutput output) throws JobExecutionException {
        evaPipelineJobLauncherCommandLineRunner.run("--dummy=true");
        assertThat(output.getOut(), containsString(NO_JOB_NAME_HAS_BEEN_PROVIDED));
    }

    @Test
    public void genotypedVcfJobTest() throws JobExecutionException, IOException {
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        String assemblyReport = GenotypedVcfJobTestUtils.getAssemblyReport();
        String outputDirStats = temporaryFolderUtil.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderUtil.newFolder().getAbsolutePath();

        File fasta = temporaryFolderUtil.newFile();

        evaPipelineJobLauncherCommandLineRunner.setJobName(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run(new EvaCommandLineBuilder()
                .annotationOverwrite("false")
                .appVepPath(GenotypedVcfJobTestUtils.getMockVep().getPath())
                .appVepTimeout("60")
                .configDbReadPreference("secondary")
                .databaseName(DB_NAME)
                .dbCollectionsAnnotationMetadataName("annotationMetadata")
                .dbCollectionsAnnotationsName(GenotypedVcfJobTestUtils.COLLECTION_ANNOTATIONS_NAME)
                .dbCollectionsFeaturesName("features")
                .dbCollectionsFilesName("files")
                .dbCollectionsStatisticsName("populationStatistics")
                .dbCollectionsVariantsName("variants")
                .inputFasta(fasta.getAbsolutePath())
                .inputAssemblyReport(assemblyReport)
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

        GenotypedVcfJobTestUtils.checkLoadStep(mongoTemplate);

        GenotypedVcfJobTestUtils.checkLoadedAnnotation(mongoTemplate);
    }

    @Test
    public void genotypedVcfJobTestWithParametersFileAndCommandLineParameters() throws JobExecutionException,
            IOException {

        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        String assemblyReport = GenotypedVcfJobTestUtils.getAssemblyReport();
        String outputDirStats = temporaryFolderUtil.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderUtil.newFolder().getAbsolutePath();

        File fasta = temporaryFolderUtil.newFile();

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
                .databaseName(DB_NAME)
                .dbCollectionsAnnotationsName(GenotypedVcfJobTestUtils.COLLECTION_ANNOTATIONS_NAME)
                .appVepPath(GenotypedVcfJobTestUtils.getMockVep().getPath())
                .appVepTimeout("60")
                .inputFasta(fasta.getAbsolutePath())
                .inputAssemblyReport(assemblyReport)
                .build()
        );

        assertEquals(EXIT_WITHOUT_ERRORS, evaPipelineJobLauncherCommandLineRunner.getExitCode());

        GenotypedVcfJobTestUtils.checkLoadStep(mongoTemplate);

        GenotypedVcfJobTestUtils.checkLoadedAnnotation(mongoTemplate);
    }

    @Test
    public void onlyFileWithoutParametersFailsValidation() throws JobExecutionException {

        //Set properties file to read
        evaPipelineJobLauncherCommandLineRunner.setPropertyFilePath(
                getResource(GENOTYPED_PROPERTIES_FILE).getAbsolutePath());

        evaPipelineJobLauncherCommandLineRunner.setJobName(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncherCommandLineRunner.run(new EvaCommandLineBuilder().build());

        assertEquals(EvaPipelineJobLauncherCommandLineRunner.EXIT_WITH_ERRORS,
                evaPipelineJobLauncherCommandLineRunner.getExitCode());
    }
}
