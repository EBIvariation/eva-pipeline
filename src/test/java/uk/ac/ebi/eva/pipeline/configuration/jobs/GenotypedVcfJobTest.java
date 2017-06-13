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

package uk.ac.ebi.eva.pipeline.configuration.jobs;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.opencga.lib.common.Config;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;

import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.COLLECTION_ANNOTATIONS_NAME;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertFailed;

/**
 * Test for {@link GenotypedVcfJobConfiguration}
 * <p>
 * TODO: FILE_WRONG_NO_ALT should be renamed because the alt allele is not missing but is the same as the reference
 */
@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {GenotypedVcfJobConfiguration.class, BatchTestConfiguration.class})
public class GenotypedVcfJobTest {

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Before
    public void setUp() throws Exception {
        Config.setOpenCGAHome(GenotypedVcfJobTestUtils.getDefaultOpencgaHome());
    }

    @Test
    public void fullGenotypedVcfJob() throws Exception {
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        File mockVep = GenotypedVcfJobTestUtils.getMockVep();
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        String outputDirStats = temporaryFolderRule.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();

        File variantsStatsFile = GenotypedVcfJobTestUtils.getVariantsStatsFile(outputDirStats);
        File sourceStatsFile = GenotypedVcfJobTestUtils.getSourceStatsFile(outputDirStats);

        File vepOutputFile = GenotypedVcfJobTestUtils.getVepOutputFile(outputDirAnnotation);

        File fasta = temporaryFolderRule.newFile();

        // Run the Job
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionAnnotationMetadataName(GenotypedVcfJobTestUtils.COLLECTION_ANNOTATION_METADATA_NAME)
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionFilesName(GenotypedVcfJobTestUtils.COLLECTION_FILES_NAME)
                .collectionVariantsName(GenotypedVcfJobTestUtils.COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputFasta(fasta.getAbsolutePath())
                .inputStudyId(GenotypedVcfJobTestUtils.INPUT_STUDY_ID)
                .inputStudyName("inputStudyName")
                .inputStudyType("COLLECTION")
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId(GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .outputDirAnnotation(outputDirAnnotation)
                .outputDirStats(outputDirStats)
                .vepCachePath("")
                .vepCacheSpecies("human")
                .vepCacheVersion("1")
                .vepNumForks("1")
                .vepPath(mockVep.getPath())
                .vepTimeout("60")
                .vepVersion("1")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

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
    public void aggregationIsNotAllowed() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        mongoRule.getTemporaryDatabase(databaseName);
        File mockVep = GenotypedVcfJobTestUtils.getMockVep();
        String outputDirStats = temporaryFolderRule.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();

        File fasta = temporaryFolderRule.newFile();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionAnnotationMetadataName(GenotypedVcfJobTestUtils.COLLECTION_ANNOTATION_METADATA_NAME)
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionFilesName(GenotypedVcfJobTestUtils.COLLECTION_FILES_NAME)
                .collectionVariantsName(GenotypedVcfJobTestUtils.COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputFasta(fasta.getAbsolutePath())
                .inputStudyId(GenotypedVcfJobTestUtils.INPUT_STUDY_ID)
                .inputStudyName("inputStudyName")
                .inputStudyType("COLLECTION")
                .inputVcf(GenotypedVcfJobTestUtils.getInputFile().getAbsolutePath())
                .inputVcfAggregation("BASIC")
                .inputVcfId(GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .outputDirAnnotation(outputDirAnnotation)
                .outputDirStats(outputDirStats)
                .vepCachePath("")
                .vepCacheSpecies("human")
                .vepCacheVersion("1")
                .vepNumForks("1")
                .vepPath(mockVep.getPath())
                .vepTimeout("60")
                .vepVersion("1")
                .timestamp()
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertFailed(jobExecution);
    }
}
