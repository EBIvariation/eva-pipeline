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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;

import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_GENOTYPE_VCF_JOB;
import static uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils.COLLECTION_ANNOTATIONS_NAME;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertFailed;

/**
 * Test for {@link GenotypedVcfJobConfiguration}
 * <p>
 * TODO: FILE_WRONG_NO_ALT should be renamed because the alt allele is not missing but is the same as the reference
 */
@ExtendWith(SpringExtension.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:application.properties"})
@ContextConfiguration(classes = {GenotypedVcfJobConfiguration.class, BatchTestConfiguration.class,
        MongoCollectionNameConfiguration.class})
public class GenotypedVcfJobTest extends MongoTestContainerHelper {
    private static final String DB_NAME = "genotype-test-db";

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @Autowired
    @Qualifier(JOB_GENOTYPE_VCF_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    private BatchTestConfiguration batchTestConfiguration;

    private MongoTemplate mongoTemplate;

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
    public void fullGenotypedVcfJob() throws Exception {
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        String assemblyReport = GenotypedVcfJobTestUtils.getAssemblyReport();
        File mockVep = GenotypedVcfJobTestUtils.getMockVep();

        String outputDirStats = temporaryFolderUtil.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderUtil.newFolder().getAbsolutePath();

        File fasta = temporaryFolderUtil.newFile();

        // Run the Job
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionAnnotationMetadataName(GenotypedVcfJobTestUtils.COLLECTION_ANNOTATION_METADATA_NAME)
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionFilesName(GenotypedVcfJobTestUtils.COLLECTION_FILES_NAME)
                .collectionVariantsName(GenotypedVcfJobTestUtils.COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
                .inputFasta(fasta.getAbsolutePath())
                .inputAssemblyReport(assemblyReport)
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

        GenotypedVcfJobTestUtils.checkLoadStep(mongoTemplate);

        GenotypedVcfJobTestUtils.checkLoadedAnnotation(mongoTemplate);
    }

    @Test
    public void aggregationIsNotAllowed() throws Exception {
        File mockVep = GenotypedVcfJobTestUtils.getMockVep();
        String outputDirStats = temporaryFolderUtil.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderUtil.newFolder().getAbsolutePath();

        File fasta = temporaryFolderUtil.newFile();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionAnnotationMetadataName(GenotypedVcfJobTestUtils.COLLECTION_ANNOTATION_METADATA_NAME)
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionFilesName(GenotypedVcfJobTestUtils.COLLECTION_FILES_NAME)
                .collectionVariantsName(GenotypedVcfJobTestUtils.COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
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
