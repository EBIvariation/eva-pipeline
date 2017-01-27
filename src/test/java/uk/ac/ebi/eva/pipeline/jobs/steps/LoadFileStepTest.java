/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.jobs.steps;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.jobs.GenotypedVcfJob;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

/**
 * Test for {@link LoadFileStep}
 */
@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:genotyped-vcf.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {GenotypedVcfJob.class, BatchTestConfiguration.class})
public class LoadFileStepTest {

    private static final int EXPECTED_FILES = 1;

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    private String input;

    @Test
    public void loaderStepShouldLoadAllFiles() throws Exception {
        String outputDir = temporaryFolderRule.getRoot().getAbsolutePath();
        jobOptions.getPipelineOptions().put(JobParametersNames.OUTPUT_DIR, outputDir);

        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName("files")
                .collectionVariantsName("variants")
                .databaseName(databaseName)
                .inputStudyId("1")
                .inputVcf(input)
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .toJobParameters();

        // When the execute method in variantsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_FILE_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And the number of documents in the DB should be equals to the number of VCF files loaded
        DBCollection fileCollection = mongoRule.getCollection(databaseName, jobOptions.getDbCollectionsFilesName());
        DBCursor cursor = fileCollection.find();
        assertEquals(EXPECTED_FILES, count(cursor));
    }

    @Before
    public void setUp() throws Exception {
        input = getResource(SMALL_VCF_FILE).getAbsolutePath();
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, input);
    }

}
