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

package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import com.mongodb.DBCursor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.rule.OutputCapture;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.PopulationStatisticsJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.configuration.MongoOperationConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertFailed;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.copyResource;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link LoadStatisticsStepConfiguration}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {PopulationStatisticsJobConfiguration.class, BatchTestConfiguration.class,
        MongoOperationConfiguration.class})
public class LoadStatisticsStepTest {
    private static final String SMALL_VCF_FILE = "/input-files/vcf/genotyped.vcf.gz";

    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    private static final String SOURCE_FILE_NAME = "/input-files/statistics/1_1.source.stats.json.gz";

    private static final String VARIANTS_FILE_NAME = "/input-files/statistics/1_1.variants.stats.json.gz";

    private static final String FILE_NOT_FOUND_EXCEPTION = "java.io.FileNotFoundException:";

    private static final String COLLECTION_FILES_NAME = "files";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoOperations mongoOperations;

    //Capture error output
    @Rule
    public OutputCapture capture = new OutputCapture();

    @Test
    public void statisticsLoaderStepShouldLoadStatsIntoDb() throws StorageManagerException, IllegalAccessException,
            ClassNotFoundException, InstantiationException, IOException, InterruptedException {
        //Given a valid VCF input file
        String input = getResource(SMALL_VCF_FILE).getAbsolutePath();
        String fileId = "1";
        String studyId = "1";
        String dbName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String statsDir = temporaryFolderRule.newFolder().getAbsolutePath();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(dbName)
                .inputStudyId(studyId)
                .inputVcf(input)
                .inputVcfId(fileId)
                .outputDirStats(statsDir)
                .toJobParameters();

        //and a valid variants load and stats create steps already completed
        copyFilesToOutpurDir(statsDir);

        // When the execute method in variantsStatsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_STATISTICS_STEP, jobParameters);

        // Then variantsStatsLoad step should complete correctly
        assertCompleted(jobExecution);

        // The DB docs should have the field "st"
        DBCursor cursor = mongoRule.getCollection(dbName, COLLECTION_VARIANTS_NAME).find();
        assertEquals(1, JobTestUtils.getCohortStatsFromFirstVariant(cursor, mongoOperations).size());
    }

    private void copyFilesToOutpurDir(String outputDir) throws IOException {
        // copy stat file to load
        copyResource(VARIANTS_FILE_NAME, outputDir);
        // copy source file to load
        copyResource(SOURCE_FILE_NAME, outputDir);
    }

    @Test
    public void statisticsLoaderStepShouldFaildBecauseVariantStatsFileIsMissing()
            throws JobExecutionException, IOException, InterruptedException {
        String input = getResource(SMALL_VCF_FILE).getAbsolutePath();
        String fileId = "1";
        String studyId = "1";
        String dbName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String statsDir = temporaryFolderRule.newFolder().getAbsolutePath();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName("files")
                .collectionVariantsName("variants")
                .databaseName(dbName)
                .inputStudyId(studyId)
                .inputVcf(input)
                .inputVcfId(fileId)
                .outputDirStats(statsDir)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_STATISTICS_STEP, jobParameters);
        assertThat(capture.toString(), containsString(FILE_NOT_FOUND_EXCEPTION));

        assertFailed(jobExecution);
    }

}
