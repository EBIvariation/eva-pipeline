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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.opencga.lib.common.Config;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.PopulationStatisticsJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertFailed;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;

/**
 * Test for {@link CalculateStatisticsStepConfiguration}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {PopulationStatisticsJobConfiguration.class, BatchTestConfiguration.class})
public class CalculateStatisticsStepTest {
    private static final String SMALL_VCF_FILE = "/input-files/vcf/genotyped.vcf.gz";

    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

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
    public void statisticsGeneratorStepShouldCalculateStats() throws IOException, InterruptedException, URISyntaxException {
        //Given a valid VCF input file
        String databaseName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String statsDir = temporaryFolderRule.newFolder().getAbsolutePath();
        String studyId = "1";
        String fileId = "1";

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(databaseName)
                .inputVcf(SMALL_VCF_FILE)
                .inputStudyId(studyId)
                .inputVcfId(fileId)
                .outputDirStats(statsDir)
                .toJobParameters();

        // and non-existent variants stats file and variantSource stats file
        File statsFile = new File(URLHelper.getVariantsStatsUri(statsDir, studyId, fileId));
        assertFalse(statsFile.exists());
        File sourceStatsFile = new File(URLHelper.getSourceStatsUri(statsDir, studyId, fileId));
        assertFalse(sourceStatsFile.exists());

        // When the execute method in variantsStatsCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.CALCULATE_STATISTICS_STEP, jobParameters);

        //Then variantsStatsCreate step should complete correctly
        assertCompleted(jobExecution);

        //and the file containing statistics should exist
        assertTrue(statsFile.exists());
        assertTrue(sourceStatsFile.exists());
    }

    /**
     * This test has to fail because it will try to extract variants from a non-existent DB.
     * Variants not loaded.. so nothing to query!
     */
    @Test
    public void statisticsGeneratorStepShouldFailIfVariantLoadStepIsNotCompleted() throws Exception {
        //Given a valid VCF input file
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        String statsDir = temporaryFolderRule.newFolder().getAbsolutePath();
        String wrongId = "non-existent-id";

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(databaseName)
                .inputVcf(SMALL_VCF_FILE)
                .inputStudyId(wrongId)
                .inputVcfId(wrongId)
                .outputDirStats(statsDir)
                .toJobParameters();

        // and non-existent variants stats file and variantSource stats file
        File statsFile = new File(URLHelper.getVariantsStatsUri(statsDir, wrongId, wrongId));
        assertFalse(statsFile.exists());
        File sourceStatsFile = new File(URLHelper.getSourceStatsUri(statsDir, wrongId, wrongId));
        assertFalse(sourceStatsFile.exists());

        // When the execute method in variantsStatsCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.CALCULATE_STATISTICS_STEP, jobParameters);
        assertFailed(jobExecution);
    }

}
