/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.CommonConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;
import uk.ac.ebi.eva.pipeline.jobs.PopulationStatisticsJob;
import uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporalMongoRule;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;
import static uk.ac.ebi.eva.utils.FileUtils.getResourceUrl;

/**
 * Test for {@link PopulationStatisticsGeneratorStep}
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {JobOptions.class, PopulationStatisticsJob.class, CommonConfiguration.class, JobLauncherTestUtils.class})
public class PopulationStatisticsGeneratorStepTest {

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";
    private static final String STATS_FILE_POSTFIX = ".variants.stats.json.gz";
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    @Rule
    public TemporalMongoRule mongoRule = new TemporalMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private JobOptions jobOptions;

    @Test
    public void statisticsGeneratorStepShouldCalculateStats() throws IOException, InterruptedException {
        //Given a valid VCF input file
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, SMALL_VCF_FILE);
        //and a valid variants load step already completed
        mongoRule.importDump(getResourceUrl(MONGO_DUMP), jobOptions.getDbName());

        VariantSource source = configureVariantSource();
        configureTempOutput();

        File statsFile = getStatsFile(source);
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        // When the execute method in variantsStatsCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.CALCULATE_STATISTICS);

        //Then variantsStatsCreate step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //and the file containing statistics should exist
        assertTrue(statsFile.exists());
    }

    /**
     * This test has to fail because it will try to extract variants from a non-existent DB.
     * Variants not loaded.. so nothing to query!
     */
    @Test
    public void statisticsGeneratorStepShouldFailIfVariantLoadStepIsNotCompleted() throws Exception {
        //Given a valid VCF input file
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, SMALL_VCF_FILE);

        VariantSource source = configureVariantSource();
        configureTempOutput();

        File statsFile = getStatsFile(source);
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        // When the execute method in variantsStatsCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.CALCULATE_STATISTICS);
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    private void configureTempOutput() throws IOException {
        String tempFolder = temporaryFolderRule.newFolder().getAbsolutePath();
        jobOptions.getPipelineOptions().put(JobParametersNames.OUTPUT_DIR_STATISTICS, tempFolder);
    }

    private VariantSource configureVariantSource() {
        VariantSource source = new VariantSource(
                SMALL_VCF_FILE,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);
        jobOptions.getVariantOptions().put(VARIANT_SOURCE, source);
        return source;
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
        jobOptions.setDbName(getClass().getSimpleName());
    }

    public File getStatsFile(VariantSource source) {
        return new File(
                Paths.get(jobOptions.getPipelineOptions().getString(JobParametersNames.OUTPUT_DIR_STATISTICS))
                        .resolve(VariantStorageManager.buildFilename(source))
                        + STATS_FILE_POSTFIX
        );
    }
}
