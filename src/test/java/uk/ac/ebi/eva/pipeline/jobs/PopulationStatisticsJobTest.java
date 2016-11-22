/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
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
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporalMongoRule;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.copyResource;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;

/**
 * Test for {@link PopulationStatisticsJob}
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {JobOptions.class, PopulationStatisticsJob.class, CommonConfiguration.class, JobLauncherTestUtils.class})
public class PopulationStatisticsJobTest {
    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";
    private static final String VARIANTS_FILE_NAME = "/1_1.variants.stats.json.gz";
    private static final String SOURCE_FILE_NAME = "/1_1.source.stats.json.gz";
    private static final String VCF_FILE_NAME = "/small20.vcf.gz.variants.json.gz";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporalMongoRule mongoRule = new TemporalMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private JobOptions jobOptions;

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;

    @Test
    public void fullPopulationStatisticsJob() throws Exception {
        //Given a valid VCF input file
        String input = SMALL_VCF_FILE;

        pipelineOptions.put(JobParametersNames.INPUT_VCF, input);

        VariantSource source = new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        variantOptions.put(VARIANT_SOURCE, source);

        initStatsLoadStepFiles();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //and the file containing statistics should exist
        File statsFile = new File(Paths.get(pipelineOptions.getString(JobParametersNames.OUTPUT_DIR_STATISTICS))
                .resolve(VariantStorageManager.buildFilename(source)) + ".variants.stats.json.gz");
        assertTrue(statsFile.exists());

        // The DB docs should have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(jobOptions.getDbName(), null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

    }

    private void initStatsLoadStepFiles() throws IOException, InterruptedException {
        String mongoDatabase = mongoRule.importDumpInTemporalDatabase(getResourceUrl(MONGO_DUMP));
        jobOptions.setDbName(mongoDatabase);

        String outputDir = temporaryFolderRule.getRoot().getAbsolutePath();
        pipelineOptions.put(JobParametersNames.OUTPUT_DIR_STATISTICS, outputDir);

        // copy stat file to load
        copyResource(VARIANTS_FILE_NAME, outputDir);

        // copy source file to load
        copyResource(SOURCE_FILE_NAME, outputDir);

        // copy transformed vcf
        copyResource(VCF_FILE_NAME, outputDir);
    }

    @Before
    public void setUp() throws Exception {
        //re-initialize common config before each test
        jobOptions.loadArgs();
        pipelineOptions = jobOptions.getPipelineOptions();
        variantOptions = jobOptions.getVariantOptions();
    }

}
