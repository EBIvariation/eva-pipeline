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
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link PopulationStatisticsJobConfiguration}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {PopulationStatisticsJobConfiguration.class, BatchTestConfiguration.class})
public class PopulationStatisticsJobTest {
    private static final String SMALL_VCF_FILE = "/input-files/vcf/genotyped.vcf.gz";

    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Before
    public void setUp() throws Exception {
        Config.setOpenCGAHome(GenotypedVcfJobTestUtils.getDefaultOpencgaHome());
    }

    @Test
    public void fullPopulationStatisticsJob() throws Exception {
        //Given a valid VCF input file
        String input = SMALL_VCF_FILE;
        String statsDir = temporaryFolderRule.getRoot().getPath();
        String dbName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String fileId = "1";
        String studyId = "1";

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName("files")
                .collectionVariantsName("variants")
                .databaseName(dbName)
                .inputStudyId(studyId)
                .inputVcf(getResource(input).getAbsolutePath())
                .inputVcfAggregation("BASIC")
                .inputVcfId(fileId)
                .outputDirStats(statsDir)
                .timestamp()
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        assertCompleted(jobExecution);

        //and the file containing statistics should exist
        File statsFile = new File(URLHelper.getVariantsStatsUri(statsDir, studyId, fileId));
        assertTrue(statsFile.exists());
        File sourceStatsFile = new File(URLHelper.getSourceStatsUri(statsDir, studyId, fileId));
        assertTrue(sourceStatsFile.exists());

        // The DB docs should have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

    }

}
