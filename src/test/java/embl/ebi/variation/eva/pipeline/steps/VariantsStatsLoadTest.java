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

package embl.ebi.variation.eva.pipeline.steps;

import embl.ebi.variation.eva.pipeline.jobs.JobTestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.restoreMongoDbFromDump;
import static org.junit.Assert.assertEquals;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantsStatsLoad}
 */
public class VariantsStatsLoadTest {
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";
    private static final String VALID_LOAD_STATS_DB = "VariantStatsConfigurationTest_vl";

    private VariantsStatsLoad variantsStatsLoad;

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;

    private ChunkContext chunkContext;
    private StepContribution stepContribution;

    @Test
    public void statsLoadTaskletShouldLoadStatsIntoDb() throws Exception {
        //Given a valid VCF input file
        String input = VariantsStatsLoadTest.class.getResource(SMALL_VCF_FILE).getFile();
        VariantSource source = new VariantSource(input, "1", "1", "studyName");

        String dbName = VALID_LOAD_STATS_DB;

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        ReflectionTestUtils.setField(variantsStatsLoad, "pipelineOptions", pipelineOptions);
        ReflectionTestUtils.setField(variantsStatsLoad, "variantOptions", variantOptions);

        //and a valid variants load and stats create steps already completed
        String dump = VariantsStatsCreateTest.class.getResource("/dump/").getFile();
        restoreMongoDbFromDump(dump);

        String outputDir = pipelineOptions.getString("outputDir");

        // copy stat file to load
        String variantsFileName = "/1_1.variants.stats.json.gz";
        File statsFileToLoad = new File(outputDir, variantsFileName);
        File variantStatsFile = new File(VariantsStatsLoadTest.class.getResource(variantsFileName).getFile());
        FileUtils.copyFile(variantStatsFile, statsFileToLoad);

        // copy source file to load
        String sourceFileName = "/1_1.source.stats.json.gz";
        File sourceFileToLoad = new File(outputDir, sourceFileName);
        File sourceStatsFile = new File(VariantsStatsLoadTest.class.getResource(sourceFileName).getFile());
        FileUtils.copyFile(sourceStatsFile, sourceFileToLoad);

        // copy transformed vcf
        String vcfFileName = "/small20.vcf.gz.variants.json.gz";
        File vcfFileToLoad = new File(outputDir, vcfFileName);
        File vcfFile = new File(VariantsStatsLoadTest.class.getResource(vcfFileName).getFile());
        FileUtils.copyFile(vcfFile, vcfFileToLoad);

        // When the execute method in variantsStatsLoad is executed
        RepeatStatus repeatStatus = variantsStatsLoad.execute(stepContribution, chunkContext);

        // Then variantsStatsLoad step should complete correctly
        assertEquals(RepeatStatus.FINISHED, repeatStatus);

        // The DB docs should have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

        statsFileToLoad.delete();
        sourceFileToLoad.delete();
        vcfFileToLoad.delete();
    }

    @Before
    public void setUp() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        variantsStatsLoad = new VariantsStatsLoad();
        variantOptions = new ObjectMap();
        pipelineOptions = new ObjectMap();

        pipelineOptions.put("outputDir", "/tmp");
        pipelineOptions.put("pedigree", "FIRST_8_COLUMNS");
        pipelineOptions.put("compressExtension", ".gz");
        variantOptions.put("compressExtension", ".gz");

        chunkContext = new ChunkContext(null);
        stepContribution = new StepContribution(new StepExecution("variantTransformStep", null));
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(VALID_LOAD_STATS_DB);
    }
}
