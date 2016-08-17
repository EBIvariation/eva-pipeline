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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.nio.file.Paths;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.restoreMongoDbFromDump;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantsStatsCreate}
 */
public class VariantsStatsCreateTest {
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";
    private static final String VALID_CREATE_STATS_DB = "VariantStatsConfigurationTest_vl";   //this name should be the same of the dump DB in /dump

    private VariantsStatsCreate variantsStatsCreate;

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;

    private ChunkContext chunkContext;
    private StepContribution stepContribution;

    @Test
    public void statsCreateTaskletShouldCalculateStats() throws Exception {
        //Given a valid VCF input file
        String input = SMALL_VCF_FILE;

        //and a valid variants load step already completed
        String dump = VariantsStatsCreateTest.class.getResource("/dump/").getFile();
        restoreMongoDbFromDump(dump);

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, VALID_CREATE_STATS_DB);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);

        String outputDir = pipelineOptions.getString("outputDir");

        VariantSource source = new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        variantOptions.put(VARIANT_SOURCE, source);

        ReflectionTestUtils.setField(variantsStatsCreate, "pipelineOptions", pipelineOptions);
        ReflectionTestUtils.setField(variantsStatsCreate, "variantOptions", variantOptions);

        File statsFile = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source))
                + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        // When the execute method in variantsStatsCreate is executed
        RepeatStatus repeatStatus = variantsStatsCreate.execute(stepContribution, chunkContext);

        //Then variantsStatsCreate step should complete correctly
        assertEquals(RepeatStatus.FINISHED, repeatStatus);

        //and the file containing statistics should exist
        assertTrue(statsFile.exists());

        //delete created files
        statsFile.delete();
        new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source))
                + ".source.stats.json.gz").delete();
    }

    /**
     * Variants not loaded.. so nothing to query!
     * @throws Exception
     */
    @Test(expected=IllegalArgumentException.class)
    public void statsCreateTaskletShouldFailIfPreviousStepsAreNotCompleted() throws Exception {
        //Given a valid VCF input file
        String input = SMALL_VCF_FILE;

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, VALID_CREATE_STATS_DB);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);

        String outputDir = pipelineOptions.getString("outputDir");

        VariantSource source = new VariantSource(
                input,
                "5",
                "7",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        variantOptions.put(VARIANT_SOURCE, source);

        ReflectionTestUtils.setField(variantsStatsCreate, "pipelineOptions", pipelineOptions);
        ReflectionTestUtils.setField(variantsStatsCreate, "variantOptions", variantOptions);

        File statsFile = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source))
                + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        // When the execute method in variantsStatsCreate is executed
        variantsStatsCreate.execute(stepContribution, chunkContext);
    }

    @Before
    public void setUp() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        variantsStatsCreate = new VariantsStatsCreate();
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
        JobTestUtils.cleanDBs(VALID_CREATE_STATS_DB);
    }
}
