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
import org.opencb.biodata.models.variant.VariantStudy;
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
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.countRows;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getLines;
import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantsLoad}
 */
public class VariantsLoadTest {
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";
    private static final String VALID_LOAD_DB = "VariantConfigurationTest_vl";

    private VariantsLoad variantsLoad;

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;

    private ChunkContext chunkContext;
    private StepContribution stepContribution;

    @Test
    public void loadTaskletShouldLoadAllVariants() throws Exception {
        //Given a valid VCF input file
        String input = SMALL_VCF_FILE;
        String dbName = VALID_LOAD_DB;

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);

        String outputDir = pipelineOptions.getString("outputDir");

        variantOptions.put(VARIANT_SOURCE, new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE));

        //and a variants transform step already executed
        File transformedVcfVariantsFile =
                new File(VariantsLoadTest.class.getResource("/small20.vcf.gz.variants.json.gz").getFile());
        File tmpTransformedVcfVariantsFile = new File(outputDir, transformedVcfVariantsFile.getName());
        FileUtils.copyFile(transformedVcfVariantsFile, tmpTransformedVcfVariantsFile);

        File transformedVariantsFile =
                new File(VariantsLoadTest.class.getResource("/small20.vcf.gz.file.json.gz").getFile());
        File tmpTransformedVariantsFile = new File(outputDir, transformedVariantsFile.getName());
        FileUtils.copyFile(transformedVariantsFile, tmpTransformedVariantsFile);

        ReflectionTestUtils.setField(variantsLoad, "pipelineOptions", pipelineOptions);
        ReflectionTestUtils.setField(variantsLoad, "variantOptions", variantOptions);

        // When the execute method in variantsLoad is executed
        RepeatStatus repeatStatus = variantsLoad.execute(stepContribution, chunkContext);

        //Then variantsLoad step should complete correctly
        assertEquals(RepeatStatus.FINISHED, repeatStatus);

        // And the number of documents in db should be the same number of line of the vcf transformed file
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        long lines = getLines(new GZIPInputStream(new FileInputStream(transformedVcfVariantsFile)));

        assertEquals(countRows(iterator), lines);

        tmpTransformedVcfVariantsFile.delete();
        tmpTransformedVariantsFile.delete();
    }

    @Before
    public void setUp() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        variantsLoad = new VariantsLoad();
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
        JobTestUtils.cleanDBs(VALID_LOAD_DB);
    }

}