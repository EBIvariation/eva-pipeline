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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getLines;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getTransformedOutputPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.DB_NAME;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantsTransform}
 *
 * TODO
 * FILE_WRONG_NO_ALT should be renamed because the alt allele is not missing but is the same as the reference
 */
public class VariantsTransformTest {
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";
    private static final String VALID_TRANSFORM_DB = "VariantConfigurationTest_vt";

    private static final String FILE_WRONG_NO_ALT = "/wrong_no_alt.vcf.gz";
    private static final String INVALID_TRANSFORM_DB = "VariantConfigurationTest_it";

    private VariantsTransform variantsTransform;

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;

    private ChunkContext chunkContext;
    private StepContribution stepContribution;

    @Test
    public void transformTaskletShouldTransformAllVariants() throws Exception {
        //Given a valid VCF input file
        String input = VariantsTransformTest.class.getResource(SMALL_VCF_FILE).getFile();

        pipelineOptions.put("input", input);
        variantOptions.put(DB_NAME, VALID_TRANSFORM_DB);

        variantOptions.put(VARIANT_SOURCE, new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE));

        ReflectionTestUtils.setField(variantsTransform, "pipelineOptions", pipelineOptions);
        ReflectionTestUtils.setField(variantsTransform, "variantOptions", variantOptions);

        String outputFilename = getTransformedOutputPath(Paths.get(SMALL_VCF_FILE).getFileName(), ".gz", "/tmp");

        File file = new File(outputFilename);
        if(file.exists())
            file.delete();
        assertFalse(file.exists());

        // When the execute method in variantsTransform is executed
        RepeatStatus repeatStatus = variantsTransform.execute(stepContribution, chunkContext);

        //Then variantsTransform should complete correctly
        assertEquals(RepeatStatus.FINISHED, repeatStatus);
        // And the transformed file should contain the same number of line of the Vcf input file
        assertEquals(300, getLines(new GZIPInputStream(new FileInputStream(outputFilename))));

        file.delete();
        new File(pipelineOptions.getString("outputDir"), "small20.vcf.gz.file.json.gz").delete();
    }

    /**
     * This test has to fail because the vcf FILE_WRONG_NO_ALT is malformed:
     * in a variant a reference and a alternate allele are the same
     */
    @Test(expected=StorageManagerException.class)
    public void transformTaskletShouldFailIfVariantsAreMalformed() throws Exception {
        //Given a malformed VCF input file
        String input = VariantsTransformTest.class.getResource(FILE_WRONG_NO_ALT).getFile();

        pipelineOptions.put("input", input);
        variantOptions.put(DB_NAME, INVALID_TRANSFORM_DB);

        variantOptions.put(VARIANT_SOURCE, new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE));

        ReflectionTestUtils.setField(variantsTransform, "pipelineOptions", pipelineOptions);
        ReflectionTestUtils.setField(variantsTransform, "variantOptions", variantOptions);

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_WRONG_NO_ALT).getFileName(), ".gz", "/tmp");

        File file = new File(outputFilename);
        file.delete();
        assertFalse(file.exists());

        //When the execute method in variantsTransform is invoked then a StorageManagerException is thrown
        variantsTransform.execute(stepContribution, chunkContext);
    }

    @Before
    public void setUp() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        variantsTransform = new VariantsTransform();
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
        new File(pipelineOptions.getString("outputDir"), "wrong_no_alt.vcf.gz.file.json.gz").delete();
        new File(pipelineOptions.getString("outputDir"), "wrong_no_alt.vcf.gz.variants.json.gz").delete();
    }

}
