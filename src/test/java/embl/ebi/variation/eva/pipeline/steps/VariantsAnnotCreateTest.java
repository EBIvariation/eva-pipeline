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
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
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
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantsAnnotCreate}
 */
public class VariantsAnnotCreateTest {
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    //Given a valid VCF input file
    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";
    private static final String VALID_ANNOT_DB = "VariantConfigurationTest_va";

    private VariantsAnnotCreate variantsAnnotCreate;

    private ObjectMap pipelineOptions;

    private ChunkContext chunkContext;
    private StepContribution stepContribution;

    @Test
    public void annotCreateTaskletShouldGenerateAnnotations() throws Exception {
        String input = VariantsAnnotCreateTest.class.getResource(SMALL_VCF_FILE).getFile();
        VariantSource source = new VariantSource(input, "annotTest", "1", "studyName");

        pipelineOptions.put("input", input);

        //and a variants annotation generate input step already executed
        File vepInput = new File(VariantsAnnotCreateTest.class.getResource("/preannot.sorted").getFile());
        File vepOutput = new File(Paths.get(pipelineOptions.getString("outputDir")).resolve(VariantStorageManager.buildFilename(source)) + ".variants.annot.gz");

        pipelineOptions.put("vepInput", vepInput.toString());
        pipelineOptions.put("vepOutput", vepOutput.toString());

        ReflectionTestUtils.setField(variantsAnnotCreate, "pipelineOptions", pipelineOptions);

        vepOutput.delete();
        assertFalse(vepOutput.exists());  // ensure the annot file doesn't exist from previous executions

        // When the execute method in variantsAnnotCreate is executed
        RepeatStatus repeatStatus = variantsAnnotCreate.execute(stepContribution, chunkContext);

        //Then variantsAnnotCreate step should complete correctly
        assertEquals(RepeatStatus.FINISHED, repeatStatus);

        // And VEP output should exist and annotations should be in the file
        assertTrue(vepOutput.exists());
        assertEquals(537, getLines(new GZIPInputStream(new FileInputStream(vepOutput))));
        vepOutput.delete();
    }

    @Before
    public void setUp() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        variantsAnnotCreate = new VariantsAnnotCreate();
        pipelineOptions = new ObjectMap();

        pipelineOptions.put("outputDir", "/tmp");
        pipelineOptions.put("pedigree", "FIRST_8_COLUMNS");
        pipelineOptions.put("compressExtension", ".gz");

        //VEP init
        pipelineOptions.put("vepPath", VariantsAnnotCreateTest.class.getResource("/mockvep.pl").getFile());
        pipelineOptions.put("vepCacheVersion", "79");
        pipelineOptions.put("vepCacheDirectory", "/path/to/cache");
        pipelineOptions.put("vepSpecies", "homo_sapiens");
        pipelineOptions.put("vepFasta", "/path/to/file.fa");
        pipelineOptions.put("vepNumForks", "4");

        chunkContext = new ChunkContext(null);
        stepContribution = new StepContribution(new StepExecution("variantTransformStep", null));
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(VALID_ANNOT_DB);
    }
}
