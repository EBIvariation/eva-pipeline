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

package uk.ac.ebi.eva.pipeline.io.writers;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class VepAnnotationFileWriterTest {

    private static final int EXPECTED_ANNOTATIONS = 537;

    private static final long TIMEOUT_IN_SECONDS = 10L;

    private static final int EXTRA_ANNOTATIONS = 1;

    private AnnotationParameters annotationParameters;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    @Before
    public void setUp() throws Exception {
        annotationParameters = new AnnotationParameters();
        annotationParameters.setFileId("fid");
        annotationParameters.setStudyId("sid");
        annotationParameters.setVepCacheVersion("1");
        annotationParameters.setVepCachePath("cache");
        annotationParameters.setVepPath(getResource("/mockvep_writeToFile.pl").getAbsolutePath());
        annotationParameters.setVepCacheSpecies("hsapiens");
        annotationParameters.setInputFasta("fasta");
        annotationParameters.setVepNumForks("4");

        File annotationFolder = temporaryFolder.newFolder();
        annotationParameters.setOutputDirAnnotation(annotationFolder.getAbsolutePath());
    }

    @Test
    public void testMockVep() throws Exception {
        int chunkSize = 10;
        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(TIMEOUT_IN_SECONDS, chunkSize,
                annotationParameters);
        Variant variant = new Variant("20", 100, 105, "A", "T");
        List<VariantWrapper> variantWrappers = Collections.singletonList(new VariantWrapper(variant));

        vepAnnotationFileWriter.open(null);
        vepAnnotationFileWriter.write(variantWrappers);
        vepAnnotationFileWriter.close();

        File vepOutputFile = new File(annotationParameters.getVepOutput());
        assertTrue(vepOutputFile.exists());
        assertEquals(variantWrappers.size() + EXTRA_ANNOTATIONS, getLines(new FileInputStream(vepOutputFile)));
    }

}
