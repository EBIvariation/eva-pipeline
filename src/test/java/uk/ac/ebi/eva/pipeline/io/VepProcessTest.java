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

package uk.ac.ebi.eva.pipeline.io;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class VepProcessTest {

    private AnnotationParameters annotationParameters;

    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 10;

    private static final long VEP_TIMEOUT = 1;

    private final EnsemblVariant VARIANT_WRAPPER = new EnsemblVariant("1", 100, 105, "A", "T");

    @BeforeEach
    public void setUp() throws Exception {
        annotationParameters = new AnnotationParameters();
        annotationParameters.setFileId("fid");
        annotationParameters.setStudyId("sid");
        annotationParameters.setVepCacheVersion("1");
        annotationParameters.setVepCachePath("cache");
        annotationParameters.setVepPath(getResource("/mockvep_writeToFile.pl").getAbsolutePath());
        annotationParameters.setVepCacheSpecies("hsapiens");
        annotationParameters.setInputFasta("fasta");
        annotationParameters.setVepNumForks(4);

        annotationParameters.setOutputDirAnnotation(tempDir.toAbsolutePath().toString());
    }

    @Test
    public void testWorkflowWriteWithoutOpening() {
        StringWriter writer = new StringWriter();
        boolean skipComments = true;
        VepProcess vepProcess = new VepProcess(annotationParameters, CHUNK_SIZE, VEP_TIMEOUT, writer, skipComments);

        assertThrows(IllegalStateException.class, () -> vepProcess.write(getVariantInVepInputFormat(VARIANT_WRAPPER).getBytes()));
    }

    private String getVariantInVepInputFormat(EnsemblVariant ensemblVariant) {
        return String.join("\t",
                ensemblVariant.getChr(),
                Long.toString(ensemblVariant.getStart()),
                Long.toString(ensemblVariant.getEnd()),
                ensemblVariant.getRefAlt(),
                ensemblVariant.getStrand());
    }

    @Test
    public void testWorkflowFlushWithoutOpening() {
        StringWriter writer = new StringWriter();
        boolean skipComments = true;
        VepProcess vepProcess = new VepProcess(annotationParameters, CHUNK_SIZE, VEP_TIMEOUT, writer, skipComments);

        assertThrows(IllegalStateException.class, () -> vepProcess.flush());
    }

    @Test
    public void testWorkflowFlushAfterClosing() {
        StringWriter writer = new StringWriter();
        boolean skipComments = true;
        VepProcess vepProcess = new VepProcess(annotationParameters, CHUNK_SIZE, VEP_TIMEOUT, writer, skipComments);

        vepProcess.open();
        vepProcess.close();

        assertThrows(IllegalStateException.class, () -> vepProcess.flush());
    }

    @Test
    public void testWorkflowCloseWithoutOpening() {
        StringWriter writer = new StringWriter();
        boolean skipComments = true;
        VepProcess vepProcess = new VepProcess(annotationParameters, CHUNK_SIZE, VEP_TIMEOUT, writer, skipComments);
        vepProcess.close();
    }
}
