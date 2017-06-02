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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.File;

import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class VepProcessTest {

    private AnnotationParameters annotationParameters;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final int CHUNK_SIZE = 10;

    private static final long VEP_TIMEOUT = 1;

    private final EnsemblVariant VARIANT_WRAPPER = new EnsemblVariant("1", 100, 105, "A", "T");

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
        annotationParameters.setVepNumForks(4);

        File annotationFolder = temporaryFolder.newFolder();
        annotationParameters.setOutputDirAnnotation(annotationFolder.getAbsolutePath());
    }

    @Test
    public void testWorkflowWriteWithoutOpening() throws Exception {
        VepProcess vepAnnotationFileWriter = new VepProcess(annotationParameters, CHUNK_SIZE, VEP_TIMEOUT);

        exception.expect(IllegalStateException.class);
        vepAnnotationFileWriter.write(getVariantInVepInputFormat(VARIANT_WRAPPER).getBytes());
    }

    private String getVariantInVepInputFormat(EnsemblVariant ensemblVariant) {
        return String.join("\t",
                           ensemblVariant.getChr(),
                           Integer.toString(ensemblVariant.getStart()),
                           Integer.toString(ensemblVariant.getEnd()),
                           ensemblVariant.getRefAlt(),
                           ensemblVariant.getStrand());
    }

    @Test
    public void testWorkflowFlushWithoutOpening() throws Exception {
        VepProcess vepAnnotationFileWriter = new VepProcess(annotationParameters, CHUNK_SIZE, VEP_TIMEOUT);

        exception.expect(IllegalStateException.class);
        vepAnnotationFileWriter.flush();
    }

    @Test
    public void testWorkflowFlushAfterClosing() throws Exception {
        VepProcess vepAnnotationFileWriter = new VepProcess(annotationParameters, CHUNK_SIZE, VEP_TIMEOUT);

        vepAnnotationFileWriter.open();
        vepAnnotationFileWriter.close();

        exception.expect(IllegalStateException.class);
        vepAnnotationFileWriter.flush();
    }

    @Test
    public void testWorkflowCloseWithoutOpening() throws Exception {
        VepProcess vepAnnotationFileWriter = new VepProcess(annotationParameters, CHUNK_SIZE, VEP_TIMEOUT);
        vepAnnotationFileWriter.close();
    }
}
