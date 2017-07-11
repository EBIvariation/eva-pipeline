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

package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.batch.item.ItemStreamException;

import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class VepAnnotationProcessorTest {

    private static final long TIMEOUT_IN_SECONDS = 5L;

    /**
     * the mockvep writes an extra line as if some variant had two annotations, to check that the writer is not assuming
     * that the count of variants to annotate is the same as annotations to write in the file.
     */
    private static final int EXTRA_ANNOTATIONS = 1;

    private static final int HEADER_LINES = 3;

    private final EnsemblVariant VARIANT_WRAPPER = new EnsemblVariant("1", 100, 105, "A", "T");

    private AnnotationParameters annotationParameters;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    @Rule
    public ExpectedException exception = ExpectedException.none();


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
    public void testMockVep() throws Exception {
        List<EnsemblVariant> ensemblVariants = Collections.singletonList(VARIANT_WRAPPER);
        int chunkSize = ensemblVariants.size();

        VepAnnotationProcessor vepAnnotationProcessor = new VepAnnotationProcessor(annotationParameters, chunkSize,
                                                                                   TIMEOUT_IN_SECONDS);

        List<String> annotations = vepAnnotationProcessor.process(ensemblVariants);
        assertEquals(ensemblVariants.size() + EXTRA_ANNOTATIONS, annotations.size());
    }

    @Test
    public void testMockVepSeveralChunks() throws Exception {
        List<EnsemblVariant> ensemblVariants = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            ensemblVariants.add(VARIANT_WRAPPER);
        }
        int chunkSize = 5;

        VepAnnotationProcessor vepAnnotationProcessor = new VepAnnotationProcessor(annotationParameters, chunkSize,
                                                                                   TIMEOUT_IN_SECONDS);

        List<String> annotations = vepAnnotationProcessor.process(ensemblVariants);
        assertEquals(ensemblVariants.size() + EXTRA_ANNOTATIONS, annotations.size());
    }

    @Test
    public void testVepWriterWritesLastSmallerChunk() throws Exception {
        List<EnsemblVariant> ensemblVariants = Collections.singletonList(VARIANT_WRAPPER);
        int chunkSizeGreaterThanActualVariants = ensemblVariants.size() * 10;

        VepAnnotationProcessor vepAnnotationProcessor = new VepAnnotationProcessor(annotationParameters,
                                                                                   chunkSizeGreaterThanActualVariants,
                                                                                   TIMEOUT_IN_SECONDS);

        List<String> annotations = vepAnnotationProcessor.process(ensemblVariants);
        assertEquals(ensemblVariants.size() + EXTRA_ANNOTATIONS, annotations.size());
    }

    @Test
    public void testHeaderIsNotWritten() throws Exception {
        List<EnsemblVariant> ensemblVariants = Collections.singletonList(VARIANT_WRAPPER);
        int chunkSize = ensemblVariants.size();

        VepAnnotationProcessor vepAnnotationProcessor = new VepAnnotationProcessor(annotationParameters,
                                                                                   chunkSize, TIMEOUT_IN_SECONDS);

        long chunks = 3;
        List<String> annotations = new ArrayList<>();
        for (int i = 0; i < chunks; i++) {
            annotations.addAll(vepAnnotationProcessor.process(ensemblVariants));
        }

        assertEquals((ensemblVariants.size() + EXTRA_ANNOTATIONS)*chunks, annotations.size());
        assertTrue(annotations.stream().noneMatch(line -> line.startsWith("#")));
    }

    @Test
    public void testVepTimeouts() throws Exception {
        List<EnsemblVariant> ensemblVariants = Collections.singletonList(VARIANT_WRAPPER);
        int chunkSizeGreaterThanActualVariants = ensemblVariants.size() * 10;
        annotationParameters.setVepPath(getResource("/mockvep_writeToFile_delayed.pl").getAbsolutePath());

        long vepTimeouts = 1;
        VepAnnotationProcessor vepAnnotationProcessor = new VepAnnotationProcessor(annotationParameters,
                                                                                   chunkSizeGreaterThanActualVariants, vepTimeouts);

        exception.expect(ItemStreamException.class);
        vepAnnotationProcessor.process(ensemblVariants);
    }

}
