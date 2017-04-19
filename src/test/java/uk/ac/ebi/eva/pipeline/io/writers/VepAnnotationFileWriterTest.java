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
import org.junit.rules.ExpectedException;
import org.springframework.batch.item.ItemStreamException;

import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class VepAnnotationFileWriterTest {

    private static final long TIMEOUT_IN_SECONDS = 5L;

    /**
     * the mockvep writes an extra line as if some variant had two annotations, to check that the writer is not assuming
     * that the count of variants to annotate is the same as annotations to write in the file.
     */
    private static final int EXTRA_ANNOTATIONS = 1;

    private static final int HEADER_LINES = 3;

    private final VariantWrapper VARIANT_WRAPPER = new VariantWrapper("1", 100, 105, "A", "T");

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
        List<VariantWrapper> variantWrappers = Collections.singletonList(VARIANT_WRAPPER);
        int chunkSize = variantWrappers.size();

        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(annotationParameters, chunkSize,
                TIMEOUT_IN_SECONDS);

        vepAnnotationFileWriter.write(variantWrappers);

        File vepOutputFile = new File(annotationParameters.getVepOutput());
        assertTrue(vepOutputFile.exists());
        assertEquals(variantWrappers.size() + EXTRA_ANNOTATIONS,
                getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
    }

    @Test
    public void testMockVepSeveralChunks() throws Exception {
        List<VariantWrapper> variantWrappers = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            variantWrappers.add(VARIANT_WRAPPER);
        }
        int chunkSize = 5;

        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(annotationParameters, chunkSize,
                TIMEOUT_IN_SECONDS);

        vepAnnotationFileWriter.write(variantWrappers);

        File vepOutputFile = new File(annotationParameters.getVepOutput());
        assertTrue(vepOutputFile.exists());
        assertEquals(variantWrappers.size() + EXTRA_ANNOTATIONS,
                getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
    }

    @Test
    public void testVepWriterWritesLastSmallerChunk() throws Exception {
        List<VariantWrapper> variantWrappers = Collections.singletonList(VARIANT_WRAPPER);
        int chunkSizeGreaterThanActualVariants = variantWrappers.size() * 10;

        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(annotationParameters,
                chunkSizeGreaterThanActualVariants, TIMEOUT_IN_SECONDS);

        vepAnnotationFileWriter.write(variantWrappers);

        File vepOutputFile = new File(annotationParameters.getVepOutput());
        assertTrue(vepOutputFile.exists());
        assertEquals(variantWrappers.size() + EXTRA_ANNOTATIONS,
                getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
    }

    @Test
    public void testHeaderIsWrittenOnlyOnce() throws Exception {
        List<VariantWrapper> variantWrappers = Collections.singletonList(VARIANT_WRAPPER);
        int chunkSize = variantWrappers.size();

        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(annotationParameters,
                chunkSize, TIMEOUT_IN_SECONDS);

        long chunks = 3;
        for (int i = 0; i < chunks; i++) {
            vepAnnotationFileWriter.write(variantWrappers);
        }

        File vepOutputFile = new File(annotationParameters.getVepOutput());
        assertTrue(vepOutputFile.exists());
        assertEquals((variantWrappers.size() + EXTRA_ANNOTATIONS)*chunks,
                getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));

        assertEquals(HEADER_LINES, getCommentLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(
                new FileInputStream(vepOutputFile))));
        for (int i = 0; i < HEADER_LINES; i++) {
            assertEquals('#', reader.readLine().charAt(0));
        }
    }

    /**
     * counts non-comment lines in an InputStream
     */
    public static long getCommentLines(InputStream in) throws IOException {
        BufferedReader file = new BufferedReader(new InputStreamReader(in));
        long lines = 0;
        String line;
        while ((line = file.readLine()) != null) {
            if (line.charAt(0) == '#') {
                lines++;
            }
        }
        file.close();
        return lines;
    }

    @Test
    public void testVepTimeouts() throws Exception {
        List<VariantWrapper> variantWrappers = Collections.singletonList(VARIANT_WRAPPER);
        int chunkSizeGreaterThanActualVariants = variantWrappers.size() * 10;
        annotationParameters.setVepPath(getResource("/mockvep_writeToFile_delayed.pl").getAbsolutePath());

        long vepTimeouts = 1;
        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(annotationParameters,
                chunkSizeGreaterThanActualVariants, vepTimeouts);

        exception.expect(ItemStreamException.class);
        vepAnnotationFileWriter.write(variantWrappers);
    }

}
