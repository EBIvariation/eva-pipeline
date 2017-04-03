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
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter;
import org.springframework.batch.item.ItemStreamException;

import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.rules.TemporaryMongoRule.constructDbObject;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class VepAnnotationFileWriterTest {

    private static final long TIMEOUT_IN_SECONDS = 5L;

    /**
     * the mockvep writes an extra line as if some variant had two annotations, to check that the writer is not assuming
     * that the count of variants to annotate is the same as variantAnnotations to write in the file.
     */
    private static final int EXTRA_ANNOTATIONS = 1;

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
        annotationParameters.setVepNumForks("4");

        File annotationFolder = temporaryFolder.newFolder();
        annotationParameters.setOutputDirAnnotation(annotationFolder.getAbsolutePath());
    }

    @Test
    public void testMockVep() throws Exception {
        DBObjectToVariantConverter converter = new DBObjectToVariantConverter();
        VariantWrapper variantWrapper = new VariantWrapper(
                converter.convertToDataModelType(constructDbObject(VariantData.getVariantWithAnnotation())));
        List<VariantWrapper> variantWrappers = Collections.singletonList(variantWrapper);
        int chunkSize = variantWrappers.size();

        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(TIMEOUT_IN_SECONDS, chunkSize,
                annotationParameters);

        vepAnnotationFileWriter.open(null);
        vepAnnotationFileWriter.write(variantWrappers);
        vepAnnotationFileWriter.close();

        File vepOutputFile = new File(annotationParameters.getVepOutput());
        assertTrue(vepOutputFile.exists());
        assertEquals(variantWrappers.size() + EXTRA_ANNOTATIONS,
                getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
    }

    @Test
    public void testMockVepSeveralChunks() throws Exception {
        DBObjectToVariantConverter converter = new DBObjectToVariantConverter();
        VariantWrapper variantWrapper = new VariantWrapper(
                converter.convertToDataModelType(constructDbObject(VariantData.getVariantWithAnnotation())));
        List<VariantWrapper> variantWrappers = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            variantWrappers.add(variantWrapper);
        }
        int chunkSize = 5;

        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(TIMEOUT_IN_SECONDS, chunkSize,
                annotationParameters);

        vepAnnotationFileWriter.open(null);
        vepAnnotationFileWriter.write(variantWrappers);
        vepAnnotationFileWriter.close();

        File vepOutputFile = new File(annotationParameters.getVepOutput());
        assertTrue(vepOutputFile.exists());
        assertEquals(variantWrappers.size() + EXTRA_ANNOTATIONS,
                getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
    }

    @Test
    public void testVepWriterWritesLastSmallerChunk() throws Exception {
        DBObjectToVariantConverter converter = new DBObjectToVariantConverter();
        VariantWrapper variantWrapper = new VariantWrapper(
                converter.convertToDataModelType(constructDbObject(VariantData.getVariantWithAnnotation())));
        List<VariantWrapper> variantWrappers = Collections.singletonList(variantWrapper);
        int chunkSizeGreaterThanActualVariants = variantWrappers.size() * 10;

        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(TIMEOUT_IN_SECONDS,
                chunkSizeGreaterThanActualVariants, annotationParameters);

        vepAnnotationFileWriter.open(null);
        vepAnnotationFileWriter.write(variantWrappers);
        vepAnnotationFileWriter.close();

        File vepOutputFile = new File(annotationParameters.getVepOutput());
        assertTrue(vepOutputFile.exists());
        assertEquals(variantWrappers.size() + EXTRA_ANNOTATIONS,
                getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
    }

    @Test
    public void testVepTimeouts() throws Exception {
        DBObjectToVariantConverter converter = new DBObjectToVariantConverter();
        VariantWrapper variantWrapper = new VariantWrapper(
                converter.convertToDataModelType(constructDbObject(VariantData.getVariantWithAnnotation())));
        List<VariantWrapper> variantWrappers = Collections.singletonList(variantWrapper);
        int chunkSizeGreaterThanActualVariants = variantWrappers.size() * 10;
        annotationParameters.setVepPath(getResource("/mockvep_writeToFile_delayed.pl").getAbsolutePath());

        long vepTimeouts = 1;
        VepAnnotationFileWriter vepAnnotationFileWriter = new VepAnnotationFileWriter(vepTimeouts,
                chunkSizeGreaterThanActualVariants, annotationParameters);

        vepAnnotationFileWriter.open(null);
        vepAnnotationFileWriter.write(variantWrappers);

        exception.expect(ItemStreamException.class);
        exception.expectMessage("Reached the timeout (" + vepTimeouts
                + " seconds) while waiting for VEP to finish. Killed the process.");
        vepAnnotationFileWriter.close();
    }

}
