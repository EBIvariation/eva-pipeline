/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.io.readers;

import org.junit.Rule;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.test.MetaDataInstanceFactory;

import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;
import uk.ac.ebi.eva.test.data.VepOutputContent;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;

/**
 * {@link AnnotationFlatFileReader}
 * input: a File written by VEP
 * output: a Annotation each time its `.read()` is called
 * <p>
 * incorrect input lines should not make the reader fail.
 */
public class AnnotationFlatFileReaderTest {

    private static final String VEP_VERSION = "1";

    private static final String VEP_CACHE_VERSION = "1";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Test
    public void shouldReadAllLinesInVepOutput() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        //simulate VEP output file
        File file = temporaryFolderRule.newGzipFile(VepOutputContent.vepOutputContent);

        AnnotationFlatFileReader annotationFlatFileReader = new AnnotationFlatFileReader(file, VEP_VERSION,
                VEP_CACHE_VERSION);
        annotationFlatFileReader.setSaveState(false);
        annotationFlatFileReader.open(executionContext);

        Annotation annotation;
        int consequenceTypeCount = 0;
        int count = 0;
        while ((annotation = annotationFlatFileReader.read()) != null) {
            count++;
            if (annotation.getConsequenceTypes() != null && !annotation.getConsequenceTypes().isEmpty()) {
                consequenceTypeCount++;
            }
        }
        // all should have at least consequence type annotations
        assertEquals(count, consequenceTypeCount);

        // annotationFlatFileReader should get all the lines from the file
        long expectedCount = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(file)));
        assertEquals(expectedCount, count);
    }

    // Missing ':' in 20_63351 (should be 20:63351)
    @Test(expected = FlatFileParseException.class)
    public void malformedCoordinatesAnnotationLinesShouldBeSkipped() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        File file = temporaryFolderRule.newGzipFile(VepOutputContent.vepOutputContentMalformedCoordinates);
        AnnotationFlatFileReader annotationFlatFileReader = new AnnotationFlatFileReader(file, VEP_VERSION,
                VEP_CACHE_VERSION);
        annotationFlatFileReader.open(executionContext);
        annotationFlatFileReader.read();
    }

    // Missing '/' in 20_63351_AG (should be 20_63351_A/G)
    @Test(expected = FlatFileParseException.class)
    public void malformedVariantFieldsAnnotationLinesShouldBeSkipped() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        File file = temporaryFolderRule.newGzipFile(VepOutputContent.vepOutputContentMalformedVariantFields);
        AnnotationFlatFileReader annotationFlatFileReader = new AnnotationFlatFileReader(file, VEP_VERSION,
                VEP_CACHE_VERSION);
        annotationFlatFileReader.open(executionContext);
        annotationFlatFileReader.read();
    }

}
