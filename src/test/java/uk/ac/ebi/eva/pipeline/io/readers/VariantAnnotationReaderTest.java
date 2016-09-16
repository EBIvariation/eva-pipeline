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

import org.junit.Test;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.test.MetaDataInstanceFactory;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.test.data.VepOutputContent;

import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import static junit.framework.TestCase.assertEquals;

/**
 * {@link VariantAnnotationReader}
 * input: a File written by VEP
 * output: a VariantAnnotation each time its `.read()` is called
 *
 * incorrect input lines should not make the reader fail.
 */
public class VariantAnnotationReaderTest {

    @Test
    public void shouldReadAllLinesInVepOutput() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        //simulate VEP output file
        File file = JobTestUtils.makeGzipFile(VepOutputContent.vepOutputContent);

        VariantAnnotationReader variantAnnotationReader = new VariantAnnotationReader(file);
        variantAnnotationReader.setSaveState(false);
        variantAnnotationReader.open(executionContext);

        VariantAnnotation variantAnnotation;
        int consequenceTypeCount = 0;
        int count = 0;
        while ((variantAnnotation = variantAnnotationReader.read()) != null) {
            count++;
            if (variantAnnotation.getConsequenceTypes() != null && !variantAnnotation.getConsequenceTypes().isEmpty()) {
                consequenceTypeCount++;
            }
        }
        // all should have at least consequence type annotations
        assertEquals(count, consequenceTypeCount);

        // variantAnnotationReader should get all the lines from the file
        long actualCount = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(file)));
        assertEquals(actualCount, count);
    }

    // Missing ':' in 20_63351 (should be 20:63351)
    @Test(expected = FlatFileParseException.class)
    public void malformedCoordinatesAnnotationLinesShouldBeSkipped() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        File file = JobTestUtils.makeGzipFile(VepOutputContent.vepOutputContentMalformedCoordinates);
        VariantAnnotationReader variantAnnotationReader = new VariantAnnotationReader(file);
        variantAnnotationReader.open(executionContext);
        variantAnnotationReader.read();
    }

    // Missing '/' in 20_63351_AG (sould be 20_63351_A/G)
    @Test(expected = FlatFileParseException.class)
    public void malformedVariantFieldsAnnotationLinesShouldBeSkipped() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        File file = JobTestUtils.makeGzipFile(VepOutputContent.vepOutputContentMalformedVariantFields);
        VariantAnnotationReader variantAnnotationReader = new VariantAnnotationReader(file);
        variantAnnotationReader.open(executionContext);
        variantAnnotationReader.read();
    }

}
