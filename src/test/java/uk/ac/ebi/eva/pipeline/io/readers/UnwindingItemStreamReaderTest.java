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
import org.junit.rules.ExpectedException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.test.MetaDataInstanceFactory;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class UnwindingItemStreamReaderTest {
    
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    private static final String INPUT_FILE_PATH = "/input-files/vcf/genotyped.vcf.gz";

    private static final String INPUT_WRONG_FILE_PATH = "/input-files/vcf/wrong_same_ref_alt.vcf.gz";

    private static final String FILE_ID = "5";

    private static final String STUDY_ID = "7";

    @Test
    public void shouldReadAllLines() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // input vcf
        File input = getResource(INPUT_FILE_PATH);

        VcfReader vcfReader = new VcfReader(FILE_ID, STUDY_ID, input);
        vcfReader.setSaveState(false);

        UnwindingItemStreamReader<Variant> unwindingItemStreamReader = new UnwindingItemStreamReader<>(vcfReader);
        unwindingItemStreamReader.open(executionContext);

        consumeReader(input, unwindingItemStreamReader);
    }

    @Test
    public void invalidFileShouldFail() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // input vcf
        File input = getResource(INPUT_WRONG_FILE_PATH);

        VcfReader vcfReader = new VcfReader(FILE_ID, STUDY_ID, input);
        vcfReader.setSaveState(false);

        UnwindingItemStreamReader<Variant> unwindingItemStreamReader = new UnwindingItemStreamReader<>(vcfReader);
        unwindingItemStreamReader.open(executionContext);

        // consume the reader and check that a wrong variant raise an exception
        exception.expect(FlatFileParseException.class);
        while (unwindingItemStreamReader.read() != null) {
        }
    }

    @Test
    public void testUncompressedVcf() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // uncompress the input VCF into a temporary file
        File input = getResource(INPUT_FILE_PATH);
        File tempFile = temporaryFolderRule.newFile();
        JobTestUtils.uncompress(input.getAbsolutePath(), tempFile);

        VcfReader vcfReader = new VcfReader(FILE_ID, STUDY_ID, tempFile);
        vcfReader.setSaveState(false);

        UnwindingItemStreamReader<Variant> unwindingItemStreamReader = new UnwindingItemStreamReader<>(vcfReader);
        unwindingItemStreamReader.open(executionContext);

        consumeReader(input, unwindingItemStreamReader);
    }

    private void consumeReader(File inputFile, ItemReader<Variant> reader) throws Exception {
        Variant variant;
        Long count = 0L;

        // consume the reader and check that the variants and the VariantSource have meaningful data
        while ((variant = reader.read()) != null) {
            assertTrue(variant.getSourceEntries().size() > 0);
            VariantSourceEntry sourceEntry = variant.getSourceEntries().entrySet().iterator().next().getValue();
            assertTrue(sourceEntry.getSamplesData().size() > 0);

            count++;
        }

        // VcfReader should get all the lines from the file
        Long expectedCount = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(inputFile)));
        assertThat(expectedCount, lessThanOrEqualTo(count));
    }


}
