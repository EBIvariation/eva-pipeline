package uk.ac.ebi.eva.pipeline.io.readers;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.test.MetaDataInstanceFactory;

import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link VcfReader}
 * <p>
 * input: a Vcf file
 * <p>
 * output: a list of variants each time its `.read()` is called
 */
public class VcfReaderTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldReadAllLines() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // input vcf
        final String inputFilePath = "/small20.vcf.gz";
        String input = VcfReaderTest.class.getResource(inputFilePath).getFile();

        String fileId = "5";
        String studyId = "7";
        String studyName = "study name";
        VcfHeaderReader headerReader = new VcfHeaderReader(new File(input), fileId, studyId, studyName,
                                                           VariantStudy.StudyType.COLLECTION,
                                                           VariantSource.Aggregation.NONE);
        VariantSource source = headerReader.read();

        VcfReader vcfReader = new VcfReader(source, input);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(input, vcfReader);
    }

    @Test
    public void invalidFileShouldFail() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // input vcf
        final String inputFilePath = "/wrong_no_alt.vcf.gz";
        String inputFile = VcfReaderTest.class.getResource(inputFilePath).getFile();

        String fileId = "5";
        String studyId = "7";
        String studyName = "study name";
        VcfHeaderReader headerReader = new VcfHeaderReader(new File(inputFile), fileId, studyId, studyName,
                                                           VariantStudy.StudyType.COLLECTION,
                                                           VariantSource.Aggregation.NONE);
        VariantSource source = headerReader.read();

        VcfReader vcfReader = new VcfReader(source, inputFile);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        // consume the reader and check that a wrong variant raise an exception
        exception.expect(FlatFileParseException.class);
        while (vcfReader.read() != null) {
        }
    }

    @Test
    public void testUncompressedVcf() throws Exception {

        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // uncompress the input VCF into a temporal file
        final String inputFilePath = "/small20.vcf.gz";
        String inputFile = VcfReaderTest.class.getResource(inputFilePath).getFile();
        File tempFile = JobTestUtils.createTempFile();
        JobTestUtils.uncompress(inputFile, tempFile);

        String fileId = "5";
        String studyId = "7";
        String studyName = "study name";
        VcfHeaderReader headerReader = new VcfHeaderReader(new File(inputFile), fileId, studyId, studyName,
                                                           VariantStudy.StudyType.COLLECTION,
                                                           VariantSource.Aggregation.NONE);
        VariantSource source = headerReader.read();

        VcfReader vcfReader = new VcfReader(source, tempFile);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(inputFile, vcfReader);
    }

    private void consumeReader(String inputFile, VcfReader vcfReader) throws Exception {
        List<Variant> variants;
        int count = 0;

        // consume the reader and check that the variants and the VariantSource have meaningful data
        while ((variants = vcfReader.read()) != null) {
            assertTrue(variants.size() > 0);
            assertTrue(variants.get(0).getSourceEntries().size() > 0);
            VariantSourceEntry sourceEntry = variants.get(0).getSourceEntries().entrySet().iterator().next().getValue();
            assertTrue(sourceEntry.getSamplesData().size() > 0);

            count++;
        }

        // VcfReader should get all the lines from the file
        long expectedCount = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(inputFile)));
        assertEquals(expectedCount, count);
    }
}
