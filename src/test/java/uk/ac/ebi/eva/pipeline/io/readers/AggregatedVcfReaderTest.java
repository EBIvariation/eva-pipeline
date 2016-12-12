package uk.ac.ebi.eva.pipeline.io.readers;

import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.test.utils.TestFileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link AggregatedVcfReader}
 * input: a Vcf file
 * output: a list of variants each time its `.read()` is called
 */
public class AggregatedVcfReaderTest {

    private static final String FILE_ID = "5";

    private static final String STUDY_ID = "7";

    private static final String STUDY_NAME = "study name";

    private static final String INPUT_FILE_PATH = "/aggregated.vcf.gz";

    private static final String INPUT_FILE_PATH_EXAC = "/aggregated.exac.vcf.gz";

    private static final String INPUT_FILE_PATH_EVS = "/aggregated.evs.vcf.gz";

    @Test
    public void shouldReadAllLines() throws Exception {
        shouldReadAllLinesHelper(VariantSource.Aggregation.BASIC, INPUT_FILE_PATH);
    }

    @Test
    public void shouldReadAllLinesInExac() throws Exception {
        shouldReadAllLinesHelper(VariantSource.Aggregation.EXAC, INPUT_FILE_PATH_EXAC);
    }

    @Test
    public void shouldReadAllLinesInEvs() throws Exception {
        shouldReadAllLinesHelper(VariantSource.Aggregation.EVS, INPUT_FILE_PATH_EVS);
    }

    private void shouldReadAllLinesHelper(VariantSource.Aggregation aggregationType,
                                          String inputFilePath) throws Exception {
        
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // input vcf
        File input = TestFileUtils.getResource(inputFilePath);

        VcfHeaderReader headerReader = new VcfHeaderReader(input, FILE_ID, STUDY_ID, STUDY_NAME,
                                                           VariantStudy.StudyType.COLLECTION,
                                                           aggregationType);
        VariantSource source = headerReader.read();

        AggregatedVcfReader vcfReader = new AggregatedVcfReader(source, input);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(input, vcfReader);
    }

    @Test
    public void testUncompressedVcf() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // uncompress the input VCF into a temporal file
        File inputFile = TestFileUtils.getResource(INPUT_FILE_PATH);
        File tempFile = JobTestUtils.createTempFile();
        JobTestUtils.uncompress(inputFile.getAbsolutePath(), tempFile);

        VcfHeaderReader headerReader = new VcfHeaderReader(inputFile, FILE_ID, STUDY_ID, STUDY_NAME,
                                                           VariantStudy.StudyType.COLLECTION,
                                                           VariantSource.Aggregation.BASIC);
        VariantSource source = headerReader.read();

        AggregatedVcfReader vcfReader = new AggregatedVcfReader(source, tempFile);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(inputFile, vcfReader);
    }

    private void consumeReader(File inputFile, AggregatedVcfReader vcfReader) throws Exception {
        List<Variant> variants;
        int count = 0;

        // consume the reader and check that the variants and the VariantSource have meaningful data
        while ((variants = vcfReader.read()) != null) {
            assertTrue(variants.size() > 0);
            assertTrue(variants.get(0).getSourceEntries().size() > 0);
            VariantSourceEntry sourceEntry = variants.get(0).getSourceEntries().entrySet().iterator().next().getValue();
            assertTrue(sourceEntry.getSamplesData().isEmpty()); // by definition, aggregated VCFs don't have sample data
            assertFalse(sourceEntry.getCohortStats(VariantSourceEntry.DEFAULT_COHORT).getGenotypesCount().isEmpty());

            count++;
        }

        // AggregatedVcfReader should get all the lines from the file
        long expectedCount = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(inputFile)));
        assertEquals(expectedCount, count);
    }
}
