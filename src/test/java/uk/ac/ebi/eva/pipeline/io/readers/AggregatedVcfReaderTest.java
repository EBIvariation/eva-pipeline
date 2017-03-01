package uk.ac.ebi.eva.pipeline.io.readers;

import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * {@link AggregatedVcfReader}
 * input: a Vcf file
 * output: a list of variants each time its `.read()` is called
 */
public class AggregatedVcfReaderTest {

    private static final String FILE_ID = "5";

    private static final String STUDY_ID = "7";

    private static final String INPUT_FILE_PATH = "/input-files/vcf/aggregated.vcf.gz";

    private static final String INPUT_FILE_PATH_EXAC = "/input-files/vcf/aggregated.exac.vcf.gz";

    private static final String INPUT_FILE_PATH_EVS = "/input-files/vcf/aggregated.evs.vcf.gz";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

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
        File input = getResource(inputFilePath);

        AggregatedVcfReader vcfReader = new AggregatedVcfReader(FILE_ID, STUDY_ID, aggregationType, null,
                input);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(input, vcfReader);
    }

    @Test
    public void testUncompressedVcf() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // uncompress the input VCF into a temporal file
        File input = getResource(INPUT_FILE_PATH);
        File tempFile = temporaryFolderRule.newFile();
        JobTestUtils.uncompress(input.getAbsolutePath(), tempFile);

        AggregatedVcfReader vcfReader = new AggregatedVcfReader(FILE_ID, STUDY_ID, VariantSource.Aggregation.BASIC,
                null, tempFile);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(input, vcfReader);
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
