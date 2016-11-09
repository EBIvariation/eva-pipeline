package uk.ac.ebi.eva.pipeline.io.readers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link VcfReader}
 * input: a Vcf file
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
        VariantSource source = new VariantSource(input, fileId, studyId, studyName, VariantStudy.StudyType.COLLECTION,
                                                 VariantSource.Aggregation.NONE);

        VcfReader vcfReader = new VcfReader(source, input);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(input, source, vcfReader);
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
        VariantSource source = new VariantSource(inputFile, fileId, studyId, studyName,
                                                 VariantStudy.StudyType.COLLECTION, VariantSource.Aggregation.NONE);

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
        VariantSource source = new VariantSource(tempFile.getAbsolutePath(), fileId, studyId, studyName,
                                                 VariantStudy.StudyType.COLLECTION, VariantSource.Aggregation.NONE);

        VcfReader vcfReader = new VcfReader(source, tempFile);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(inputFile, source, vcfReader);
    }

    /**
     * This test is intended to check that the metadata of the VariantSource is properly filled when using a VcfReader,
     * in a way that mostly maintains the structure we already have in mongo, in the files collection. See the doc in
     * {@link VcfReader#prepareVariantSource()}
     * @throws Exception
     */
    @Test
    public void testConversion() throws Exception {

        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // uncompress the input VCF into a temporal file
        final String inputFilePath = "/small20.vcf.gz";
        String inputFile = VcfReaderTest.class.getResource(inputFilePath).getFile();
        File tempFile = JobTestUtils.createTempFile();
        JobTestUtils.uncompress(inputFile, tempFile);

        String fileId = "5";
        String studyId = "7";
        String studyName = "study name";
        VariantSource source = new VariantSource(tempFile.getAbsolutePath(), fileId, studyId, studyName,
                                                 VariantStudy.StudyType.COLLECTION, VariantSource.Aggregation.NONE);

        VcfReader vcfReader = new VcfReader(source, tempFile);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(inputFile, source, vcfReader);

        char CHARACTER_TO_REPLACE_DOTS = (char) 163;
        Map<String, Object> meta = source.getMetadata();
        BasicDBObject metadataMongo = new BasicDBObject();
        for (Map.Entry<String, Object> metaEntry : meta.entrySet()) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectWriter writer = mapper.writer();
            String key = metaEntry.getKey().replace('.', CHARACTER_TO_REPLACE_DOTS);
            Object value = metaEntry.getValue();
            String jsonString = writer.writeValueAsString(value);
            metadataMongo.append(key, JSON.parse(jsonString));
        }
        checkFieldsInsideList(metadataMongo, "INFO", Arrays.asList("id", "description", "number", "type"));
        checkFieldsInsideList(metadataMongo, "FORMAT", Arrays.asList("id", "description", "number", "type"));
        checkFieldsInsideList(metadataMongo, "ALT", Arrays.asList("id", "description"));
        checkFieldsInsideList(metadataMongo, "FILTER", Arrays.asList("id", "description"));
        checkStringInsideList(metadataMongo, "contig");
    }

    private void checkStringInsideList(BasicDBObject metadataMongo, String field) {
        assertTrue(metadataMongo.containsField(field));
        Object objectList = metadataMongo.get(field);
        assertTrue(objectList instanceof BasicDBList);
        BasicDBList list = (BasicDBList) objectList;
        for (Object element : list) {
            assertTrue(element instanceof String);
            assertNotNull(element);
            assertFalse(element.toString().isEmpty());
        }
    }

    private void checkFieldsInsideList(BasicDBObject metadataMongo, String field, List<String> innerFields) {
        assertTrue(metadataMongo.containsField(field));
        Object objectList = metadataMongo.get(field);
        assertTrue(objectList instanceof BasicDBList);
        BasicDBList list = (BasicDBList) objectList;
        for (Object element : list) {
            assertTrue(element instanceof BasicDBObject);
            for (String innerField : innerFields) {
                assertNotNull(((BasicDBObject) element).get(innerField));
                assertFalse(((BasicDBObject) element).get(innerField).toString().isEmpty());
            }
        }
    }

    private void consumeReader(String inputFile, VariantSource source, VcfReader vcfReader) throws Exception {
        List<Variant> variants;
        int count = 0;

        // consume the reader and check that the variants and the VariantSource have meaningful data
        while ((variants = vcfReader.read()) != null) {
            assertTrue(variants.size() > 0);
            assertTrue(variants.get(0).getSourceEntries().size() > 0);
            VariantSourceEntry sourceEntry = variants.get(0).getSourceEntries().entrySet().iterator().next().getValue();
            assertTrue(sourceEntry.getSamplesData().size() > 0);

            assertTrue(source.getMetadata().containsKey(VcfReader.VARIANT_FILE_HEADER_KEY));

            count++;
        }

        // VcfReader should get all the lines from the file
        long expectedCount = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(inputFile)));
        assertEquals(expectedCount, count);
    }
}
