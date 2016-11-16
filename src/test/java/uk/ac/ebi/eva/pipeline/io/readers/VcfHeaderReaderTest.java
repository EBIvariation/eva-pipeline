package uk.ac.ebi.eva.pipeline.io.readers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.checkFieldsInsideList;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.checkStringInsideList;

/**
 * {@link VcfHeaderReader}
 * <p>
 * input: a Vcf file and some parameters configuring the VariantSource
 * <p>
 * output: a VariantSource when its `.read()` is called
 */
public class VcfHeaderReaderTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testRead() throws Exception {
        final String inputFilePath = "/small20.vcf.gz";
        String inputFile = VcfHeaderReaderTest.class.getResource(inputFilePath).getFile();

        String fileId = "5";
        String studyId = "7";
        String studyName = "study name";
        VariantStudy.StudyType studyType = VariantStudy.StudyType.COLLECTION;
        VariantSource.Aggregation aggregation = VariantSource.Aggregation.NONE;

        VcfHeaderReader headerReader = new VcfHeaderReader(new File(inputFile), fileId, studyId, studyName,
                                                           studyType, aggregation);
        VariantSource source = headerReader.read();

        assertEquals(fileId, source.getFileId());
        assertEquals(studyId, source.getStudyId());
        assertEquals(studyName, source.getStudyName());
        assertEquals(studyType, source.getType());
        assertEquals(aggregation, source.getAggregation());

        assertFalse(source.getSamples().isEmpty());
        assertFalse(source.getMetadata().isEmpty());
        assertTrue(source.getMetadata().containsKey(VcfHeaderReader.VARIANT_FILE_HEADER_KEY));
        assertFalse(((String) source.getMetadata().get(VcfHeaderReader.VARIANT_FILE_HEADER_KEY)).isEmpty());
        assertFalse(((Collection) source.getMetadata().get("INFO")).isEmpty());
        assertFalse(((Collection) source.getMetadata().get("FORMAT")).isEmpty());
        assertFalse(((Collection) source.getMetadata().get("FILTER")).isEmpty());
        assertFalse(((Collection) source.getMetadata().get("ALT")).isEmpty());
        assertFalse(((Collection) source.getMetadata().get("contig")).isEmpty());

    }

    /**
     * This test is intended to check that the metadata of the VariantSource is properly filled when using a VcfReader,
     * in a way that mostly maintains the structure we already have in mongo, in the files collection. See the doc in
     * {@link VcfHeaderReader#read()}
     *
     * @throws Exception
     */
    @Test
    public void testConversion() throws Exception {
        final String inputFilePath = "/small20.vcf.gz";
        String inputFile = VcfHeaderReaderTest.class.getResource(inputFilePath).getFile();

        String fileId = "5";
        String studyId = "7";
        String studyName = "study name";
        VcfHeaderReader headerReader = new VcfHeaderReader(new File(inputFile), fileId, studyId, studyName,
                                                           VariantStudy.StudyType.COLLECTION,
                                                           VariantSource.Aggregation.NONE);
        VariantSource source = headerReader.read();

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

    @Test
    public void testConversionAggregated() throws Exception {
        // uncompress the input VCF into a temporal file
        final String inputFilePath = "/aggregated.vcf.gz";
        String inputFile = AggregatedVcfReaderTest.class.getResource(inputFilePath).getFile();
        File tempFile = JobTestUtils.createTempFile();
        JobTestUtils.uncompress(inputFile, tempFile);

        String fileId = "5";
        String studyId = "7";
        String studyName = "study name";
        VcfHeaderReader headerReader = new VcfHeaderReader(new File(inputFile), fileId, studyId, studyName,
                                                           VariantStudy.StudyType.COLLECTION,
                                                           VariantSource.Aggregation.NONE);
        VariantSource source = headerReader.read();

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
        checkStringInsideList(metadataMongo, "contig");

        // the current aggregated.vcf.gz doesn't have FORMAT, FILTER or ALT tags
//        checkFieldsInsideList(metadataMongo, "FILTER", Arrays.asList("id", "description"));
//        checkFieldsInsideList(metadataMongo, "ALT", Arrays.asList("id", "description"));
//        checkFieldsInsideList(metadataMongo, "FORMAT", Arrays.asList("id", "description", "number", "type"));
    }

}
