package uk.ac.ebi.eva.pipeline.io.readers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
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
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * {@link VcfHeaderReader}
 * <p>
 * input: a Vcf file and some parameters configuring the VariantSource
 * <p>
 * output: a VariantSource when its `.read()` is called
 */
public class VcfHeaderReaderTest {

    private static final String INPUT_FILE_PATH = "/input-files/vcf/genotyped.vcf.gz";

    private static final String FILE_ID = "5";

    private static final String STUDY_ID = "7";

    private static final String STUDY_NAME = "study name";

    private static final String INPUT_AGGREGATED_FILE_PATH = "/input-files/vcf/aggregated.vcf.gz";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Test
    public void testRead() throws Exception {
        File input = getResource(INPUT_FILE_PATH);

        VariantStudy.StudyType studyType = VariantStudy.StudyType.COLLECTION;
        VariantSource.Aggregation aggregation = VariantSource.Aggregation.NONE;

        VcfHeaderReader headerReader = new VcfHeaderReader(input, FILE_ID, STUDY_ID, STUDY_NAME,
                studyType, aggregation);
        headerReader.open(null);
        VariantSourceEntity source = headerReader.read();

        assertEquals(FILE_ID, source.getFileId());
        assertEquals(STUDY_ID, source.getStudyId());
        assertEquals(STUDY_NAME, source.getStudyName());
        assertEquals(studyType, source.getType());
        assertEquals(aggregation, source.getAggregation());

        assertFalse(source.getSamplesPosition().isEmpty());
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
        File input = getResource(INPUT_FILE_PATH);

        VcfHeaderReader headerReader = new VcfHeaderReader(input, FILE_ID, STUDY_ID, STUDY_NAME,
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);
        headerReader.open(null);
        VariantSourceEntity source = headerReader.read();

        Map<String, Object> meta = source.getMetadata();
        BasicDBObject metadataMongo = mapMetadataToDBObject(meta);

        checkFieldsInsideList(metadataMongo, "INFO", Arrays.asList("id", "description", "number", "type"));
        checkFieldsInsideList(metadataMongo, "FORMAT", Arrays.asList("id", "description", "number", "type"));
        checkFieldsInsideList(metadataMongo, "ALT", Arrays.asList("id", "description"));
        checkFieldsInsideList(metadataMongo, "FILTER", Arrays.asList("id", "description"));
        checkStringInsideList(metadataMongo, "contig");
    }

    @Test
    public void testConversionAggregated() throws Exception {
        // uncompress the input VCF into a temporal file
        File input = getResource(INPUT_AGGREGATED_FILE_PATH);
        File tempFile = temporaryFolderRule.newFile();
        JobTestUtils.uncompress(input.getAbsolutePath(), tempFile);

        VcfHeaderReader headerReader = new VcfHeaderReader(input, FILE_ID, STUDY_ID, STUDY_NAME,
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);
        headerReader.open(null);
        VariantSourceEntity source = headerReader.read();

        Map<String, Object> meta = source.getMetadata();
        BasicDBObject metadataMongo = mapMetadataToDBObject(meta);

        checkFieldsInsideList(metadataMongo, "INFO", Arrays.asList("id", "description", "number", "type"));
        checkStringInsideList(metadataMongo, "contig");
    }

    private BasicDBObject mapMetadataToDBObject(Map<String, Object> meta) throws JsonProcessingException {
        char CHARACTER_TO_REPLACE_DOTS = (char) 163;
        BasicDBObject metadataMongo = new BasicDBObject();
        for (Map.Entry<String, Object> metaEntry : meta.entrySet()) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectWriter writer = mapper.writer();
            String key = metaEntry.getKey().replace('.', CHARACTER_TO_REPLACE_DOTS);
            Object value = metaEntry.getValue();
            String jsonString = writer.writeValueAsString(value);
            metadataMongo.append(key, JSON.parse(jsonString));
        }
        return metadataMongo;
    }

}
