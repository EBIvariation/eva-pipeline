package uk.ac.ebi.eva.test.utils;

import com.mongodb.BasicDBList;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.commons.models.converters.data.AnnotationFieldNames.CONSEQUENCE_TYPE_FIELD;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * GenotypedVcfJob test assertion functions and constants for testing.
 */
public class GenotypedVcfJobTestUtils {

    private static final String MOCK_VEP = "/mockvep.pl";

    public static final String INPUT_VCF_ID = "1";

    public static final String INPUT_STUDY_ID = "genotyped-job";

    private static final String INPUT_FILE = "/input-files/vcf/genotyped.vcf.gz";

    public static final String COLLECTION_FILES_NAME = "files";

    public static final String COLLECTION_VARIANTS_NAME = "variants";

    public static final String COLLECTION_ANNOTATIONS_NAME = "annotations";

    public static final String COLLECTION_ANNOTATION_METADATA_NAME = "annotationMetadata";

    private static final int EXPECTED_ANNOTATIONS = 537;

    private static final int EXPECTED_VARIANTS = 300;

    private static final int EXPECTED_VALID_ANNOTATIONS = 536;

    public static VariantDBIterator getVariantDBIterator(String dbName) throws IllegalAccessException,
            ClassNotFoundException, InstantiationException, StorageManagerException {
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        return variantDBAdaptor.iterator(new QueryOptions());
    }

    public static DBCursor getAnnotationDBCursor(TemporaryMongoRule mongoRule, String databaseName){
        return mongoRule.getCollection(databaseName, COLLECTION_ANNOTATIONS_NAME).find();
    }

    /**
     * 4 annotation flow annotation input vep generate step
     *
     * @param vepInputFile
     * @throws IOException
     */
    public static void checkAnnotationInput(File vepInputFile) throws IOException {
        BufferedReader testReader = new BufferedReader(new InputStreamReader(new FileInputStream(
                getResource("/expected-output/preannot.sorted"))));
        BufferedReader actualReader = new BufferedReader(new InputStreamReader(new FileInputStream(
                vepInputFile.toString())));

        ArrayList<String> rows = new ArrayList<>();

        String s;
        while ((s = actualReader.readLine()) != null) {
            rows.add(s);
        }
        Collections.sort(rows);

        String testLine = testReader.readLine();
        for (String row : rows) {
            assertEquals(testLine, row);
            testLine = testReader.readLine();
        }
        assertNull(testLine); // if both files have the same length testReader should be after the last line
    }


    /**
     * Annotation load step: check documents in DB have annotation (only consequence type)
     */
    public static void checkLoadedAnnotation(TemporaryMongoRule mongoRule, String databaseName) {
        DBCursor cursor = getAnnotationDBCursor(mongoRule, databaseName);

        int count = 0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            count++;
            DBObject annotation = cursor.next();
            BasicDBList consequenceTypes = (BasicDBList) annotation.get(CONSEQUENCE_TYPE_FIELD);
            assertNotNull(consequenceTypes);
            consequenceTypeCount += consequenceTypes.size();
        }

        assertTrue(count > 0);
        assertEquals(EXPECTED_VALID_ANNOTATIONS, consequenceTypeCount);

    }

    public static void checkOutputFileLength(File vepOutputFile) throws IOException {
        assertEquals(EXPECTED_ANNOTATIONS, getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
    }

    public static void checkAnnotationCreateStep(File vepOutputFile) {
        assertTrue(vepOutputFile.exists());
    }

    /**
     * load stats step: check the DB docs have the field "st"
     *
     * @param dbName
     */
    public static void checkLoadStatsStep(String dbName) throws ClassNotFoundException, StorageManagerException,
            InstantiationException, IllegalAccessException {
        VariantDBIterator iterator = GenotypedVcfJobTestUtils.getVariantDBIterator(dbName);
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());
    }

    /**
     * 1 load step: check ((documents in DB) == (lines in transformed file))
     * variantStorageManager = StorageManagerFactory.getVariantStorageManager();
     * variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
     *
     * @param dbName
     */
    public static void checkLoadStep(String dbName) throws ClassNotFoundException, StorageManagerException,
            InstantiationException, IllegalAccessException {
        VariantDBIterator iterator = GenotypedVcfJobTestUtils.getVariantDBIterator(dbName);
        assertEquals(EXPECTED_VARIANTS, count(iterator));
    }

    /**
     * 2 create stats step
     *
     * @param variantsStatsFile
     * @param sourceStatsFile
     */
    public static void checkCreateStatsStep(File variantsStatsFile, File sourceStatsFile) {
        assertTrue(variantsStatsFile.exists());
        assertTrue(sourceStatsFile.exists());
    }

    public static void checkSkippedOneMalformedLine(JobExecution jobExecution) {
        //check that one line is skipped because malformed
        List<StepExecution> variantAnnotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals(BeanNames.LOAD_VEP_ANNOTATION_STEP))
                .collect(Collectors.toList());
        assertEquals(1, variantAnnotationLoadStepExecution.get(0).getReadSkipCount());
    }

    public static File getVariantsStatsFile(String outputDirStats) throws URISyntaxException {
        return new File(URLHelper.getVariantsStatsUri(outputDirStats, INPUT_STUDY_ID, INPUT_VCF_ID));
    }

    public static File getSourceStatsFile(String outputDirStats) throws URISyntaxException {
        return new File(URLHelper.getSourceStatsUri(outputDirStats, INPUT_STUDY_ID, INPUT_VCF_ID));
    }

    public static File getVepOutputFile(String outputDirAnnotation) {
        return new File(URLHelper.resolveVepOutput(outputDirAnnotation, INPUT_STUDY_ID, INPUT_VCF_ID));
    }

    public static File getInputFile() {
        return getResource(INPUT_FILE);
    }

    public static File getMockVep() {
        return getResource(MOCK_VEP);
    }
}
