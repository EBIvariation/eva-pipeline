package uk.ac.ebi.eva.test.utils;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.commons.models.mongo.entity.Annotation.CONSEQUENCE_TYPE_FIELD;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * GenotypedVcfJob test assertion functions and constants for testing.
 */
public class GenotypedVcfJobTestUtils {

    private static final String MOCK_VEP = "/mockvep.pl";

    private static final String FAILING_MOCK_VEP = "/mockvep_writeToFile_error.pl";

    public static final String INPUT_VCF_ID = "1";

    public static final String INPUT_STUDY_ID = "genotyped-job";

    private static final String INPUT_FILE = "/input-files/vcf/genotyped.vcf.gz";

    private static final String INPUT_ASSEMBLY_REPORT = "/input-files/assembly-report/assembly_report.txt";

    public static final String COLLECTION_FILES_NAME = "files";

    public static final String COLLECTION_VARIANTS_NAME = "variants";

    public static final String COLLECTION_ANNOTATIONS_NAME = "annotations";

    public static final String COLLECTION_ANNOTATION_METADATA_NAME = "annotationMetadata";

    private static final int EXPECTED_ANNOTATIONS = 537;

    private static final int EXPECTED_VARIANTS = 300;

    private static final int EXPECTED_VALID_ANNOTATIONS = 536;

    public static MongoCursor<Document> getVariantDBCursor(TemporaryMongoRule mongoRule, String databaseName) {
        return mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).find().iterator();
    }

    public static MongoCursor<Document> getAnnotationDBCursor(TemporaryMongoRule mongoRule, String databaseName) {
        return mongoRule.getCollection(databaseName, COLLECTION_ANNOTATIONS_NAME).find().iterator();
    }

    /**
     * Annotation load step: check documents in DB have annotation (only consequence type)
     */
    public static void checkLoadedAnnotation(TemporaryMongoRule mongoRule, String databaseName) {
        MongoCursor<Document> cursor = getAnnotationDBCursor(mongoRule, databaseName);

        int count = 0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            count++;
            Document annotation = cursor.next();
            List<String> consequenceTypes = (List<String>) annotation.get(CONSEQUENCE_TYPE_FIELD);
            assertNotNull(consequenceTypes);
            consequenceTypeCount += consequenceTypes.size();
        }
        cursor.close();

        assertTrue(count > 0);
        assertEquals(EXPECTED_ANNOTATIONS, consequenceTypeCount);
    }

    public static void checkOutputFileLength(File vepOutputFile) throws IOException {
        assertEquals(EXPECTED_ANNOTATIONS, getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
    }

    public static void checkAnnotationCreateStep(File vepOutputFile) {
        assertTrue(vepOutputFile.exists());
    }

    /**
     * 1 load step: check ((documents in DB) == (lines in transformed file))
     * variantStorageManager = StorageManagerFactory.getVariantStorageManager();
     * variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
     */
    public static void checkLoadStep(TemporaryMongoRule mongoRule,
                                     String databaseName) throws ClassNotFoundException, StorageManagerException,
            InstantiationException, IllegalAccessException {
        MongoCursor<Document> iterator = getVariantDBCursor(mongoRule, databaseName);

        assertEquals(EXPECTED_VARIANTS, count(iterator));
        iterator.close();
    }


    public static void checkSkippedOneMalformedLine(JobExecution jobExecution) {
        //check that one line is skipped because malformed
        List<StepExecution> annotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals(BeanNames.LOAD_VEP_ANNOTATION_STEP))
                .collect(Collectors.toList());
        assertEquals(1, annotationLoadStepExecution.get(0).getReadSkipCount());
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

    public static String getAssemblyReport() {
        return "file://" + getResource(INPUT_ASSEMBLY_REPORT).getAbsolutePath();
    }

    public static File getMockVep() {
        return getResource(MOCK_VEP);
    }

    public static File getFailingMockVep() {
        return getResource(FAILING_MOCK_VEP);
    }

    public static Path createLinkToWorkingMockVep(String linkPathName) throws IOException {
        Path linkPath = Paths.get(linkPathName);
        Files.deleteIfExists(linkPath);
        return Files.createSymbolicLink(linkPath, getMockVep().toPath());
    }

    public static Path createLinkToFailingMockVep(String linkPathName) throws IOException {
        Path linkPath = Paths.get(linkPathName);
        Files.deleteIfExists(linkPath);
        return Files.createSymbolicLink(linkPath, getFailingMockVep().toPath());
    }

    public static String getDefaultOpencgaHome() {
        return System.getenv("OPENCGA_HOME") != null ?
                System.getenv("OPENCGA_HOME") :
                GenotypedVcfJobTestUtils.class.getResource("/opencga/").getFile();
    }
}
