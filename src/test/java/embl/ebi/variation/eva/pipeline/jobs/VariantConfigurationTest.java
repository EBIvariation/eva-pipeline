package embl.ebi.variation.eva.pipeline.jobs;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.countRows;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getLines;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getTransformedOutputPath;
import static org.junit.Assert.*;

/**
 * Created by diego on 23/05/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantConfiguration.class, VariantConfigurationTest.Configs.class})
public class VariantConfigurationTest {

    public static final String FILE_20 = "/small20.vcf.gz";
    public static final String FILE_22 = "/small22.vcf.gz";
    public static final String FILE_WRONG_NO_ALT = "/wrong_no_alt.vcf.gz";

    // iterable doing an enum. Does it worth it? yes
    private static final String VALID_TRANSFORM = "VariantConfigurationTest_vt";
    private static final String INVALID_TRANSFORM = "VariantConfigurationTest_it";
    private static final String VALID_LOAD = "VariantConfigurationTest_vl";
    //    private static final String INVALID_LOAD = "invalidLoad";
    private static final String VALID_CREATE_STATS = "VariantConfigurationTest_vcs";
    //    private static final String INVALID_CREATE_STATS = "invalidCreateStats";
    private static final String VALID_LOAD_STATS = "VariantConfigurationTest_vls";
    //    private static final String INVALID_LOAD_STATS = "invalidLoadStats";
    private static final String VALID_PRE_ANNOT = "VariantConfigurationTest_vpa";
    private static final String VALID_ANNOT = "VariantConfigurationTest_va";
    private static final String VALID_ANNOT_LOAD = "VariantConfigurationTest_val";

    @Autowired
    PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @Autowired
    public ObjectMap variantOptions;

    @Autowired
    public ObjectMap pipelineOptions;

    @Configuration
    static class Configs {
        private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

        @Bean
        static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
            PropertySourcesPlaceholderConfigurer configurer = new PropertySourcesPlaceholderConfigurer();

            Properties properties = new Properties();
            properties.put("input", "");
            properties.put("overwriteStats", "false");
            properties.put("calculateStats", "false");
            properties.put("outputDir", "/tmp");
            properties.put("dbName", "");
            properties.put("compressExtension", ".gz");
            properties.put("compressGenotypes", "true");
            properties.put("includeSrc", "FIRST_8_COLUMNS");
            properties.put("pedigree", "FIRST_8_COLUMNS");
            properties.put("annotate", "false");
            properties.put("includeSamples", "false");
            properties.put("includeStats", "false");
            properties.put("aggregated", "NONE");
            properties.put("studyType", "COLLECTION");
            properties.put("studyName", "studyName");
            properties.put("studyId", "1");
            properties.put("fileId", "1");
            properties.put("opencga.app.home", opencgaHome);
            properties.put("skipLoad", "true");
            properties.put("skipStatsCreate", "true");
            properties.put("skipStatsLoad", "true");
            properties.put("skipAnnotGenerateInput", "true");
            properties.put("skipAnnotCreate", "true");
            properties.put("skipAnnotLoad", "true");
            properties.put("vepInput", "");
            properties.put("vepOutput", "");
            properties.put("vepPath", "");
            properties.put("vepCacheDirectory", "");
            properties.put("vepCacheVersion", "");
            properties.put("vepSpecies", "");
            properties.put("vepFasta", "");
            properties.put("vepNumForks", "3");

            configurer.setProperties(properties);

            return configurer;
        }
    }

    @Test
    public void validTransform() throws Exception {
        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        String dbName = VALID_TRANSFORM;

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);

        //// TODO: 07/06/2016 move this in a method
        VariantSource source = (VariantSource) variantOptions.get(VariantStorageManager.VARIANT_SOURCE);

        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                input,
                source.getFileId(),
                source.getStudyId(),
                source.getStudyName(),
                source.getType(),
                source.getAggregation()));

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_20).getFileName(), ".gz", "/tmp");

        File file = new File(outputFilename);
        file.delete();
        assertFalse(file.exists());

        JobExecution execution = jobLauncher.run(job, new JobParameters());

        assertEquals(input, pipelineOptions.getString("input"));
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());

        ////////// check transformed file
        long lines = getLines(new GZIPInputStream(new FileInputStream(outputFilename)));
        assertEquals(300, lines);
    }

    /**
     * This test has to fail because the vcf FILE_WRONG_NO_ALT is malformed, in
     * a variant has an empty alternate allele
     */
    @Test
    public void invalidTransform() throws JobExecutionException {
        String input = VariantConfigurationTest.class.getResource(FILE_WRONG_NO_ALT).getFile();
        String dbName = INVALID_TRANSFORM;

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);

        VariantSource source = (VariantSource) variantOptions.get(VariantStorageManager.VARIANT_SOURCE);

        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                input,
                source.getFileId(),
                source.getStudyId(),
                source.getStudyName(),
                source.getType(),
                source.getAggregation()));

        JobExecution execution = jobLauncher.run(job, new JobParameters());

        assertEquals(input, pipelineOptions.getString("input"));
        assertEquals(ExitStatus.FAILED.getExitCode(), execution.getExitStatus().getExitCode());
    }

/*    @Test
    public void validLoad() throws JobExecutionException, IllegalAccessException, ClassNotFoundException,
            InstantiationException, IOException, StorageManagerException {

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        String dbName = VALID_LOAD;

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put("skipLoad", false);

        VariantSource source = (VariantSource) variantOptions.get(VariantStorageManager.VARIANT_SOURCE);

        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                input,
                source.getFileId(),
                source.getStudyId(),
                source.getStudyName(),
                source.getType(),
                source.getAggregation()));

        JobExecution execution = jobLauncher.run(job, new JobParameters());

        assertEquals(input, pipelineOptions.getString("input"));
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());

        // check ((documents in DB) == (lines in transformed file))
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_20).getFileName(),
                variantOptions.getString("compressExtension"), pipelineOptions.getString("outputDir"));
        long lines = getLines(new GZIPInputStream(new FileInputStream(outputFilename)));

        assertEquals(countRows(iterator), lines);
    }*/

    @Test
    public void validCreateStats() throws JobExecutionException, IOException, InterruptedException,
            IllegalAccessException, ClassNotFoundException, InstantiationException, StorageManagerException {

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        String dbName = VALID_CREATE_STATS;
        String outputDir = "/tmp";
        VariantSource source = new VariantSource(input, "1", "1", "studyName");

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put("skipLoad", false);
        pipelineOptions.put("skipStatsCreate", false);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        File statsFile = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source))
                + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        JobExecution execution = jobLauncher.run(job, new JobParameters());

        assertEquals(input, pipelineOptions.getString("input"));
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());
        assertTrue(statsFile.exists());
    }

/*    @Test
    public void validLoadStats() throws JobExecutionException, IOException, IllegalAccessException,
            ClassNotFoundException, InstantiationException, StorageManagerException {

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        VariantSource source = new VariantSource(input, "1", "1", "studyName");

        String dbName = VALID_LOAD_STATS;
        String outputDir = "/tmp";

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put("skipLoad", false);
        pipelineOptions.put("skipStatsCreate", false);
        pipelineOptions.put("skipStatsLoad", false);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        File statsFile = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source)) + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions
        JobExecution execution = jobLauncher.run(job, new JobParameters());

        assertEquals(input, pipelineOptions.getString("input"));
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());
        assertTrue(statsFile.exists());

        // check ((documents in DB) == (lines in transformed file))
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_20).getFileName(),
                variantOptions.getString("compressExtension"), pipelineOptions.getString("outputDir"));
        long lines = getLines(new GZIPInputStream(new FileInputStream(outputFilename)));

        assertEquals(countRows(iterator), lines);

        // check the DB docs have the field "st"
        variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        iterator = variantDBAdaptor.iterator(new QueryOptions());

        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());
    }*/

    @Test
    public void validAnnotGenerateInput() throws Exception {

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        VariantSource source = new VariantSource(input, "annotTest", "1", "studyName");
        String dbName = VALID_PRE_ANNOT;
        String outputDir = "/tmp";
        File annotFile = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source)) + ".variants.preannot.gz");

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put("skipLoad", false);
        pipelineOptions.put("skipAnnotGenerateInput", false);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
        pipelineOptions.put("vepInput", annotFile.toString());

        annotFile.delete();
        assertFalse(annotFile.exists());  // ensure the stats file doesn't exist from previous executions
        JobExecution execution = jobLauncher.run(job, new JobParameters());
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());
        assertTrue(annotFile.exists());

        // compare files
        BufferedReader testReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(
                VariantConfigurationTest.class.getResource("/preannot.sorted.gz").getFile()))));
        BufferedReader actualReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(
                annotFile.toString()))));

        ArrayList<String> rows = new ArrayList<>();

        String s;
        while((s = actualReader.readLine()) != null) {
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
     * This test uses a mock of VEP, that takes every line and appends a string " annotated". The idea is to test only
     * that the pipes are working as expected.
     * @throws Exception
     */
    @Test
    public void validAnnotCreate() throws Exception {

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        VariantSource source = new VariantSource(input, "annotTest", "1", "studyName");

        String dbName = VALID_ANNOT;
        String outputDir = "/tmp";
        File vepInput = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source)) + ".variants.preannot.gz");
        File vepOutput = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source)) + ".variants.annot.gz");
        String mockVep = VariantConfigurationTest.class.getResource("/mockvep.pl").getFile();

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put("skipLoad", false);
        pipelineOptions.put("skipAnnotGenerateInput", false);
        pipelineOptions.put("skipAnnotCreate", false);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        pipelineOptions.put("vepInput", vepInput.toString());
        pipelineOptions.put("vepPath", mockVep);
        pipelineOptions.put("vepCacheVersion", "79");
        pipelineOptions.put("vepCacheDirectory", "/path/to/cache");
        pipelineOptions.put("vepSpecies", "homo_sapiens");
        pipelineOptions.put("vepFasta", "/path/to/file.fa");
        pipelineOptions.put("vepNumForks", "4");
        pipelineOptions.put("vepOutput", vepOutput.toString());

        vepInput.delete();
        vepOutput.delete();
        assertFalse(vepInput.exists());  // ensure the pre annot file doesn't exist from previous executions
        assertFalse(vepOutput.exists());  // ensure the annot file doesn't exist from previous executions
        JobExecution execution = jobLauncher.run(job, new JobParameters());
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());
        assertTrue(vepInput.exists());
        assertTrue(vepOutput.exists());

        BufferedReader outputReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(vepOutput))));

        // Check output file contents
        int numLinesRead;
        String outputLine = outputReader.readLine();
        for (numLinesRead = 0; outputLine != null; numLinesRead++) {
            assertEquals(numLinesRead + " annotated", outputLine);
            outputLine = outputReader.readLine();
        }

        // Check output file length
        assertEquals(getLines(new GZIPInputStream(new FileInputStream(vepInput))),
                getLines(new GZIPInputStream(new FileInputStream(vepOutput))));
    }

    /**
     * TODO findout how to call cellbase annotator
     * @throws Exception
     */
    @Test
    public void testAnnotLoad() throws Exception {
        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        VariantSource source = new VariantSource(input, "annotTest", "1", "studyName");
        String dbName = VALID_ANNOT_LOAD;
        String vepOutput = VariantConfigurationTest.class.getResource("/annot.tsv.gz").getFile();

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put("skipAnnotLoad", false);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
        pipelineOptions.put("vepOutput", vepOutput.toString());

        JobExecution execution = jobLauncher.run(job, new JobParameters());
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());

        // check documents in DB have annotation (only consequence type)
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        while (iterator.hasNext()) {
            Variant next = iterator.next();
            assertTrue(next.getAnnotation().getConsequenceTypes() != null);
        }
    }

    @BeforeClass
    public static void beforeTests() throws UnknownHostException {
        cleanDBs();
    }

    @AfterClass
    public static void afterTests() throws UnknownHostException {
        cleanDBs();
    }

    private static void cleanDBs() throws UnknownHostException {
        JobTestUtils.cleanDBs(
                VALID_TRANSFORM,
                INVALID_TRANSFORM,
                VALID_LOAD,
                VALID_CREATE_STATS,
                VALID_LOAD_STATS,
                VALID_PRE_ANNOT,
                VALID_ANNOT,
                VALID_ANNOT_LOAD
        );
    }
}