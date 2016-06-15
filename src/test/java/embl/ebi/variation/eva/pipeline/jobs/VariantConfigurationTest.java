/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.VariantJobsArgs;
import embl.ebi.variation.eva.pipeline.steps.*;
import org.junit.AfterClass;
import org.junit.Before;
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
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.*;
import static org.junit.Assert.*;

/**
 * Created by jmmut on 2015-10-14.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantConfiguration.class, CommonConfig.class})
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

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;

    @Autowired
    public VariantJobsArgs variantJobsArgs;

    @Test
    public void validTransform() throws Exception {
        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        String dbName = VALID_TRANSFORM;

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

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_20).getFileName(), ".gz", "/tmp");

        File file = new File(outputFilename);
        file.delete();
        assertFalse(file.exists());

        JobExecution execution = jobLauncher.run(job, getJobParameters());

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

        JobExecution execution = jobLauncher.run(job, getJobParameters());

        assertEquals(input, pipelineOptions.getString("input"));
        assertEquals(ExitStatus.FAILED.getExitCode(), execution.getExitStatus().getExitCode());
    }

    // TODO: 15/06/2016 double check if this is a duplicate of:
    // embl.ebi.variation.eva.pipeline.jobs.VariantLoadConfigurationTest.validLoad
    @Test
    public void validLoad() throws JobExecutionException, IllegalAccessException, ClassNotFoundException,
            InstantiationException, IOException, StorageManagerException {

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        String dbName = VALID_LOAD;

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);

        VariantSource source = (VariantSource) variantOptions.get(VariantStorageManager.VARIANT_SOURCE);

        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                input,
                source.getFileId(),
                source.getStudyId(),
                source.getStudyName(),
                source.getType(),
                source.getAggregation()));

        JobExecution execution = jobLauncher.run(job, getJobParameters());

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
    }

    @Test
    public void validCreateStats() throws JobExecutionException, IOException, InterruptedException,
            IllegalAccessException, ClassNotFoundException, InstantiationException, StorageManagerException {

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        String dbName = VALID_CREATE_STATS;
        String outputDir = "/tmp";
        VariantSource source = new VariantSource(input, "1", "1", "studyName");

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);
        pipelineOptions.put(VariantsStatsCreate.SKIP_STATS_CREATE, false);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        File statsFile = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source))
                + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        JobExecution execution = jobLauncher.run(job, getJobParameters());

        assertEquals(input, pipelineOptions.getString("input"));
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());
        assertTrue(statsFile.exists());
    }

    //// TODO: 15/06/2016 double check if this is a duplicate of:
    // embl.ebi.variation.eva.pipeline.jobs.VariantStatsConfigurationTest.validLoadStats()
    @Test
    public void validLoadStats() throws JobExecutionException, IOException, IllegalAccessException,
            ClassNotFoundException, InstantiationException, StorageManagerException {

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        VariantSource source = new VariantSource(input, "1", "1", "studyName");

        String dbName = VALID_LOAD_STATS;
        String outputDir = "/tmp";

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);
        pipelineOptions.put(VariantsStatsCreate.SKIP_STATS_CREATE, false);
        pipelineOptions.put(VariantsStatsLoad.SKIP_STATS_LOAD, false);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        File statsFile = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source)) + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        JobExecution execution = jobLauncher.run(job, getJobParameters());

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
    }

    @Test
    public void validAnnotGenerateInput() throws Exception {

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        VariantSource source = new VariantSource(input, "annotTest", "1", "studyName");
        String dbName = VALID_PRE_ANNOT;
        String outputDir = "/tmp";
        File annotFile = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source)) + ".variants.preannot.gz");

        pipelineOptions.put("input", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);
        pipelineOptions.put(VariantsAnnotGenerateInput.SKIP_ANNOT_GENERATE_INPUT, false);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
        pipelineOptions.put("vepInput", annotFile.toString());

        annotFile.delete();
        assertFalse(annotFile.exists());  // ensure the stats file doesn't exist from previous executions
        JobExecution execution = jobLauncher.run(job, getJobParameters());
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
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);
        pipelineOptions.put(VariantsAnnotGenerateInput.SKIP_ANNOT_GENERATE_INPUT, false);
        pipelineOptions.put(VariantsAnnotCreate.SKIP_ANNOT_CREATE, false);
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

        JobExecution execution = jobLauncher.run(job, getJobParameters());

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
        pipelineOptions.put(VariantsAnnotLoad.SKIP_ANNOT_LOAD, false);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false); //this is needed because we need the DB to load the annotations to
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
        pipelineOptions.put("vepOutput", vepOutput.toString());

        JobExecution execution = jobLauncher.run(job, getJobParameters());

        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());

        // check documents in DB have annotation (only consequence type)
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        int cnt=0;
        while (iterator.hasNext()) {
            cnt++;
            Variant next = iterator.next();
            assertTrue(next.getAnnotation().getConsequenceTypes() != null);
        }
        assertTrue(cnt>0);
    }

    @BeforeClass
    public static void beforeTests() throws UnknownHostException {
        cleanDBs();
    }

    @Before
    public void setUp() throws Exception {
        //re-initialize common config before each test
        variantJobsArgs.loadArgs();
        pipelineOptions = variantJobsArgs.getPipelineOptions();
        variantOptions = variantJobsArgs.getVariantOptions();
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