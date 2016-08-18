/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.VariantJobsArgs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.*;
import static org.junit.Assert.*;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantConfiguration}
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantConfiguration.class, VariantConfig.class})
public class VariantConfigurationTest {

    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired public VariantJobsArgs variantJobsArgs;

    @Autowired private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("variantJob")
    public Job job;

    private String inputFileResouce;
    private String outputDir;
    private String compressExtension;
    private String dbName;
    private String vepInput;
    private String vepOutput;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void fullVariantConfig() throws Exception {

        String inputFile = VariantConfigurationTest.class.getResource(inputFileResouce).getFile();
        String mockVep = VariantConfigurationTest.class.getResource("/mockvep.pl").getFile();

        variantJobsArgs.getPipelineOptions().put("input.vcf", inputFile);
        variantJobsArgs.getPipelineOptions().put("app.vep.path", mockVep);

        Config.setOpenCGAHome(opencgaHome);

        // transformedVcf file init
        String transformedVcf = outputDir + inputFileResouce + ".variants.json" + compressExtension;
        File transformedVcfFile = new File(transformedVcf);
        transformedVcfFile.delete();
        assertFalse(transformedVcfFile.exists());

        //stats file init
        VariantSource source = (VariantSource) variantJobsArgs.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE);
        File statsFile = new File(Paths.get(outputDir).resolve(VariantStorageManager.buildFilename(source))
                + ".variants.stats.json.gz");
        statsFile.delete();
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        // annotation files init
        File vepInputFile = new File(vepInput);
        vepInputFile.delete();
        assertFalse(vepInputFile.exists());

        File vepOutputFile = new File(vepOutput);
        vepOutputFile.delete();
        assertFalse(vepOutputFile.exists());

        VariantDBIterator iterator;

        // Run the Job
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // 1 transform step: check transformed file
        long transformedLinesCount = getLines(new GZIPInputStream(new FileInputStream(transformedVcf)));
        assertEquals(300, transformedLinesCount);

        // 2 load step: check ((documents in DB) == (lines in transformed file))
        //variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        //variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        iterator = getVariantDBIterator();
        assertEquals(transformedLinesCount, countRows(iterator));

        // 3 create stats step
        assertTrue(statsFile.exists());

        // 4 load stats step: check ((documents in DB) == (lines in transformed file))
        //variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        //variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        iterator = getVariantDBIterator();
        assertEquals(transformedLinesCount, countRows(iterator));

        // check the DB docs have the field "st"
        iterator = getVariantDBIterator();

        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

        // 5 annotation flow
        // annotation input vep generate step
        BufferedReader testReader = new BufferedReader(new InputStreamReader(new FileInputStream(
                VariantConfigurationTest.class.getResource("/preannot.sorted").getFile())));
        BufferedReader actualReader = new BufferedReader(new InputStreamReader(new FileInputStream(
                vepInputFile.toString())));

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

        // 6 annotation create step
        assertTrue(vepInputFile.exists());
        assertTrue(vepOutputFile.exists());

        // Check output file length
        assertEquals(537, getLines(new GZIPInputStream(new FileInputStream(vepOutput))) );

        // 8 Annotation load step: check documents in DB have annotation (only consequence type)
        //variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        //variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        iterator = getVariantDBIterator();

        int cnt=0;
        while (iterator.hasNext()) {
            cnt++;
            Variant next = iterator.next();
            assertTrue(next.getAnnotation().getConsequenceTypes() != null);
        }
        assertTrue(cnt>0);

    }

    /**
     * JobLauncherTestUtils is initialized here because in VariantConfiguration there are two Job beans
     * in this way it is possible to specify the Job to run (and avoid NoUniqueBeanDefinitionException)
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
        jobLauncherTestUtils = new JobLauncherTestUtils();
        jobLauncherTestUtils.setJob(job);
        jobLauncherTestUtils.setJobLauncher(jobLauncher);

        inputFileResouce = variantJobsArgs.getPipelineOptions().getString("input.vcf");
        outputDir = variantJobsArgs.getPipelineOptions().getString("output.dir");
        compressExtension = variantJobsArgs.getPipelineOptions().getString("compressExtension");
        dbName = variantJobsArgs.getPipelineOptions().getString("dbName");
        vepInput = variantJobsArgs.getPipelineOptions().getString("vep.input");
        vepOutput = variantJobsArgs.getPipelineOptions().getString("vep.output");
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
    }

    private VariantDBIterator getVariantDBIterator() throws IllegalAccessException,
            ClassNotFoundException, InstantiationException, StorageManagerException {
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        return variantDBAdaptor.iterator(new QueryOptions());
    }

}
