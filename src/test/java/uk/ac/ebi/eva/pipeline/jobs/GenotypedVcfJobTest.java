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

package uk.ac.ebi.eva.pipeline.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import uk.ac.ebi.eva.pipeline.configuration.GenotypedVcfConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

/**
 * @author Diego Poggioli
 *
 * Test for {@link GenotypedVcfJob}
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { JobOptions.class, GenotypedVcfJob.class, GenotypedVcfConfiguration.class, JobLauncherTestUtils.class})
public class GenotypedVcfJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    
    @Autowired
    private JobOptions jobOptions;

    private String dbName;
    private String vepInput;
    private String vepOutput;

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();
    
    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void fullGenotypedVcfJob() throws Exception {
        final String inputFilePath = "/job-genotyped/small20.vcf.gz";
        String inputFile = GenotypedVcfJobTest.class.getResource(inputFilePath).getFile();
        String mockVep = GenotypedVcfJobTest.class.getResource("/mockvep.pl").getFile();

        jobOptions.getPipelineOptions().put("output.dir", outputFolder.getRoot().getCanonicalPath());
        jobOptions.getPipelineOptions().put("output.dir.statistics", outputFolder.getRoot().getCanonicalPath());
        jobOptions.getPipelineOptions().put("app.vep.path", mockVep);

        Config.setOpenCGAHome(opencgaHome);

        // Annotation files initialization / clean-up
        // TODO Write these to the temporary folder after fixing AnnotationFlatItemReader
        File vepInputFile = new File(vepInput);
        vepInputFile.delete();
        assertFalse(vepInputFile.exists());

        File vepOutputFile = new File(vepOutput);
        vepOutputFile.delete();
        assertFalse(vepOutputFile.exists());

        // Run the Job
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        VariantDBIterator iterator;
        
        // 1 transform step: check transformed file
        String transformedVcf = JobTestUtils.getTransformedOutputPath(
                Paths.get(inputFile).getFileName(), jobOptions.getCompressExtension(), outputFolder.getRoot().getCanonicalPath());
        long transformedLinesCount = getLines(new GZIPInputStream(new FileInputStream(transformedVcf)));
        assertEquals(300, transformedLinesCount);

        // 2 load step: check ((documents in DB) == (lines in transformed file))
        iterator = getVariantDBIterator();
        assertEquals(transformedLinesCount, count(iterator));

        // 3 create stats step
        VariantSource source = (VariantSource) jobOptions.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE);
        File statsFile = new File(Paths.get(outputFolder.getRoot().getCanonicalPath())
                .resolve(VariantStorageManager.buildFilename(source)) + ".variants.stats.json.gz");
        assertTrue(statsFile.exists());

        // 4 load stats step: check ((documents in DB) == (lines in transformed file))
        iterator = getVariantDBIterator();
        assertEquals(transformedLinesCount, count(iterator));

        // check the DB docs have the field "st"
        iterator = getVariantDBIterator();
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

        // 5 annotation flow
        // annotation input vep generate step
        BufferedReader testReader = new BufferedReader(new InputStreamReader(new FileInputStream(
                GenotypedVcfJobTest.class.getResource("/preannot.sorted").getFile())));
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

        testReader.close();
        actualReader.close();
        
        // 6 annotation create step
        assertTrue(vepInputFile.exists());
        assertTrue(vepOutputFile.exists());

        // Check output file length
        assertEquals(537, getLines(new GZIPInputStream(new FileInputStream(vepOutput))) );

        // 8 Annotation load step: check documents in DB have annotation (only consequence type)
        iterator = getVariantDBIterator();

        int cnt = 0;
        int consequenceTypeCount = 0;
        while (iterator.hasNext()) {
            cnt++;
            Variant next = iterator.next();
            if(next.getAnnotation().getConsequenceTypes() != null){
                consequenceTypeCount += next.getAnnotation().getConsequenceTypes().size();
            }
        }

        assertEquals(300, cnt);
        assertEquals(536, consequenceTypeCount);

        //check that one line is skipped because malformed
        List<StepExecution> variantAnnotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals(AnnotationLoaderStep.LOAD_VEP_ANNOTATION))
                .collect(Collectors.toList());
        assertEquals(1, variantAnnotationLoadStepExecution.get(0).getReadSkipCount());

    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();

        dbName = jobOptions.getPipelineOptions().getString("db.name");
        vepInput = jobOptions.getPipelineOptions().getString("vep.input");
        vepOutput = jobOptions.getPipelineOptions().getString("vep.output");
        JobTestUtils.cleanDBs(dbName);
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
