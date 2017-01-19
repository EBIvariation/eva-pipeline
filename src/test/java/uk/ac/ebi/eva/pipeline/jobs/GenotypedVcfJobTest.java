/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

import org.junit.Before;
import org.junit.Rule;
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
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

/**
 * Test for {@link GenotypedVcfJob}
 * <p>
 * TODO:
 * FILE_WRONG_NO_ALT should be renamed because the alt allele is not missing but is the same as the reference
 */
@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE,Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:genotyped-vcf.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {GenotypedVcfJob.class, BatchTestConfiguration.class})
public class GenotypedVcfJobTest {
    //TODO check later to substitute files for temporary ones / pay attention to vep Input file

    private static final String MOCK_VEP = "/mockvep.pl";

    private static final int EXPECTED_VALID_ANNOTATIONS = 536;

    private static final int EXPECTED_ANNOTATIONS = 537;

    private static final int EXPECTED_VARIANTS = 300;

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    private String input;
    private String outputDir;
    private String compressExtension;
    private String dbName;
    private String vepInput;
    private String vepOutput;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void fullGenotypedVcfJob() throws Exception {
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, getResource(input).getAbsolutePath());

        Config.setOpenCGAHome(opencgaHome);
        mongoRule.getTemporaryDatabase(dbName);

        //stats file init
        VariantSource source = (VariantSource) jobOptions.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE);
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
        JobParameters jobParameters = new EvaJobParameterBuilder().inputVcf(getResource(input).getAbsolutePath())
                .databaseName(dbName)
                .collectionVariantsName("variants")
                .inputVcfId("1")
                .inputStudyId("genotyped-job")
                .inputVcfAggregation("NONE")
                .vepPath(getResource(MOCK_VEP).getPath())
                .vepCacheVersion("")
                .vepCachePath("")
                .vepCacheSpecies("")
                .inputFasta("")
                .vepNumForks("")
                .outputDirAnnotation("/tmp/")
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // 1 load step: check ((documents in DB) == (lines in transformed file))
        //variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        //variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        iterator = getVariantDBIterator();
        assertEquals(EXPECTED_VARIANTS, count(iterator));

        // 2 create stats step
        assertTrue(statsFile.exists());

        // 3 load stats step: check ((documents in DB) == (lines in transformed file))
        //variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        //variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        iterator = getVariantDBIterator();
        assertEquals(EXPECTED_VARIANTS, count(iterator));

        // check the DB docs have the field "st"
        iterator = getVariantDBIterator();

        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

        // 4 annotation flow
        // annotation input vep generate step
        BufferedReader testReader = new BufferedReader(new InputStreamReader(new FileInputStream(
                getResource("/preannot.sorted"))));
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

        // 5 annotation create step
        assertTrue(vepInputFile.exists());
        assertTrue(vepOutputFile.exists());

        // Check output file length
        assertEquals(EXPECTED_ANNOTATIONS, getLines(new GZIPInputStream(new FileInputStream(vepOutput))));

        // 6 Annotation load step: check documents in DB have annotation (only consequence type)
        iterator = getVariantDBIterator();

        int cnt = 0;
        int consequenceTypeCount = 0;
        while (iterator.hasNext()) {
            cnt++;
            Variant next = iterator.next();
            if (next.getAnnotation().getConsequenceTypes() != null) {
                consequenceTypeCount += next.getAnnotation().getConsequenceTypes().size();
            }
        }

        assertEquals(EXPECTED_VARIANTS, cnt);
        assertEquals(EXPECTED_VALID_ANNOTATIONS, consequenceTypeCount);

        //check that one line is skipped because malformed
        List<StepExecution> variantAnnotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals(BeanNames.LOAD_VEP_ANNOTATION_STEP))
                .collect(Collectors.toList());
        assertEquals(1, variantAnnotationLoadStepExecution.get(0).getReadSkipCount());

    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();

        input = jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF);
        outputDir = jobOptions.getOutputDir();
        compressExtension = jobOptions.getPipelineOptions().getString("compressExtension");
        dbName = jobOptions.getPipelineOptions().getString(JobParametersNames.DB_NAME);
        vepInput = jobOptions.getPipelineOptions().getString(JobOptions.VEP_INPUT);
        vepOutput = jobOptions.getPipelineOptions().getString(JobOptions.VEP_OUTPUT);
    }

    private VariantDBIterator getVariantDBIterator() throws IllegalAccessException,
            ClassNotFoundException, InstantiationException, StorageManagerException {
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        return variantDBAdaptor.iterator(new QueryOptions());
    }

}
