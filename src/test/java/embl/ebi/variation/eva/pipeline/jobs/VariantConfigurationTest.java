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
import embl.ebi.variation.eva.pipeline.config.VariantConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.*;
import embl.ebi.variation.eva.pipeline.steps.VariantsAnnotLoad;
import static junit.framework.TestCase.assertEquals;
import org.apache.commons.io.FileUtils;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantConfiguration}
 *
 * JobLauncherTestUtils is initialized in @Before because in VariantConfiguration there are two Job beans:
 * variantJob and variantAnnotationBatchJob (used by test). In this way it is possible to specify the Job to run
 * and avoid NoUniqueBeanDefinitionException. There are also other solutions like:
 *  - http://stackoverflow.com/questions/29655796/how-can-i-qualify-an-autowired-setter-that-i-dont-own
 *  - https://jira.spring.io/browse/BATCH-2366
 *
 * TODO:
 * FILE_WRONG_NO_ALT should be renamed because the alt allele is not missing but is the same as the reference
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantJobsArgs.class, VariantConfiguration.class, VariantConfig.class})
public class VariantConfigurationTest {

    private JobLauncherTestUtils jobLauncherTestUtils;
    
    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobRepository jobRepository;

    @Autowired
    @Qualifier("variantJob")
    public Job job;

    private String input;
    private String outputDir;
    private String compressExtension;
    private String dbName;
    private String vepInput;
    private String vepOutput;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void transformStepShouldTransformAllVariants() throws Exception {
        Config.setOpenCGAHome(opencgaHome);

        String inputFile = VariantConfigurationTest.class.getResource(input).getFile();
        variantJobsArgs.getPipelineOptions().put("input.vcf", inputFile);

        String outputFilename = getTransformedOutputPath(Paths.get(input).getFileName(), ".gz", "/tmp");

        File file = new File(outputFilename);
        if(file.exists())
            file.delete();
        assertFalse(file.exists());

        // When the execute method in variantsTransform is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(VariantConfiguration.NORMALIZE_VARIANTS);

        //Then variantsTransform should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And the transformed file should contain the same number of line of the Vcf input file
        Assert.assertEquals(300, getLines(new GZIPInputStream(new FileInputStream(outputFilename))));

        file.delete();
        new File(outputDir, "small20.vcf.gz.file.json.gz").delete();
    }

    /**
     * This test has to fail because the vcf FILE_WRONG_NO_ALT is malformed:
     * in a variant a reference and a alternate allele are the same
     */
    @Test
    public void transformStepShouldFailIfVariantsAreMalformed(){
        final String FILE_WRONG_NO_ALT = "/wrong_no_alt.vcf.gz";
        Config.setOpenCGAHome(opencgaHome);

        //Given a malformed VCF input file
        String inputFile = VariantConfigurationTest.class.getResource(FILE_WRONG_NO_ALT).getFile();
        variantJobsArgs.getPipelineOptions().put("input.vcf", inputFile);

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_WRONG_NO_ALT).getFileName(), ".gz", "/tmp");

        File file = new File(outputFilename);
        file.delete();
        assertFalse(file.exists());

        //When the execute method in variantsTransform is invoked then a StorageManagerException is thrown
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(VariantConfiguration.NORMALIZE_VARIANTS);
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Test
    public void loadStepShouldLoadAllVariants() throws Exception {
        variantJobsArgs.getVariantOptions().put(VariantStorageManager.DB_NAME, dbName);

        variantJobsArgs.getVariantOptions().put(VARIANT_SOURCE, new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE));

        //and a variants transform step already executed
        File transformedVcfVariantsFile =
                new File(VariantConfigurationTest.class.getResource("/small20.vcf.gz.variants.json.gz").getFile());
        File tmpTransformedVcfVariantsFile = new File(outputDir, transformedVcfVariantsFile.getName());
        FileUtils.copyFile(transformedVcfVariantsFile, tmpTransformedVcfVariantsFile);

        File transformedVariantsFile =
                new File(VariantConfigurationTest.class.getResource("/small20.vcf.gz.file.json.gz").getFile());
        File tmpTransformedVariantsFile = new File(outputDir, transformedVariantsFile.getName());
        FileUtils.copyFile(transformedVariantsFile, tmpTransformedVariantsFile);

        // When the execute method in variantsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(VariantConfiguration.LOAD_VARIANTS);

        //Then variantsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And the number of documents in db should be the same number of line of the vcf transformed file
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        long lines = getLines(new GZIPInputStream(new FileInputStream(transformedVcfVariantsFile)));

        assertEquals(countRows(iterator), lines);

        tmpTransformedVcfVariantsFile.delete();
        tmpTransformedVariantsFile.delete();
    }

    @Test
    public void loadStepShouldFailBecauseOpenCGAHomeIsWrong() throws JobExecutionException {
        String inputFile = VariantConfigurationTest.class.getResource(input).getFile();

        Config.setOpenCGAHome("");

        variantJobsArgs.getPipelineOptions().put("input.vcf", inputFile);
        variantJobsArgs.getVariantOptions().put(VariantStorageManager.DB_NAME, dbName);

        VariantSource source = (VariantSource) variantJobsArgs.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE);

        variantJobsArgs.getVariantOptions().put(VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                input,
                source.getFileId(),
                source.getStudyId(),
                source.getStudyName(),
                source.getType(),
                source.getAggregation()));

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(VariantConfiguration.LOAD_VARIANTS);

        assertEquals(inputFile, variantJobsArgs.getPipelineOptions().getString("input.vcf"));
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Test
    public void fullVariantConfig() throws Exception {
        String inputFile = VariantConfigurationTest.class.getResource(input).getFile();
        String mockVep = VariantConfigurationTest.class.getResource("/mockvep.pl").getFile();

        variantJobsArgs.getPipelineOptions().put("input.vcf", inputFile);
        variantJobsArgs.getPipelineOptions().put("app.vep.path", mockVep);

        Config.setOpenCGAHome(opencgaHome);

        // transformedVcf file init
        String transformedVcf = outputDir + input + ".variants.json" + compressExtension;
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
        iterator = getVariantDBIterator();

        int cnt=0;
        int consequenceTypeCount = 0;
        while (iterator.hasNext()) {
            cnt++;
            Variant next = iterator.next();
            if(next.getAnnotation().getConsequenceTypes() != null){
                consequenceTypeCount += next.getAnnotation().getConsequenceTypes().size();
            }
        }

        assertEquals(300, cnt);
        assertEquals(533, consequenceTypeCount);

        //check that one line is skipped because malformed
        List<StepExecution> variantAnnotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals(VariantsAnnotLoad.LOAD_VEP_ANNOTATION))
                .collect(Collectors.toList());
        assertEquals(1, variantAnnotationLoadStepExecution.get(0).getReadSkipCount());

    }

    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
        jobLauncherTestUtils = new JobLauncherTestUtils();
        jobLauncherTestUtils.setJob(job);
        jobLauncherTestUtils.setJobLauncher(jobLauncher);
        jobLauncherTestUtils.setJobRepository(jobRepository);

        input = variantJobsArgs.getPipelineOptions().getString("input.vcf");
        outputDir = variantJobsArgs.getPipelineOptions().getString("output.dir");
        compressExtension = variantJobsArgs.getPipelineOptions().getString("compressExtension");
        dbName = variantJobsArgs.getPipelineOptions().getString("db.name");
        vepInput = variantJobsArgs.getPipelineOptions().getString("vep.input");
        vepOutput = variantJobsArgs.getPipelineOptions().getString("vep.output");
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
