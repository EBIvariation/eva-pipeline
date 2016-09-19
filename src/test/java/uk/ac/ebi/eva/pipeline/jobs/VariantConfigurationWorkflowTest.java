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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.configuration.VariantWorkflowConfig;
import uk.ac.ebi.eva.pipeline.jobs.steps.VariantsAnnotCreate;
import uk.ac.ebi.eva.pipeline.jobs.steps.VariantsAnnotGenerateInput;
import uk.ac.ebi.eva.pipeline.jobs.steps.VariantsAnnotLoad;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantConfiguration}
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantJobsArgs.class, VariantConfiguration.class, VariantWorkflowConfig.class})
public class VariantConfigurationWorkflowTest {

    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Autowired
    private JobLauncher jobLauncher;

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
    public void allStepsShouldBeExecuted() throws Exception {
        initVariantConfigurationJob();

        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(7, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for(int i=2; i<=steps.size()-1; i++){
            parallelStepsNameToStepExecution.put(steps.get(i).getStepName(), steps.get(i));
        }

        assertEquals(VariantConfiguration.NORMALIZE_VARIANTS, transformStep.getStepName());
        assertEquals(VariantConfiguration.LOAD_VARIANTS, loadStep.getStepName());

        Set<String> parallelStepNamesExecuted = parallelStepsNameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new HashSet<>(Arrays.asList(
                VariantStatsConfiguration.CALCULATE_STATISTICS,
                VariantStatsConfiguration.LOAD_STATISTICS,
                VariantsAnnotGenerateInput.FIND_VARIANTS_TO_ANNOTATE,
                VariantsAnnotCreate.GENERATE_VEP_ANNOTATION,
                VariantsAnnotLoad.LOAD_VEP_ANNOTATION));

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(VariantStatsConfiguration.CALCULATE_STATISTICS).getStartTime()));
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(VariantsAnnotGenerateInput.FIND_VARIANTS_TO_ANNOTATE).getStartTime()));

        assertTrue(parallelStepsNameToStepExecution.get(VariantStatsConfiguration.CALCULATE_STATISTICS).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VariantStatsConfiguration.LOAD_STATISTICS).getStartTime()));
        assertTrue(parallelStepsNameToStepExecution.get(VariantsAnnotGenerateInput.FIND_VARIANTS_TO_ANNOTATE).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VariantsAnnotCreate.GENERATE_VEP_ANNOTATION).getStartTime()));
        assertTrue(parallelStepsNameToStepExecution.get(VariantsAnnotCreate.GENERATE_VEP_ANNOTATION).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VariantsAnnotLoad.LOAD_VEP_ANNOTATION).getStartTime()));
    }

    @Test
    public void optionalStepsShouldBeSkipped() throws Exception {
        initVariantConfigurationJob();

        variantJobsArgs.getPipelineOptions().put(VariantAnnotConfiguration.SKIP_ANNOT, true);
        variantJobsArgs.getPipelineOptions().put(VariantStatsConfiguration.SKIP_STATS, true);

        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(2, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        assertEquals(VariantConfiguration.NORMALIZE_VARIANTS, transformStep.getStepName());
        assertEquals(VariantConfiguration.LOAD_VARIANTS, loadStep.getStepName());

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));
    }

    @Test
    public void statsStepsShouldBeSkipped() throws Exception {
        initVariantConfigurationJob();
        variantJobsArgs.getPipelineOptions().put(VariantStatsConfiguration.SKIP_STATS, true);

        variantJobsArgs.getPipelineOptions().put("db.name", "diegoTest");


        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(5, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for(int i=2; i<=steps.size()-1; i++){
            parallelStepsNameToStepExecution.put(steps.get(i).getStepName(), steps.get(i));
        }

        assertEquals(VariantConfiguration.NORMALIZE_VARIANTS, transformStep.getStepName());
        assertEquals(VariantConfiguration.LOAD_VARIANTS, loadStep.getStepName());

        Set<String> parallelStepNamesExecuted = parallelStepsNameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new HashSet<>(Arrays.asList(
                VariantsAnnotGenerateInput.FIND_VARIANTS_TO_ANNOTATE,
                VariantsAnnotCreate.GENERATE_VEP_ANNOTATION,
                VariantsAnnotLoad.LOAD_VEP_ANNOTATION));

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(VariantsAnnotGenerateInput.FIND_VARIANTS_TO_ANNOTATE).getStartTime()));

        assertTrue(parallelStepsNameToStepExecution.get(VariantsAnnotGenerateInput.FIND_VARIANTS_TO_ANNOTATE).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VariantsAnnotCreate.GENERATE_VEP_ANNOTATION).getStartTime()));
        assertTrue(parallelStepsNameToStepExecution.get(VariantsAnnotCreate.GENERATE_VEP_ANNOTATION).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VariantsAnnotLoad.LOAD_VEP_ANNOTATION).getStartTime()));
    }

    @Test
    public void annotationStepsShouldBeSkipped() throws Exception {
        initVariantConfigurationJob();
        variantJobsArgs.getPipelineOptions().put(VariantAnnotConfiguration.SKIP_ANNOT, true);

        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(4, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for(int i=2; i<=steps.size()-1; i++){
            parallelStepsNameToStepExecution.put(steps.get(i).getStepName(), steps.get(i));
        }

        assertEquals(VariantConfiguration.NORMALIZE_VARIANTS, transformStep.getStepName());
        assertEquals(VariantConfiguration.LOAD_VARIANTS, loadStep.getStepName());

        Set<String> parallelStepNamesExecuted = parallelStepsNameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new HashSet<>(Arrays.asList(
                VariantStatsConfiguration.CALCULATE_STATISTICS,
                VariantStatsConfiguration.LOAD_STATISTICS));

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(VariantStatsConfiguration.CALCULATE_STATISTICS).getStartTime()));

        assertTrue(parallelStepsNameToStepExecution.get(VariantStatsConfiguration.CALCULATE_STATISTICS).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VariantStatsConfiguration.LOAD_STATISTICS).getStartTime()));
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
        dbName = variantJobsArgs.getPipelineOptions().getString("db.name");
        vepInput = variantJobsArgs.getPipelineOptions().getString("vep.input");
        vepOutput = variantJobsArgs.getPipelineOptions().getString("vep.output");
        JobTestUtils.cleanDBs(dbName);
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
    }

    private void initVariantConfigurationJob() {
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
    }

}
