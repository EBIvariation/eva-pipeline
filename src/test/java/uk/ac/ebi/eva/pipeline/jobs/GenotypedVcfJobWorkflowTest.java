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
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.configuration.GenotypedVcfWorkflowConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;
import uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow;
import uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Workflow test for {@link GenotypedVcfJob}
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = {JobOptions.class, GenotypedVcfJob.class, GenotypedVcfWorkflowConfiguration.class,
        JobLauncherTestUtils.class})
public class GenotypedVcfJobWorkflowTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    private String inputFileResouce;
    private String outputDir;
    private String compressExtension;
    private String dbName;
    private String vepInput;
    private String vepOutput;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") :
            "/opt/opencga";

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
        for (int i = 2; i <= steps.size() - 1; i++) {
            parallelStepsNameToStepExecution.put(steps.get(i).getStepName(), steps.get(i));
        }

        assertEquals(GenotypedVcfJob.NORMALIZE_VARIANTS, transformStep.getStepName());
        assertEquals(GenotypedVcfJob.LOAD_VARIANTS, loadStep.getStepName());

        Set<String> parallelStepNamesExecuted = parallelStepsNameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new HashSet<>(Arrays.asList(
                PopulationStatisticsFlow.CALCULATE_STATISTICS,
                PopulationStatisticsFlow.LOAD_STATISTICS,
                VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE,
                VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION,
                AnnotationLoaderStep.LOAD_VEP_ANNOTATION));

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow
                .CALCULATE_STATISTICS).getStartTime()));
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(VepInputGeneratorStep
                .FIND_VARIANTS_TO_ANNOTATE).getStartTime()));

        assertTrue(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.CALCULATE_STATISTICS).getEndTime()
                .before(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.LOAD_STATISTICS).getStartTime()));
        assertTrue(parallelStepsNameToStepExecution.get(VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION)
                        .getStartTime()));
        assertTrue(parallelStepsNameToStepExecution.get(VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION).getEndTime()
                .before(parallelStepsNameToStepExecution.get(AnnotationLoaderStep.LOAD_VEP_ANNOTATION).getStartTime()));
    }

    @Test
    public void optionalStepsShouldBeSkipped() throws Exception {
        initVariantConfigurationJob();

        jobOptions.getPipelineOptions().put(JobParametersNames.ANNOTATION_SKIP, true);
        jobOptions.getPipelineOptions().put(JobParametersNames.STATISTICS_SKIP, true);

        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(2, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        assertEquals(GenotypedVcfJob.NORMALIZE_VARIANTS, transformStep.getStepName());
        assertEquals(GenotypedVcfJob.LOAD_VARIANTS, loadStep.getStepName());

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));
    }

    @Test
    public void statsStepsShouldBeSkipped() throws Exception {
        initVariantConfigurationJob();
        jobOptions.getPipelineOptions().put(JobParametersNames.STATISTICS_SKIP, true);

        jobOptions.getPipelineOptions().put(JobParametersNames.DB_NAME, "diegoTest");


        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(5, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for (int i = 2; i <= steps.size() - 1; i++) {
            parallelStepsNameToStepExecution.put(steps.get(i).getStepName(), steps.get(i));
        }

        assertEquals(GenotypedVcfJob.NORMALIZE_VARIANTS, transformStep.getStepName());
        assertEquals(GenotypedVcfJob.LOAD_VARIANTS, loadStep.getStepName());

        Set<String> parallelStepNamesExecuted = parallelStepsNameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new HashSet<>(Arrays.asList(
                VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE,
                VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION,
                AnnotationLoaderStep.LOAD_VEP_ANNOTATION));

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(VepInputGeneratorStep
                .FIND_VARIANTS_TO_ANNOTATE).getStartTime()));

        assertTrue(parallelStepsNameToStepExecution.get(VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION)
                        .getStartTime()));
        assertTrue(parallelStepsNameToStepExecution.get(VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION).getEndTime()
                .before(parallelStepsNameToStepExecution.get(AnnotationLoaderStep.LOAD_VEP_ANNOTATION).getStartTime()));
    }

    @Test
    public void annotationStepsShouldBeSkipped() throws Exception {
        initVariantConfigurationJob();
        jobOptions.getPipelineOptions().put(JobParametersNames.ANNOTATION_SKIP, true);

        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(4, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for (int i = 2; i <= steps.size() - 1; i++) {
            parallelStepsNameToStepExecution.put(steps.get(i).getStepName(), steps.get(i));
        }

        assertEquals(GenotypedVcfJob.NORMALIZE_VARIANTS, transformStep.getStepName());
        assertEquals(GenotypedVcfJob.LOAD_VARIANTS, loadStep.getStepName());

        Set<String> parallelStepNamesExecuted = parallelStepsNameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new HashSet<>(Arrays.asList(
                PopulationStatisticsFlow.CALCULATE_STATISTICS,
                PopulationStatisticsFlow.LOAD_STATISTICS));

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        assertTrue(transformStep.getEndTime().before(loadStep.getStartTime()));
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow
                .CALCULATE_STATISTICS).getStartTime()));

        assertTrue(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.CALCULATE_STATISTICS).getEndTime()
                .before(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.LOAD_STATISTICS).getStartTime()));
    }

    /**
     * JobLauncherTestUtils is initialized here because in GenotypedVcfJob there are two Job beans
     * in this way it is possible to specify the Job to run (and avoid NoUniqueBeanDefinitionException)
     *
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();

        inputFileResouce = jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF);
        outputDir = jobOptions.getOutputDir();
        compressExtension = jobOptions.getPipelineOptions().getString("compressExtension");
        dbName = jobOptions.getPipelineOptions().getString(JobParametersNames.DB_NAME);
        vepInput = jobOptions.getPipelineOptions().getString("vep.input");
        vepOutput = jobOptions.getPipelineOptions().getString("vep.output");
        JobTestUtils.cleanDBs(dbName);
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
    }

    private void initVariantConfigurationJob() {
        String inputFile = GenotypedVcfJobTest.class.getResource(inputFileResouce).getFile();
        String mockVep = GenotypedVcfJobTest.class.getResource("/mockvep.pl").getFile();

        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, inputFile);
        jobOptions.getPipelineOptions().put(JobParametersNames.APP_VEP_PATH, mockVep);

        Config.setOpenCGAHome(opencgaHome);

        // transformedVcf file init
        String transformedVcf = outputDir + inputFileResouce + ".variants.json" + compressExtension;
        File transformedVcfFile = new File(transformedVcf);
        transformedVcfFile.delete();
        assertFalse(transformedVcfFile.exists());

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
    }

}
