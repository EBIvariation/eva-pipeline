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

import org.junit.Before;
import org.junit.Rule;
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

import uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow;
import uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.configuration.GenotypedVcfWorkflowConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

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
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

/**
 * Workflow test for {@link GenotypedVcfJob}
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = {JobOptions.class, GenotypedVcfJob.class, GenotypedVcfWorkflowConfiguration.class,
        JobLauncherTestUtils.class})
public class GenotypedVcfJobWorkflowTest {
    private static final String MOCK_VEP = "/mockvep.pl";
    //TODO check later to substitute files for temporary ones / pay attention to vep Input file

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    private String inputFileResouce;
    private String outputDir;
    private String compressExtension;
    private String vepInput;
    private String vepOutput;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") :
            "/opt/opencga";

    @Test
    public void allStepsShouldBeExecuted() throws Exception {
        initVariantConfigurationJob();

        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(6, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution loadStep = steps.get(0);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for (int i = 1; i < steps.size(); i++) {
            parallelStepsNameToStepExecution.put(steps.get(i).getStepName(), steps.get(i));
        }

        assertEquals(GenotypedVcfJob.LOAD_VARIANTS, loadStep.getStepName());

        Set<String> parallelStepNamesExecuted = parallelStepsNameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new HashSet<>(Arrays.asList(
                PopulationStatisticsFlow.CALCULATE_STATISTICS,
                PopulationStatisticsFlow.LOAD_STATISTICS,
                VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE,
                VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION,
                AnnotationLoaderStep.LOAD_VEP_ANNOTATION));

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

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
        assertEquals(1, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution loadStep = steps.get(0);

        assertEquals(GenotypedVcfJob.LOAD_VARIANTS, loadStep.getStepName());
    }

    @Test
    public void statsStepsShouldBeSkipped() throws Exception {
        initVariantConfigurationJob();
        jobOptions.getPipelineOptions().put(JobParametersNames.STATISTICS_SKIP, true);

        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(4, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution loadStep = steps.get(0);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for (int i = 1; i < steps.size(); i++) {
            parallelStepsNameToStepExecution.put(steps.get(i).getStepName(), steps.get(i));
        }

        assertEquals(GenotypedVcfJob.LOAD_VARIANTS, loadStep.getStepName());

        Set<String> parallelStepNamesExecuted = parallelStepsNameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new HashSet<>(Arrays.asList(
                VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE,
                VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION,
                AnnotationLoaderStep.LOAD_VEP_ANNOTATION));

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

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
        assertEquals(3, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution loadStep = steps.get(0);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for (int i = 1; i < steps.size(); i++) {
            parallelStepsNameToStepExecution.put(steps.get(i).getStepName(), steps.get(i));
        }

        assertEquals(GenotypedVcfJob.LOAD_VARIANTS, loadStep.getStepName());

        Set<String> parallelStepNamesExecuted = parallelStepsNameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new HashSet<>(Arrays.asList(
                PopulationStatisticsFlow.CALCULATE_STATISTICS,
                PopulationStatisticsFlow.LOAD_STATISTICS));

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

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
        vepInput = jobOptions.getPipelineOptions().getString(JobOptions.VEP_INPUT);
        vepOutput = jobOptions.getPipelineOptions().getString(JobOptions.VEP_OUTPUT);
    }

    private void initVariantConfigurationJob() {
        mongoRule.getTemporaryDatabase(jobOptions.getDbName());
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, getResource(inputFileResouce).getAbsolutePath());
        jobOptions.getPipelineOptions().put(JobParametersNames.APP_VEP_PATH, getResource(MOCK_VEP).getAbsolutePath());

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
