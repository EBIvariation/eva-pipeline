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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.opencb.opencga.lib.common.Config;
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

import uk.ac.ebi.eva.pipeline.configuration.GenotypedVcfWorkflowConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.flows.AnnotationFlow;
import uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow;
import uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

/**
 * @author Diego Poggioli
 * @author Cristina Yenyxe Gonzalez Garcia
 *
 * Workflow test for {@link GenotypedVcfJob}
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { JobOptions.class, GenotypedVcfJob.class, GenotypedVcfWorkflowConfiguration.class, JobLauncherTestUtils.class})
public class GenotypedVcfJobWorkflowTest {

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
    public void allStepsShouldBeExecuted() throws Exception {
        JobParameters jobParameters = initVariantConfigurationJob();

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(7, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for(int i=2; i<=steps.size()-1; i++){
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
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.CALCULATE_STATISTICS).getStartTime()));
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE).getStartTime()));

        assertTrue(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.CALCULATE_STATISTICS).getEndTime()
                .before(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.LOAD_STATISTICS).getStartTime()));
        assertTrue(parallelStepsNameToStepExecution.get(VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION).getStartTime()));
        assertTrue(parallelStepsNameToStepExecution.get(VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION).getEndTime()
                .before(parallelStepsNameToStepExecution.get(AnnotationLoaderStep.LOAD_VEP_ANNOTATION).getStartTime()));
    }

    @Test
    public void optionalStepsShouldBeSkipped() throws Exception {
        JobParameters jobParameters = initVariantConfigurationJob();
        jobOptions.getPipelineOptions().put(AnnotationFlow.SKIP_ANNOT, true);
        jobOptions.getPipelineOptions().put(PopulationStatisticsFlow.SKIP_STATS, true);

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

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
        JobParameters jobParameters = initVariantConfigurationJob();
        jobOptions.getPipelineOptions().put(PopulationStatisticsFlow.SKIP_STATS, true);
        jobOptions.getPipelineOptions().put("db.name", "statsStepsShouldBeSkipped");

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(5, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for(int i=2; i<=steps.size()-1; i++){
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
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE).getStartTime()));

        assertTrue(parallelStepsNameToStepExecution.get(VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE).getEndTime()
                .before(parallelStepsNameToStepExecution.get(VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION).getStartTime()));
        assertTrue(parallelStepsNameToStepExecution.get(VepAnnotationGeneratorStep.GENERATE_VEP_ANNOTATION).getEndTime()
                .before(parallelStepsNameToStepExecution.get(AnnotationLoaderStep.LOAD_VEP_ANNOTATION).getStartTime()));
    }

    @Test
    public void annotationStepsShouldBeSkipped() throws Exception {
        JobParameters jobParameters = initVariantConfigurationJob();
        jobOptions.getPipelineOptions().put(AnnotationFlow.SKIP_ANNOT, true);

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());
        assertEquals(4, execution.getStepExecutions().size());

        List<StepExecution> steps = new ArrayList<>(execution.getStepExecutions());
        StepExecution transformStep = steps.get(0);
        StepExecution loadStep = steps.get(1);

        Map<String, StepExecution> parallelStepsNameToStepExecution = new HashMap<>();
        for(int i=2; i<=steps.size()-1; i++){
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
        assertTrue(loadStep.getEndTime().before(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.CALCULATE_STATISTICS).getStartTime()));

        assertTrue(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.CALCULATE_STATISTICS).getEndTime()
                .before(parallelStepsNameToStepExecution.get(PopulationStatisticsFlow.LOAD_STATISTICS).getStartTime()));
    }

    /**
     * JobLauncherTestUtils is initialized here because in GenotypedVcfJob there are two Job beans
     * in this way it is possible to specify the Job to run (and avoid NoUniqueBeanDefinitionException)
     * @throws Exception
     */
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

    private JobParameters initVariantConfigurationJob() throws IOException {
        final String inputFilePath = "/job-genotyped/small20.vcf.gz";
        String inputFile = GenotypedVcfJobWorkflowTest.class.getResource(inputFilePath).getFile();
        String mockVep = GenotypedVcfJobWorkflowTest.class.getResource("/mockvep.pl").getFile();

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

        return new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .toJobParameters();
    }

}
