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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

/**
 * Workflow test for {@link GenotypedVcfJob}
 *
 * TODO The test should fail when we will integrate the JobParameter validation since there are empty parameters for VEP
 */
@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE,Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:genotyped-vcf-workflow.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {GenotypedVcfJob.class, BatchTestConfiguration.class})
public class GenotypedVcfJobWorkflowTest {

    private static final String MOCK_VEP = "/mockvep.pl";

    private static final String INPUT_FILE = "/small20.vcf.gz";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;  // we need this for stats.skip and annot.skip

    public static final Set<String> EXPECTED_REQUIRED_STEP_NAMES = new TreeSet<>(
            Arrays.asList(BeanNames.LOAD_VARIANTS_STEP, BeanNames.LOAD_FILE_STEP));

    public static final Set<String> EXPECTED_STATS_STEP_NAMES = new TreeSet<>(
            Arrays.asList(BeanNames.CALCULATE_STATISTICS_STEP, BeanNames.LOAD_STATISTICS_STEP));

    public static final Set<String> EXPECTED_ANNOTATION_STEP_NAMES = new TreeSet<>(
            Arrays.asList(BeanNames.GENERATE_VEP_INPUT_STEP, BeanNames.GENERATE_VEP_ANNOTATION_STEP,
                          BeanNames.LOAD_VEP_ANNOTATION_STEP));

    @Test
    public void allStepsShouldBeExecuted() throws Exception {
        JobParameters jobParameters = initVariantConfigurationJob();

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());

        Collection<StepExecution> stepExecutions = execution.getStepExecutions();
        Map<String, StepExecution> nameToStepExecution = stepExecutions.stream().collect(
                Collectors.toMap(StepExecution::getStepName, Function.identity()));

        Set<String> parallelStepNamesExecuted = nameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new TreeSet<>();
        parallelStepNamesToCheck.addAll(EXPECTED_REQUIRED_STEP_NAMES);
        parallelStepNamesToCheck.addAll(EXPECTED_ANNOTATION_STEP_NAMES);
        parallelStepNamesToCheck.addAll(EXPECTED_STATS_STEP_NAMES);

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        StepExecution lastRequiredStep = new ArrayList<>(stepExecutions).get(EXPECTED_REQUIRED_STEP_NAMES.size() - 1);
        assertEquals(BeanNames.LOAD_FILE_STEP, lastRequiredStep.getStepName());

        assertTrue(lastRequiredStep.getEndTime()
                .before(nameToStepExecution.get(BeanNames.CALCULATE_STATISTICS_STEP).getStartTime()));
        assertTrue(lastRequiredStep.getEndTime()
                .before(nameToStepExecution.get(BeanNames.GENERATE_VEP_INPUT_STEP).getStartTime()));

        assertTrue(nameToStepExecution.get(BeanNames.CALCULATE_STATISTICS_STEP).getEndTime()
                .before(nameToStepExecution.get(BeanNames.LOAD_STATISTICS_STEP).getStartTime()));
        assertTrue(nameToStepExecution.get(BeanNames.GENERATE_VEP_INPUT_STEP).getEndTime()
                .before(nameToStepExecution.get(BeanNames.GENERATE_VEP_ANNOTATION_STEP).getStartTime()));
        assertTrue(nameToStepExecution.get(BeanNames.GENERATE_VEP_ANNOTATION_STEP).getEndTime()
                .before(nameToStepExecution.get(BeanNames.LOAD_VEP_ANNOTATION_STEP).getStartTime()));
    }

    @Test
    public void optionalStepsShouldBeSkipped() throws Exception {
        JobParameters jobParameters = initVariantConfigurationJob();

        jobOptions.getPipelineOptions().put(JobParametersNames.ANNOTATION_SKIP, true);
        jobOptions.getPipelineOptions().put(JobParametersNames.STATISTICS_SKIP, true);

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());

        Set<String> names = execution.getStepExecutions().stream().map(StepExecution::getStepName)
                                      .collect(Collectors.toSet());

        assertEquals(EXPECTED_REQUIRED_STEP_NAMES, names);
    }

    @Test
    public void statsStepsShouldBeSkipped() throws Exception {
        JobParameters jobParameters = initVariantConfigurationJob();
        jobOptions.getPipelineOptions().put(JobParametersNames.STATISTICS_SKIP, true);

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());

        Collection<StepExecution> stepExecutions = execution.getStepExecutions();
        Map<String, StepExecution> nameToStepExecution = stepExecutions.stream().collect(
                Collectors.toMap(StepExecution::getStepName, Function.identity()));

        Set<String> parallelStepNamesExecuted = nameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new TreeSet<>();
        parallelStepNamesToCheck.addAll(EXPECTED_REQUIRED_STEP_NAMES);
        parallelStepNamesToCheck.addAll(EXPECTED_ANNOTATION_STEP_NAMES);

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        StepExecution lastRequiredStep = new ArrayList<>(stepExecutions).get(EXPECTED_REQUIRED_STEP_NAMES.size() - 1);
        assertEquals(BeanNames.LOAD_FILE_STEP, lastRequiredStep.getStepName());

        assertTrue(lastRequiredStep.getEndTime()
                .before(nameToStepExecution.get(BeanNames.GENERATE_VEP_INPUT_STEP).getStartTime()));

        assertTrue(nameToStepExecution.get(BeanNames.GENERATE_VEP_INPUT_STEP).getEndTime()
                .before(nameToStepExecution.get(BeanNames.GENERATE_VEP_ANNOTATION_STEP).getStartTime()));
        assertTrue(nameToStepExecution.get(BeanNames.GENERATE_VEP_ANNOTATION_STEP).getEndTime()
                .before(nameToStepExecution.get(BeanNames.LOAD_VEP_ANNOTATION_STEP).getStartTime()));
    }

    @Test
    public void annotationStepsShouldBeSkipped() throws Exception {
        JobParameters jobParameters = initVariantConfigurationJob();
        jobOptions.getPipelineOptions().put(JobParametersNames.ANNOTATION_SKIP, true);

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, execution.getExitStatus());

        Collection<StepExecution> stepExecutions = execution.getStepExecutions();
        Map<String, StepExecution> nameToStepExecution = stepExecutions.stream().collect(
                Collectors.toMap(StepExecution::getStepName, Function.identity()));

        Set<String> parallelStepNamesExecuted = nameToStepExecution.keySet();
        Set<String> parallelStepNamesToCheck = new TreeSet<>();
        parallelStepNamesToCheck.addAll(EXPECTED_REQUIRED_STEP_NAMES);
        parallelStepNamesToCheck.addAll(EXPECTED_STATS_STEP_NAMES);

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        StepExecution lastRequiredStep = new ArrayList<>(stepExecutions).get(EXPECTED_REQUIRED_STEP_NAMES.size() - 1);
        assertEquals(BeanNames.LOAD_FILE_STEP, lastRequiredStep.getStepName());

        assertEquals(parallelStepNamesToCheck, parallelStepNamesExecuted);

        assertTrue(lastRequiredStep.getEndTime().before(nameToStepExecution.get(
                BeanNames.CALCULATE_STATISTICS_STEP).getStartTime()));

        assertTrue(nameToStepExecution.get(BeanNames.CALCULATE_STATISTICS_STEP).getEndTime()
                .before(nameToStepExecution.get(BeanNames.LOAD_STATISTICS_STEP).getStartTime()));
    }

    private JobParameters initVariantConfigurationJob() throws IOException {
        File inputFile = getResource(INPUT_FILE);
        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        String outputDirStats = temporaryFolderRule.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();

        EvaJobParameterBuilder evaJobParameterBuilder = new EvaJobParameterBuilder()
                .collectionFilesName("files")
                .collectionVariantsName("variants")
                .databaseName(dbName)
                .inputFasta("")
                .inputStudyId("genotyped-job-workflow")
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .outputDirAnnotation(outputDirAnnotation)
                .outputDirStats(outputDirStats)
                .timestamp()
                .vepCachePath("")
                .vepCacheSpecies("")
                .vepCacheVersion("")
                .vepNumForks("")
                .vepPath(getResource(MOCK_VEP).getPath());

        jobOptions.getPipelineOptions().put(JobParametersNames.ANNOTATION_SKIP, false);
        jobOptions.getPipelineOptions().put(JobParametersNames.STATISTICS_SKIP, false);
        return evaJobParameterBuilder.toJobParameters();
    }

}
