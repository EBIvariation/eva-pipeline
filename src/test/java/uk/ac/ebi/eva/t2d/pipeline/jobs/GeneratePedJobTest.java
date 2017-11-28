/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.t2d.pipeline.jobs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.configuration.T2dDataSourceConfiguration;
import uk.ac.ebi.eva.t2d.jobs.GeneratePedJob;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.t2d.configuration.BatchJobExecutorInMemory;
import uk.ac.ebi.eva.test.t2d.configuration.TestJpaConfiguration;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

@RunWith(SpringRunner.class)
@ActiveProfiles({Application.T2D_PROFILE})
@ContextConfiguration(classes = {T2dDataSourceConfiguration.class, TestJpaConfiguration.class,
        BatchJobExecutorInMemory.class, GeneratePedJob.class})
@TestPropertySource({"classpath:application-t2d.properties"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class GeneratePedJobTest {

    private static final String INPUT_SAMPLES_FILE = "/t2d/short.tsv";
    private static final String INPUT_SAMPLES_DEFINITION_FILE = "/t2d/short.definition.tsv";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Test(expected = JobParametersInvalidException.class)
    public void testGeneratePedJobMissingParameter() throws Exception {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputSamplesDefinition(getResource(INPUT_SAMPLES_DEFINITION_FILE).getPath())
                .toJobParameters();

        jobLauncherTestUtils.launchJob(jobParameters);
    }

    @Test
    public void testGeneratePedJob() throws Exception {
        final Path pedOutputPath = Paths.get(temporaryFolderRule.getRoot().getAbsolutePath(), "test.ped");
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .t2dInputSamples(getResource(INPUT_SAMPLES_FILE).getPath())
                .t2dInputSamplesDefinition(getResource(INPUT_SAMPLES_DEFINITION_FILE).getPath())
                .t2dPedFileOutput(pedOutputPath)
                .toJobParameters();

        assertCompleted(jobLauncherTestUtils.launchJob(jobParameters));
        assertEquals(10, Files.lines(pedOutputPath).count());
    }

}
