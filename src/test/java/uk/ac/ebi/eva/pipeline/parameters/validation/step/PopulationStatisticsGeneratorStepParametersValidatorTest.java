/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.parameters.validation.step;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.PopulationStatisticsGeneratorStep;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tests that the arguments necessary to run a {@link PopulationStatisticsGeneratorStep}
 * are correctly validated
 */
public class PopulationStatisticsGeneratorStepParametersValidatorTest {
    private PopulationStatisticsGeneratorStepParametersValidator validator;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    private Map<String, JobParameter> parameters;
    private Map<String, JobParameter> optionalParameters;

    @Before
    public void initialize() throws IOException {
        validator = new PopulationStatisticsGeneratorStepParametersValidator();
        parameters = new TreeMap<>();
        parameters.put(JobParametersNames.DB_NAME, new JobParameter("dbName"));
        parameters.put(JobParametersNames.OUTPUT_DIR_STATISTICS,
                       new JobParameter(temporaryFolderRule.getRoot().getCanonicalPath()));
        parameters.put(JobParametersNames.INPUT_STUDY_ID, new JobParameter("inputStudyId"));
        parameters.put(JobParametersNames.INPUT_VCF_ID, new JobParameter("inputVcfId"));

        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.STATISTICS_OVERWRITE, new JobParameter("true"));
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException, IOException {
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void allJobParametersIncludingOptionalAreValid() throws JobParametersInvalidException, IOException {
        parameters.putAll(optionalParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsMissing() throws JobParametersInvalidException, IOException {
        parameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void outputDirStatisticsIsMissing() throws JobParametersInvalidException, IOException {
        parameters.remove(JobParametersNames.OUTPUT_DIR_STATISTICS);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsMissing() throws JobParametersInvalidException, IOException {
        parameters.remove(JobParametersNames.INPUT_STUDY_ID);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIdIsMissing() throws JobParametersInvalidException, IOException {
        parameters.remove(JobParametersNames.INPUT_VCF_ID);
        validator.validate(new JobParameters(parameters));
    }
}
