/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
import org.junit.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadVepAnnotationStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tests that the arguments necessary to run a {@link LoadVepAnnotationStepConfiguration} are
 * correctly validated
 */
public class CreateDatabaseIndexesStepParametersValidatorTest {
    private CreateDatabaseIndexesStepParametersValidator validator;

    private Map<String, JobParameter> requiredParameters;

    private Map<String, JobParameter> optionalParameters;

    @Before
    public void setUp() throws Exception {
        validator = new CreateDatabaseIndexesStepParametersValidator();

        requiredParameters = new TreeMap<>();
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("dbName"));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME,
                               new JobParameter("dbCollectionsFeaturesName"));

        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, new JobParameter("true"));
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException, IOException {
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test
    public void allJobParametersIncludingOptionalAreValid() throws JobParametersInvalidException, IOException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbCollectionsFeaturesNameIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(requiredParameters));
    }

}
