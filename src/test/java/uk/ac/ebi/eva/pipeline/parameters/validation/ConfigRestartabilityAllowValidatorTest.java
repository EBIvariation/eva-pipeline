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
package uk.ac.ebi.eva.pipeline.parameters.validation;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

public class ConfigRestartabilityAllowValidatorTest {

    private ConfigRestartabilityAllowValidator validator;

    private JobParametersBuilder jobParametersBuilder;

    @Before
    public void setUp() throws Exception {
        validator = new ConfigRestartabilityAllowValidator();
    }

    @Test
    public void configRestartabilityAllowIsTrue() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "true");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void configRestartabilityAllowIsTrueAllCapital() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "TRUE");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void configRestartabilityAllowIsFalse() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "false");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void configRestartabilityAllowIsFalseAllCapital() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "FALSE");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsNotValid() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "blabla");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsEmpty() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsWhitespace() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, " ");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsNull() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, null);
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
