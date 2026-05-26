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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigRestartabilityAllowValidatorTest {

    private ConfigRestartabilityAllowValidator validator;

    private JobParametersBuilder jobParametersBuilder;

    @BeforeEach
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

    @Test
    public void configRestartabilityAllowIsNotValid() {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "blabla");
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    public void configRestartabilityAllowIsEmpty() {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "");
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    public void configRestartabilityAllowIsWhitespace() {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, " ");
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    public void configRestartabilityAllowIsNull() {
        jobParametersBuilder = new JobParametersBuilder();
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }
}
