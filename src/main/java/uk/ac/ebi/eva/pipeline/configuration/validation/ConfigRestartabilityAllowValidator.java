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
package uk.ac.ebi.eva.pipeline.configuration.validation;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;

/**
 * ConfigRestartabilityAllow is a Boolean option to allow the restartability of a {@link org.springframework.batch.core.Step}
 */
public class ConfigRestartabilityAllowValidator implements JobParametersValidator {

    /**
     * Checks that the option to allow restartability has been filled in and it is "true" or "false".
     *
     * @param parameters
     * @throws JobParametersInvalidException If the allow restartability option is null or empty or any text different
     *                                       from 'true' or 'false'
     */
    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        String configRestartabilityAllowValue = parameters.getString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW);

        ParametersValidatorUtil
                .checkNullOrEmptyString(configRestartabilityAllowValue, JobParametersNames.CONFIG_RESTARTABILITY_ALLOW);
        ParametersValidatorUtil.checkBooleanStringSyntax(configRestartabilityAllowValue,
                                                         JobParametersNames.CONFIG_RESTARTABILITY_ALLOW);
    }
}
