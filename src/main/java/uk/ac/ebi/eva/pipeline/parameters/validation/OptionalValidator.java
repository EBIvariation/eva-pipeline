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

import com.google.common.base.Strings;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

/**
 * Wrapper to make a {@link JobParametersValidator} optional. The validate method is triggered only if the key
 * {@link JobParametersNames} is present in {@link JobParameters}
 */
public class OptionalValidator implements JobParametersValidator {
    private JobParametersValidator jobParametersValidator;

    private String jobParametersName;

    public OptionalValidator(JobParametersValidator jobParametersValidator, String jobParametersNames) {
        this.jobParametersValidator = jobParametersValidator;
        this.jobParametersName = jobParametersNames;
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        if (!Strings.isNullOrEmpty(parameters.getString(jobParametersName))) {
            jobParametersValidator.validate(parameters);
        }
    }
}
