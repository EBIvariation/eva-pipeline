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
package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.job.DefaultJobParametersValidator;

public abstract class SingleKeyValidator extends DefaultJobParametersValidator {

    private String optionalParameterName;

    public SingleKeyValidator(boolean mandatory, String parameterName) {
        super();
        if (mandatory) {
            setRequiredKeys(new String[]{parameterName});
        } else {
            optionalParameterName = parameterName;
        }
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        if (optionalParameterName == null) {
            super.validate(parameters);
        }
        doValidate(parameters);
    }

    protected abstract void doValidate(JobParameters parameters);
}
