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
package uk.ac.ebi.eva.pipeline.parameters.validation;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

/**
 * Checks that the option to overwrite annotation has been filled in and it is "true" or "false".
 *
 * Throws JobParametersInvalidException If the overwrite annotation option is null or empty or any text different
 * from 'true' or 'false'
 */
public class AnnotationOverwriteValidator implements JobParametersValidator {

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        String annotationOverwriteValue = parameters.getString(JobParametersNames.ANNOTATION_OVERWRITE);

        ParametersValidatorUtil.checkIsValidString(
                annotationOverwriteValue, JobParametersNames.ANNOTATION_OVERWRITE);
        ParametersValidatorUtil.checkIsBoolean(
                annotationOverwriteValue,JobParametersNames.ANNOTATION_OVERWRITE);
    }
}
