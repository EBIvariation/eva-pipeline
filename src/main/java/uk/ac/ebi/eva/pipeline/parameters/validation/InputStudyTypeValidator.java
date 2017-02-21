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
package uk.ac.ebi.eva.pipeline.parameters.validation;

import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

/**
 * Checks that the study type has been filled in and it is valid.
 *
 * @throws JobParametersInvalidException If the study type is null, empty or invalid
 */
public class InputStudyTypeValidator implements JobParametersValidator {
    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsValidString(parameters.getString(JobParametersNames.INPUT_STUDY_TYPE),
                                                   JobParametersNames.INPUT_STUDY_TYPE);

        try {
            VariantStudy.StudyType.valueOf(parameters.getString(JobParametersNames.INPUT_STUDY_TYPE));
        } catch (IllegalArgumentException e) {
            throw new JobParametersInvalidException(e.getMessage());
        }
    }
}
