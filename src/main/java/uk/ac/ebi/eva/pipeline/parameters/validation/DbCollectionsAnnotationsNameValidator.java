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
 * Checks that the name of the annotations collection has been filled in.
 *
 * @throws JobParametersInvalidException If the annotation collection name is null or empty
 */
public class DbCollectionsAnnotationsNameValidator implements JobParametersValidator {
    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        ParametersValidatorUtil
                .checkIsValidString(parameters.getString(JobParametersNames.DB_COLLECTIONS_ANNOTATIONS_NAME),
                                    JobParametersNames.DB_COLLECTIONS_ANNOTATIONS_NAME);
    }
}
