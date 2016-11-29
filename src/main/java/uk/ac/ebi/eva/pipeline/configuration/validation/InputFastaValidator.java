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

public class InputFastaValidator implements JobParametersValidator {
    /**
     * Checks that the fasta input file exist and is readable
     *
     * @param parameters
     * @throws JobParametersInvalidException If the file is not a valid path, does not exist or is not readable
     */
    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        ParametersValidatorUtil.checkFileExistsAndIsReadable(parameters.getString(JobParametersNames.INPUT_FASTA),
                                                             JobParametersNames.INPUT_FASTA);
    }
}
