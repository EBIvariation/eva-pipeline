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

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;

import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadGenesStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.pipeline.parameters.validation.ConfigChunkSizeValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.ConfigRestartabilityAllowValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbCollectionsFeaturesNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputGtfValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.OptionalValidator;

import java.util.Arrays;
import java.util.List;

/**
 * Validates the job parameters necessary to execute an {@link LoadGenesStepConfiguration}
 */
public class LoadGenesStepParametersValidator extends DefaultJobParametersValidator {

    public LoadGenesStepParametersValidator() {
        super(new String[]{JobParametersNames.DB_COLLECTIONS_FEATURES_NAME,
                           JobParametersNames.DB_NAME,
                           JobParametersNames.INPUT_GTF},
                new String[]{});
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);
        compositeJobParametersValidator().validate(parameters);
    }

    private CompositeJobParametersValidator compositeJobParametersValidator() {
        final List<JobParametersValidator> jobParametersValidators = Arrays.asList(
                new DbCollectionsFeaturesNameValidator(),
                new DbNameValidator(),
                new InputGtfValidator(),
                new OptionalValidator(new ConfigRestartabilityAllowValidator(),
                                      JobParametersNames.CONFIG_RESTARTABILITY_ALLOW),
                new OptionalValidator(new ConfigChunkSizeValidator(), JobParametersNames.CONFIG_CHUNK_SIZE)
        );

        CompositeJobParametersValidator compositeJobParametersValidator = new CompositeJobParametersValidator();
        compositeJobParametersValidator.setValidators(jobParametersValidators);
        return compositeJobParametersValidator;
    }

}
