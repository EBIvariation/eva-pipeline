/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.parameters.validation.job;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.CreateDatabaseIndexesStepParametersValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.LoadGenesStepParametersValidator;

import java.util.ArrayList;
import java.util.List;

public class DatabaseInitializationJobParametersValidator extends DefaultJobParametersValidator {

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        compositeJobParametersValidator(parameters).validate(parameters);
    }

    private CompositeJobParametersValidator compositeJobParametersValidator(JobParameters jobParameters) {
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();

        jobParametersValidators.add(new CreateDatabaseIndexesStepParametersValidator());
        jobParametersValidators.add(new LoadGenesStepParametersValidator());

        CompositeJobParametersValidator compositeJobParametersValidator = new CompositeJobParametersValidator();
        compositeJobParametersValidator.setValidators(jobParametersValidators);
        return compositeJobParametersValidator;
    }
}
