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
package uk.ac.ebi.eva.pipeline.parameters.validation.step;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;

import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.CalculateStatisticsTasklet;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputStudyIdValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputVcfIdValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.OptionalValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.OutputDirStatisticsValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.StatisticsOverwriteValidator;

import java.util.Arrays;
import java.util.List;

/**
 * Validates the job parameters necessary to execute a
 * {@link CalculateStatisticsTasklet}
 */
public class CalculateStatisticsStepParametersValidator extends DefaultJobParametersValidator {

    public CalculateStatisticsStepParametersValidator() {
        super(new String[]{JobParametersNames.DB_NAME,
                           JobParametersNames.INPUT_STUDY_ID,
                           JobParametersNames.INPUT_VCF_ID,
                           JobParametersNames.OUTPUT_DIR_STATISTICS},
              new String[]{});
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);
        compositeJobParametersValidator().validate(parameters);
    }

    private CompositeJobParametersValidator compositeJobParametersValidator() {
        final List<JobParametersValidator> jobParametersValidators = Arrays.asList(
                new OptionalValidator(new StatisticsOverwriteValidator(), JobParametersNames.STATISTICS_OVERWRITE),
                new DbNameValidator(),
                new InputStudyIdValidator(),
                new InputVcfIdValidator(),
                new OutputDirStatisticsValidator()
        );

        CompositeJobParametersValidator compositeJobParametersValidator = new CompositeJobParametersValidator();
        compositeJobParametersValidator.setValidators(jobParametersValidators);
        return compositeJobParametersValidator;
    }
}
