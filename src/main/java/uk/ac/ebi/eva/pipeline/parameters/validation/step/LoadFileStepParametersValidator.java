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

import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadFileStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbCollectionsFilesNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputStudyIdValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputStudyNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputStudyTypeValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputVcfAggregationValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputVcfIdValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputVcfValidator;

import java.util.Arrays;
import java.util.List;

/**
 * Validates the job parameters necessary to execute a {@link LoadFileStepConfiguration}
 */
public class LoadFileStepParametersValidator extends DefaultJobParametersValidator {

    public LoadFileStepParametersValidator() {
        super(new String[]{JobParametersNames.DB_NAME,
                      JobParametersNames.DB_COLLECTIONS_FILES_NAME,
                      JobParametersNames.INPUT_STUDY_ID,
                      JobParametersNames.INPUT_STUDY_NAME,
                      JobParametersNames.INPUT_STUDY_TYPE,
                      JobParametersNames.INPUT_VCF,
                      JobParametersNames.INPUT_VCF_ID,
                      JobParametersNames.INPUT_VCF_AGGREGATION},
              new String[]{});
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);
        compositeJobParametersValidator().validate(parameters);
    }

    private CompositeJobParametersValidator compositeJobParametersValidator() {
        final List<JobParametersValidator> jobParametersValidators = Arrays.asList(
                new DbNameValidator(),
                new DbCollectionsFilesNameValidator(),
                new InputStudyIdValidator(),
                new InputStudyNameValidator(),
                new InputStudyTypeValidator(),
                new InputVcfValidator(),
                new InputVcfIdValidator(),
                new InputVcfAggregationValidator()
        );

        CompositeJobParametersValidator compositeJobParametersValidator = new CompositeJobParametersValidator();
        compositeJobParametersValidator.setValidators(jobParametersValidators);
        return compositeJobParametersValidator;
    }
}
