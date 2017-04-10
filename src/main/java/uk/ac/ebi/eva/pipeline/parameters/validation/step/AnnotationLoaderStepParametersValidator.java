/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.pipeline.parameters.validation.ConfigChunkSizeValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.ConfigRestartabilityAllowValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbCollectionsAnnotationsNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbCollectionsVariantsNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputStudyIdValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputVcfIdValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.OptionalValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.OutputDirAnnotationValidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Validates the job parameters necessary to execute an {@link uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep}
 */
public class AnnotationLoaderStepParametersValidator extends DefaultJobParametersValidator {

    private boolean isStudyIdRequired;

    public AnnotationLoaderStepParametersValidator(boolean isStudyIdRequired) {
        super(new String[]{JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME,
                           JobParametersNames.DB_NAME,
                           JobParametersNames.OUTPUT_DIR_ANNOTATION},
                new String[]{});
        this.isStudyIdRequired = isStudyIdRequired;
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);
        compositeJobParametersValidator().validate(parameters);
    }

    private CompositeJobParametersValidator compositeJobParametersValidator() {
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        Collections.addAll(jobParametersValidators,
                           new DbCollectionsAnnotationsNameValidator(),
                new DbCollectionsVariantsNameValidator(),
                new DbNameValidator(),
                new OutputDirAnnotationValidator(),
                new OptionalValidator(new ConfigRestartabilityAllowValidator(),
                                      JobParametersNames.CONFIG_RESTARTABILITY_ALLOW),
                new OptionalValidator(new ConfigChunkSizeValidator(), JobParametersNames.CONFIG_CHUNK_SIZE)
        );

        if (isStudyIdRequired) {
            jobParametersValidators.add(new InputStudyIdValidator());
            jobParametersValidators.add(new InputVcfIdValidator());
        }

        CompositeJobParametersValidator compositeJobParametersValidator = new CompositeJobParametersValidator();
        compositeJobParametersValidator.setValidators(jobParametersValidators);
        return compositeJobParametersValidator;
    }

}
