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
package uk.ac.ebi.eva.pipeline.configuration.validation.step;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;
import uk.ac.ebi.eva.pipeline.configuration.validation.InputFastaValidator;
import uk.ac.ebi.eva.pipeline.configuration.validation.InputStudyIdValidator;
import uk.ac.ebi.eva.pipeline.configuration.validation.InputVcfIdValidator;
import uk.ac.ebi.eva.pipeline.configuration.validation.OutputDirAnnotationValidator;
import uk.ac.ebi.eva.pipeline.configuration.validation.VepCachePathValidator;
import uk.ac.ebi.eva.pipeline.configuration.validation.VepCacheSpeciesValidator;
import uk.ac.ebi.eva.pipeline.configuration.validation.VepCacheVersionValidator;
import uk.ac.ebi.eva.pipeline.configuration.validation.VepNumForksValidator;
import uk.ac.ebi.eva.pipeline.configuration.validation.VepPathValidator;

import java.util.Arrays;
import java.util.List;

/**
 * Validates the job parameters necessary to execute a {@link uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep}
 * <p>
 * The parameters OUTPUT_DIR_ANNOTATION, INPUT_STUDY_ID and INPUT_VCF_ID are used to build the vep input/output options
 */
public class VepAnnotationGeneratorStepParametersValidator extends DefaultJobParametersValidator {

    public VepAnnotationGeneratorStepParametersValidator() {
        super(new String[]{JobParametersNames.APP_VEP_CACHE_PATH, JobParametersNames.APP_VEP_CACHE_SPECIES,
                      JobParametersNames.APP_VEP_CACHE_VERSION, JobParametersNames.APP_VEP_NUMFORKS,
                      JobParametersNames.APP_VEP_PATH, JobParametersNames.INPUT_FASTA, JobParametersNames.INPUT_STUDY_ID,
                      JobParametersNames.INPUT_VCF_ID, JobParametersNames.OUTPUT_DIR_ANNOTATION},
              new String[]{});
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);
        compositeJobParametersValidator().validate(parameters);
    }

    private CompositeJobParametersValidator compositeJobParametersValidator() {
        final List<JobParametersValidator> jobParametersValidators = Arrays.asList(
                new VepPathValidator(),
                new VepCacheVersionValidator(),
                new VepCachePathValidator(),
                new VepCacheSpeciesValidator(),
                new InputFastaValidator(),
                new VepNumForksValidator(),
                new OutputDirAnnotationValidator(),
                new InputStudyIdValidator(),
                new InputVcfIdValidator()
        );

        CompositeJobParametersValidator compositeJobParametersValidator = new CompositeJobParametersValidator();
        compositeJobParametersValidator.setValidators(jobParametersValidators);
        return compositeJobParametersValidator;
    }

}
