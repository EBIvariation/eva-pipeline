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
import org.springframework.batch.core.job.DefaultJobParametersValidator;

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;

/**
 * Validates the job parameters necessary to execute a {@link uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep}
 * <p>
 * The parameters OUTPUT_DIR_ANNOTATION, INPUT_STUDY_ID and INPUT_VCF_ID are used to build the vep input/output options
 */
public class VepAnnotationGeneratorStepParametersValidator extends DefaultJobParametersValidator {

    private StepParametersValidatorUtil parametersValidator;

    public VepAnnotationGeneratorStepParametersValidator() {
        super(new String[]{JobParametersNames.APP_VEP_CACHE_PATH, JobParametersNames.APP_VEP_CACHE_SPECIES,
                      JobParametersNames.APP_VEP_CACHE_VERSION, JobParametersNames.APP_VEP_NUMFORKS,
                      JobParametersNames.APP_VEP_PATH, JobParametersNames.INPUT_FASTA, JobParametersNames.INPUT_STUDY_ID,
                      JobParametersNames.INPUT_VCF_ID, JobParametersNames.OUTPUT_DIR_ANNOTATION,
              },
              new String[]{});

        parametersValidator = new StepParametersValidatorUtil();
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);

        parametersValidator.validateVepPath(parameters.getString(JobParametersNames.APP_VEP_PATH),
                                            JobParametersNames.APP_VEP_PATH);
        parametersValidator.validateVepCacheVersion(parameters.getString(JobParametersNames.APP_VEP_CACHE_VERSION),
                                                    JobParametersNames.APP_VEP_CACHE_VERSION);
        parametersValidator.validateVepCachePath(parameters.getString(JobParametersNames.APP_VEP_CACHE_PATH),
                                                 JobParametersNames.APP_VEP_CACHE_PATH);
        parametersValidator.validateVepCacheSpecies(parameters.getString(JobParametersNames.APP_VEP_CACHE_SPECIES),
                                                    JobParametersNames.APP_VEP_CACHE_SPECIES);
        parametersValidator.validateInputFasta(parameters.getString(JobParametersNames.INPUT_FASTA),
                                               JobParametersNames.INPUT_FASTA);
        parametersValidator.validateVepNumForks(parameters.getString(JobParametersNames.APP_VEP_NUMFORKS),
                                                JobParametersNames.APP_VEP_NUMFORKS);
        parametersValidator.validateOutputDirAnnotation(parameters.getString(JobParametersNames.OUTPUT_DIR_ANNOTATION),
                                                        JobParametersNames.OUTPUT_DIR_ANNOTATION);
        parametersValidator.validateInputStudyId(parameters.getString(JobParametersNames.INPUT_STUDY_ID),
                                                 JobParametersNames.INPUT_STUDY_ID);
        parametersValidator.validateInputVcfId(parameters.getString(JobParametersNames.INPUT_VCF_ID),
                                               JobParametersNames.INPUT_VCF_ID);
    }

}
