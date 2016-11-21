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

    public VepAnnotationGeneratorStepParametersValidator() {
        super(new String[]{JobParametersNames.APP_VEP_PATH, JobParametersNames.APP_VEP_CACHE_VERSION,
                      JobParametersNames.APP_VEP_CACHE_PATH, JobParametersNames.APP_VEP_CACHE_SPECIES,
                      JobParametersNames.INPUT_FASTA, JobParametersNames.APP_VEP_NUMFORKS,
                      JobParametersNames.OUTPUT_DIR_ANNOTATION, JobParametersNames.INPUT_STUDY_ID,
                      JobParametersNames.INPUT_VCF_ID},
              new String[]{});
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);

        validateVepPath(parameters.getString(JobParametersNames.APP_VEP_PATH));
        validateVepCacheVersion(parameters.getString(JobParametersNames.APP_VEP_CACHE_VERSION));
        validateVepCachePath(parameters.getString(JobParametersNames.APP_VEP_CACHE_PATH));
        validateVepCacheSpecies(parameters.getString(JobParametersNames.APP_VEP_CACHE_SPECIES));
        validateInputFasta(parameters.getString(JobParametersNames.INPUT_FASTA));
        validateVepNumForks(parameters.getString(JobParametersNames.APP_VEP_NUMFORKS));

        //// TODO: the following validations are shared with VepInputGeneratorStepParametersValidator
        validateOutputDirAnnotation(parameters.getString(JobParametersNames.OUTPUT_DIR_ANNOTATION));
        validateInputStudyId(parameters.getString(JobParametersNames.INPUT_STUDY_ID));
        validateInputVcfId(parameters.getString(JobParametersNames.INPUT_VCF_ID));
    }

    /**
     * Checks that the vep path has been filled in
     *
     * @param vepPath The directory where vep is
     * @throws JobParametersInvalidException If the vep path is null or empty
     */
    void validateVepPath(String vepPath) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkFileExistAndReadable(vepPath);
    }

    /**
     * Checks that the cache version option has benn filled in
     *
     * @param vepCacheVersion The version of the vep cache
     * @throws JobParametersInvalidException If the vep cache version is null or empty
     */
    void validateVepCacheVersion(String vepCacheVersion) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkNullOrEmptyString(vepCacheVersion,
                                                           "A cache version for vep must be specified");
    }

    /**
     * Checks that the path of the vep cache is a valid directory
     *
     * @param vepCachePath The path of the vep cache
     * @throws JobParametersInvalidException If the vep cache path is not a valid directory
     */
    void validateVepCachePath(String vepCachePath) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkDirectory(vepCachePath, String.format(
                "The vep cache path provided %s is not valid", vepCachePath));
    }

    /**
     * Checks that the species of the vep cache has been filled in
     *
     * @param vepCacheSpecies The cache species used by vep
     * @throws JobParametersInvalidException If the vep cache species is null or empty
     */
    void validateVepCacheSpecies(String vepCacheSpecies) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkNullOrEmptyString(vepCacheSpecies, "Species option for vep must be specified");
    }

    /**
     * Checks that the fasta input file exist and is readable
     *
     * @param inputFasta Path to the fasta file used by vep
     * @throws JobParametersInvalidException If the file is not a valid path, does not exist or is not readable
     */
    void validateInputFasta(String inputFasta) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkFileExistAndReadable(inputFasta);
    }

    /**
     * Checks that the number of forks is a valid integer number
     *
     * @param vepNumForks The number of fork to use by vep
     * @throws JobParametersInvalidException If the number of fork is not a valid number
     */
    void validateVepNumForks(String vepNumForks) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkInteger(vepNumForks, String.format("%s is not a valid number of forks for vep",
                                                                            vepNumForks));
    }


    //// TODO: the following validations are shared with VepInputGeneratorStepParametersValidator

    /**
     * Checks that the output directory for annotations is a directory and it is writable
     *
     * @param outputDirAnnotation Output directory to store VEP annotations
     * @throws JobParametersInvalidException Id the output directory is not a directory
     */
    void validateOutputDirAnnotation(String outputDirAnnotation) throws JobParametersInvalidException {
        StepParametersValidatorUtil
                .checkDirectory(outputDirAnnotation, "The output directory for annotations is not valid");
    }

    /**
     * Checks that the vcf id has been filled in.
     *
     * @param inputVcfId
     * @throws JobParametersInvalidException If the vcf id is null or empty
     */
    void validateInputVcfId(String inputVcfId) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkNullOrEmptyString(inputVcfId, "A vcf id name must be specified");
    }

    /**
     * Checks that the study id has been filled in.
     *
     * @param inputStudyId
     * @throws JobParametersInvalidException If the study id is null or empty
     */
    void validateInputStudyId(String inputStudyId) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkNullOrEmptyString(inputStudyId, "A study ID name must be specified");
    }

}
