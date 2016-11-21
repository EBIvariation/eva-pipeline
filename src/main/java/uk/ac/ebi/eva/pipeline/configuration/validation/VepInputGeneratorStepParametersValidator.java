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
 * Validates the job parameters necessary to execute a {@link uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep}
 * <p>
 * The parameters OUTPUT_DIR_ANNOTATION, INPUT_STUDY_ID and INPUT_VCF_ID are used to build the vep input option.
 */
public class VepInputGeneratorStepParametersValidator extends DefaultJobParametersValidator {

    public VepInputGeneratorStepParametersValidator() {
        super(new String[]{JobParametersNames.DB_NAME, JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME,
                      JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, JobParametersNames.OUTPUT_DIR_ANNOTATION,
                      JobParametersNames.INPUT_STUDY_ID, JobParametersNames.INPUT_VCF_ID},
              new String[]{});
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);

        validateDbName(parameters.getString(JobParametersNames.DB_NAME));
        validateDbCollectionsVariantsName(parameters.getString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME));
        validateConfigRestartabilityAllow(parameters.getString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW));

        //// TODO: the following validations are shared with VepInputGeneratorStepParametersValidator
        validateOutputDirAnnotation(parameters.getString(JobParametersNames.OUTPUT_DIR_ANNOTATION));
        validateInputStudyId(parameters.getString(JobParametersNames.INPUT_STUDY_ID));
        validateInputVcfId(parameters.getString(JobParametersNames.INPUT_VCF_ID));
    }

    /**
     * Checks that the database name has been filled in.
     *
     * @param dbName The database name
     * @throws JobParametersInvalidException If the database name is null or empty
     */
    void validateDbName(String dbName) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkNullOrEmptyString(dbName, "A database name must be specified");
    }

    /**
     * Checks that the name of the variants collection has been filled in.
     *
     * @param collectionsVariantsName The name of the variants collection
     * @throws JobParametersInvalidException If the variant collection name is null or empty
     */
    void validateDbCollectionsVariantsName(String collectionsVariantsName) throws JobParametersInvalidException {
        StepParametersValidatorUtil
                .checkNullOrEmptyString(collectionsVariantsName, "A collection variants name must be specified");
    }

    /**
     * Checks that the option to allow restartability has been filled in.
     *
     * @param configRestartabilityAllow Boolean option to allow restartability of a {@link org.springframework.batch.core.Step}
     * @throws JobParametersInvalidException If the allow restartability option is null or empty or any text different
     *                                       from 'true' or 'false'
     */
    void validateConfigRestartabilityAllow(String configRestartabilityAllow) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkNullOrEmptyString(configRestartabilityAllow,
                                                           "The option to allow restartability must be specified");

        StepParametersValidatorUtil.checkBooleanStringSyntax(
                configRestartabilityAllow,
                String.format("The option to allow restartability must be 'true' or 'false'. " +
                                      "The provided value is %s", configRestartabilityAllow));
    }


    //// TODO: the following validations are shared with VepInputGeneratorStepParametersValidator

    /**
     * Checks that the output directory for annotations is a directory and it is writable
     *
     * @param outputDirAnnotation Output directory to store VEP annotations
     * @throws JobParametersInvalidException If the output directory is not a directory
     */
    void validateOutputDirAnnotation(String outputDirAnnotation) throws JobParametersInvalidException {
        StepParametersValidatorUtil.checkDirectory(outputDirAnnotation, String.format(
                "The output directory for annotations provided %s is not valid", outputDirAnnotation));
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
