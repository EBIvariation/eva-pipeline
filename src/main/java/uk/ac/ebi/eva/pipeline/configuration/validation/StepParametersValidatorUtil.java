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

import com.google.common.base.Strings;
import org.springframework.batch.core.JobParametersInvalidException;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility to hold the checks on {@link org.springframework.batch.core.JobParameters}
 * and low level checks for strings, dirs, files... parameters
 */
public class StepParametersValidatorUtil {

    /**
     * Checks that the vep path has been filled in
     *
     * @param vepPath           The directory where vep is
     * @param jobParametersName
     * @throws JobParametersInvalidException If the vep path is null or empty
     */
    void validateVepPath(String vepPath, String jobParametersName) throws JobParametersInvalidException {
        checkFileExistsAndIsReadable(vepPath, jobParametersName);
    }

    /**
     * Checks that the cache version option has benn filled in
     *
     * @param vepCacheVersion   The version of the vep cache
     * @param jobParametersName
     * @throws JobParametersInvalidException If the vep cache version is null or empty
     */
    void validateVepCacheVersion(String vepCacheVersion,
                                 String jobParametersName) throws JobParametersInvalidException {
        checkNullOrEmptyString(vepCacheVersion, jobParametersName);
    }

    /**
     * Checks that the path of the vep cache is a valid directory
     *
     * @param vepCachePath      The path of the vep cache
     * @param jobParametersName
     * @throws JobParametersInvalidException If the vep cache path is not a valid directory
     */
    void validateVepCachePath(String vepCachePath, String jobParametersName) throws JobParametersInvalidException {
        checkDirectory(vepCachePath, jobParametersName);
    }

    /**
     * Checks that the species of the vep cache has been filled in
     *
     * @param vepCacheSpecies   The cache species used by vep
     * @param jobParametersName
     * @throws JobParametersInvalidException If the vep cache species is null or empty
     */
    void validateVepCacheSpecies(String vepCacheSpecies,
                                 String jobParametersName) throws JobParametersInvalidException {
        checkNullOrEmptyString(vepCacheSpecies, jobParametersName);
    }

    /**
     * Checks that the fasta input file exist and is readable
     *
     * @param inputFasta        Path to the fasta file used by vep
     * @param jobParametersName
     * @throws JobParametersInvalidException If the file is not a valid path, does not exist or is not readable
     */
    void validateInputFasta(String inputFasta, String jobParametersName) throws JobParametersInvalidException {
        checkFileExistsAndIsReadable(inputFasta, jobParametersName);
    }

    /**
     * Checks that the number of forks is a valid integer number
     *
     * @param vepNumForks       The number of fork to use by vep
     * @param jobParametersName
     * @throws JobParametersInvalidException If the number of fork is not a valid number
     */
    void validateVepNumForks(String vepNumForks, String jobParametersName) throws JobParametersInvalidException {
        checkPositiveInteger(vepNumForks, jobParametersName);
    }

    /**
     * Checks that the output directory for annotations is a directory and it is writable
     *
     * @param outputDirAnnotation Output directory to store VEP annotations
     * @param jobParametersName
     * @throws JobParametersInvalidException Id the output directory is not a directory
     */
    void validateOutputDirAnnotation(String outputDirAnnotation,
                                     String jobParametersName) throws JobParametersInvalidException {
        checkDirectory(outputDirAnnotation, jobParametersName);
    }

    /**
     * Checks that the VCF id has been filled in.
     *
     * @param inputVcfId
     * @param jobParametersName
     * @throws JobParametersInvalidException If the VCF id is null or empty
     */
    void validateInputVcfId(String inputVcfId, String jobParametersName) throws JobParametersInvalidException {
        checkNullOrEmptyString(inputVcfId, jobParametersName);
    }

    /**
     * Checks that the study id has been filled in.
     *
     * @param inputStudyId
     * @param jobParametersName
     * @throws JobParametersInvalidException If the study id is null or empty
     */
    void validateInputStudyId(String inputStudyId, String jobParametersName) throws JobParametersInvalidException {
        checkNullOrEmptyString(inputStudyId, jobParametersName);
    }

    /**
     * Checks that the database name has been filled in.
     *
     * @param dbName            The database name
     * @param jobParametersName
     * @throws JobParametersInvalidException If the database name is null or empty
     */
    void validateDbName(String dbName, String jobParametersName) throws JobParametersInvalidException {
        checkNullOrEmptyString(dbName, jobParametersName);
    }

    /**
     * Checks that the name of the variants collection has been filled in.
     *
     * @param collectionsVariantsName The name of the variants collection
     * @param jobParametersName
     * @throws JobParametersInvalidException If the variant collection name is null or empty
     */
    void validateDbCollectionsVariantsName(String collectionsVariantsName,
                                           String jobParametersName) throws JobParametersInvalidException {
        checkNullOrEmptyString(collectionsVariantsName, jobParametersName);
    }

    /**
     * Checks that the option to allow restartability has been filled in and it is "true" or "false".
     *
     * @param configRestartabilityAllow Boolean option to allow restartability of a {@link org.springframework.batch.core.Step}
     * @param jobParametersName
     * @throws JobParametersInvalidException If the allow restartability option is null or empty or any text different
     *                                       from 'true' or 'false'
     */
    void validateConfigRestartabilityAllow(String configRestartabilityAllow,
                                           String jobParametersName) throws JobParametersInvalidException {
        checkNullOrEmptyString(configRestartabilityAllow, jobParametersName);
        checkBooleanStringSyntax(configRestartabilityAllow, jobParametersName);
    }


    private void checkNullOrEmptyString(String stringToValidate,
                                        String jobParametersName) throws JobParametersInvalidException {
        if (Strings.isNullOrEmpty(stringToValidate) || stringToValidate.trim().length() == 0) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s must be specified", stringToValidate, jobParametersName));
        }
    }

    private void checkBooleanStringSyntax(String booleanStringToValidate,
                                          String jobParametersName) throws JobParametersInvalidException {
        if (!booleanStringToValidate.equalsIgnoreCase("true") && !booleanStringToValidate.equalsIgnoreCase("false")) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s must be 'true' or 'false'", booleanStringToValidate, jobParametersName));
        }
    }

    private void checkDirectory(String dirToValidate, String jobParametersName) throws JobParametersInvalidException {
        Path path;
        try {
            path = Paths.get(dirToValidate);
        } catch (InvalidPathException e) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s is not a valid path", dirToValidate, jobParametersName));
        }

        if (!Files.isDirectory(path)) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s is not a valid directory", dirToValidate, jobParametersName));
        }
    }

    private void checkFileExistsAndIsReadable(String fileToValidate,
                                              String jobParametersName) throws JobParametersInvalidException {
        Path path;
        try {
            path = Paths.get(fileToValidate);
        } catch (InvalidPathException e) {
            throw new JobParametersInvalidException(
                    String.format("File %s in %s is not a valid path", fileToValidate, jobParametersName));
        }

        if (Files.notExists(path)) {
            throw new JobParametersInvalidException(
                    String.format("File %s in %s does not exist", fileToValidate, jobParametersName));
        }

        if (!Files.isReadable(path)) {
            throw new JobParametersInvalidException(
                    String.format("File %s in %s is not readable", fileToValidate, jobParametersName));
        }
    }

    private int checkInteger(String numberToValidate, String jobParametersName) throws JobParametersInvalidException {
        try {
            return Integer.parseInt(numberToValidate);
        } catch (NumberFormatException e) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s is not a valid number", numberToValidate, jobParametersName));
        }
    }

    private void checkPositiveInteger(String numberToValidate,
                                      String jobParametersName) throws JobParametersInvalidException {
        int integer = checkInteger(numberToValidate, jobParametersName);

        if (integer <= 0) {
            throw new JobParametersInvalidException(
                    String.format("%s is %s, please provide a positive number", jobParametersName, numberToValidate));
        }
    }
}
