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
 * Utility to hold the low level checks on strings, dirs, files... parameters
 */
public class ParametersValidatorUtil {

    static void checkNullOrEmptyString(String stringToValidate,
                                       String jobParametersName) throws JobParametersInvalidException {
        if (Strings.isNullOrEmpty(stringToValidate) || stringToValidate.trim().length() == 0) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s must be specified", stringToValidate, jobParametersName));
        }
    }

    static void checkBooleanStringSyntax(String booleanStringToValidate,
                                         String jobParametersName) throws JobParametersInvalidException {
        if (booleanStringToValidate == null || (!booleanStringToValidate
                .equalsIgnoreCase("true") && !booleanStringToValidate.equalsIgnoreCase("false"))) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s must be 'true' or 'false'", booleanStringToValidate, jobParametersName));
        }
    }

    static void checkDirectory(String dirToValidate, String jobParametersName) throws JobParametersInvalidException {
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

    static void checkFileExistsAndIsReadable(String fileToValidate,
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

    static int checkInteger(String numberToValidate, String jobParametersName) throws JobParametersInvalidException {
        try {
            return Integer.parseInt(numberToValidate);
        } catch (NumberFormatException e) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s is not a valid number", numberToValidate, jobParametersName));
        }
    }

    static void checkPositiveInteger(String numberToValidate,
                                     String jobParametersName) throws JobParametersInvalidException {
        int integer = checkInteger(numberToValidate, jobParametersName);

        if (integer <= 0) {
            throw new JobParametersInvalidException(
                    String.format("%s is %s, please provide a positive number", jobParametersName, numberToValidate));
        }
    }
}
