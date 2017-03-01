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
package uk.ac.ebi.eva.pipeline.parameters.validation;

import org.springframework.batch.core.JobParametersInvalidException;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Utility class to hold the low level checks on strings, dirs, files... parameters
 */
public class ParametersValidatorUtil {

    static void checkIsValidString(String stringToValidate,
                                   String jobParametersName) throws JobParametersInvalidException {
        checkIsNotNullString(stringToValidate, jobParametersName);
        checkDoesNotContainPrintableCharacters(stringToValidate, jobParametersName);
        checkLength(stringToValidate, jobParametersName);
    }

    /**
     * \n or \r are valid non-printable characters
     */
    static void checkDoesNotContainPrintableCharacters(String stringToValidate,
                                                       String jobParametersName) throws JobParametersInvalidException {
        Pattern regex = Pattern.compile("[\\p{C}&&[^\n]&&[^\r]]");

        if (regex.matcher(stringToValidate).find()) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s contains non printable characters", stringToValidate, jobParametersName));
        }
    }

    static void checkIsNotNullString(String stringToValidate,
                                     String jobParametersName) throws JobParametersInvalidException {
        if (stringToValidate == null) {
            throw new JobParametersInvalidException(
                    String.format("%s value is null", jobParametersName));
        }
    }

    static void checkLength(String stringToValidate, String jobParametersName) throws JobParametersInvalidException {
        if (stringToValidate.length() >= 250) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s can't exceed 250 characters", stringToValidate, jobParametersName));
        }

        if (stringToValidate.trim().length() == 0) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s must be specified", stringToValidate, jobParametersName));
        }
    }

    static void checkIsBoolean(String booleanStringToValidate,
                               String jobParametersName) throws JobParametersInvalidException {
        if (booleanStringToValidate == null || (!booleanStringToValidate
                .equalsIgnoreCase("true") && !booleanStringToValidate.equalsIgnoreCase("false"))) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s must be 'true' or 'false'", booleanStringToValidate, jobParametersName));
        }
    }

    static void checkDirectoryExists(String dirToValidate,
                                     String jobParametersName) throws JobParametersInvalidException {
        Path path = getPath(dirToValidate, jobParametersName);

        if (!Files.isDirectory(path)) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s is not a valid directory", dirToValidate, jobParametersName));
        }
    }

    static void checkFileExists(String fileToValidate,
                                String jobParametersName) throws JobParametersInvalidException {
        Path path = getPath(fileToValidate, jobParametersName);

        if (Files.notExists(path) || Files.isDirectory(path)) {
            throw new JobParametersInvalidException(
                    String.format("File %s in %s does not exist", fileToValidate, jobParametersName));
        }
    }

    static void checkFileIsReadable(String fileToValidate,
                                    String jobParametersName) throws JobParametersInvalidException {
        Path path = getPath(fileToValidate, jobParametersName);

        if (!Files.isReadable(path)) {
            throw new JobParametersInvalidException(
                    String.format("File %s in %s is not readable", fileToValidate, jobParametersName));
        }
    }

    static void checkFileIsWritable(String fileToValidate,
                                    String jobParametersName) throws JobParametersInvalidException {
        Path path = getPath(fileToValidate, jobParametersName);

        if (!Files.isWritable(path)) {
            throw new JobParametersInvalidException(
                    String.format("File %s in %s is not writable", fileToValidate, jobParametersName));
        }
    }

    private static Path getPath(String pathToValidate, String jobParametersName) throws JobParametersInvalidException {
        Path path;
        try {
            path = Paths.get(pathToValidate);
        } catch (InvalidPathException e) {
            throw new JobParametersInvalidException(
                    String.format("Path %s in %s is not valid", pathToValidate, jobParametersName));
        }
        return path;
    }

    static int checkIsInteger(String numberToValidate, String jobParametersName) throws JobParametersInvalidException {
        try {
            return Integer.parseInt(numberToValidate);
        } catch (NumberFormatException e) {
            throw new JobParametersInvalidException(
                    String.format("%s in %s is not a valid number", numberToValidate, jobParametersName));
        }
    }

    static void checkIsPositiveInteger(String numberToValidate,
                                       String jobParametersName) throws JobParametersInvalidException {
        int integer = checkIsInteger(numberToValidate, jobParametersName);

        if (integer <= 0) {
            throw new JobParametersInvalidException(
                    String.format("%s is %s, please provide a positive number", jobParametersName, numberToValidate));
        }
    }
}
