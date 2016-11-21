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
 * Utility to hold the checks on strings, dirs, files... parameters
 */
public class StepParametersValidatorUtil {

    public static void checkNullOrEmptyString(String stringToValidate,
                                              String message) throws JobParametersInvalidException {
        if (Strings.isNullOrEmpty(stringToValidate)) {
            throw new JobParametersInvalidException(message);
        }
    }

    public static void checkBooleanStringSyntax(String booleanStringToValidate,
                                                String message) throws JobParametersInvalidException {
        if (!booleanStringToValidate.equalsIgnoreCase("true") && !booleanStringToValidate.equalsIgnoreCase("false")) {
            throw new JobParametersInvalidException(message);
        }
    }

    public static void checkDirectory(String dirToValidate, String message) throws JobParametersInvalidException {
        Path path;
        try {
            path = Paths.get(dirToValidate);
        } catch (InvalidPathException e) {
            throw new JobParametersInvalidException(e.getMessage());
        }

        if (!Files.isDirectory(path)) {
            throw new JobParametersInvalidException(message);
        }
    }

    public static void checkFileExistAndReadable(String fileToValidate) throws JobParametersInvalidException {
        Path path;
        try {
            path = Paths.get(fileToValidate);
        } catch (InvalidPathException e) {
            throw new JobParametersInvalidException(e.getMessage());
        }

        if (Files.notExists(path)) {
            throw new JobParametersInvalidException(String.format("File %s does not exist", fileToValidate));
        }

        if (!Files.isReadable(path)) {
            throw new JobParametersInvalidException(String.format("File %s is not readable", fileToValidate));
        }
    }

    public static void checkInteger(String numberToValidate, String message) throws JobParametersInvalidException {
        try {
            Integer.parseInt(numberToValidate);
        } catch (NumberFormatException e) {
            throw new JobParametersInvalidException(message);
        }
    }
}
