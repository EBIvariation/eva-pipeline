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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParametersInvalidException;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for {@link ParametersValidatorUtil}
 */
public class ParametersValidatorUtilTest {

    public static final String JOB_PARAMETER_NAME = "any-job-parameter-name";

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @Test
    public void validString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsValidString("any string", JOB_PARAMETER_NAME);
    }

    @Test
    public void stringIsNull() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsNotNullString(null, JOB_PARAMETER_NAME));
    }

    @Test
    public void stringSmallerThan250Characters() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkLength("1000 Genomes Phase 3 Version 5", JOB_PARAMETER_NAME);
    }

    @Test
    public void stringBiggerThan250Characters() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkLength(
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
                JOB_PARAMETER_NAME));
    }

    @Test
    public void stringIsEmpty() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkLength("", JOB_PARAMETER_NAME));
    }

    @Test
    public void stringIsAWhitespace() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkLength(" ", JOB_PARAMETER_NAME));
    }


    @Test
    public void validBooleanFalseString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("false", JOB_PARAMETER_NAME);
    }

    @Test
    public void validBooleanFalseStringAllCapitals() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("FALSE", JOB_PARAMETER_NAME);
    }

    @Test
    public void validBooleanTrueString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("true", JOB_PARAMETER_NAME);
    }

    @Test
    public void validBooleanTrueStringAllCapitals() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("TRUE", JOB_PARAMETER_NAME);
    }

    @Test
    public void booleanStringIsInvalid() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsBoolean("blabla", JOB_PARAMETER_NAME));
    }

    @Test
    public void booleanStringIsEmpty() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsBoolean("", JOB_PARAMETER_NAME));
    }

    @Test
    public void booleanStringIsWhitespace() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsBoolean(" ", JOB_PARAMETER_NAME));
    }

    @Test
    public void booleanStringIsNull() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsBoolean(null, JOB_PARAMETER_NAME));
    }


    @Test
    public void directoryStringExist() throws JobParametersInvalidException, IOException {
        ParametersValidatorUtil.checkDirectoryExists(temporaryFolderUtil.getRoot().getCanonicalPath(), JOB_PARAMETER_NAME);
    }

    @Test
    public void directoryStringDoesNotExist() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkDirectoryExists("file://path/to/", JOB_PARAMETER_NAME));
    }

    @Test
    public void directoryStringIsAFile() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkDirectoryExists(temporaryFolderUtil.newFile().getCanonicalPath(), JOB_PARAMETER_NAME));
    }


    @Test
    public void fileStringExists() throws JobParametersInvalidException, IOException {
        ParametersValidatorUtil.checkFileExists(temporaryFolderUtil.newFile().getCanonicalPath(), JOB_PARAMETER_NAME);
    }

    @Test
    public void fileStringDoesNotExist() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkFileExists("file://path/to/file.vcf", JOB_PARAMETER_NAME));
    }

    @Test
    public void fileStringIsADirectory() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkFileExists(temporaryFolderUtil.getRoot().getCanonicalPath(), JOB_PARAMETER_NAME));
    }


    @Test
    public void pathIsReadable() throws JobParametersInvalidException, IOException {
        ParametersValidatorUtil.checkFileIsReadable(temporaryFolderUtil.newFile().getCanonicalPath(), JOB_PARAMETER_NAME);
    }

    @Test
    @Disabled
    public void pathIsNotReadable() throws IOException {
        File file = temporaryFolderUtil.newFile("not_readable.fa");
        file.setReadable(false);
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkFileIsReadable(file.getCanonicalPath(), JOB_PARAMETER_NAME));
    }

    @Test
    public void pathIsWritable() throws JobParametersInvalidException, IOException {
        ParametersValidatorUtil.checkFileIsWritable(temporaryFolderUtil.newFile().getCanonicalPath(), JOB_PARAMETER_NAME);
    }

    @Test
    @Disabled
    public void pathIsNotWritable() throws IOException {
        File file = temporaryFolderUtil.newFile("not_writable.vcf");
        file.setWritable(false);
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkFileIsWritable(file.getCanonicalPath(), JOB_PARAMETER_NAME));
    }


    @Test
    public void integerStringIsValid() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("1", JOB_PARAMETER_NAME);
        ParametersValidatorUtil.checkIsPositiveInteger("11", JOB_PARAMETER_NAME);
        ParametersValidatorUtil.checkIsPositiveInteger(String.valueOf(Integer.MAX_VALUE), JOB_PARAMETER_NAME);
    }

    @Test
    public void integerStringIsZero() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsPositiveInteger("0", JOB_PARAMETER_NAME));
    }

    @Test
    public void integerStringIsNegative() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsPositiveInteger("-1", JOB_PARAMETER_NAME));
    }

    @Test
    public void integerStringIsNotValid() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsPositiveInteger("hello", JOB_PARAMETER_NAME));
    }

    @Test
    public void integerStringIsEmpty() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsPositiveInteger("", JOB_PARAMETER_NAME));
    }

    @Test
    public void integerStringIsNull() {
        assertThrows(JobParametersInvalidException.class, () -> ParametersValidatorUtil.checkIsPositiveInteger(null, JOB_PARAMETER_NAME));
    }
}
