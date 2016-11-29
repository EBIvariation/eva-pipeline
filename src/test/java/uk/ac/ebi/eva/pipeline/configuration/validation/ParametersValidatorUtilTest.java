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

import org.junit.Test;
import org.springframework.batch.core.JobParametersInvalidException;

import java.io.File;
import java.io.IOException;

/**
 * Test for {@link ParametersValidatorUtil}
 */
public class ParametersValidatorUtilTest {

    @Test
    public void validString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsNotNullOrEmptyString("any string", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void stringIsEmpty() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsNotNullOrEmptyString("", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void stringIsAWhitespace() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsNotNullOrEmptyString(" ", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void stringIsNull() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsNotNullOrEmptyString(null, "any job parameters name");
    }


    @Test
    public void validBooleanFalseString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("false", "any job parameters name");
    }

    @Test
    public void validBooleanFalseStringAllCapitals() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("FALSE", "any job parameters name");
    }

    @Test
    public void validBooleanTrueString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("true", "any job parameters name");
    }

    @Test
    public void validBooleanTrueStringAllCapitals() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("TRUE", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void booleanStringIsInvalid() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("blabla", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void booleanStringIsEmpty() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void booleanStringIsWhitespace() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean(" ", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void booleanStringIsNull() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean(null, "any job parameters name");
    }


    @Test
    public void directoryStringExist() throws JobParametersInvalidException {
        ParametersValidatorUtil
                .checkDirectoryExists(
                        ParametersValidatorUtilTest.class.getResource("/parameters-validation/").getFile(),
                        "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void directoryStringDoesNotExist() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkDirectoryExists("file://path/to/", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void directoryStringIsAFile() throws JobParametersInvalidException, IOException {
        File file = new File(ParametersValidatorUtilTest.class.getResource(
                "/parameters-validation/fasta_not_readable.fa").getFile());
        ParametersValidatorUtil.checkDirectoryExists(file.getCanonicalPath(), "any job parameters name");
    }


    @Test
    public void fileStringExists() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkFileExists(
                ParametersValidatorUtilTest.class.getResource("/parameters-validation/fasta.fa").getFile(),
                "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void fileStringDoesNotExist() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkFileExists("file://path/to/file.vcf", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void fileStringIsADirectory() throws JobParametersInvalidException {
        ParametersValidatorUtil
                .checkFileExists(ParametersValidatorUtilTest.class.getResource("/parameters-validation/").getFile(),
                                 "any job parameters name");
    }


    @Test
    public void pathIsReadable() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkFileIsReadable(
                ParametersValidatorUtilTest.class.getResource("/parameters-validation/fasta.fa").getFile(),
                "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void pathIsNorReadable() throws JobParametersInvalidException, IOException {
        File file = new File(
                ParametersValidatorUtilTest.class.getResource("/parameters-validation/fasta_not_readable.fa")
                        .getFile());
        file.setReadable(false);

        ParametersValidatorUtil.checkFileIsReadable(file.getCanonicalPath(), "any job parameters name");
    }

    @Test
    public void pathIsWritable() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkFileIsWritable(
                ParametersValidatorUtilTest.class.getResource("/parameters-validation/fasta.fa").getFile(),
                "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void pathIsNorWritable() throws JobParametersInvalidException, IOException {
        File file = new File(
                ParametersValidatorUtilTest.class.getResource("/parameters-validation/fasta_not_readable.fa")
                        .getFile());
        file.setReadable(false);

        ParametersValidatorUtil.checkFileIsReadable(file.getCanonicalPath(), "any job parameters name");
    }


    @Test
    public void integerStringIsValid() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("11", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsZero() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("0", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsNegative() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("-1", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsNotValid() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("hello", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsEmpty() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsNull() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger(null, "any job parameters name");
    }
}
