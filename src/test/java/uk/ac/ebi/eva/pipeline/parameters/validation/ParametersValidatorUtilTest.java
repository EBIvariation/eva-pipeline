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

import org.junit.Rule;
import org.junit.Test;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.utils.TestFileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Test for {@link ParametersValidatorUtil}
 */
public class ParametersValidatorUtilTest {

    public static final String JOB_PARAMETER_NAME = "any-job-parameter-name";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    @Test
    public void validString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsNotNullOrEmptyString("any string", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void stringIsEmpty() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsNotNullOrEmptyString("", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void stringIsAWhitespace() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsNotNullOrEmptyString(" ", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void stringIsNull() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsNotNullOrEmptyString(null, JOB_PARAMETER_NAME);
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

    @Test(expected = JobParametersInvalidException.class)
    public void booleanStringIsInvalid() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("blabla", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void booleanStringIsEmpty() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean("", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void booleanStringIsWhitespace() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean(" ", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void booleanStringIsNull() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsBoolean(null, JOB_PARAMETER_NAME);
    }


    @Test
    public void directoryStringExist() throws JobParametersInvalidException, IOException {
        ParametersValidatorUtil
                .checkDirectoryExists(TestFileUtils.getResource("/parameters-validation/").getCanonicalPath(),
                                      JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void directoryStringDoesNotExist() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkDirectoryExists("file://path/to/", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void directoryStringIsAFile() throws JobParametersInvalidException, IOException {
        File file = TestFileUtils.getResource("/parameters-validation/fasta.fa");
        ParametersValidatorUtil.checkDirectoryExists(file.getCanonicalPath(), JOB_PARAMETER_NAME);
    }


    @Test
    public void fileStringExists() throws JobParametersInvalidException, IOException {
        ParametersValidatorUtil
                .checkFileExists(TestFileUtils.getResource("/parameters-validation/fasta.fa").getCanonicalPath(),
                                 JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void fileStringDoesNotExist() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkFileExists("file://path/to/file.vcf", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void fileStringIsADirectory() throws JobParametersInvalidException, IOException {
        ParametersValidatorUtil
                .checkFileExists(TestFileUtils.getResource("/parameters-validation/").getCanonicalPath(),
                                 JOB_PARAMETER_NAME);
    }


    @Test
    public void pathIsReadable() throws JobParametersInvalidException, IOException {
        ParametersValidatorUtil
                .checkFileIsReadable(TestFileUtils.getResource("/parameters-validation/fasta.fa").getCanonicalPath(),
                                     JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void pathIsNotReadable() throws JobParametersInvalidException, IOException {
        File file = temporaryFolder.newFile("not_readable.fa");
        file.setReadable(false);

        ParametersValidatorUtil.checkFileIsReadable(file.getCanonicalPath(), JOB_PARAMETER_NAME);
    }

    @Test
    public void pathIsWritable() throws JobParametersInvalidException, IOException {
        ParametersValidatorUtil
                .checkFileIsWritable(TestFileUtils.getResource("/parameters-validation/fasta.fa").getCanonicalPath(),
                                     JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void pathIsNotWritable() throws JobParametersInvalidException, IOException {
        File file = temporaryFolder.newFile("not_writable.vcf");
        file.setWritable(false);

        ParametersValidatorUtil.checkFileIsWritable(file.getCanonicalPath(), JOB_PARAMETER_NAME);
    }


    @Test
    public void integerStringIsValid() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("1", JOB_PARAMETER_NAME);
        ParametersValidatorUtil.checkIsPositiveInteger("11", JOB_PARAMETER_NAME);
        ParametersValidatorUtil.checkIsPositiveInteger(String.valueOf(Integer.MAX_VALUE), JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsZero() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("0", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsNegative() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("-1", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsNotValid() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("hello", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsEmpty() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger("", JOB_PARAMETER_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsNull() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkIsPositiveInteger(null, JOB_PARAMETER_NAME);
    }
}
