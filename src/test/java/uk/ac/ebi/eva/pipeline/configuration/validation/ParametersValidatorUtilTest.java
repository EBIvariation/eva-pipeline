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

import uk.ac.ebi.eva.pipeline.configuration.validation.step.VepAnnotationGeneratorStepParametersValidatorTest;

import java.io.File;
import java.io.IOException;

/**
 * Test for {@link ParametersValidatorUtil}
 */
public class ParametersValidatorUtilTest {

    @Test
    public void validString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkNullOrEmptyString("any string", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void emptyString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkNullOrEmptyString("", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void spaceString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkNullOrEmptyString(" ", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void nullString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkNullOrEmptyString(null, "any job parameters name");
    }


    @Test
    public void validBooleanString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkBooleanStringSyntax("false", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void invalidBooleanString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkBooleanStringSyntax("blabla", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void emptyBooleanString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkBooleanStringSyntax("", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void spaceBooleanString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkBooleanStringSyntax(" ", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void nullBooleanString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkBooleanStringSyntax(null, "any job parameters name");
    }


    @Test
    public void validDirectoryString() throws JobParametersInvalidException {
        ParametersValidatorUtil
                .checkDirectory(ParametersValidatorUtilTest.class.getResource("/parameters-validation/").getFile(),
                                "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void directoryStringDoesNotExist() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkDirectory("file://path/to/", "any job parameters name");
    }


    @Test
    public void validFileString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkFileExistsAndIsReadable(
                ParametersValidatorUtilTest.class.getResource("/parameters-validation/fasta.fa").getFile(),
                "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void fileStringDoesNotExist() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkFileExistsAndIsReadable("file://path/to/file.vcf", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void fileStringIsNotReadable() throws JobParametersInvalidException, IOException {
        File file = new File(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/fasta_not_readable.fa").getFile());
        file.setReadable(false);

        ParametersValidatorUtil.checkFileExistsAndIsReadable(file.getCanonicalPath(), "any job parameters name");
    }


    @Test
    public void validPositiveIntegerString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkPositiveInteger("11", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsZero() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkPositiveInteger("0", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsNegative() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkPositiveInteger("-1", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void integerStringIsNotValid() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkPositiveInteger("hello", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void notValidIntegerString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkPositiveInteger("", "any job parameters name");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void nullIntegerString() throws JobParametersInvalidException {
        ParametersValidatorUtil.checkPositiveInteger(null, "any job parameters name");
    }
}
