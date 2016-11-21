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

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobParametersInvalidException;

import java.io.File;
import java.io.IOException;

/**
 * Tests that the arguments necessary to run a {@link uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep} are
 * correctly validated
 */
public class VepAnnotationGeneratorStepParametersValidatorTest {
    private VepAnnotationGeneratorStepParametersValidator validator;

    @Before
    public void initialize() {
        validator = new VepAnnotationGeneratorStepParametersValidator();
    }

    @Test
    public void vepPathIsValid() throws JobParametersInvalidException {
        validator.validateVepPath(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/vepapp.pl").getFile());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepPathNotExist() throws JobParametersInvalidException {
        validator.validateVepPath("file://path/to/file.vcf");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepPathNotReadable() throws JobParametersInvalidException, IOException {
        File file = new File(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/input_not_readable.vcf.gz").getFile());
        file.setReadable(false);

        validator.validateVepPath(file.getCanonicalPath());
    }


    @Test
    public void vepCacheVersionIsValid() throws JobParametersInvalidException {
        validator.validateVepCacheVersion("vepCacheVersion");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCacheVersionIsEmpty() throws JobParametersInvalidException {
        validator.validateVepCacheVersion("");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCacheVersionIsNull() throws JobParametersInvalidException {
        validator.validateVepCacheVersion(null);
    }


    @Test
    public void vepCachePathIsValid() throws JobParametersInvalidException {
        validator.validateVepCachePath(
                VepInputGeneratorStepParametersValidatorTest.class.getResource("/parameters-validation/").getFile());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCachePathNotExist() throws JobParametersInvalidException {
        validator.validateVepCachePath("file://path/to/");
    }


    @Test
    public void vepChacheSpeciesIsValid() throws JobParametersInvalidException {
        validator.validateVepCacheSpecies("vepChacheSpecies");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepChacheSpeciesIsEmpty() throws JobParametersInvalidException {
        validator.validateVepCacheSpecies("");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepChacheSpeciesIsNull() throws JobParametersInvalidException {
        validator.validateVepCacheSpecies(null);
    }


    @Test
    public void inputFastaIsValid() throws JobParametersInvalidException {
        validator.validateInputFasta(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/fasta.fa").getFile());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputFastaNotExist() throws JobParametersInvalidException {
        validator.validateInputFasta("file://path/to/file.vcf");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputFastaNotReadable() throws JobParametersInvalidException, IOException {
        File file = new File(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/fasta_not_readable.fa").getFile());
        file.setReadable(false);

        validator.validateInputFasta(file.getCanonicalPath());
    }


    @Test
    public void vepNumForksIsValid() throws JobParametersInvalidException {
        validator.validateVepNumForks("11");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsNotValid() throws JobParametersInvalidException {
        validator.validateVepNumForks("hello");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsEmpty() throws JobParametersInvalidException {
        validator.validateVepNumForks("");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsNull() throws JobParametersInvalidException {
        validator.validateVepNumForks(null);
    }

    //TODO outputDirAnnotation, inputVcfId, inputStudyId validations test are the same in
    // VepInputGeneratorStepParametersValidatorTest
}
