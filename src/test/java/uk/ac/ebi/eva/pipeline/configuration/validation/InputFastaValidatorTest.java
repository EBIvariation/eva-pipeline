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
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;
import uk.ac.ebi.eva.pipeline.configuration.validation.step.VepAnnotationGeneratorStepParametersValidatorTest;

import java.io.File;
import java.io.IOException;

public class InputFastaValidatorTest {
    private InputFastaValidator validator;
    private JobParametersBuilder jobParametersBuilder;

    @Before
    public void setUp() throws Exception {
        validator = new InputFastaValidator();
    }

    @Test
    public void inputFastaIsValid() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_FASTA, (VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/fasta.fa").getFile()));
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputFastaNotExist() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_FASTA, "file://path/to/file.vcf");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputFastaNotReadable() throws JobParametersInvalidException, IOException {
        File file = new File(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/fasta_not_readable.fa").getFile());
        file.setReadable(false);

        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_FASTA, file.getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
