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
import uk.ac.ebi.eva.pipeline.configuration.validation.step.VepInputGeneratorStepParametersValidatorTest;

import java.io.File;
import java.io.IOException;

public class OutputDirAnnotationValidatorTest {
    private OutputDirAnnotationValidator validator;

    private JobParametersBuilder jobParametersBuilder;

    @Before
    public void setUp() throws Exception {
        validator = new OutputDirAnnotationValidator();
    }

    @Test
    public void outputDirAnnotationIsValid() throws JobParametersInvalidException, IOException {
        File writableOutputDir = new File(
                VepInputGeneratorStepParametersValidatorTest.class.getResource("/parameters-validation/").getFile());
        writableOutputDir.setWritable(true);

        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, writableOutputDir.getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void outputDirAnnotationDoesNotExist() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, "file://path/to/");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void outputDirAnnotationIsNotWritable() throws JobParametersInvalidException, IOException {
        File notWritablefile = new File(
                VepInputGeneratorStepParametersValidatorTest.class.getResource("/parameters-validation/").getFile());
        notWritablefile.setWritable(false);

        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, notWritablefile.getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void outputDirAnnotationIsAFile() throws JobParametersInvalidException, IOException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, OutputDirAnnotationValidatorTest.class.getResource(
                "/parameters-validation/vepapp.pl").getFile());
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
