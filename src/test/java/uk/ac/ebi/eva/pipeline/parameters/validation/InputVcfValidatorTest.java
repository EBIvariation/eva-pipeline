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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.File;
import java.io.IOException;

public class InputVcfValidatorTest {

    private InputVcfValidator validator;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    @Before
    public void setUp() throws Exception {
        validator = new InputVcfValidator();
    }

    @Test
    public void inputVcfIsValid() throws JobParametersInvalidException, IOException {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF, temporaryFolder.newFile().getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfNotExist() throws JobParametersInvalidException {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF, "file://path/to/file.vcf");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfNotReadable() throws JobParametersInvalidException, IOException {
        File file = temporaryFolder.newFile("not_readable.vcf");
        file.setReadable(false);

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF, file.getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIsADirectory() throws JobParametersInvalidException, IOException {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF, temporaryFolder.getRoot().getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
