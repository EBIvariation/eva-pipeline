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

public class VepPathValidatorTest {

    private VepPathValidator validator;

    private JobParametersBuilder jobParametersBuilder;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    @Before
    public void setUp() throws Exception {
        validator = new VepPathValidator();
    }

    @Test
    public void vepPathIsValid() throws JobParametersInvalidException, IOException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH,
                                       temporaryFolder.newFile().getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepPathNotExist() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH, "file://path/to/file.vcf");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepPathNotReadable() throws JobParametersInvalidException, IOException {
        File file = temporaryFolder.newFile("not_readable.vcf.gz");
        file.setReadable(false);

        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH, file.getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepPathIsADirectory() throws JobParametersInvalidException, IOException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH,
                                       temporaryFolder.getRoot().getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
