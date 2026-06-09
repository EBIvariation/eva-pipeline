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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class VepPathValidatorTest {

    private VepPathValidator validator;

    private JobParametersBuilder jobParametersBuilder;

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @BeforeEach
    public void setUp() throws Exception {
        validator = new VepPathValidator();
    }

    @Test
    public void vepPathIsValid() throws JobParametersInvalidException, IOException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH,
                temporaryFolderUtil.newFile().getCanonicalPath());
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void vepPathNotExist() {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH, "file://path/to/file.vcf");
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    @Disabled
    public void vepPathNotReadable() throws IOException {
        File file = temporaryFolderUtil.newFile("not_readable.vcf.gz");
        file.setReadable(false);

        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH, file.getCanonicalPath());
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    public void vepPathIsADirectory() throws IOException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH,
                temporaryFolderUtil.getRoot().getCanonicalPath());
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }
}
