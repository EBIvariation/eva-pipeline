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
import org.junit.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

public class VepNumForksValidatorTest {
    private VepNumForksValidator validator;

    private JobParametersBuilder jobParametersBuilder;

    @Before
    public void setUp() throws Exception {
        validator = new VepNumForksValidator();
    }

    @Test
    public void vepNumForksIsValid() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_NUMFORKS, "11");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsZero() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_NUMFORKS, "0");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsNegative() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_NUMFORKS, "-1");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsNotValid() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_NUMFORKS, "hello");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsEmpty() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_NUMFORKS, "");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsNull() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_NUMFORKS, null);
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
