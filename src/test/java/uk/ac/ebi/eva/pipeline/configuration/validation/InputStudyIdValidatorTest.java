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

public class InputStudyIdValidatorTest {
    private InputStudyIdValidator validator;
    private JobParametersBuilder jobParametersBuilder;

    @Before
    public void setUp() throws Exception {
        validator = new InputStudyIdValidator();
    }

    @Test
    public void inputStudyIdIsValid() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_ID, "inputStudyId");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsEmpty() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_ID, "");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsWhitespace() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_ID, " ");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsNull() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_ID, null);
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
