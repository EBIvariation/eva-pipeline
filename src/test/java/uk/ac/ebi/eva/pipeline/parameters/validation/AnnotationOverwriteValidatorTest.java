/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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

public class AnnotationOverwriteValidatorTest {

    private AnnotationOverwriteValidator validator;

    private JobParametersBuilder jobParametersBuilder;

    @Before
    public void setUp() throws Exception {
        validator = new AnnotationOverwriteValidator();
    }

    @Test
    public void annotationOverwriteIsTrue() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, "true");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void annotationOverwriteIsTrueAllCapital() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, "TRUE");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void annotationOverwriteIsFalse() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, "false");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void annotationOverwriteIsFalseAllCapital() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, "FALSE");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void annotationOverwriteIsNotValid() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, "blabla");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void annotationOverwriteIsEmpty() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, "");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void annotationOverwriteIsWhitespace() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, " ");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void annotationOverwriteIsNull() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, null);
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
