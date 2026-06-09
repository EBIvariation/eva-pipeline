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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class AnnotationOverwriteValidatorTest {

    private AnnotationOverwriteValidator validator;

    private JobParametersBuilder jobParametersBuilder;

    @BeforeEach
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

    @Test
    public void annotationOverwriteIsNotValid() {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, "blabla");
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    public void annotationOverwriteIsEmpty() {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, "");
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    public void annotationOverwriteIsWhitespace() {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.ANNOTATION_OVERWRITE, " ");
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    public void annotationOverwriteIsNull() {
        jobParametersBuilder = new JobParametersBuilder();
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }
}
