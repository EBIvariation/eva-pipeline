/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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

public class DbCollectionsFeaturesNameValidatorTest {

    private DbCollectionsFeaturesNameValidator validator;

    private JobParametersBuilder jobParametersBuilder;

    @BeforeEach
    public void setUp() throws Exception {
        validator = new DbCollectionsFeaturesNameValidator();
    }

    @Test
    public void collectionsFeaturesNameIsValid() throws JobParametersInvalidException {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME, "collectionsFeaturesName");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void collectionsFeaturesNameIsEmpty() {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME, "");
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    public void collectionsFeaturesNameIsWhitespace() {
        jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME, " ");
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }

    @Test
    public void collectionsFeaturesNameIsNull() {
        jobParametersBuilder = new JobParametersBuilder();
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(jobParametersBuilder.toJobParameters()));
    }
}
