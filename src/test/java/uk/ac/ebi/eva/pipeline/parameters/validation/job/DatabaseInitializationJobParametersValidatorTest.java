/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.parameters.validation.job;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class DatabaseInitializationJobParametersValidatorTest {

    private static final String COLLECTION_FEATURES_NAME = "features";

    private static final String DATABASE_NAME = "databaseInitializationTestDb";

    private static final String INPUT_FILE = "/input-files/gtf/small_sample.gtf.gz";

    private DatabaseInitializationJobParametersValidator validator;

    private Map<String, JobParameter<?>> requiredParameters;

    private Map<String, JobParameter<?>> optionalParameters;

    @BeforeEach
    public void setUp() throws Exception {
        validator = new DatabaseInitializationJobParametersValidator();

        requiredParameters = new TreeMap<>();
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter(DATABASE_NAME, String.class));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME,
                new JobParameter(COLLECTION_FEATURES_NAME, String.class));
        requiredParameters.put(JobParametersNames.INPUT_GTF, new JobParameter(getResource(INPUT_FILE)
                .getAbsolutePath(), String.class));

        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_CHUNK_SIZE, new JobParameter("100", String.class));
    }

    @Test
    public void allParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void allRequiredJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void dbNameIsRequired() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }

    @Test
    public void dbCollectionFeatureNameIsRequired() {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }

    @Test
    public void inputGTFIsRequired() {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.INPUT_GTF);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }
}
