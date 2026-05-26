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
package uk.ac.ebi.eva.pipeline.parameters.validation.step;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadFeatureCoordinatesStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that the arguments necessary to run a {@link LoadFeatureCoordinatesStepConfiguration} are
 * correctly validated
 */
public class LoadFeatureCoordinatesStepParameteresValidatorTest {
    private LoadFeatureCoordinatesStepParameteresValidator validator;

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    private Map<String, JobParameter<?>> requiredParameters;

    private Map<String, JobParameter<?>> optionalParameters;

    @BeforeEach
    public void setUp() throws Exception {
        validator = new LoadFeatureCoordinatesStepParameteresValidator();

        requiredParameters = new TreeMap<>();
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("dbName", String.class));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME,
                new JobParameter("dbCollectionsFeaturesName", String.class));
        requiredParameters.put(JobParametersNames.INPUT_GTF,
                new JobParameter(temporaryFolderUtil.newFile().getCanonicalPath(), String.class));

        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_CHUNK_SIZE, new JobParameter("100", String.class));
        optionalParameters.put(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, new JobParameter("true", String.class));
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException, IOException {
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test
    public void allJobParametersIncludingOptionalAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void dbCollectionsFeaturesNameIsRequired() {
        requiredParameters.remove(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void dbNameIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.DB_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputGtfIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_GTF);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

}
