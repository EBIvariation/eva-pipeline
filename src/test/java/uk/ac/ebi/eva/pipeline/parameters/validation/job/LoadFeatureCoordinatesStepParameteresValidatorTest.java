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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.util.Map;
import java.util.TreeMap;

import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class LoadFeatureCoordinatesStepParameteresValidatorTest {

    private static final String COLLECTION_FEATURES_NAME = "features";

    private static final String DATABASE_NAME = "databaseInitializationTestDb";

    private static final String INPUT_FILE = "/input-files/gtf/small_sample.gtf.gz";

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    private LoadFeatureCoordinatesStepParameteresValidator validator;

    private Map<String, JobParameter> requiredParameters;

    private Map<String, JobParameter> optionalParameters;

    @Before
    public void setUp() throws Exception {
        validator = new LoadFeatureCoordinatesStepParameteresValidator();

        requiredParameters = new TreeMap<>();
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter(DATABASE_NAME));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME,
                               new JobParameter(COLLECTION_FEATURES_NAME));
        requiredParameters.put(JobParametersNames.INPUT_GTF, new JobParameter(getResource(INPUT_FILE)
                .getAbsolutePath()));

        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_CHUNK_SIZE, new JobParameter("100"));
    }

    @Test
    public void allParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void allRequiredJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequired() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbCollectionFeatureNameIsRequired() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputGTFIsRequired() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.INPUT_GTF);
        validator.validate(new JobParameters(parameters));
    }
}
