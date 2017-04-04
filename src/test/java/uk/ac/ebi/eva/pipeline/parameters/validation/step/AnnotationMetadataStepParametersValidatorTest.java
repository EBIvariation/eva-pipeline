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
package uk.ac.ebi.eva.pipeline.parameters.validation.step;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadVepAnnotationStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tests that the arguments necessary to run a {@link LoadVepAnnotationStepConfiguration} are
 * correctly validated
 */
public class AnnotationMetadataStepParametersValidatorTest {
    private AnnotationMetadataStepParametersValidator validator;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    private Map<String, JobParameter> requiredParameters;

    @Before
    public void setUp() throws Exception {
        validator = new AnnotationMetadataStepParametersValidator();

        requiredParameters = new TreeMap<>();
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_ANNOTATION_METADATA_NAME,
                               new JobParameter("dbCollectionsVariantName"));
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("eva_testing"));
        requiredParameters.put(JobParametersNames.APP_VEP_VERSION, new JobParameter("80"));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_VERSION, new JobParameter("81"));
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException, IOException {
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbCollectionsAnnotationMetadataNameIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.DB_COLLECTIONS_ANNOTATION_METADATA_NAME);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void appVepVersionIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.APP_VEP_VERSION);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void appVepCacheVersionIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.APP_VEP_CACHE_VERSION);
        validator.validate(new JobParameters(requiredParameters));
    }
}
