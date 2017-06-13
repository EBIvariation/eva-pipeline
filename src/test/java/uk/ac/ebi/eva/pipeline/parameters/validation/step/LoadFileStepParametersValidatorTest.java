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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadFileStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tests that the arguments necessary to run a {@link LoadFileStepConfiguration} are
 * correctly validated
 */
public class LoadFileStepParametersValidatorTest {

    private LoadFileStepParametersValidator validator;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    private Map<String, JobParameter> requiredParameters;

    @Before
    public void setUp() throws Exception {
        validator = new LoadFileStepParametersValidator();

        requiredParameters = new TreeMap<>();
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("database"));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_FILES_NAME, new JobParameter("collectionsFilesName"));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_ID, new JobParameter("inputStudyId"));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_NAME, new JobParameter("inputStudyName"));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_TYPE, new JobParameter("COLLECTION"));
        requiredParameters.put(JobParametersNames.INPUT_VCF_ID, new JobParameter("inputVcfId"));
        requiredParameters.put(JobParametersNames.INPUT_VCF_AGGREGATION, new JobParameter("NONE"));
        requiredParameters
                .put(JobParametersNames.INPUT_VCF, new JobParameter(temporaryFolderRule.newFile().getCanonicalPath()));
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException, IOException {
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequired() throws Exception {
        requiredParameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbCollectionsFilesNameIsRequired() throws Exception {
        requiredParameters.remove(JobParametersNames.DB_COLLECTIONS_FILES_NAME);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsRequired() throws Exception {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_ID);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyNameIsRequired() throws Exception {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_NAME);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyTypeIsRequired() throws Exception {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_TYPE);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIdIsRequired() throws Exception {
        requiredParameters.remove(JobParametersNames.INPUT_VCF_ID);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIsRequired() throws Exception {
        requiredParameters.remove(JobParametersNames.INPUT_VCF);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfAggregationIsRequired() throws Exception {
        requiredParameters.remove(JobParametersNames.INPUT_VCF_AGGREGATION);
        validator.validate(new JobParameters(requiredParameters));
    }

}
