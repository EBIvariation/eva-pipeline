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
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadFileStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that the arguments necessary to run a {@link LoadFileStepConfiguration} are
 * correctly validated
 */
public class LoadFileStepParametersValidatorTest {

    private LoadFileStepParametersValidator validator;

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    private Map<String, JobParameter<?>> requiredParameters;

    @BeforeEach
    public void setUp() throws Exception {
        validator = new LoadFileStepParametersValidator();

        requiredParameters = new TreeMap<>();
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("database", String.class));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_FILES_NAME, new JobParameter("collectionsFilesName", String.class));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_ID, new JobParameter("inputStudyId", String.class));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_NAME, new JobParameter("inputStudyName", String.class));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_TYPE, new JobParameter("COLLECTION", String.class));
        requiredParameters.put(JobParametersNames.INPUT_VCF_ID, new JobParameter("inputVcfId", String.class));
        requiredParameters.put(JobParametersNames.INPUT_VCF_AGGREGATION, new JobParameter("NONE", String.class));
        requiredParameters
                .put(JobParametersNames.INPUT_VCF, new JobParameter(temporaryFolderUtil.newFile().getCanonicalPath(), String.class));
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException, IOException {
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test
    public void dbNameIsRequired() throws Exception {
        requiredParameters.remove(JobParametersNames.DB_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void dbCollectionsFilesNameIsRequired() {
        requiredParameters.remove(JobParametersNames.DB_COLLECTIONS_FILES_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputStudyIdIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_ID);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputStudyNameIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputStudyTypeIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_TYPE);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputVcfIdIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_VCF_ID);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputVcfIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_VCF);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputVcfAggregationIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_VCF_AGGREGATION);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

}
