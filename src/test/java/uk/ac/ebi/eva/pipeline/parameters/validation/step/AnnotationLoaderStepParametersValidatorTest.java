/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tests that the arguments necessary to run a {@link uk.ac.ebi.eva.pipeline.jobs.steps.AnnotationLoaderStep} are
 * correctly validated
 */
public class AnnotationLoaderStepParametersValidatorTest {
    private AnnotationLoaderStepParametersValidator validator;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    private Map<String, JobParameter> requiredParameters;

    private Map<String, JobParameter> optionalParameters;

    @Before
    public void setUp() throws Exception {
        boolean studyIdRequired = true;
        validator = new AnnotationLoaderStepParametersValidator(studyIdRequired);
        final String dir = temporaryFolder.getRoot().getCanonicalPath();

        requiredParameters = new TreeMap<>();
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME,
                               new JobParameter("dbCollectionsVariantName"));
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("dbName"));
        requiredParameters.put(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter(dir));
        requiredParameters.put(JobParametersNames.INPUT_VCF_ID, new JobParameter("fid"));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_ID, new JobParameter("sid"));

        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_CHUNK_SIZE, new JobParameter("100"));
        optionalParameters.put(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, new JobParameter("true"));
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException, IOException {
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test
    public void allJobParametersIncludingOptionalAreValid() throws JobParametersInvalidException, IOException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbCollectionsVariantsNameIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void outputDirAnnotationIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.OUTPUT_DIR_ANNOTATION);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIdIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.INPUT_VCF_ID);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test
    public void inputVcfIdIsNotRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.INPUT_VCF_ID);

        boolean studyIdNotRequired = false;
        validator = new AnnotationLoaderStepParametersValidator(studyIdNotRequired);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_ID);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test
    public void inputStudyIdIsNotRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_ID);

        boolean studyIdNotRequired = false;
        validator = new AnnotationLoaderStepParametersValidator(studyIdNotRequired);
        validator.validate(new JobParameters(requiredParameters));
    }
}
