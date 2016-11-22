/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.validation;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tests that the arguments necessary to run a {@link uk.ac.ebi.eva.pipeline.jobs.steps.VepInputGeneratorStep} are
 * correctly validated
 */
public class VepInputGeneratorStepParametersValidatorTest {
    private VepInputGeneratorStepParametersValidator validator;

    @Before
    public void initialize() {
        validator = new VepInputGeneratorStepParametersValidator();
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException {
        final String DIR = VepAnnotationGeneratorStepParametersValidatorTest.class
                .getResource("/parameters-validation/").getPath();

        final Map<String, JobParameter> parameters = new LinkedHashMap<>();
        parameters.putIfAbsent(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, new JobParameter("true"));
        parameters.putIfAbsent(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME,
                               new JobParameter("dbCollectionsVariantName"));
        parameters.putIfAbsent(JobParametersNames.DB_NAME, new JobParameter("dbName"));
        parameters.putIfAbsent(JobParametersNames.INPUT_STUDY_ID, new JobParameter("inputStudyId"));
        parameters.putIfAbsent(JobParametersNames.INPUT_VCF_ID, new JobParameter("inputVcfId"));
        parameters.putIfAbsent(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter(DIR));

        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void invalidAndMissingParameters() throws JobParametersInvalidException {
        final Map<String, JobParameter> parameters = new LinkedHashMap<>();
        parameters.putIfAbsent(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, new JobParameter("maybe"));
        parameters.putIfAbsent(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, new JobParameter(""));
        parameters.putIfAbsent(JobParametersNames.DB_NAME, new JobParameter(""));
        parameters.putIfAbsent(JobParametersNames.INPUT_STUDY_ID, new JobParameter(""));
        parameters.putIfAbsent(JobParametersNames.INPUT_VCF_ID, new JobParameter(""));
        parameters.putIfAbsent(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter("file://path/to/"));

        validator.validate(new JobParameters(parameters));
    }
}