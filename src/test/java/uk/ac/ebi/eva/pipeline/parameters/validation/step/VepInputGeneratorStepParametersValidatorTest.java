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
package uk.ac.ebi.eva.pipeline.parameters.validation.step;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.VepInputGeneratorStepParametersValidator;

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

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "true");
        jobParametersBuilder.addString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, "dbCollectionsVariantName");
        jobParametersBuilder.addString(JobParametersNames.DB_NAME, "dbName");
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_ID, "inputStudyId");
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_ID, "inputVcfId");
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, DIR);

        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void optionalConfigRestartabilityAllowIsMissing() throws JobParametersInvalidException {
        final String DIR = VepAnnotationGeneratorStepParametersValidatorTest.class
                .getResource("/parameters-validation/").getPath();

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, "dbCollectionsVariantName");
        jobParametersBuilder.addString(JobParametersNames.DB_NAME, "dbName");
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_ID, "inputStudyId");
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_ID, "inputVcfId");
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, DIR);

        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void invalidAndMissingParameters() throws JobParametersInvalidException {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, "maybe");
        jobParametersBuilder.addString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, "");
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_ID, "");
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, "file://path/to/");

        validator.validate(jobParametersBuilder.toJobParameters());
    }
}