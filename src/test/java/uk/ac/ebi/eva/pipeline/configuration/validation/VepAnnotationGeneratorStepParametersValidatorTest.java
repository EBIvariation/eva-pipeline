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
 * Tests that the arguments necessary to run a {@link uk.ac.ebi.eva.pipeline.jobs.steps.VepAnnotationGeneratorStep} are
 * correctly validated
 */
public class VepAnnotationGeneratorStepParametersValidatorTest {
    private VepAnnotationGeneratorStepParametersValidator validator;

    @Before
    public void initialize() {
        validator = new VepAnnotationGeneratorStepParametersValidator();
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException {
        final String APP_VEP_PATH = VepAnnotationGeneratorStepParametersValidatorTest.class
                .getResource("/parameters-validation/vepapp.pl").getPath();
        final String DIR = VepAnnotationGeneratorStepParametersValidatorTest.class
                .getResource("/parameters-validation/").getPath();
        final String INPUT_FASTA = VepAnnotationGeneratorStepParametersValidatorTest.class
                .getResource("/parameters-validation/fasta.fa").getPath();

        final Map<String, JobParameter> parameters = new LinkedHashMap<>();
        parameters.putIfAbsent(JobParametersNames.APP_VEP_PATH, new JobParameter(APP_VEP_PATH));
        parameters.putIfAbsent(JobParametersNames.APP_VEP_CACHE_VERSION, new JobParameter("100_A"));
        parameters.putIfAbsent(JobParametersNames.APP_VEP_CACHE_PATH, new JobParameter(DIR));
        parameters.putIfAbsent(JobParametersNames.APP_VEP_CACHE_SPECIES, new JobParameter("Human"));
        parameters.putIfAbsent(JobParametersNames.INPUT_FASTA, new JobParameter(INPUT_FASTA));
        parameters.putIfAbsent(JobParametersNames.APP_VEP_NUMFORKS, new JobParameter("6"));
        parameters.putIfAbsent(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter(DIR));
        parameters.putIfAbsent(JobParametersNames.INPUT_STUDY_ID, new JobParameter("inputStudyId"));
        parameters.putIfAbsent(JobParametersNames.INPUT_VCF_ID, new JobParameter("inputVcfId"));

        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void invalidAndMissingParameters() throws JobParametersInvalidException {
        final Map<String, JobParameter> parameters = new LinkedHashMap<>();
        parameters.putIfAbsent(JobParametersNames.APP_VEP_PATH, new JobParameter("file://path/to/file.vcf"));
        parameters.putIfAbsent(JobParametersNames.APP_VEP_CACHE_PATH, new JobParameter("file://path/to/"));
        parameters.putIfAbsent(JobParametersNames.APP_VEP_CACHE_SPECIES, new JobParameter(""));
        parameters.putIfAbsent(JobParametersNames.INPUT_FASTA, new JobParameter("file://path/to/file.vcf"));
        parameters.putIfAbsent(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter("file://path/to/"));
        parameters.putIfAbsent(JobParametersNames.INPUT_STUDY_ID, new JobParameter(""));
        parameters.putIfAbsent(JobParametersNames.INPUT_VCF_ID, new JobParameter(""));

        validator.validate(new JobParameters(parameters));
    }
}
