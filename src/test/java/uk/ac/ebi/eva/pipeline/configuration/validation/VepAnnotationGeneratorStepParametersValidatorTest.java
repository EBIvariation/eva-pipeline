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
import org.springframework.batch.core.JobParametersBuilder;
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

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH, APP_VEP_PATH);
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_CACHE_VERSION, "100_A");
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_CACHE_PATH, DIR);
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_CACHE_SPECIES, "Human");
        jobParametersBuilder.addString(JobParametersNames.INPUT_FASTA, INPUT_FASTA);
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_NUMFORKS, "6");
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, DIR);
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_ID, "inputStudyId");
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_ID, "inputVcfId");

        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void invalidAndMissingParameters() throws JobParametersInvalidException {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH, "file://path/to/file.vcf");
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_CACHE_PATH, "file://path/to/");
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_CACHE_SPECIES, "");
        jobParametersBuilder.addString(JobParametersNames.INPUT_FASTA, "file://path/to/file.vcf");
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, "file://path/to/");
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_ID, "");
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_ID, "");

        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
