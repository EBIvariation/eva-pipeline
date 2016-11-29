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
package uk.ac.ebi.eva.pipeline.configuration.validation.step;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;

import java.io.File;
import java.io.IOException;

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
    public void allJobParametersAreValid() throws JobParametersInvalidException, IOException {
        final File APP_VEP_PATH = new File(VepAnnotationGeneratorStepParametersValidatorTest.class
                .getResource("/parameters-validation/vepapp.pl").getFile());
        APP_VEP_PATH.setReadable(true);
        final File DIR = new File(VepAnnotationGeneratorStepParametersValidatorTest.class
                .getResource("/parameters-validation/").getFile());
        DIR.setReadable(true);
        DIR.setWritable(true);
        final File INPUT_FASTA = new File(VepAnnotationGeneratorStepParametersValidatorTest.class
                .getResource("/parameters-validation/fasta.fa").getFile());
        INPUT_FASTA.setReadable(true);

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_PATH, APP_VEP_PATH.getCanonicalPath());
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_CACHE_VERSION, "100_A");
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_CACHE_PATH, DIR.getCanonicalPath());
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_CACHE_SPECIES, "Human");
        jobParametersBuilder.addString(JobParametersNames.INPUT_FASTA, INPUT_FASTA.getCanonicalPath());
        jobParametersBuilder.addString(JobParametersNames.APP_VEP_NUMFORKS, "6");
        jobParametersBuilder.addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, DIR.getCanonicalPath());
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
