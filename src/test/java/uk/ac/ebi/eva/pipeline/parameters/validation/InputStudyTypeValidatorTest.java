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
package uk.ac.ebi.eva.pipeline.parameters.validation;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

public class InputStudyTypeValidatorTest {

    private InputStudyTypeValidator validator;

    @Before
    public void setUp() throws Exception {
        validator = new InputStudyTypeValidator();
    }

    @Test(expected = JobParametersInvalidException.class)
    public void invalidStudyTypeShouldThrow() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "invalid");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void emptyStudyTypeShouldThrow() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void collectionStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "COLLECTION");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void familyStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "FAMILY");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void trioStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "TRIO");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void controlStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "CONTROL");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void caseStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "CASE");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void caseControlStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "CASE_CONTROL");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void pairedStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "PAIRED");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void pairedTumorStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "PAIRED_TUMOR");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void timeSeriesStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "TIME_SERIES");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void aggregateStudyTypeIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_STUDY_TYPE, "AGGREGATE");
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
