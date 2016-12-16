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
package uk.ac.ebi.eva.pipeline.parameters.validation;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

public class InputVcfAggregationValidatorTest {

    private InputVcfAggregationValidator validator;

    @Before
    public void setUp() throws Exception {
        validator = new InputVcfAggregationValidator();
    }

    @Test(expected = JobParametersInvalidException.class)
    public void invalidAggregationShouldThrow() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_AGGREGATION, "invalid");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void emptyAggregationShouldThrow() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_AGGREGATION, "");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void noneAggregationIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_AGGREGATION, "NONE");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void basicAggregationIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_AGGREGATION, "BASIC");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void evsAggregationIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_AGGREGATION, "EVS");
        validator.validate(jobParametersBuilder.toJobParameters());
    }

    @Test
    public void exacAggregationIsValid() throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(JobParametersNames.INPUT_VCF_AGGREGATION, "EXAC");
        validator.validate(jobParametersBuilder.toJobParameters());
    }
}
