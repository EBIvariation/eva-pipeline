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
import org.springframework.batch.core.JobParametersInvalidException;

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
    public void dbNameIsValid() throws JobParametersInvalidException {
        validator.validateDbName("dbName");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsEmpty() throws JobParametersInvalidException {
        validator.validateDbName("");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsNull() throws JobParametersInvalidException {
        validator.validateDbName(null);
    }


    @Test
    public void collectionsVariantsNameIsValid() throws JobParametersInvalidException {
        validator.validateDbCollectionsVariantsName("collectionsVariantsName");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void collectionsVariantsNameIsEmpty() throws JobParametersInvalidException {
        validator.validateDbCollectionsVariantsName("");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void collectionsVariantsNameIsNull() throws JobParametersInvalidException {
        validator.validateDbCollectionsVariantsName(null);
    }


    @Test
    public void configRestartabilityAllowIsValid() throws JobParametersInvalidException {
        validator.validateConfigRestartabilityAllow("false");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsNotValid() throws JobParametersInvalidException {
        validator.validateConfigRestartabilityAllow("blabla");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsEmpty() throws JobParametersInvalidException {
        validator.validateConfigRestartabilityAllow("");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsNull() throws JobParametersInvalidException {
        validator.validateConfigRestartabilityAllow(null);
    }


    @Test
    public void outputDirAnnotationIsValid() throws JobParametersInvalidException {
        validator.validateOutputDirAnnotation(
                VepInputGeneratorStepParametersValidatorTest.class.getResource("/parameters-validation/").getFile());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void outputDirAnnotationNotExist() throws JobParametersInvalidException {
        validator.validateOutputDirAnnotation("file://path/to/");
    }


    @Test
    public void inputVcfIdIsValid() throws JobParametersInvalidException {
        validator.validateInputVcfId("inputVcfId");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIdIsEmpty() throws JobParametersInvalidException {
        validator.validateInputVcfId("");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIdIsNull() throws JobParametersInvalidException {
        validator.validateInputVcfId(null);
    }


    @Test
    public void inputStudyIdIsValid() throws JobParametersInvalidException {
        validator.validateInputStudyId("inputStudyId");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsEmpty() throws JobParametersInvalidException {
        validator.validateInputStudyId("");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsNull() throws JobParametersInvalidException {
        validator.validateInputStudyId(null);
    }
}