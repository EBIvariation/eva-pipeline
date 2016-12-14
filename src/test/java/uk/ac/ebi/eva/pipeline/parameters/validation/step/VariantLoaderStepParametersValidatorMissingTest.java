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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.utils.TestFileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tests that the arguments necessary to run a {@link uk.ac.ebi.eva.pipeline.jobs.steps.PopulationStatisticsLoaderStep}
 * are correctly validated
 */
@RunWith(Parameterized.class)
public class VariantLoaderStepParametersValidatorMissingTest {

    private JobParametersValidator validator;

    private String key;

    private String value;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    private static final Logger logger = LoggerFactory.getLogger(VariantLoaderStepParametersValidatorMissingTest.class);

    public VariantLoaderStepParametersValidatorMissingTest(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Before
    public void setUp() {
        validator = new VariantLoaderStepParametersValidator();
    }

    @Parameterized.Parameters
    public static Collection<String[]> data() throws IOException {
        final File dir = TestFileUtils.getResource("/parameters-validation/");
        final File inputVcf = TestFileUtils.getResource("/parameters-validation/file.vcf.gz");

        dir.setReadable(true);
        dir.setWritable(true);
        inputVcf.setReadable(true);

        return Arrays.asList(new String[][]{
                {JobParametersNames.DB_NAME, "database"},
                {JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, "variants"},
                {JobParametersNames.DB_COLLECTIONS_FILES_NAME, "files"},
                {JobParametersNames.INPUT_STUDY_ID, "inputStudyId"},
                {JobParametersNames.INPUT_VCF_ID, "inputVcfId"},
                {JobParametersNames.INPUT_VCF, inputVcf.getCanonicalPath()},
        });
    }

    @Test(expected = JobParametersInvalidException.class)
    public void shouldFailIfAnyParameterIsMissing() throws JobParametersInvalidException, IOException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        for (String[] parameter : data()) {
            parameters.put(parameter[0], new JobParameter(parameter[1]));
        }

        parameters.remove(key);
        JobParameters jobParameters = new JobParameters(parameters);
        validator.validate(jobParameters);
        logger.error("parameter " + key + " should be required, but the validator didn't throw an exception");
    }
}
