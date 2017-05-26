/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.parameters.validation.job;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;

import java.util.Map;
import java.util.TreeMap;

/**
 * Tests that the arguments necessary to run a {@link uk.ac.ebi.eva.pipeline.jobs.GenotypedVcfJob} are
 * correctly validated
 */
public class AnnotationJobParametersValidatorTest {

    private AnnotationJobParametersValidator validator;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    private Map<String, JobParameter> requiredParameters;

    private Map<String, JobParameter> optionalParameters;

    @Before
    public void setUp() throws Exception {
        validator = new AnnotationJobParametersValidator();
        final String dir = temporaryFolder.getRoot().getCanonicalPath();

        requiredParameters = new TreeMap<>();

        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("eva_testing"));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, new JobParameter("variants"));
        requiredParameters.put(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter(dir));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_SPECIES, new JobParameter("Human"));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_VERSION, new JobParameter("100_A"));
        requiredParameters.put(JobParametersNames.APP_VEP_VERSION, new JobParameter("80"));
        requiredParameters.put(JobParametersNames.APP_VEP_NUMFORKS, new JobParameter("6"));
        requiredParameters.put(JobParametersNames.APP_VEP_TIMEOUT, new JobParameter("600"));
        requiredParameters.put(JobParametersNames.ANNOTATION_OVERWRITE, new JobParameter("false"));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_ANNOTATIONS_NAME, new JobParameter("annotations"));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_ANNOTATION_METADATA_NAME,
                new JobParameter("annotationMetadata"));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_PATH,
                new JobParameter(temporaryFolder.getRoot().getCanonicalPath()));
        requiredParameters.put(JobParametersNames.APP_VEP_PATH,
                new JobParameter(temporaryFolder.newFile().getCanonicalPath()));
        requiredParameters.put(JobParametersNames.INPUT_FASTA,
                new JobParameter(temporaryFolder.newFile().getCanonicalPath()));

        // optionals
        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_CHUNK_SIZE, new JobParameter("100"));
        optionalParameters.put(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, new JobParameter("true"));
    }

    // The next tests show behaviour about the required parameters

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void allRequiredJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequiredSkippingAnnotationAndStats() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        parameters.put(JobParametersNames.ANNOTATION_SKIP, new JobParameter("true"));
        parameters.put(JobParametersNames.STATISTICS_SKIP, new JobParameter("true"));
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequiredWithoutSkippingAnnotation() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        parameters.put(JobParametersNames.STATISTICS_SKIP, new JobParameter("true"));
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequiredWithoutSkippingStats() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.put(JobParametersNames.ANNOTATION_SKIP, new JobParameter("true"));
        parameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequiredWithoutSkippingAnnotationAndStats() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(parameters));
    }
}
