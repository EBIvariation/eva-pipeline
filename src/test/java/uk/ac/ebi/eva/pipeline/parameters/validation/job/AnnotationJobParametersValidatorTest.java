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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import uk.ac.ebi.eva.pipeline.configuration.jobs.AnnotationJobConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that the arguments necessary to run a {@link AnnotationJobConfiguration} are
 * correctly validated
 */
public class AnnotationJobParametersValidatorTest {

    private AnnotationJobParametersValidator validator;

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    private Map<String, JobParameter<?>> requiredParameters;

    private Map<String, JobParameter<?>> optionalParameters;

    @BeforeEach
    public void setUp() throws Exception {
        validator = new AnnotationJobParametersValidator();
        final String dir = temporaryFolderUtil.getRoot().getCanonicalPath();

        requiredParameters = new TreeMap<>();

        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("eva_testing", String.class));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, new JobParameter("variants", String.class));
        requiredParameters.put(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter(dir, String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_SPECIES, new JobParameter("Human", String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_VERSION, new JobParameter("100_A", String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_VERSION, new JobParameter("80", String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_NUMFORKS, new JobParameter("6", String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_TIMEOUT, new JobParameter("600", String.class));
        requiredParameters.put(JobParametersNames.ANNOTATION_OVERWRITE, new JobParameter("false", String.class));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_ANNOTATIONS_NAME, new JobParameter("annotations", String.class));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_ANNOTATION_METADATA_NAME,
                new JobParameter("annotationMetadata", String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_PATH,
                new JobParameter(temporaryFolderUtil.getRoot().getCanonicalPath(), String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_PATH,
                new JobParameter(temporaryFolderUtil.newFile().getCanonicalPath(), String.class));
        requiredParameters.put(JobParametersNames.INPUT_FASTA,
                new JobParameter(temporaryFolderUtil.newFile().getCanonicalPath(), String.class));

        // optionals
        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_CHUNK_SIZE, new JobParameter("100", String.class));
        optionalParameters.put(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, new JobParameter("true", String.class));
    }

    // The next tests show behaviour about the required parameters

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void allRequiredJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void dbNameIsRequiredSkippingAnnotationAndStats() {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        parameters.put(JobParametersNames.ANNOTATION_SKIP, new JobParameter("true", String.class));
        parameters.put(JobParametersNames.STATISTICS_SKIP, new JobParameter("true", String.class));
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }

    @Test
    public void dbNameIsRequiredWithoutSkippingAnnotation() {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        parameters.put(JobParametersNames.STATISTICS_SKIP, new JobParameter("true", String.class));
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }

    @Test
    public void dbNameIsRequiredWithoutSkippingStats() {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.put(JobParametersNames.ANNOTATION_SKIP, new JobParameter("true", String.class));
        parameters.remove(JobParametersNames.DB_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }

    @Test
    public void dbNameIsRequiredWithoutSkippingAnnotationAndStats() {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }
}
