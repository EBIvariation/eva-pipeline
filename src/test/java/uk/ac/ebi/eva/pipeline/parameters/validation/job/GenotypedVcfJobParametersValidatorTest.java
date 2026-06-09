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
package uk.ac.ebi.eva.pipeline.parameters.validation.job;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import uk.ac.ebi.eva.pipeline.configuration.jobs.GenotypedVcfJobConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that the arguments necessary to run a {@link GenotypedVcfJobConfiguration} are
 * correctly validated
 */
public class GenotypedVcfJobParametersValidatorTest {

    private GenotypedVcfJobParametersValidator validator;

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    private Map<String, JobParameter<?>> requiredParameters;

    private Map<String, JobParameter<?>> annotationParameters;

    private Map<String, JobParameter<?>> optionalParameters;

    @BeforeEach
    public void setUp() throws Exception {
        validator = new GenotypedVcfJobParametersValidator();
        final String dir = temporaryFolderUtil.getRoot().getCanonicalPath();

        requiredParameters = new TreeMap<>();

        // variant load step
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("database", String.class));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, new JobParameter("variants", String.class));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_ID, new JobParameter("inputStudyId", String.class));
        requiredParameters.put(JobParametersNames.INPUT_VCF_ID, new JobParameter("inputVcfId", String.class));
        requiredParameters.put(JobParametersNames.INPUT_VCF_AGGREGATION, new JobParameter("NONE", String.class));
        requiredParameters.put(JobParametersNames.INPUT_VCF,
                new JobParameter(temporaryFolderUtil.newFile().getCanonicalPath(), String.class));

        // file load step
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_FILES_NAME, new JobParameter("collectionsFilesName", String.class));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_NAME, new JobParameter("inputStudyName", String.class));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_TYPE, new JobParameter("COLLECTION", String.class));

        // skips
        requiredParameters.put(JobParametersNames.ANNOTATION_SKIP, new JobParameter("false", String.class));
        requiredParameters.put(JobParametersNames.STATISTICS_SKIP, new JobParameter("false", String.class));

        // annotation
        annotationParameters = new TreeMap<>();
        annotationParameters.put(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter(dir, String.class));
        annotationParameters.put(JobParametersNames.APP_VEP_CACHE_SPECIES, new JobParameter("Human", String.class));
        annotationParameters.put(JobParametersNames.APP_VEP_CACHE_VERSION, new JobParameter("100_A", String.class));
        annotationParameters.put(JobParametersNames.APP_VEP_VERSION, new JobParameter("80", String.class));
        annotationParameters.put(JobParametersNames.APP_VEP_NUMFORKS, new JobParameter("6", String.class));
        annotationParameters.put(JobParametersNames.APP_VEP_TIMEOUT, new JobParameter("600", String.class));
        annotationParameters.put(JobParametersNames.ANNOTATION_OVERWRITE, new JobParameter("false", String.class));
        annotationParameters.put(JobParametersNames.DB_COLLECTIONS_ANNOTATIONS_NAME, new JobParameter("annotations", String.class));
        annotationParameters.put(JobParametersNames.DB_COLLECTIONS_ANNOTATION_METADATA_NAME,
                new JobParameter("annotationMetadata", String.class));
        annotationParameters.put(JobParametersNames.APP_VEP_CACHE_PATH,
                new JobParameter(temporaryFolderUtil.getRoot().getCanonicalPath(), String.class));
        annotationParameters.put(JobParametersNames.APP_VEP_PATH,
                new JobParameter(temporaryFolderUtil.newFile().getCanonicalPath(), String.class));
        annotationParameters.put(JobParametersNames.INPUT_FASTA,
                new JobParameter(temporaryFolderUtil.newFile().getCanonicalPath(), String.class));

        // optionals
        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_CHUNK_SIZE, new JobParameter("100", String.class));
        optionalParameters.put(JobParametersNames.STATISTICS_OVERWRITE, new JobParameter("true", String.class));
    }

    // The next tests show behaviour about the required parameters

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(annotationParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void allRequiredJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(annotationParameters);
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
        parameters.putAll(annotationParameters);
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
        parameters.putAll(annotationParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }

    // The next tests show what happens when not all the annotation parameters are present

    @Test
    public void annotationParametersAreNotRequiredIfAnnotationIsSkipped() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.put(JobParametersNames.ANNOTATION_SKIP, new JobParameter("true", String.class));
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void annotationParametersAreRequiredIfAnnotationIsNotSkipped() {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }

    /**
     * The parameter APP_VEP_CACHE_SPECIES is chosen as one belonging to the annotation parameters. We don't check
     * for every annotation parameter, because in AnnotationLoaderStepParametersValidatorTest and
     * GenerateVepAnnotationStepParametersValidatorTest, it is already
     * checked that every missing required parameter makes the validation fail.
     */
    @Test
    public void appVepCacheSpeciesIsRequiredIfAnnotationIsNotSkipped() {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(annotationParameters);
        parameters.remove(JobParametersNames.APP_VEP_CACHE_SPECIES);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(parameters)));
    }

}
