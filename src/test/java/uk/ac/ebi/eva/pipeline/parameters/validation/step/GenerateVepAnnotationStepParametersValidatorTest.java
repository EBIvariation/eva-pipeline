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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.GenerateVepAnnotationStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that the arguments necessary to run a {@link GenerateVepAnnotationStepConfiguration} are
 * correctly validated
 */
public class GenerateVepAnnotationStepParametersValidatorTest {

    private GenerateVepAnnotationStepParametersValidator validator;

    private Map<String, JobParameter<?>> requiredParameters;

    private Map<String, JobParameter<?>> optionalParameters;

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @BeforeEach
    public void setUp() throws IOException {
        boolean studyIdRequired = true;
        validator = new GenerateVepAnnotationStepParametersValidator(studyIdRequired);

        requiredParameters = new TreeMap<>();

        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("eva_testing", String.class));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, new JobParameter("variants", String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_PATH,
                new JobParameter(temporaryFolderUtil.getRoot().getCanonicalPath(), String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_SPECIES, new JobParameter("Human", String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_CACHE_VERSION, new JobParameter("100_A", String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_NUMFORKS, new JobParameter("6", String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_PATH,
                new JobParameter(temporaryFolderUtil.newFile().getCanonicalPath(), String.class));
        requiredParameters.put(JobParametersNames.INPUT_FASTA,
                new JobParameter(temporaryFolderUtil.newFile().getCanonicalPath(), String.class));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_ID, new JobParameter("inputStudyId", String.class));
        requiredParameters.put(JobParametersNames.INPUT_VCF_ID, new JobParameter("inputVcfId", String.class));
        requiredParameters.put(JobParametersNames.OUTPUT_DIR_ANNOTATION,
                new JobParameter(temporaryFolderUtil.getRoot().getCanonicalPath(), String.class));
        requiredParameters.put(JobParametersNames.APP_VEP_TIMEOUT, new JobParameter("600", String.class));
        requiredParameters.put(JobParametersNames.ANNOTATION_OVERWRITE, new JobParameter("false", String.class));

        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_CHUNK_SIZE, new JobParameter("100", String.class));
    }

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException, IOException {
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test
    public void allJobParametersIncludingOptionalAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter<?>> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void dbNameIsRequired() throws JobParametersInvalidException, IOException {
        requiredParameters.remove(JobParametersNames.DB_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void dbCollectionsVariantsNameIsRequired() {
        requiredParameters.remove(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void appVepCachePathIsRequired() {
        requiredParameters.remove(JobParametersNames.APP_VEP_CACHE_PATH);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void appVepCacheSpeciesIsRequired() {
        requiredParameters.remove(JobParametersNames.APP_VEP_CACHE_SPECIES);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void appVepCacheVersionIsRequired() {
        requiredParameters.remove(JobParametersNames.APP_VEP_CACHE_VERSION);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void appVepNumForksIsRequired() {
        requiredParameters.remove(JobParametersNames.APP_VEP_NUMFORKS);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void appVepPathIsRequired() {
        requiredParameters.remove(JobParametersNames.APP_VEP_PATH);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputFastaIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_FASTA);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputStudyIdIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_ID);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputStudyIdIsNotRequired() throws JobParametersInvalidException {
        requiredParameters.remove(JobParametersNames.INPUT_STUDY_ID);

        boolean studyIdNotRequired = false;
        validator = new GenerateVepAnnotationStepParametersValidator(studyIdNotRequired);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test
    public void inputVcfIdIsRequired() {
        requiredParameters.remove(JobParametersNames.INPUT_VCF_ID);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void inputVcfIdIsNotRequired() throws JobParametersInvalidException {
        requiredParameters.remove(JobParametersNames.INPUT_VCF_ID);

        boolean studyIdNotRequired = false;
        validator = new GenerateVepAnnotationStepParametersValidator(studyIdNotRequired);
        validator.validate(new JobParameters(requiredParameters));
    }

    @Test
    public void outputDirAnnotationIsRequired() {
        requiredParameters.remove(JobParametersNames.OUTPUT_DIR_ANNOTATION);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void appVepTimeoutIsRequired() {
        requiredParameters.remove(JobParametersNames.APP_VEP_TIMEOUT);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }

    @Test
    public void annotationOverwriteIsRequired() {
        requiredParameters.remove(JobParametersNames.ANNOTATION_OVERWRITE);
        assertThrows(JobParametersInvalidException.class, () -> validator.validate(new JobParameters(requiredParameters)));
    }
}
