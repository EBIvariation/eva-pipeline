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
public class GenotypedVcfJobParametersValidatorTest {

    private GenotypedVcfJobParametersValidator validator;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolder = new PipelineTemporaryFolderRule();

    private Map<String, JobParameter> requiredParameters;

    private Map<String, JobParameter> annotationParameters;

    private Map<String, JobParameter> statsParameters;

    private Map<String, JobParameter> optionalParameters;

    @Before
    public void setUp() throws Exception {
        validator = new GenotypedVcfJobParametersValidator();
        final String dir = temporaryFolder.getRoot().getCanonicalPath();

        requiredParameters = new TreeMap<>();

        // variant load step
        requiredParameters.put(JobParametersNames.DB_NAME, new JobParameter("database"));
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, new JobParameter("variants"));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_ID, new JobParameter("inputStudyId"));
        requiredParameters.put(JobParametersNames.INPUT_VCF_ID, new JobParameter("inputVcfId"));
        requiredParameters.put(JobParametersNames.INPUT_VCF_AGGREGATION, new JobParameter("NONE"));
        requiredParameters.put(JobParametersNames.INPUT_VCF,
                new JobParameter(temporaryFolder.newFile().getCanonicalPath()));

        // file load step
        requiredParameters.put(JobParametersNames.DB_COLLECTIONS_FILES_NAME, new JobParameter("collectionsFilesName"));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_NAME, new JobParameter("inputStudyName"));
        requiredParameters.put(JobParametersNames.INPUT_STUDY_TYPE, new JobParameter("COLLECTION"));

        // skips
        requiredParameters.put(JobParametersNames.ANNOTATION_SKIP, new JobParameter("false"));
        requiredParameters.put(JobParametersNames.STATISTICS_SKIP, new JobParameter("false"));

        // annotation
        annotationParameters = new TreeMap<>();
        annotationParameters.put(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter(dir));
        annotationParameters.put(JobParametersNames.APP_VEP_CACHE_SPECIES, new JobParameter("Human"));
        annotationParameters.put(JobParametersNames.APP_VEP_CACHE_VERSION, new JobParameter("100_A"));
        annotationParameters.put(JobParametersNames.APP_VEP_VERSION, new JobParameter("80"));
        annotationParameters.put(JobParametersNames.APP_VEP_NUMFORKS, new JobParameter("6"));
        annotationParameters.put(JobParametersNames.APP_VEP_TIMEOUT, new JobParameter("600"));
        annotationParameters.put(JobParametersNames.DB_COLLECTIONS_ANNOTATIONS_NAME, new JobParameter("annotations"));
        annotationParameters.put(JobParametersNames.DB_COLLECTIONS_ANNOTATION_METADATA_NAME,
                new JobParameter("annotationMetadata"));
        annotationParameters.put(JobParametersNames.APP_VEP_CACHE_PATH,
                new JobParameter(temporaryFolder.getRoot().getCanonicalPath()));
        annotationParameters.put(JobParametersNames.APP_VEP_PATH,
                new JobParameter(temporaryFolder.newFile().getCanonicalPath()));
        annotationParameters.put(JobParametersNames.INPUT_FASTA,
                new JobParameter(temporaryFolder.newFile().getCanonicalPath()));

        // statistics
        statsParameters = new TreeMap<>();
        statsParameters.put(JobParametersNames.OUTPUT_DIR_STATISTICS, new JobParameter(dir));


        // optionals
        optionalParameters = new TreeMap<>();
        optionalParameters.put(JobParametersNames.CONFIG_CHUNK_SIZE, new JobParameter("100"));
        optionalParameters.put(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, new JobParameter("true"));
        optionalParameters.put(JobParametersNames.STATISTICS_OVERWRITE, new JobParameter("true"));
    }

    // The next tests show behaviour about the required parameters

    @Test
    public void allJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(annotationParameters);
        parameters.putAll(statsParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test
    public void allRequiredJobParametersAreValid() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(annotationParameters);
        parameters.putAll(statsParameters);
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
        parameters.putAll(annotationParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        parameters.put(JobParametersNames.STATISTICS_SKIP, new JobParameter("true"));
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequiredWithoutSkippingStats() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(statsParameters);
        parameters.put(JobParametersNames.ANNOTATION_SKIP, new JobParameter("true"));
        parameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsRequiredWithoutSkippingAnnotationAndStats() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(annotationParameters);
        parameters.putAll(statsParameters);
        parameters.remove(JobParametersNames.DB_NAME);
        validator.validate(new JobParameters(parameters));
    }

    // The next tests show what happens when not all the annotation parameters are present

    @Test
    public void annotationParametersAreNotRequiredIfAnnotationIsSkipped() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(statsParameters);
        parameters.put(JobParametersNames.ANNOTATION_SKIP, new JobParameter("true"));
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void annotationParametersAreRequiredIfAnnotationIsNotSkipped() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(statsParameters);
        validator.validate(new JobParameters(parameters));
    }

    /**
     * The parameter APP_VEP_CACHE_SPECIES is chosen as one belonging to the annotation parameters. We don't check
     * for every annotation parameter, because in AnnotationLoaderStepParametersValidatorTest and
     * GenerateVepAnnotationStepParametersValidatorTest, it is already
     * checked that every missing required parameter makes the validation fail.
     */
    @Test(expected = JobParametersInvalidException.class)
    public void appVepCacheSpeciesIsRequiredIfAnnotationIsNotSkipped() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(annotationParameters);
        parameters.putAll(statsParameters);
        parameters.remove(JobParametersNames.APP_VEP_CACHE_SPECIES);
        validator.validate(new JobParameters(parameters));
    }

    // The next tests show what happens when not all the stats parameters are present

    @Test
    public void statsParametersAreNotRequiredIfStatsIsSkipped() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(annotationParameters);
        parameters.put(JobParametersNames.STATISTICS_SKIP, new JobParameter("true"));
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void statsParametersAreRequiredIfStatsIsNotSkipped() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(annotationParameters);
        validator.validate(new JobParameters(parameters));
    }

    @Test(expected = JobParametersInvalidException.class)
    public void outputDirStatistitcsIsRequiredIfStatsIsNotSkipped() throws JobParametersInvalidException {
        Map<String, JobParameter> parameters = new TreeMap<>();
        parameters.putAll(requiredParameters);
        parameters.putAll(optionalParameters);
        parameters.putAll(annotationParameters);
        parameters.putAll(statsParameters);
        parameters.remove(JobParametersNames.OUTPUT_DIR_STATISTICS);
        validator.validate(new JobParameters(parameters));
    }
}
