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

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;

import java.io.File;
import java.io.IOException;

/**
 * Test for {@link StepParametersValidatorUtil}
 */
public class StepParametersValidatorUtilTest {
    private StepParametersValidatorUtil validator;

    @Before
    public void initialize() {
        validator = new StepParametersValidatorUtil();
    }

    @Test
    public void dbNameIsValid() throws JobParametersInvalidException {
        validator.validateDbName("dbName", JobParametersNames.DB_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsEmpty() throws JobParametersInvalidException {
        validator.validateDbName("", JobParametersNames.DB_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsAspace() throws JobParametersInvalidException {
        validator.validateDbName(" ", JobParametersNames.DB_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void dbNameIsNull() throws JobParametersInvalidException {
        validator.validateDbName(null, JobParametersNames.DB_NAME);
    }


    @Test
    public void collectionsVariantsNameIsValid() throws JobParametersInvalidException {
        validator.validateDbCollectionsVariantsName("collectionsVariantsName",
                                                    JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void collectionsVariantsNameIsEmpty() throws JobParametersInvalidException {
        validator.validateDbCollectionsVariantsName("", JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void collectionsVariantsNameIsASpace() throws JobParametersInvalidException {
        validator.validateDbCollectionsVariantsName(" ", JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void collectionsVariantsNameIsNull() throws JobParametersInvalidException {
        validator.validateDbCollectionsVariantsName(null, JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
    }


    @Test
    public void configRestartabilityAllowIsValid() throws JobParametersInvalidException {
        validator.validateConfigRestartabilityAllow("false", JobParametersNames.CONFIG_RESTARTABILITY_ALLOW);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsNotValid() throws JobParametersInvalidException {
        validator.validateConfigRestartabilityAllow("blabla", JobParametersNames.CONFIG_RESTARTABILITY_ALLOW);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsEmpty() throws JobParametersInvalidException {
        validator.validateConfigRestartabilityAllow("", JobParametersNames.CONFIG_RESTARTABILITY_ALLOW);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsAspace() throws JobParametersInvalidException {
        validator.validateConfigRestartabilityAllow(" ", JobParametersNames.CONFIG_RESTARTABILITY_ALLOW);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void configRestartabilityAllowIsNull() throws JobParametersInvalidException {
        validator.validateConfigRestartabilityAllow(null, JobParametersNames.CONFIG_RESTARTABILITY_ALLOW);
    }


    @Test
    public void outputDirAnnotationIsValid() throws JobParametersInvalidException {
        validator.validateOutputDirAnnotation(
                VepInputGeneratorStepParametersValidatorTest.class.getResource("/parameters-validation/").getFile(),
                JobParametersNames.OUTPUT_DIR_ANNOTATION);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void outputDirAnnotationNotExist() throws JobParametersInvalidException {
        validator.validateOutputDirAnnotation("file://path/to/", JobParametersNames.OUTPUT_DIR_ANNOTATION);
    }


    @Test
    public void inputVcfIdIsValid() throws JobParametersInvalidException {
        validator.validateInputVcfId("inputVcfId", JobParametersNames.INPUT_VCF_ID);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIdIsEmpty() throws JobParametersInvalidException {
        validator.validateInputVcfId("", JobParametersNames.INPUT_VCF_ID);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIdIsASpace() throws JobParametersInvalidException {
        validator.validateInputVcfId(" ", JobParametersNames.INPUT_VCF_ID);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIdIsNull() throws JobParametersInvalidException {
        validator.validateInputVcfId(null, JobParametersNames.INPUT_VCF_ID);
    }


    @Test
    public void inputStudyIdIsValid() throws JobParametersInvalidException {
        validator.validateInputStudyId("inputStudyId", JobParametersNames.INPUT_STUDY_ID);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsEmpty() throws JobParametersInvalidException {
        validator.validateInputStudyId("", JobParametersNames.INPUT_STUDY_ID);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsAspace() throws JobParametersInvalidException {
        validator.validateInputStudyId(" ", JobParametersNames.INPUT_STUDY_ID);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputStudyIdIsNull() throws JobParametersInvalidException {
        validator.validateInputStudyId(null, JobParametersNames.INPUT_STUDY_ID);
    }


    @Test
    public void vepPathIsValid() throws JobParametersInvalidException {
        validator.validateVepPath(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/vepapp.pl").getFile(), JobParametersNames.APP_VEP_PATH);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepPathNotExist() throws JobParametersInvalidException {
        validator.validateVepPath("file://path/to/file.vcf", JobParametersNames.APP_VEP_PATH);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepPathNotReadable() throws JobParametersInvalidException, IOException {
        File file = new File(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/input_not_readable.vcf.gz").getFile());
        file.setReadable(false);

        validator.validateVepPath(file.getCanonicalPath(), JobParametersNames.APP_VEP_PATH);
    }


    @Test
    public void vepCacheVersionIsValid() throws JobParametersInvalidException {
        validator.validateVepCacheVersion("vepCacheVersion", JobParametersNames.APP_VEP_CACHE_VERSION);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCacheVersionIsEmpty() throws JobParametersInvalidException {
        validator.validateVepCacheVersion("", JobParametersNames.APP_VEP_CACHE_VERSION);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCacheVersionIsAspace() throws JobParametersInvalidException {
        validator.validateVepCacheVersion(" ", JobParametersNames.APP_VEP_CACHE_VERSION);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCacheVersionIsNull() throws JobParametersInvalidException {
        validator.validateVepCacheVersion(null, JobParametersNames.APP_VEP_CACHE_VERSION);
    }


    @Test
    public void vepCachePathIsValid() throws JobParametersInvalidException {
        validator.validateVepCachePath(
                VepInputGeneratorStepParametersValidatorTest.class.getResource("/parameters-validation/").getFile(),
                JobParametersNames.APP_VEP_CACHE_PATH);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCachePathNotExist() throws JobParametersInvalidException {
        validator.validateVepCachePath("file://path/to/", JobParametersNames.APP_VEP_CACHE_PATH);
    }


    @Test
    public void vepCacheSpeciesIsValid() throws JobParametersInvalidException {
        validator.validateVepCacheSpecies("vepCacheSpecies", JobParametersNames.APP_VEP_CACHE_SPECIES);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCacheSpeciesIsEmpty() throws JobParametersInvalidException {
        validator.validateVepCacheSpecies("", JobParametersNames.APP_VEP_CACHE_SPECIES);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCacheSpeciesIsASpace() throws JobParametersInvalidException {
        validator.validateVepCacheSpecies(" ", JobParametersNames.APP_VEP_CACHE_SPECIES);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepCacheSpeciesIsNull() throws JobParametersInvalidException {
        validator.validateVepCacheSpecies(null, JobParametersNames.APP_VEP_CACHE_SPECIES);
    }


    @Test
    public void inputFastaIsValid() throws JobParametersInvalidException {
        validator.validateInputFasta(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/fasta.fa").getFile(), JobParametersNames.INPUT_FASTA);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputFastaNotExist() throws JobParametersInvalidException {
        validator.validateInputFasta("file://path/to/file.vcf", JobParametersNames.INPUT_FASTA);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputFastaNotReadable() throws JobParametersInvalidException, IOException {
        File file = new File(VepAnnotationGeneratorStepParametersValidatorTest.class.getResource(
                "/parameters-validation/fasta_not_readable.fa").getFile());
        file.setReadable(false);

        validator.validateInputFasta(file.getCanonicalPath(), JobParametersNames.INPUT_FASTA);
    }


    @Test
    public void vepNumForksIsValid() throws JobParametersInvalidException {
        validator.validateVepNumForks("11", JobParametersNames.APP_VEP_NUMFORKS);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsZero() throws JobParametersInvalidException {
        validator.validateVepNumForks("0", JobParametersNames.APP_VEP_NUMFORKS);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsNegative() throws JobParametersInvalidException {
        validator.validateVepNumForks("-1", JobParametersNames.APP_VEP_NUMFORKS);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsNotValid() throws JobParametersInvalidException {
        validator.validateVepNumForks("hello", JobParametersNames.APP_VEP_NUMFORKS);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsEmpty() throws JobParametersInvalidException {
        validator.validateVepNumForks("", JobParametersNames.APP_VEP_NUMFORKS);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void vepNumForksIsNull() throws JobParametersInvalidException {
        validator.validateVepNumForks(null, JobParametersNames.APP_VEP_NUMFORKS);
    }
}
