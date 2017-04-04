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

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;

import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.GenerateVepAnnotationStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.pipeline.parameters.validation.AnnotationOverwriteValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.ConfigChunkSizeValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbCollectionsVariantsNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.DbNameValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputFastaValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputStudyIdValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.InputVcfIdValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.OptionalValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.OutputDirAnnotationValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.VepCachePathValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.VepCacheSpeciesValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.VepCacheVersionValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.VepNumForksValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.VepPathValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.VepTimeoutValidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Validates the job parameters necessary to execute a {@link GenerateVepAnnotationStepConfiguration}
 * <p>
 * The parameters OUTPUT_DIR_ANNOTATION, INPUT_STUDY_ID and INPUT_VCF_ID are used to build the VEP input/output options
 * {@see uk.ac.ebi.eva.pipeline.configuration.JobOptions#loadPipelineOptions()}
 */
public class GenerateVepAnnotationStepParametersValidator extends DefaultJobParametersValidator {

    private boolean isStudyIdRequired;

    public GenerateVepAnnotationStepParametersValidator(boolean isStudyIdRequired) {
        super(new String[]{JobParametersNames.ANNOTATION_OVERWRITE,
                           JobParametersNames.APP_VEP_CACHE_PATH,
                           JobParametersNames.APP_VEP_CACHE_SPECIES,
                           JobParametersNames.APP_VEP_CACHE_VERSION,
                           JobParametersNames.APP_VEP_NUMFORKS,
                           JobParametersNames.APP_VEP_PATH,
                           JobParametersNames.APP_VEP_TIMEOUT,
                           JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME,
                           JobParametersNames.DB_NAME,
                           JobParametersNames.INPUT_FASTA,
                           JobParametersNames.OUTPUT_DIR_ANNOTATION},
              new String[]{});
        this.isStudyIdRequired = isStudyIdRequired;
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);
        compositeJobParametersValidator().validate(parameters);
    }

    private CompositeJobParametersValidator compositeJobParametersValidator() {
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        Collections.addAll(jobParametersValidators,
                new AnnotationOverwriteValidator(),
                new DbCollectionsVariantsNameValidator(),
                new DbNameValidator(),
                new InputFastaValidator(),
                new OutputDirAnnotationValidator(),
                new VepCachePathValidator(),
                new VepCacheSpeciesValidator(),
                new VepCacheVersionValidator(),
                new VepNumForksValidator(),
                new VepPathValidator(),
                new VepTimeoutValidator(),
                new OptionalValidator(new ConfigChunkSizeValidator(), JobParametersNames.CONFIG_CHUNK_SIZE)
        );

        if (isStudyIdRequired) {
            jobParametersValidators.add(new InputStudyIdValidator());
            jobParametersValidators.add(new InputVcfIdValidator());
        } else {
            jobParametersValidators.add(
                    new OptionalValidator(new InputStudyIdValidator(), JobParametersNames.INPUT_STUDY_ID));
            jobParametersValidators.add(
                    new OptionalValidator(new InputVcfIdValidator(), JobParametersNames.INPUT_VCF_ID));
        }

        CompositeJobParametersValidator compositeJobParametersValidator = new CompositeJobParametersValidator();
        compositeJobParametersValidator.setValidators(jobParametersValidators);
        return compositeJobParametersValidator;
    }

}
