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

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.AnnotationLoaderStepParametersValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.AnnotationMetadataStepParametersValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.FileLoaderStepParametersValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.PopulationStatisticsGeneratorStepParametersValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.PopulationStatisticsLoaderStepParametersValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.VariantLoaderStepParametersValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.VepAnnotationGeneratorStepParametersValidator;
import uk.ac.ebi.eva.pipeline.parameters.validation.step.VepInputGeneratorStepParametersValidator;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates the job parameters necessary to execute an {@link uk.ac.ebi.eva.pipeline.jobs.GenotypedVcfJob}
 */
public class GenotypedVcfJobParametersValidator extends DefaultJobParametersValidator {

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        compositeJobParametersValidator(parameters).validate(parameters);
    }

    private CompositeJobParametersValidator compositeJobParametersValidator(JobParameters jobParameters) {
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();

        jobParametersValidators.add(new VariantLoaderStepParametersValidator());
        jobParametersValidators.add(new FileLoaderStepParametersValidator());

        Boolean skipAnnotation = Boolean.valueOf(jobParameters.getString(JobParametersNames.ANNOTATION_SKIP));
        if (!skipAnnotation) {
            boolean studyIdRequired = true;

            jobParametersValidators.add(new VepInputGeneratorStepParametersValidator());
            jobParametersValidators.add(new VepAnnotationGeneratorStepParametersValidator(studyIdRequired));
            jobParametersValidators.add(new AnnotationLoaderStepParametersValidator(studyIdRequired));
            jobParametersValidators.add(new AnnotationMetadataStepParametersValidator());
        }

        Boolean skipStats = Boolean.valueOf(jobParameters.getString(JobParametersNames.STATISTICS_SKIP));
        if (!skipStats) {
            jobParametersValidators.add(new PopulationStatisticsGeneratorStepParametersValidator());
            jobParametersValidators.add(new PopulationStatisticsLoaderStepParametersValidator());
        }

        CompositeJobParametersValidator compositeJobParametersValidator = new CompositeJobParametersValidator();
        compositeJobParametersValidator.setValidators(jobParametersValidators);
        return compositeJobParametersValidator;
    }

}
