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
package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dVepCachePath;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dVepCacheSpecies;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dVepCacheVersion;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dVepNumforks;

import java.util.ArrayList;
import java.util.List;

public class T2dVepAnnotationStepValidator extends CompositeJobParametersValidator {

    public T2dVepAnnotationStepValidator() {
        super();
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        jobParametersValidators.add(new T2dVepCachePath(true));
        jobParametersValidators.add(new T2dVepCacheVersion(true));
        jobParametersValidators.add(new T2dVepCacheSpecies(true));
        jobParametersValidators.add(new T2dVepNumforks(true));
        jobParametersValidators.add(new T2dVepPath(true));
        jobParametersValidators.add(new T2dVepTimeout(true));
        jobParametersValidators.add(new T2dInputFasta(true));
        jobParametersValidators.add(new T2dInputVcf(true));

        setValidators(jobParametersValidators);
    }
}