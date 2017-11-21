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
package uk.ac.ebi.eva.t2d.parameters.validation.job;

import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dManualVepFileValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.job.steps.T2dVepAnnotationStepValidator;

import java.util.ArrayList;
import java.util.List;

public class T2dLoadVepAnnotationParametersValidator extends CompositeJobParametersValidator {

    public T2dLoadVepAnnotationParametersValidator() {
        super();
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        jobParametersValidators.add(new T2dManualVepFileValidator(true));
        setValidators(jobParametersValidators);
    }

}
