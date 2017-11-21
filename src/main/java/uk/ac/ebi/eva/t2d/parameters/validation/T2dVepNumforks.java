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
package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.pipeline.parameters.JobParametersNames.APP_VEP_NUMFORKS;

public class T2dVepNumforks extends SingleKeyValidator {

    public T2dVepNumforks(boolean mandatory) {
        super(mandatory, APP_VEP_NUMFORKS);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        //Nothing
    }

}
