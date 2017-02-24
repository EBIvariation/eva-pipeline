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
package uk.ac.ebi.eva.pipeline.runner.exceptions;

import static uk.ac.ebi.eva.pipeline.parameters.JobParametersNames.PROPERTY_FILE_PROPERTY;

/**
 * Exception thrown by the runner when no parameters have been passed to a job.
 */
public class NoParametersHaveBeenPassedException extends Exception {

    public NoParametersHaveBeenPassedException() {
        super("No job parameters have been provided. Please list them as command line arguments, or in a file using "
                + "the argument --" + PROPERTY_FILE_PROPERTY);
    }

}
