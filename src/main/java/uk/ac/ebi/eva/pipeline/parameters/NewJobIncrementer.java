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
package uk.ac.ebi.eva.pipeline.parameters;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersIncrementer;

/**
 * Incrementer that does not reuse parameters from previous completed executions.
 */
public class NewJobIncrementer implements JobParametersIncrementer {

    private static String RUN_ID_KEY = "run.id";

    private String key = RUN_ID_KEY;

    /**
     * The name of the run id in the job parameters.  Defaults to "run.id".
     *
     * @param key the key to set
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Increment the run.id parameter (starting with 1), and ignore the other parameters.
     */
    @Override
    public JobParameters getNext(JobParameters parameters) {
        JobParameters params = (parameters == null) ? new JobParameters() : parameters;

        long id = params.getLong(key, 0L) + 1;
        return new JobParametersBuilder().addLong(key, id).toJobParameters();
    }
}
