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
package uk.ac.ebi.eva.utils;

import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class EvaJobParameterBuilder extends JobParametersBuilder{

    public EvaJobParameterBuilder inputStudyId(String inputStudyId){
        addParameter(JobParametersNames.INPUT_STUDY_ID, new JobParameter(inputStudyId));
        return this;
    }

    public EvaJobParameterBuilder inputVcfId(String inputVcfId){
        addParameter(JobParametersNames.INPUT_VCF_ID, new JobParameter(inputVcfId));
        return this;
    }

    public EvaJobParameterBuilder inputVcf(String inputVcf){
        addParameter(JobParametersNames.INPUT_VCF, new JobParameter(inputVcf));
        return this;
    }

    public EvaJobParameterBuilder inputVcfAggregation(String inputVcfAggregation){
        addParameter(JobParametersNames.INPUT_VCF_AGGREGATION, new JobParameter(inputVcfAggregation));
        return this;
    }

    public EvaJobParameterBuilder timestamp() {
        addParameter("timestamp", new JobParameter(new Timestamp(new Date().getTime())));
        return this;
    }

    public EvaJobParameterBuilder databaseName(String databaseName) {
        addParameter(JobParametersNames.DB_NAME, new JobParameter(databaseName));
        return this;
    }

    public EvaJobParameterBuilder collectionVariantsName(String collectionVariantsName) {
        addParameter(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, new JobParameter(collectionVariantsName));
        return this;
    }

}
