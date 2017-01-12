package uk.ac.ebi.eva.utils;

import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JobParameterBuilder {

    private Map<String, JobParameter> parameters;

    public JobParameterBuilder() {
        this.parameters = new HashMap<>();
    }

    public JobParameterBuilder inputStudyId(String inputStudyId){
        parameters.put(JobParametersNames.INPUT_STUDY_ID, new JobParameter(inputStudyId));
        return this;
    }

    public JobParameterBuilder inputVcfId(String inputVcfId){
        parameters.put(JobParametersNames.INPUT_VCF_ID, new JobParameter(inputVcfId));
        return this;
    }

    public JobParameterBuilder inputVcf(String inputVcf){
        parameters.put(JobParametersNames.INPUT_VCF, new JobParameter(inputVcf));
        return this;
    }

    public JobParameterBuilder inputVcfAggregation(String inputVcfAggregation){
        parameters.put(JobParametersNames.INPUT_VCF_AGGREGATION, new JobParameter(inputVcfAggregation));
        return this;
    }

    public JobParameterBuilder inputStudyName(String inputStudyName){
        parameters.put(JobParametersNames.INPUT_STUDY_NAME, new JobParameter(inputStudyName));
        return this;
    }

    public JobParameterBuilder inputStudyType(String inputStudyType){
        parameters.put(JobParametersNames.INPUT_STUDY_TYPE, new JobParameter(inputStudyType));
        return this;
    }

    public JobParameterBuilder timestamp() {
        parameters.put("timestamp", new JobParameter(new Timestamp(new Date().getTime())));
        return this;
    }

    public JobParameterBuilder databaseName(String databaseName) {
        parameters.put(JobParametersNames.DB_NAME, new JobParameter(databaseName));
        return this;
    }

    public JobParameterBuilder collectionVariantsName(String collectionVariantsName) {
        parameters.put(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, new JobParameter(collectionVariantsName));
        return this;
    }

    public JobParameters build(){
        return new JobParameters(parameters);
    }

}
