package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.pipeline.parameters.JobParametersNames.APP_VEP_CACHE_PATH;

public class T2dVepCachePath extends SingleKeyValidator {

    public T2dVepCachePath(boolean mandatory) {
        super(mandatory, APP_VEP_CACHE_PATH);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        //Nothing
    }

}
