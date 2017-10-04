package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.pipeline.parameters.JobParametersNames.APP_VEP_CACHE_VERSION;

public class T2dVepCacheVersion extends SingleKeyValidator {

    public T2dVepCacheVersion(boolean mandatory) {
        super(mandatory, APP_VEP_CACHE_VERSION);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        //Nothing
    }

}

