package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.pipeline.parameters.JobParametersNames.APP_VEP_CACHE_SPECIES;

public class T2dVepCacheSpecies extends SingleKeyValidator {

    public T2dVepCacheSpecies(boolean mandatory) {
        super(mandatory, APP_VEP_CACHE_SPECIES);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        //Nothing
    }

}
