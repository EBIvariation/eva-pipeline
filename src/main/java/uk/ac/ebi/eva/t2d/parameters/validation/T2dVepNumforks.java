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
