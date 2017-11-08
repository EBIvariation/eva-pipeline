package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.MANUAL_VEP_FILE;

public class T2dManualVepFileValidator extends SingleKeyValidator {


    public T2dManualVepFileValidator(boolean mandatory) {
        super(mandatory, MANUAL_VEP_FILE);
    }

    @Override
    protected void doValidate(JobParameters parameters) {

    }
}
