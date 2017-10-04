package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParameters;
import uk.ac.ebi.eva.t2d.parameters.validation.SingleKeyValidator;

import static uk.ac.ebi.eva.pipeline.parameters.JobParametersNames.APP_VEP_TIMEOUT;

public class T2dVepTimeout extends SingleKeyValidator {

    public T2dVepTimeout(boolean mandatory) {
        super(mandatory, APP_VEP_TIMEOUT);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        //Nothing
    }

}