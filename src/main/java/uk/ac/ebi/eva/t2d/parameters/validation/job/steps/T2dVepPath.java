package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParameters;
import uk.ac.ebi.eva.t2d.parameters.validation.SingleKeyValidator;

import static uk.ac.ebi.eva.pipeline.parameters.JobParametersNames.APP_VEP_PATH;

public class T2dVepPath extends SingleKeyValidator {

    public T2dVepPath(boolean mandatory) {
        super(mandatory, APP_VEP_PATH);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        //Nothing
    }

}
