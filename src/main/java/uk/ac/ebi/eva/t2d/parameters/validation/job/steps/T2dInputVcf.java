package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParameters;
import uk.ac.ebi.eva.t2d.parameters.validation.SingleKeyValidator;

import static uk.ac.ebi.eva.pipeline.parameters.JobParametersNames.INPUT_VCF;

public class T2dInputVcf extends SingleKeyValidator {

    public T2dInputVcf(boolean mandatory) {
        super(mandatory, INPUT_VCF);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        //Nothing
    }

}

