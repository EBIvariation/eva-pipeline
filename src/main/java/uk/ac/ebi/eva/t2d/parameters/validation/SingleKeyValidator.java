package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.job.DefaultJobParametersValidator;

public abstract class SingleKeyValidator extends DefaultJobParametersValidator {

    private String optionalParameterName;

    public SingleKeyValidator(boolean mandatory, String parameterName) {
        super();
        if (mandatory) {
            setRequiredKeys(new String[]{parameterName});
        } else {
            optionalParameterName = parameterName;
        }
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        if (optionalParameterName == null) {
            super.validate(parameters);
        }
        doValidate(parameters);
    }

    protected abstract void doValidate(JobParameters parameters);
}
