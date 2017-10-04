package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.INPUT_SAMPLES;

public class T2dInputSamplesValidator extends SingleKeyValidator {

    public T2dInputSamplesValidator(boolean mandatory) {
        super(mandatory, INPUT_SAMPLES);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        // Is a file should be present
    }
}
