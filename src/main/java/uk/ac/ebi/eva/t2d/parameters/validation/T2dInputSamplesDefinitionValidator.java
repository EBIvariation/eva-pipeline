package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.INPUT_SAMPLES_DEFINITION;

public class T2dInputSamplesDefinitionValidator extends SingleKeyValidator {

    public T2dInputSamplesDefinitionValidator(boolean mandatory) {
        super(mandatory, INPUT_SAMPLES_DEFINITION);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        // Is a file
    }
}
