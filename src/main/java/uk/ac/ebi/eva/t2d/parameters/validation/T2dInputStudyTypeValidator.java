package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.INPUT_STUDY_TYPE;

public class T2dInputStudyTypeValidator extends SingleKeyValidator {

    public T2dInputStudyTypeValidator(boolean mandatory) {
        super(mandatory, INPUT_STUDY_TYPE);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        //Any value
    }
}
