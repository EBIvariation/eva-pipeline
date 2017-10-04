package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.INPUT_STUDY_VERSION;

public class T2dInputStudyVersionValidator extends SingleKeyValidator {

    public T2dInputStudyVersionValidator(boolean mandatory) {
        super(mandatory, INPUT_STUDY_VERSION);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        // Should be number
    }
}
