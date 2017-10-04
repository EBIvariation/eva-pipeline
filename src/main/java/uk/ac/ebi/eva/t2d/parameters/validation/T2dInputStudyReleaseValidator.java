package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.INPUT_STUDY_RELEASE;

public class T2dInputStudyReleaseValidator extends SingleKeyValidator {

    public T2dInputStudyReleaseValidator(boolean mandatory) {
        super(mandatory, INPUT_STUDY_RELEASE);
    }


    @Override
    protected void doValidate(JobParameters parameters) {
        // Integer
    }
}
