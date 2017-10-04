package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.INPUT_STUDY_GENERATOR;

public class T2dInputStudyGeneratorValidator extends SingleKeyValidator {

    public T2dInputStudyGeneratorValidator(boolean mandatory) {
        super(mandatory, INPUT_STUDY_GENERATOR);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        // Any value
    }
}
