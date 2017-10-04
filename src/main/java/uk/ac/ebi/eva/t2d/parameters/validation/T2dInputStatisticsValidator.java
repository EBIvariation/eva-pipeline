package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.INPUT_STATISTICS;

public class T2dInputStatisticsValidator extends SingleKeyValidator {

    public T2dInputStatisticsValidator(boolean mandatory) {
        super(mandatory, INPUT_STATISTICS);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        // Is a file should be present
    }
}
