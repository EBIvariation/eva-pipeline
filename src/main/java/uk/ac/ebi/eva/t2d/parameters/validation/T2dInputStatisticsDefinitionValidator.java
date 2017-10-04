package uk.ac.ebi.eva.t2d.parameters.validation;

import org.springframework.batch.core.JobParameters;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.INPUT_STATISTICS_DEFINITION;

public class T2dInputStatisticsDefinitionValidator extends SingleKeyValidator {

    public T2dInputStatisticsDefinitionValidator(boolean mandatory) {
        super(mandatory, INPUT_STATISTICS_DEFINITION);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        // Is a file should be present
    }
}
