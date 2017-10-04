package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParameters;
import uk.ac.ebi.eva.t2d.parameters.validation.SingleKeyValidator;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.INPUT_STATISTICS_PHENOTYPE;

public class T2dInputStatisticsPhenotype extends SingleKeyValidator {

    public T2dInputStatisticsPhenotype(boolean mandatory) {
        super(mandatory, INPUT_STATISTICS_PHENOTYPE);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        // Is a file should be present
    }
}

