package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParameters;
import uk.ac.ebi.eva.t2d.parameters.validation.SingleKeyValidator;

import static uk.ac.ebi.eva.pipeline.parameters.JobParametersNames.INPUT_FASTA;

public class T2dInputFasta extends SingleKeyValidator {

    public T2dInputFasta(boolean mandatory) {
        super(mandatory, INPUT_FASTA);
    }

    @Override
    protected void doValidate(JobParameters parameters) {
        //Nothing
    }

}
