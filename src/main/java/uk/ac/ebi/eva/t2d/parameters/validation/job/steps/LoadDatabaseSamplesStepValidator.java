package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dInputSamplesValidator;

import java.util.ArrayList;
import java.util.List;

public class LoadDatabaseSamplesStepValidator extends CompositeJobParametersValidator {

    public LoadDatabaseSamplesStepValidator() {
        super();
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        jobParametersValidators.add(new T2dInputSamplesValidator(true));
        setValidators(jobParametersValidators);
    }
}
