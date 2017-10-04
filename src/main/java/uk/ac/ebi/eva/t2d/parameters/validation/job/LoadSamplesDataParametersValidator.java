package uk.ac.ebi.eva.t2d.parameters.validation.job;

import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.job.steps.LoadDatabaseSamplesStepValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.job.steps.PrepareDatabaseSamplesStepValidator;

import java.util.ArrayList;
import java.util.List;

public class LoadSamplesDataParametersValidator extends CompositeJobParametersValidator {

    public LoadSamplesDataParametersValidator() {
        super();
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        jobParametersValidators.add(new PrepareDatabaseSamplesStepValidator());
        jobParametersValidators.add(new LoadDatabaseSamplesStepValidator());
        setValidators(jobParametersValidators);
    }
}
