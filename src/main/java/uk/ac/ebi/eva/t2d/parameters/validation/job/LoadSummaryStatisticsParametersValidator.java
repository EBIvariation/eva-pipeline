package uk.ac.ebi.eva.t2d.parameters.validation.job;

import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.job.steps.LoadSummaryStatisticsStepValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.job.steps.PrepareDatabaseSummaryStatisticsValidator;

import java.util.ArrayList;
import java.util.List;

public class LoadSummaryStatisticsParametersValidator extends CompositeJobParametersValidator {

    public LoadSummaryStatisticsParametersValidator() {
        super();
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        jobParametersValidators.add(new PrepareDatabaseSummaryStatisticsValidator());
        jobParametersValidators.add(new LoadSummaryStatisticsStepValidator());
        setValidators(jobParametersValidators);
    }
}
