package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dInputStatisticsValidator;

import java.util.ArrayList;
import java.util.List;

public class LoadSummaryStatisticsStepValidator extends CompositeJobParametersValidator {

    public LoadSummaryStatisticsStepValidator() {
        super();
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        jobParametersValidators.add(new T2dInputStatisticsValidator(true));
        setValidators(jobParametersValidators);
    }
}