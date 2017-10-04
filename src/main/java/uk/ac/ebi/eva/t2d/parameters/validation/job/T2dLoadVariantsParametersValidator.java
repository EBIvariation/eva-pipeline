package uk.ac.ebi.eva.t2d.parameters.validation.job;

import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.job.steps.T2dVepAnnotationStepValidator;

import java.util.ArrayList;
import java.util.List;

public class T2dLoadVariantsParametersValidator extends CompositeJobParametersValidator {

    public T2dLoadVariantsParametersValidator() {
        super();
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        jobParametersValidators.add(new T2dVepAnnotationStepValidator());
        setValidators(jobParametersValidators);
    }

}
