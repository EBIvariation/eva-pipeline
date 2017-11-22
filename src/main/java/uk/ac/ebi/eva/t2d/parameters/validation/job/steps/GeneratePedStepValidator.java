package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dInputSamplesDefinitionValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dInputSamplesValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dPedFileOutputValidator;

import java.util.ArrayList;
import java.util.List;

public class GeneratePedStepValidator extends CompositeJobParametersValidator {

    public GeneratePedStepValidator() {
        super();
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        jobParametersValidators.add(new T2dInputSamplesValidator(true));
        jobParametersValidators.add(new T2dInputSamplesDefinitionValidator(true));
        jobParametersValidators.add(new T2dPedFileOutputValidator(true));
        setValidators(jobParametersValidators);
    }
}
