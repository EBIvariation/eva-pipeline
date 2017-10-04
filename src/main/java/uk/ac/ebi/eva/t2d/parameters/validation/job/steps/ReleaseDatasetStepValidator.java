package uk.ac.ebi.eva.t2d.parameters.validation.job.steps;

import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dInputStudyGeneratorValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dInputStudyReleaseValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dInputStudyTypeValidator;
import uk.ac.ebi.eva.t2d.parameters.validation.T2dInputStudyVersionValidator;

import java.util.ArrayList;
import java.util.List;

public class ReleaseDatasetStepValidator extends CompositeJobParametersValidator {

public ReleaseDatasetStepValidator() {
        super();
        List<JobParametersValidator> jobParametersValidators = new ArrayList<>();
        jobParametersValidators.add(new T2dInputStudyTypeValidator(true));
        jobParametersValidators.add(new T2dInputStudyGeneratorValidator(true));
        jobParametersValidators.add(new T2dInputStudyVersionValidator(true));
        jobParametersValidators.add(new T2dInputStudyReleaseValidator(true));
        setValidators(jobParametersValidators);
        }
        }