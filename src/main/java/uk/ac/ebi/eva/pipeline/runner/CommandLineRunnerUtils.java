package uk.ac.ebi.eva.pipeline.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandLineRunnerUtils {

    private static final Logger logger = LoggerFactory.getLogger(CommandLineRunnerUtils.class);

    private static final String RUN_ID_PARAMETER_NAME = "run.id";

    public static JobParameters addRunIDToJobParameters(String jobName, JobExplorer jobExplorer,
                                                        JobParameters jobParameters) {
        JobExecution lastJobExecution = getLastJobExecution(jobName, jobExplorer, jobParameters);
        if (lastJobExecution != null) {
            Long runIdParameterFromLastExecution = lastJobExecution.getJobParameters()
                    .getLong(RUN_ID_PARAMETER_NAME);
            if (runIdParameterFromLastExecution != 0L && lastJobExecution.getStatus() == BatchStatus.FAILED) {
                return new JobParametersBuilder(jobParameters)
                        .addLong(RUN_ID_PARAMETER_NAME, runIdParameterFromLastExecution).toJobParameters();
            }
        }

        return jobParameters;
    }

    public static JobExecution getLastJobExecution(String jobName, JobExplorer jobExplorer,
                                                   JobParameters previousJobParameters) {
        long previousJobInstanceCount = getPreviousJobInstanceCount(jobName, jobExplorer);

        List<JobInstance> jobInstanceList = jobExplorer.getJobInstances(jobName, 0, Math.toIntExact(previousJobInstanceCount));

        for (JobInstance jobInstance : jobInstanceList) {
            List<JobExecution> matchingJobExecutions = jobExplorer.getJobExecutions(jobInstance);
            for (JobExecution jobExecution : matchingJobExecutions) {
                if (areParametersEquivalentExceptRunId(jobExecution.getJobParameters(), previousJobParameters)) {
                    return jobExplorer.getLastJobExecution(jobInstance);
                }
            }
        }
        return null;
    }

    private static long getPreviousJobInstanceCount(String jobName, JobExplorer jobExplorer) {
        try {
            return jobExplorer.getJobInstanceCount(jobName);
        } catch (NoSuchJobException ex) {
            return 0L;
        }
    }

    private static boolean areParametersEquivalentExceptRunId(JobParameters parameters1, JobParameters parameters2) {
        Map<String, JobParameter<?>> firstJobParameterMap = new HashMap<>(parameters1.getParameters());
        Map<String, JobParameter<?>> secondJobParameterMap = new HashMap<>(parameters2.getParameters());
        firstJobParameterMap.remove(RUN_ID_PARAMETER_NAME);
        secondJobParameterMap.remove(RUN_ID_PARAMETER_NAME);
        return firstJobParameterMap.equals(secondJobParameterMap);
    }
}
