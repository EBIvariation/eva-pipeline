package uk.ac.ebi.eva.pipeline.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Entity;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;

import java.util.Comparator;
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
            if (runIdParameterFromLastExecution != 0 && lastJobExecution.getStatus() == BatchStatus.FAILED) {
                // Spring Batch 4 uses all job parameters (including run.id) to detect previous instances of a job - see https://github.com/spring-projects/spring-boot/blob/86fb39d5c5f474fe3544159270d4c4e2d01d43ef/spring-boot-project/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure/batch/JobLauncherCommandLineRunner.java#L222
                // as opposed to Spring 3 which uses only job name - see https://github.com/spring-projects/spring-boot/blob/541890f0e003a6e346f2234102c97105ab1292ee/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure/batch/JobLauncherCommandLineRunner.java#L131
                // Therefore, run.id needs to be supplied for failed jobs in order for the job to be detected and resumed - see https://stackoverflow.com/a/59198742/2601814
                return new JobParametersBuilder(jobParameters)
                        .addLong(RUN_ID_PARAMETER_NAME, runIdParameterFromLastExecution).toJobParameters();
            }
        }

        return jobParameters;
    }

    public static JobExecution getLastJobExecution(String jobName, JobExplorer jobExplorer,
                                                   JobParameters previousJobParameters) {
        int previousJobInstanceCount = getPreviousJobInstanceCount(jobName, jobExplorer);

        List<JobInstance> jobInstanceList = jobExplorer.getJobInstances(jobName, 0, previousJobInstanceCount);
        List<JobExecution> matchingJobExecutions;

        for (JobInstance jobInstance : jobInstanceList) {
            matchingJobExecutions = jobExplorer.getJobExecutions(jobInstance);
            for (JobExecution jobExecution : matchingJobExecutions) {
                if (areParametersEquivalentExceptRunId(jobExecution.getJobParameters(), previousJobParameters)) {
                    return matchingJobExecutions.stream().max(Comparator.comparingLong(Entity::getId)).get();
                }
            }
        }
        return null;
    }

    private static int getPreviousJobInstanceCount(String jobName, JobExplorer jobExplorer) {
        try {
            return jobExplorer.getJobInstanceCount(jobName);
        } catch (NoSuchJobException ex) {
            return 0;
        }
    }

    private static boolean areParametersEquivalentExceptRunId(JobParameters parameters1, JobParameters parameters2) {
        Map<String, JobParameter> firstJobParameterMap = parameters1.getParameters();
        Map<String, JobParameter> secondJobParameterMap = parameters2.getParameters();
        if (parameters1.getParameters().containsKey(RUN_ID_PARAMETER_NAME)) {
            firstJobParameterMap.remove(RUN_ID_PARAMETER_NAME);
        }
        if (parameters2.getParameters().containsKey(RUN_ID_PARAMETER_NAME)) {
            secondJobParameterMap.remove(RUN_ID_PARAMETER_NAME);
        }
        return firstJobParameterMap.equals(secondJobParameterMap);
    }
}
