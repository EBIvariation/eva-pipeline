package uk.ac.ebi.eva.pipeline.configuration;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.lang.Nullable;

import java.util.List;
import java.util.Set;

public class NoOpJobExplorer implements JobExplorer {
    @Override
    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        return List.of();
    }

    @Nullable
    @Override
    public JobExecution getJobExecution(@Nullable Long executionId) {
        return null;
    }

    @Nullable
    @Override
    public StepExecution getStepExecution(@Nullable Long jobExecutionId, @Nullable Long stepExecutionId) {
        return null;
    }

    @Nullable
    @Override
    public JobInstance getJobInstance(@Nullable Long instanceId) {
        return null;
    }

    @Override
    public List<JobExecution> getJobExecutions(JobInstance jobInstance) {
        return List.of();
    }

    @Override
    public Set<JobExecution> findRunningJobExecutions(@Nullable String jobName) {
        return Set.of();
    }

    @Override
    public List<String> getJobNames() {
        return List.of();
    }

    @Override
    public List<JobInstance> findJobInstancesByJobName(String jobName, int start, int count) {
        return List.of();
    }

    @Override
    public long getJobInstanceCount(@Nullable String jobName) {
        return 0;
    }

    @Override
    public JobInstance getLastJobInstance(String jobName) {
        return null;
    }
}