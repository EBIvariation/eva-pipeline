package uk.ac.ebi.eva.utils;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.transaction.PlatformTransactionManager;

public class TaskletUtils {

    public static TaskletStep generateStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                           String stepName, Tasklet tasklet) {
        return new StepBuilder(stepName, jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

}
