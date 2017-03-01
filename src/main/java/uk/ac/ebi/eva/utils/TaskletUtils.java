package uk.ac.ebi.eva.utils;

import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;

public class TaskletUtils {

    public static TaskletStep generateStep(StepBuilderFactory stepBuilderFactory, String stepName, Tasklet tasklet,
                                           boolean allowStartIfComplete) {
        StepBuilder step1 = stepBuilderFactory.get(stepName);
        final TaskletStepBuilder taskletBuilder = step1.tasklet(tasklet);
        // true: every job execution will do this step, even if this step is already COMPLETED
        // false(default): if the job was aborted and is relaunched, this step will NOT be done again
        taskletBuilder.allowStartIfComplete(allowStartIfComplete);
        return taskletBuilder.build();
    }

}
