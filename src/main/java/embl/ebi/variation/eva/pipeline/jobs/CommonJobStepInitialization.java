package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.VariantJobsArgs;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

@Import({VariantJobsArgs.class})
public class CommonJobStepInitialization {

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    /**
     * Initialize a Step with common configuration
     * @param tasklet to be initialized with common configuration
     */
    protected void initStep(final TaskletStepBuilder tasklet) {

        boolean allowStartIfComplete  = getPipelineOptions().getBoolean("config.restartability.allow");

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false(default): if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(allowStartIfComplete);
    }

    protected Step generateStep(String stepName, Tasklet tasklet) {
        StepBuilder step1 = stepBuilderFactory.get(stepName);
        final TaskletStepBuilder taskletBuilder = step1.tasklet(tasklet);
        initStep(taskletBuilder);
        return taskletBuilder.build();
    }

    public ObjectMap getPipelineOptions() {
        return variantJobsArgs.getPipelineOptions();
    }
}
