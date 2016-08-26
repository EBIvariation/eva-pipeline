package embl.ebi.variation.eva.pipeline.jobs;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.beans.factory.annotation.Autowired;

public class CommonJobStepInitialization {

    @Autowired
    private ObjectMap pipelineOptions;

    /**
     * Initialize a Step with common configuration
     * @param tasklet to be initialized with common configuration
     */
    protected void initStep(TaskletStepBuilder tasklet) {

        boolean allowStartIfComplete  = pipelineOptions.getBoolean("config.restartability.allow");

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false(default): if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(allowStartIfComplete);
    }

    public ObjectMap getPipelineOptions() {
        return pipelineOptions;
    }
}
