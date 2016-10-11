/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.jobs;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.steps.VariantLoaderStep;
import uk.ac.ebi.eva.pipeline.jobs.steps.VariantNormalizerStep;

/**
 * Helper class to build jobs.
 *
 * This is not intended to be used as an actual job, but to be extended in those actual job classes.
 */
@Import({JobOptions.class})
public abstract class CommonJobStepInitialization {

    public static final String NORMALIZE_VARIANTS = "Normalize variants";
    public static final String LOAD_VARIANTS = "Load variants";

    @Autowired
    private JobOptions jobOptions;

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

    @Bean
    @Scope("prototype")
    protected Step normalize() {
        return generateStep(NORMALIZE_VARIANTS, new VariantNormalizerStep(getVariantOptions(), getPipelineOptions()));
    }

    protected Step load(VariantLoaderStep variantLoaderStep) {
        return generateStep(LOAD_VARIANTS, variantLoaderStep);
    }

    public ObjectMap getPipelineOptions() {
        return jobOptions.getPipelineOptions();
    }

    public ObjectMap getVariantOptions() {
        return jobOptions.getVariantOptions();
    }

    public JobOptions getJobOptions() {
        return jobOptions;
    }
}
