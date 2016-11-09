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
package uk.ac.ebi.eva.pipeline.jobs.deciders;

import org.opencb.datastore.core.ObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

/**
 * Decider used in the pipeline to skip steps/tasklets
 */
public class SkipStepDecider implements JobExecutionDecider {
    private static final Logger logger = LoggerFactory.getLogger(SkipStepDecider.class);

    private String skipStep;
    private ObjectMap pipelineOptions;

    public static final String SKIP_STEP = "SKIP_STEP";
    public static final String DO_STEP = "DO_STEP";

    /**
     * @param pipelineOptions ObjectMap that will have a boolean for .get(skipStep), telling whether to skip or not
     * @param skipStep        name of the key that the user sets to skip a step, e.g. "annotation.create.skip"
     *                        It's recommended to use pre-defined constants, such as AnnotationJob.SKIP_ANNOT to avoid
     *                        misspelling mistakes.
     */
    public SkipStepDecider(ObjectMap pipelineOptions, String skipStep) {
        this.skipStep = skipStep;
        this.pipelineOptions = pipelineOptions;
    }

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        if (Boolean.parseBoolean(pipelineOptions.getString(skipStep))) {
            logger.info("Skipping step because {} is enabled", skipStep);
            return new FlowExecutionStatus(SKIP_STEP);
        }

        logger.info("Running step because {} is disabled", skipStep);
        return new FlowExecutionStatus(DO_STEP);
    }

}
