/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import uk.ac.ebi.eva.pipeline.parameters.ExecutionContextParametersNames;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.utils.FileWithHeaderNumberOfLinesEstimator;
import uk.ac.ebi.eva.utils.URLHelper;

/**
 * - Estimate the number of lines in the VEP annotation file before the step. This will be used in {@link StepProgressListener}
 * - Log a statistics summary after the step
 */
public class AnnotationLoaderStepStatisticsListener implements StepExecutionListener {
    private static final Logger logger = LoggerFactory.getLogger(AnnotationLoaderStepStatisticsListener.class);

    @Override
    public void beforeStep(StepExecution stepExecution) {
        JobParameters parameters = stepExecution.getJobExecution().getJobParameters();

        String outputDirAnnotation = parameters.getString(JobParametersNames.OUTPUT_DIR_ANNOTATION);
        String studyId = parameters.getString(JobParametersNames.INPUT_STUDY_ID);
        String fileId = parameters.getString(JobParametersNames.INPUT_VCF_ID);

        String vepAnnotationFilePath = URLHelper.resolveVepOutput(outputDirAnnotation, studyId, fileId);

        long estimatedTotalNumberOfLines = new FileWithHeaderNumberOfLinesEstimator()
                .estimateNumberOfLines(vepAnnotationFilePath);
        stepExecution.getExecutionContext()
                .put(ExecutionContextParametersNames.NUMBER_OF_LINES, estimatedTotalNumberOfLines);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Items read = " + stepExecution.getReadCount()
                            + ", items written = " + stepExecution.getWriteCount()
                            + ", items skipped = " + stepExecution.getSkipCount());

        return null;
    }
}
