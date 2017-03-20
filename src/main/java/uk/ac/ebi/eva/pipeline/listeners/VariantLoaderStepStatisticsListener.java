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
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import uk.ac.ebi.eva.pipeline.parameters.InternalParametersNames;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.utils.VcfNumberOfLinesEstimator;

/**
 * - Estimate the number of lines in the VCF file before the step. This will be used in {@link StepProgressListener}
 * - Log a statistics summary after the step
 */
public class VariantLoaderStepStatisticsListener implements StepExecutionListener {
    private static final Logger logger = LoggerFactory.getLogger(VariantLoaderStepStatisticsListener.class);

    @Override
    public void beforeStep(StepExecution stepExecution) {
        String vcfFilePath = stepExecution.getJobExecution().getJobParameters().getString(JobParametersNames.INPUT_VCF);
        long estimatedTotalNumberOfLines = new VcfNumberOfLinesEstimator().estimateVcfNumberOfLines(vcfFilePath);
        stepExecution.getExecutionContext().put(InternalParametersNames.NUMBER_OF_LINES, estimatedTotalNumberOfLines);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Items read = " + stepExecution.getReadCount()
                            + ", items written = " + stepExecution.getWriteCount()
                            + " items skipped = " + stepExecution.getSkipCount());

        return null;
    }

}
