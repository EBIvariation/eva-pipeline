/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

import uk.ac.ebi.eva.pipeline.parameters.ExecutionContextParametersNames;

/**
 * Log the number of read, write and skip items for each chunk.
 * Should be wired into a {@link org.springframework.batch.core.Step}
 */
public class StepProgressListener implements ChunkListener {
    private static final Logger logger = LoggerFactory.getLogger(StepProgressListener.class);

    @Override
    public void beforeChunk(ChunkContext context) {
    }

    @Override
    public void afterChunk(ChunkContext context) {
        long estimatedTotalNumberOfLines = (long) context.getStepContext().getStepExecutionContext()
                .getOrDefault(ExecutionContextParametersNames.NUMBER_OF_LINES, 0L);

        long read = context.getStepContext().getStepExecution().getReadCount();
        long write = context.getStepContext().getStepExecution().getWriteCount();
        long skip = context.getStepContext().getStepExecution().getReadSkipCount()
                + context.getStepContext().getStepExecution().getProcessSkipCount()
                + context.getStepContext().getStepExecution().getWriteSkipCount();

        String chunkStatisticsMessage = "Items read = " + read + ", items written = " + write + ", items skipped = " + skip;
        String stepName = context.getStepContext().getStepName() + ": ";

        if (estimatedTotalNumberOfLines != 0) {
            int percent = (int) ((read * 100) / estimatedTotalNumberOfLines);
            logger.info(stepName + percent + "% complete: " + chunkStatisticsMessage);
        } else {
            logger.info(stepName + chunkStatisticsMessage);
        }

    }

    @Override
    public void afterChunkError(ChunkContext context) {
    }
}
