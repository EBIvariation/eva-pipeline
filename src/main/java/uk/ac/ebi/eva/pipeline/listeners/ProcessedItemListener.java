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
package uk.ac.ebi.eva.pipeline.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

/**
 * Log the number of items processed every 1000 items.
 * Should be wired into a {@link org.springframework.batch.core.Step}
 *
 */
public class ProcessedItemListener implements ChunkListener {
    private static final Logger logger = LoggerFactory.getLogger(ProcessedItemListener.class);

    @Override
    public void beforeChunk(ChunkContext context) {
    }

    @Override
    public void afterChunk(ChunkContext context) {
        int itemProcessed = context.getStepContext().getStepExecution().getReadCount();
        if(itemProcessed % 1000 == 0) {
            logger.debug("Items processed " + itemProcessed);
        }
    }

    @Override
    public void afterChunkError(ChunkContext context) {

    }
}
