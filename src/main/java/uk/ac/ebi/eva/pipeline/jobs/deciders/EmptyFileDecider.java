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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Decider used to skip step(s) in case a previous step is generating an empty file
 */
public class EmptyFileDecider implements JobExecutionDecider {
    private static final Logger logger = LoggerFactory.getLogger(EmptyFileDecider.class);

    private String file;

    public static final String STOP_FLOW = "STOP_FLOW";
    public static final String CONTINUE_FLOW = "CONTINUE_FLOW";

    public EmptyFileDecider(String file) {
        this.file = file;
    }

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

        if (getFileSize() <= 0) {
            logger.info("File {} is empty so subsequent steps will not run", file);
            return new FlowExecutionStatus(STOP_FLOW);
        }

        return new FlowExecutionStatus(CONTINUE_FLOW);
    }

    private long getFileSize() {
        long fileSize;

        try {
            fileSize = Files.size(Paths.get(file));
        } catch (IOException e) {
            throw new RuntimeException("File {} is not readable", e);
        }
        return fileSize;
    }
}
