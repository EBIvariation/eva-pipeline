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
package uk.ac.ebi.eva.pipeline.jobs.deciders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Decider used to skip step(s) if the file vepOutput is empty
 */
public class EmptyVepOutputDecider implements JobExecutionDecider {
    private static final Logger logger = LoggerFactory.getLogger(EmptyVepOutputDecider.class);

    public static final String STOP_FLOW = "STOP_FLOW";

    public static final String CONTINUE_FLOW = "CONTINUE_FLOW";

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        String vepOutput = getVepOutput(jobExecution);

        if (getFileSize(vepOutput) > 0) {
            return new FlowExecutionStatus(CONTINUE_FLOW);
        }

        logger.info("File {} is empty so subsequent steps will not run", vepOutput);
        return new FlowExecutionStatus(STOP_FLOW);
    }

    private String getVepOutput(JobExecution jobExecution) {
        JobParameters jobParameters = jobExecution.getJobParameters();

        return URLHelper.resolveVepOutput(
                jobParameters.getString(JobParametersNames.OUTPUT_DIR_ANNOTATION),
                jobParameters.getString(JobParametersNames.INPUT_STUDY_ID),
                jobParameters.getString(JobParametersNames.INPUT_VCF_ID));
    }

    private long getFileSize(String file) {
        long fileSize;

        if (!new File(file).exists()) {
            fileSize = 0;
        } else {
            try {
                fileSize = Files.size(Paths.get(file));
            } catch (IOException e) {
                throw new RuntimeException("File " + file + " is not readable", e);
            }
        }
        return fileSize;
    }
}
