/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package embl.ebi.variation.eva.pipeline.steps;

import embl.ebi.variation.eva.pipeline.listeners.JobParametersListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.io.*;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jmmut on 2015-12-09.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 * @author Cristina Yenyxe Gonzalez Garcia &lt;cyenyxe@ebi.ac.uk&gt;
 */
public class VariantsAnnotCreate implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VariantsAnnotCreate.class);

    private JobParametersListener listener;
    public static final String SKIP_ANNOT_CREATE = "skipAnnotCreate";

    public VariantsAnnotCreate(JobParametersListener listener) {
        this.listener = listener;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        JobParameters parameters = chunkContext.getStepContext().getStepExecution().getJobParameters();

        if (Boolean.parseBoolean(parameters.getString(SKIP_ANNOT_CREATE, "false"))) {
            logger.info("skipping annotation creation step, requested " + SKIP_ANNOT_CREATE + "=" + parameters.getString(SKIP_ANNOT_CREATE));
        } else {
            ProcessBuilder processBuilder = new ProcessBuilder("perl", 
                    parameters.getString("vepPath"), 
                    parameters.getString("vepParameters"), 
                    parameters.getString("vepFasta"),
                    " -i " + parameters.getString("vepInput")
            );
            
            logger.debug("starting read from perl output");
            Process process = processBuilder.start();
            
            int written = connectStreams(
                    new BufferedInputStream(process.getInputStream()), 
                    new GZIPOutputStream(new FileOutputStream(parameters.getString("vepOutput"))));
            
            int exitValue = process.waitFor();
            logger.debug("finishing read from perl output, bytes written: " + written); 
            
            if (exitValue > 0) {
                throw new Exception("Error while running VEP (exit status " + exitValue + ")");
            }
        }

        return RepeatStatus.FINISHED;
    }

    /**
     * read all the inputStream and write it into the outputStream
     *
     * TODO: optimize with buffers?
     * @throws IOException
     */
    private int connectStreams(InputStream inputStream, OutputStream outputStream) throws IOException {
        int read = inputStream.read();
        int written = 0;
        while (read != -1) {
            written++;
            outputStream.write(read);
            read = inputStream.read();
        }

        outputStream.close();
        inputStream.close();
        return written;
    }
}
