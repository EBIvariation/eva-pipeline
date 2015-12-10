/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.io.*;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jmmut on 2015-12-09.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
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

        final AtomicBoolean asyncWriteOk = new AtomicBoolean(true);

        if (Boolean.parseBoolean(parameters.getString(SKIP_ANNOT_CREATE, "false"))) {
            logger.info("skipping annotation pre creation step, requested " + SKIP_ANNOT_CREATE + "=" + parameters.getString(SKIP_ANNOT_CREATE));
        } else {
            final String vepInput = parameters.getString("vepInput");
            String vepOutput = parameters.getString("vepOutput");
            String vepParameters = parameters.getString("vepParameters");

            // perl's streams
            Process cat = Runtime.getRuntime().exec("perl " + vepParameters);
            final OutputStream perlStdin = new BufferedOutputStream(cat.getOutputStream());
            InputStream perlStdout = new BufferedInputStream(cat.getInputStream());
//            BufferedReader perlStderr = new BufferedReader(new InputStreamReader(cat.getErrorStream()));


            // input data streams
            final InputStream fileInputStream = new GZIPInputStream(new FileInputStream(vepInput));

            // output data streams
            OutputStream fileOutputStream = new GZIPOutputStream(new FileOutputStream(vepOutput));

            // chain
            asyncWriteOk.set(false);
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.debug("starting read from vepinput");
                        int written = connectStreams(fileInputStream, perlStdin);
                        logger.debug("finishing read from vepinput, bytes written: " + written);
                        asyncWriteOk.set(true);
                    } catch (IOException e) {
                        logger.error("read from " + vepInput + " failed: ", e);
                    }
                }
            };
            thread.start();

            logger.debug("starting read from perl output");
            int written = connectStreams(perlStdout, fileOutputStream);
            logger.debug("finishing read from perl output, bytes written: " + written);
        }

        if (asyncWriteOk.get()) {
            return RepeatStatus.FINISHED;
        } else {
            throw new Exception("Asynchronous reading process failed");
        }
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
