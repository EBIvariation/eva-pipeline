/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.io.writers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemStreamException;

import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

/**
 * Class that launches a VEP process, and allows to write bytes to it (variant coordinates seralized), which will be
 * annotated and written to a file.
 */
public class VepProcess {
    private static final Logger logger = LoggerFactory.getLogger(VepProcess.class);

    private AnnotationParameters annotationParameters;

    private Integer chunkSize;

    private final Long timeoutInSeconds;

    private Process process;

    private OutputStream processStandardInput;

    private Thread outputCapturer;

    private AtomicBoolean writingOk;

    private static final boolean APPEND = true;

    private static final long CONVERT_SECONDS_TO_MILLISECONDS = 1000L;

    public VepProcess(AnnotationParameters annotationParameters, int chunkSize, Long timeoutInSeconds) {
        if (timeoutInSeconds <= 0) {
            throw new IllegalArgumentException(
                    "timeout (" + timeoutInSeconds + " seconds) must be strictly greater than 0");
        }
        this.annotationParameters = annotationParameters;
        this.chunkSize = chunkSize;
        this.timeoutInSeconds = timeoutInSeconds;
    }

    public void open() throws ItemStreamException {
        ProcessBuilder processBuilder = new ProcessBuilder("perl",
                annotationParameters.getVepPath(),
                "--cache",
                "--cache_version", annotationParameters.getVepCacheVersion(),
                "-dir", annotationParameters.getVepCachePath(),
                "--species", annotationParameters.getVepCacheSpecies(),
                "--fasta", annotationParameters.getInputFasta(),
                "--fork", annotationParameters.getVepNumForks(),
                "--buffer_size", chunkSize.toString(),
                "-o", "STDOUT",
                "--force_overwrite",
                "--offline",
                "--everything"
        );

        logger.debug("VEP annotation parameters = " + Arrays.toString(processBuilder.command().toArray()));

        logger.info("Starting VEP process");

        try {
            process = processBuilder.start();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }

        processStandardInput = new BufferedOutputStream(process.getOutputStream());
        String vepOutput = annotationParameters.getVepOutput();

        captureOutput(process, vepOutput);
    }

    private void captureOutput(Process process, String vepOutput) {
        InputStream processStandardOutput = process.getInputStream();
        writingOk = new AtomicBoolean(false);
        outputCapturer = new Thread(() -> {
            long written = 0;
            try (GZIPOutputStream outputStream = new GZIPOutputStream(new FileOutputStream(vepOutput, APPEND))) {
                written = connectStreams(new BufferedInputStream(processStandardOutput), outputStream);
                writingOk.set(true);
            } catch (IOException e) {
                logger.error("Writing the VEP output to " + vepOutput + " failed. ", e);
            }
            logger.info("Finished writing VEP output (" + written + " bytes written) to " + vepOutput);
        });
        logger.info("Starting writing VEP output to " + vepOutput);
        outputCapturer.start();
    }

    public void write(byte[] bytes) throws IOException {
        if (process == null) {
            throw new IllegalStateException("Process must be initialized (hint: call open() before write())");
        }
        processStandardInput.write(bytes);
    }

    public void flush() throws IOException {
        if (process == null) {
            throw new IllegalStateException("Process must be initialized (hint: call open() before flush())");
        }
        processStandardInput.flush();
    }

    public void close () {
        if (process != null) {
            flushToPerlStdin();
            waitUntilProcessEnds(timeoutInSeconds);
            checkExitStatus();
            checkOutputWritingStatus();
            process = null;
            logger.info("VEP process finished");
        }
    }

    private void flushToPerlStdin() {
        try {
            processStandardInput.flush();
            processStandardInput.close();
        } catch (IOException e) {
            logger.error("Could not close stream for VEP's stdin", e);
        }
    }

    private void waitUntilProcessEnds(Long timeoutInSeconds) {
        boolean finished;
        try {
            finished = process.waitFor(timeoutInSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new ItemStreamException(e);
        }

        if (!finished) {
            String timeoutReachedMessage = "Reached the timeout (" + timeoutInSeconds
                    + " seconds) while waiting for VEP to finish. Killed the process.";
            logger.error(timeoutReachedMessage);
            process.destroy();
            throw new ItemStreamException(timeoutReachedMessage);
        }
    }

    private void checkExitStatus() {
        int exitValue = process.exitValue();
        if (exitValue != 0) {
            String errorLog = annotationParameters.getVepOutput() + ".errors.txt";
            try {
                connectStreams(new BufferedInputStream(process.getErrorStream()), new FileOutputStream(errorLog));
            } catch (IOException e) {
                throw new ItemStreamException("VEP exited with code " + exitValue
                        + " but the file to dump the errors could not be created: " + errorLog,
                        e);
            }
            throw new ItemStreamException("Error while running VEP (exit status " + exitValue + "). See "
                    + errorLog + " for the errors description from VEP.");
        }
    }

    private void checkOutputWritingStatus() {
        try {
            outputCapturer.join(timeoutInSeconds * CONVERT_SECONDS_TO_MILLISECONDS);
        } catch (InterruptedException e) {
            throw new ItemStreamException("Interrupted while waiting for the VEP output writer thread to finish. ", e);
        }
        if (outputCapturer.isAlive()) {
            outputCapturer.interrupt();
            throw new ItemStreamException("Reached the timeout (" + timeoutInSeconds
                    + " seconds) while waiting for VEP output writing to finish. Killed the thread.");
        }
        if (!writingOk.get()) {
            throw new ItemStreamException("VEP output writer thread could not finish properly. ");
        }
    }


    /**
     * read all the inputStream and write it into the outputStream
     */
    private long connectStreams(InputStream inputStream, OutputStream outputStream) throws IOException {
        int read = inputStream.read();
        long written = 0;
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
