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
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class VepAnnotationFileWriter implements ItemStreamWriter<VariantWrapper> {
    private static final Logger logger = LoggerFactory.getLogger(VepAnnotationFileWriter.class);

    private AnnotationParameters annotationParameters;

    private Integer chunkSize;

    private Process process;

    private OutputStream perlStdin;

    private final Long timeoutInSeconds;

    public VepAnnotationFileWriter(Long timeoutInSeconds, Integer chunkSize, AnnotationParameters annotationParameters) {
        this.timeoutInSeconds = timeoutInSeconds;
        this.chunkSize = chunkSize;
        this.annotationParameters = annotationParameters;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        ProcessBuilder processBuilder = new ProcessBuilder("perl", annotationParameters.getVepPath(),
                "--cache",
                "--cache_version", annotationParameters.getVepCacheVersion(),
                "-dir", annotationParameters.getVepCachePath(),
                "--species", annotationParameters.getVepCacheSpecies(),
                "--fasta", annotationParameters.getInputFasta(),
                "--fork", annotationParameters.getVepNumForks(),
                "--buffer_size", chunkSize.toString(),
                "-o", annotationParameters.getVepOutput(),
                "--force_overwrite",
                "--offline",
                "--everything"
        );

        logger.debug("VEP annotation parameters = " + Arrays.toString(processBuilder.command().toArray()));

        logger.info("Starting VEP process");
        process = null;
        try {
            process = processBuilder.start();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }

        perlStdin = new BufferedOutputStream(process.getOutputStream());
    }

    @Override
    public void write(List<? extends VariantWrapper> variantWrappers) throws Exception {
        for (VariantWrapper variantWrapper : variantWrappers) {
            String line = getVariantInVepInputFormat(variantWrapper);
            perlStdin.write(line.getBytes());
        }
        perlStdin.flush();
    }

    private String getVariantInVepInputFormat(VariantWrapper variantWrapper) {
        return String.join("\t",
                variantWrapper.getChr(),
                Integer.toString(variantWrapper.getStart()),
                Integer.toString(variantWrapper.getEnd()),
                variantWrapper.getRefAlt(),
                variantWrapper.getStrand());
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        flushToPerlStdin();
        waitUntilProcessEnds(timeoutInSeconds);
        checkExitStatus();
    }

    private void flushToPerlStdin() {
        try {
            perlStdin.flush();
            perlStdin.close();
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
        logger.info("VEP process finished");
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
