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

package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Autowired;

import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;
import uk.ac.ebi.eva.pipeline.io.mappers.AnnotationLineMapper;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.pipeline.parameters.ChunkSizeParameters;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Convert a {@link DBObject} into {@link VariantWrapper}
 * Any extra filter, check, validation... should be placed here
 */
public class AnnotationProcessor implements ItemProcessor<VariantWrapper, List<VariantAnnotation>>, ItemStream {
    private static final Logger logger = LoggerFactory.getLogger(AnnotationProcessor.class);

    @Autowired
    private AnnotationParameters annotationParameters;

    @Autowired
    private ChunkSizeParameters chunkSizeParameters;

    private AtomicBoolean asyncWriteOk;

    private OutputStream perlStdin;

    private Process process;

    private Integer bufferedVariants;

    private BlockingQueue<VariantAnnotation> readVariantAnnotations;

    private final long timeout;

    public AnnotationProcessor(long timeoutInSeconds) {
        this.timeout = timeoutInSeconds;
        bufferedVariants = 0;
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
                "--buffer_size", chunkSizeParameters.getChunkSize().toString(),
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

        Thread outputCapturer = buildVepOutputCapturer(process);
        outputCapturer.start();
    }

    private Thread buildVepOutputCapturer(Process process) {
        BufferedReader perlStdout = new BufferedReader(new InputStreamReader(process.getInputStream()));

        return new Thread(() -> {
            AnnotationLineMapper lineMapper = new AnnotationLineMapper();
            int unused = 0;
            try {
                String line = perlStdout.readLine();
                while (line != null) {
                    VariantAnnotation variantAnnotation = lineMapper.mapLine(line, unused);
                    readVariantAnnotations.add(variantAnnotation);
                    line = perlStdout.readLine();
                }
            } catch (IOException e) {
                throw new ItemStreamException("Could not read variant annotation from VEP", e);
            }
        });
    }

    @Override
    public List<VariantAnnotation> process(VariantWrapper variant) throws Exception {
        Integer writtenVariantsToVepBuffer = writeVariantToVep(variant);
        if (writtenVariantsToVepBuffer.equals(chunkSizeParameters.getChunkSize())) {
            return readVariantAnnotationsFromVep();
        } else {
            return null;
        }
    }

    private Integer writeVariantToVep(VariantWrapper variantWrapper) {
        String line = String.join("\t",
                variantWrapper.getChr(),
                Integer.toString(variantWrapper.getStart()),
                Integer.toString(variantWrapper.getEnd()),
                variantWrapper.getRefAlt(),
                variantWrapper.getStrand());

        try {
            perlStdin.write(line.getBytes());
            ++bufferedVariants;
        } catch (IOException e) {
            throw new ItemStreamException("Could not pass variant to VEP: " + line, e);
        }

        try {
            if (bufferedVariants.equals(chunkSizeParameters.getChunkSize())) {
                perlStdin.flush();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Could not flush " + bufferedVariants + " variants to VEP: " + line, e);
        }
        return bufferedVariants;
    }

    private List<VariantAnnotation> readVariantAnnotationsFromVep() {
        List<VariantAnnotation> annotations = null;
        if (readVariantAnnotations.size() > 0) {
            annotations = new ArrayList<>(readVariantAnnotations.size());
            for (VariantAnnotation variantAnnotation : readVariantAnnotations) {
                annotations.add(variantAnnotation);
            }
        }

        return annotations;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        try {
            perlStdin.flush();
            perlStdin.close();
        } catch (IOException e) {
            throw new ItemStreamException("Could not close stream for VEP's stdin", e);
        }
        boolean finished;
        try {
            finished = process.waitFor(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new ItemStreamException(e);
        }

        if (!finished) {
            String timeoutReachedMessage = "Reached the timeout (" + timeout
                    + " seconds) while waiting for VEP to finish. Killed the process";
            logger.error(timeoutReachedMessage);
            process.destroy();
            throw new ItemStreamException(timeoutReachedMessage);
        }

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

        if (readVariantAnnotations.size() != 0) {
            throw new ItemStreamException(
                    "Error: there were some variant annotations that VEP wrote but weren't used by the Step");
        }
    }

    /**
     * read all the inputStream and write it into the outputStream
     * <p>
     * TODO: optimize with buffers?
     *
     * @throws IOException
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
