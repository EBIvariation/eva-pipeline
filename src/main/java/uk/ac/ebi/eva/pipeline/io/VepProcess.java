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

package uk.ac.ebi.eva.pipeline.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemStreamException;

import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * Class that launches a VEP process (@see <a href="http://www.ensembl.org/info/docs/tools/vep/index.html">VEP</a>)
 * that generates variant annotation for a given set of variants. Variant coordinates
 * are piped into the process via its standard input; variant annotations are read from the process' standard output
 * and written to a compressed file.
 * <p>
 * Input: each line (in bytes) of the coordinates of variants and nucleotide changes like:
 * {@code
 * 20	60343	60343	G/A	+
 * 20	60419	60419	A/G	+
 * 20	60479	60479	C/T	+
 * ...
 * }
 * <p>
 * {@code
 * Output: file containing the VEP output
 * 20_60343_G/A	20:60343	A	-	-	-	intergenic_variant	-	-	-	-	-	-
 * 20_60419_A/G	20:60419	G	-	-	-	intergenic_variant	-	-	-	-	-	-
 * 20_60479_C/T	20:60479	T	-	-	-	intergenic_variant	-	-	-	-	-	rs149529999	GMAF=T:0.0018;AFR_MAF=T:0.01;AMR_MAF=T:0.0028
 * ..
 * }
 */
public class VepProcess {
    private static final Logger logger = LoggerFactory.getLogger(VepProcess.class);

    private static final boolean APPEND = true;

    private AnnotationParameters annotationParameters;

    private int chunkSize;

    private final Long timeoutInSeconds;

    private Process process;

    private OutputStream processStandardInput;

    private Thread outputCaptureThread;

    private AtomicBoolean writingOk;

    private AtomicLong outputIdleSince;

    public VepProcess(AnnotationParameters annotationParameters, int chunkSize, Long timeoutInSeconds) {
        if (timeoutInSeconds <= 0) {
            throw new IllegalArgumentException(
                    "timeout (" + timeoutInSeconds + " seconds) must be strictly greater than 0");
        }
        this.annotationParameters = annotationParameters;
        this.chunkSize = chunkSize;
        this.timeoutInSeconds = timeoutInSeconds;
        this.outputIdleSince = new AtomicLong(System.currentTimeMillis());
    }

    public void open() throws ItemStreamException {
        ProcessBuilder processBuilder = new ProcessBuilder("perl",
                annotationParameters.getVepPath(),
                "--cache",
                "--cache_version", annotationParameters.getVepCacheVersion(),
                "-dir", annotationParameters.getVepCachePath(),
                "--species", annotationParameters.getVepCacheSpecies(),
                "--fasta", annotationParameters.getInputFasta(),
                "--fork", Integer.toString(annotationParameters.getVepNumForks()),
                "--buffer_size", Integer.toString(chunkSize),
                "-o", "STDOUT",
                "--force_overwrite",
                "--offline",
                "--everything",
                "--format", "ensembl"
        );

        logger.trace("Starting VEP annotation with parameters = {}", Arrays.toString(processBuilder.command().toArray()));

        try {
            process = processBuilder.start();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }

        processStandardInput = new BufferedOutputStream(process.getOutputStream());
        String vepOutputPath = annotationParameters.getVepOutput();

        captureOutput(process, vepOutputPath);
    }


    private void captureOutput(Process process, String vepOutputPath) {
        writingOk = new AtomicBoolean(false);
        outputCaptureThread = new Thread(() -> {
            long writtenLines = 0;

            // if vepOutput exists, the header (the comments) is already written, and the header should appear only once
            boolean skipComments = new File(vepOutputPath).exists();

            try (OutputStreamWriter writer = getOutputStreamWriter(vepOutputPath);
                    BufferedReader processStandardOutput = getBufferedReader(process)) {
                writtenLines = copyVepOutput(processStandardOutput, writer, skipComments);
                writingOk.set(true);
            } catch (IOException e) {
                logger.error("Writing the VEP output to " + vepOutputPath + " failed. ", e);
            }
            logger.trace("Finished writing VEP output ({} lines written) to {}", writtenLines, vepOutputPath);
        });
        logger.trace("Starting writing VEP output to {}", vepOutputPath);
        outputCaptureThread.start();
    }

    private BufferedReader getBufferedReader(Process process) {
        return new BufferedReader(new InputStreamReader(process.getInputStream()));
    }

    private OutputStreamWriter getOutputStreamWriter(String vepOutputPath) throws IOException {
        return new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(vepOutputPath, APPEND)));
    }

    public void write(byte[] bytes) throws IOException {
        if (!isOpen()) {
            throw new IllegalStateException("Process must be initialized (hint: call open() before write())");
        }
        tryWithTimeout(() -> {
            processStandardInput.write(bytes);
            return null;
        });
    }

    private void tryWithTimeout(Callable<Void> callable) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Void> future = executorService.submit(callable);
        try {
            future.get(timeoutInSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new ItemStreamException("Error writing to VEP: timeout reached", e);
        } catch (Exception e) {
            throw new ItemStreamException("Error writing to VEP", e);
        } finally {
            executorService.shutdown();
            executorService.shutdownNow();
        }
    }

    public boolean isOpen() {
        return process != null;
    }

    public void flush() throws IOException {
        if (!isOpen()) {
            throw new IllegalStateException("Process must be initialized (hint: call open() before flush())");
        }
        tryWithTimeout(() -> {
            processStandardInput.flush();
            return null;
        });
    }

    /**
     * It is safe to call this method several times; it's idempotent.
     */
    public void close() {
        if (isOpen()) {
            try {
                logger.trace("About to close VEP process");
                flushProcessStdin();
                waitUntilProcessEnds(timeoutInSeconds);
                checkExitStatus();
                checkOutputWritingStatus();
            } finally {
                process = null;
                logger.trace("VEP process finished");
            }
        }
    }

    private void flushProcessStdin() {
        try {
            tryWithTimeout(() -> {
                processStandardInput.flush();
                return null;
            });
            processStandardInput.close();
        } catch (IOException e) {
            logger.error("Could not close stream for VEP's stdin", e);
        }
    }

    private void waitUntilProcessEnds(Long timeoutInSeconds) {
        boolean finished;
        try {
            boolean processWroteDuringWait;
            do {
                long beforeWaiting = System.currentTimeMillis();
                finished = process.waitFor(timeoutInSeconds, TimeUnit.SECONDS);
                processWroteDuringWait = beforeWaiting < outputIdleSince.get();
                if (processWroteDuringWait && !finished) {
                    logger.debug("Extending the timeout, as the process wrote more lines (it's still active)");
                }
            } while (processWroteDuringWait && !finished);
        } catch (InterruptedException e) {
            throw new ItemStreamException(e);
        }

        if (!finished) {
            process.destroy();
            String timeoutReachedMessage = "VEP has been idle for more than the timeout (" + timeoutInSeconds
                    + " seconds). The process has been killed.";
            logger.error(timeoutReachedMessage);
            throw new ItemStreamException(timeoutReachedMessage);
        }
    }

    private void checkExitStatus() {
        int exitValue = process.exitValue();
        if (exitValue != 0) {
            String timestamp = Long.toString(System.currentTimeMillis());
            String backupVepOutput = annotationParameters.getVepOutput().replaceFirst("tsv\\.gz$",
                    timestamp + ".tsv.gz");

            String errorLog = backupVepOutput + ".errors.txt";
            try {
                connectStreams(process.getErrorStream(), new FileOutputStream(errorLog));
            } catch (IOException e) {
                throw new ItemStreamException("VEP exited with code " + exitValue
                        + " but the file to dump the errors could not be created: " + errorLog,
                        e);
            }
            boolean renamed = new File(annotationParameters.getVepOutput()).renameTo(new File(backupVepOutput));
            if (renamed) {
                logger.info("Failed VEP output saved to " + backupVepOutput);
            }
            throw new ItemStreamException("Error while running VEP (exit status " + exitValue + "). See "
                    + errorLog + " for the errors description from VEP.");
        }
    }

    private void checkOutputWritingStatus() {
        try {
            outputCaptureThread.join(timeoutInSeconds * 1000L);
        } catch (InterruptedException e) {
            throw new ItemStreamException("Interrupted while waiting for the VEP output writer thread to finish. ", e);
        }
        if (outputCaptureThread.isAlive()) {
            outputCaptureThread.interrupt();
            throw new ItemStreamException("Reached the timeout (" + timeoutInSeconds
                    + " seconds) while waiting for VEP output writing to finish. The thread has been killed.");
        }
        if (!writingOk.get()) {
            throw new ItemStreamException("VEP output writer thread could not finish properly. ");
        }
    }

    /**
     * Read the whole VEP output from the input stream and write it into the output stream,
     * logging the coordinates once per chunk.
     *
     * @param reader must be closed externally
     * @param writer must be closed externally
     * @param skipComments if false, will write all lines starting with '#', if true, will not write any.
     * @return written lines.
     */
    private long copyVepOutput(BufferedReader reader, OutputStreamWriter writer, boolean skipComments)
            throws IOException {
        long writtenLines = 0;

        String line = getNextLine(reader, skipComments);
        String lastLine = line;
        while (line != null) {
            writer.write(line);
            writer.write('\n');
            writtenLines++;

            lastLine = line;
            line = getNextLine(reader, skipComments);
        }

        writer.flush();
        outputIdleSince.set(System.currentTimeMillis());
        logCoordinates(lastLine, writtenLines);

        return writtenLines;
    }

    private String getNextLine(BufferedReader reader, boolean skipComments) throws IOException {
        String line = reader.readLine();
        if (skipComments) {
            while (line != null && isComment(line)) {
                line = reader.readLine();
            }
        }
        return line;
    }

    private boolean isComment(String line) {
        return line.charAt(0) == '#';
    }

    private void logCoordinates(String line, long chunkSize) {
        if (chunkSize == 0 || line == null) {
            logger.warn("VEP didn't write any annotations, this might be a symptom of a previous error");
        } else if (isComment(line)) {
            logger.trace("VEP wrote {} more lines (still writing the header)", chunkSize);
        } else {
            Scanner scanner = new Scanner(line);
            logger.trace("VEP wrote {} more lines, last one was {}", chunkSize, scanner.next());
        }
    }

    /**
     * Read the whole input stream and write it into the output stream.
     * This method blocks until input data is available, the end of the stream is detected, or an exception is thrown.
     * @return written bytes.
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
