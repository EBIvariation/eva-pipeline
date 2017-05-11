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
package uk.ac.ebi.eva.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Estimate the number of lines in a file.
 */
public class FileWithHeaderNumberOfLinesEstimator {
    private static final Logger logger = LoggerFactory.getLogger(FileWithHeaderNumberOfLinesEstimator.class);

    private static final int MAX_NUMBER_OF_LINES = 10000;

    private static final String HEADER_PREFIX = "#";

    private File bodyFile;

    private File headFile;

    /**
     * Given that a file with header could be VERY big then we estimate the number of lines using the following steps:
     * 1) retrieve the size in bytes of the whole zipped file (fileSize)
     * 2) retrieve the size in bytes of the head of the file in a zipped file (headFileSize)
     * 3) retrieve the size in bytes of the first MAX_NUMBER_OF_LINES lines of the file in a zipped file and divide the
     * value by MAX_NUMBER_OF_LINES to obtain the size of a single line (singleLineSize)
     * 4) calculate the (fileSize - headFileSize) / singleLineSize to estimate the total number of line in the file.
     * <p>
     * Why MAX_NUMBER_OF_LINES in point 3?
     * Tested on a file with 157049 lines, MAX_NUMBER_OF_LINES is the best and minimum number of lines to compress. This
     * should generate an estimated total number of lines similar to the real one.
     * <p>
     * In case of small files the MAX_NUMBER_OF_LINES will be the actual number of lines.
     *
     * @param filePath location of the file
     * @return the approximated number of lines in the file
     */
    public long estimateNumberOfLines(String filePath) {
        logger.debug("Estimating the number of lines in file {}", filePath);

        long linesInBody = copyHeadAndBody(filePath);

        long estimatedTotalNumberOfLines;
        if (skipEstimation(linesInBody)) {
            estimatedTotalNumberOfLines = linesInBody;
            logger.info("Number of lines in file {}: {} lines", filePath, estimatedTotalNumberOfLines);
        } else {
            estimatedTotalNumberOfLines = getEstimatedTotalNumberOfLines(filePath);
            logger.info("Estimated number of lines in file {}: {} lines", filePath, estimatedTotalNumberOfLines);
        }

        return estimatedTotalNumberOfLines;
    }

    private boolean skipEstimation(long linesReadInBody) {
        return linesReadInBody < MAX_NUMBER_OF_LINES;
    }

    /**
     * @return lines in the body. It will be min(MAX_NUMBER_OF_LINES, actualLinesInTheBody)
     */
    private long copyHeadAndBody(String filePath) {
        long linesInBody = 0;
        try {
            headFile = File.createTempFile("headFile", ".gz");
            bodyFile = File.createTempFile("bodyFile", ".gz");
        } catch (IOException e) {
            throw new RuntimeException("Error creating file", e);
        }

        try (Writer head = getOutputStreamWriter(headFile); Writer body = getOutputStreamWriter(bodyFile);
             Scanner scanner = new Scanner(new GZIPInputStream(new FileInputStream(filePath)))) {
            while (scanner.hasNextLine() && linesInBody < MAX_NUMBER_OF_LINES) {
                String line = scanner.nextLine();
                if (line.startsWith(HEADER_PREFIX)) {
                    head.write(line);
                    head.write("\n");
                } else {
                    linesInBody++;
                    body.write(line);
                    body.write("\n");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file", e);
        }
        return linesInBody;
    }

    private OutputStreamWriter getOutputStreamWriter(File file) throws IOException {
        return new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)), "UTF-8");
    }

    private long getEstimatedTotalNumberOfLines(String filePath) {
        long fileSize = new File(filePath).length();
        long singleLineSize = bodyFile.length() / MAX_NUMBER_OF_LINES;
        long headFileSize = headFile.length();

        headFile.deleteOnExit();
        bodyFile.deleteOnExit();

        long estimatedTotalNumberOfLines = ((fileSize - headFileSize) / singleLineSize);
        return estimatedTotalNumberOfLines;
    }

}
