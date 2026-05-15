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
package uk.ac.ebi.eva.test.utils;

import org.bson.Document;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantSourceMongo.FILEID_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantSourceMongo.STUDYID_FIELD;

public abstract class JobTestUtils {
    /**
     * reads the file and sorts it in memory to return the first ordered line. Don't use for big files!
     *
     * @param file to be sorted
     * @return String, the first orderec line
     * @throws IOException
     */
    public static String readFirstLine(File file) throws IOException {
        Set<String> lines = new TreeSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            while (line != null) {
                lines.add(line);
                line = reader.readLine();
            }
        }
        return lines.iterator().next();
    }

    /**
     * counts non-comment lines in an InputStream
     */
    public static long getLines(InputStream in) throws IOException {
        BufferedReader file = new BufferedReader(new InputStreamReader(in));
        long lines = 0;
        String line;
        while ((line = file.readLine()) != null) {
            if (line.charAt(0) != '#') {
                lines++;
            }
        }
        file.close();
        return lines;
    }

    public static <T> long count(Iterator<T> iterator) {
        int rows = 0;
        while (iterator.hasNext()) {
            iterator.next();
            rows++;
        }
        return rows;
    }

    public static JobParameters getJobParameters() {
        return new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis()).toJobParameters();
    }

    public static void checkStringInsideList(Document metadataMongo, String field) {
        assertTrue(metadataMongo.containsKey(field));
        Object objectList = metadataMongo.get(field);
        assertTrue(objectList instanceof List<?>);

        for (Object element : (List<?>) objectList) {
            assertTrue(element instanceof String);
            assertNotNull(element);
            assertFalse(element.toString().isEmpty());
        }
    }

    public static void checkFieldsInsideList(Document metadataMongo, String field, List<String> innerFields) {
        assertTrue(metadataMongo.containsKey(field));
        Object objectList = metadataMongo.get(field);
        assertTrue(objectList instanceof List<?>);

        for (Object element : (List<?>) objectList) {
            Map<String, Object> elementMap;

            if (element instanceof Document) {
                elementMap = (Document) element;
            } else if (element instanceof Map<?, ?>) {
                elementMap = (Map<String, Object>) element;
            } else {
                fail("Unexpected element type: " + element.getClass().getName());
                return;
            }

            for (String innerField : innerFields) {
                Object value = elementMap.get(innerField);
                assertNotNull(value);
                assertFalse(value.toString().isEmpty());
            }
        }
    }

    public static void uncompress(String inputCompressedFile, File outputFile) throws IOException {
        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(inputCompressedFile));
        FileOutputStream fileOutputStream = new FileOutputStream(outputFile);

        byte[] buffer = new byte[1024];
        final int offset = 0;
        int length;
        while ((length = gzipInputStream.read(buffer)) > 0) {
            fileOutputStream.write(buffer, offset, length);
        }

        gzipInputStream.close();
        fileOutputStream.close();
    }

    public static Document buildFilesDocument(String studyId, String fileId) {
        return new Document(STUDYID_FIELD, studyId).append(FILEID_FIELD, fileId);
    }

    public static void assertCompleted(JobExecution jobExecution) {
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    public static void assertFailed(JobExecution jobExecution) {
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
        assertEquals(BatchStatus.FAILED, jobExecution.getStatus());
    }
}
