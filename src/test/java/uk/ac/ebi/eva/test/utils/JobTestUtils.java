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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class JobTestUtils {
    private static final Logger logger = LoggerFactory.getLogger(JobTestUtils.class);

    private static final String EVA_PIPELINE_TEMP_PREFIX = "eva-pipeline-test";

    private static final java.lang.String EVA_PIPELINE_TEMP_POSTFIX = ".tmp";

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

    public static String getTransformedOutputPath(Path input, String compressExtension, String outputDir) {
        return Paths.get(outputDir).resolve(input) + ".variants.json" + compressExtension;
    }

    public static JobParameters getJobParameters() {
        return new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis()).toJobParameters();
    }

    public static File createTempFile() throws IOException {
        File tempFile = File.createTempFile(EVA_PIPELINE_TEMP_PREFIX, EVA_PIPELINE_TEMP_POSTFIX);
        tempFile.deleteOnExit();
        return tempFile;
    }

    /**
     * Returns a DBObject obtained by parsing a given string
     *
     * @param variant string in JSON format
     * @return DBObject
     */
    public static DBObject constructDbo(String variant) {
        return (DBObject) JSON.parse(variant);
    }

    public static void checkStringInsideList(BasicDBObject metadataMongo, String field) {
        assertTrue(metadataMongo.containsField(field));
        Object objectList = metadataMongo.get(field);
        assertTrue(objectList instanceof BasicDBList);
        BasicDBList list = (BasicDBList) objectList;
        for (Object element : list) {
            assertTrue(element instanceof String);
            assertNotNull(element);
            assertFalse(element.toString().isEmpty());
        }
    }

    public static void checkFieldsInsideList(BasicDBObject metadataMongo, String field, List<String> innerFields) {
        assertTrue(metadataMongo.containsField(field));
        Object objectList = metadataMongo.get(field);
        assertTrue(objectList instanceof BasicDBList);
        BasicDBList list = (BasicDBList) objectList;
        for (Object element : list) {
            assertTrue(element instanceof BasicDBObject);
            for (String innerField : innerFields) {
                assertNotNull(((BasicDBObject) element).get(innerField));
                assertFalse(((BasicDBObject) element).get(innerField).toString().isEmpty());
            }
        }
    }

    public static void uncompress(String inputCompressedFile, File outputFile) throws IOException {
        GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(inputCompressedFile));
        FileOutputStream out = new FileOutputStream(outputFile);

        byte[] buffer = new byte[1024];
        int len;
        while ((len = gzis.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }

        gzis.close();
        out.close();
    }
}
