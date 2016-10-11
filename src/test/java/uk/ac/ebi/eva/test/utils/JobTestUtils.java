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

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

/**
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class JobTestUtils {
    private static final Logger logger = LoggerFactory.getLogger(JobTestUtils.class);
    private static final String EVA_PIPELINE_TEMP_PREFIX = "eva-pipeline-test";
    private static final java.lang.String EVA_PIPELINE_TEMP_POSTFIX = ".tmp";

    /**
     * reads the file and sorts it in memory to return the first ordered line. Don't use for big files!
     * @param file to be sorted
     * @return String, the first orderec line
     * @throws IOException
     */
    public static String readFirstLine(File file) throws IOException {
        Set<String> lines = new TreeSet<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
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
        while(iterator.hasNext()) {
            iterator.next();
            rows++;
        }
        return rows;
    }

    public static String getTransformedOutputPath(Path input, String compressExtension, String outputDir) {
        return Paths.get(outputDir).resolve(input) + ".variants.json" + compressExtension;
    }

    public static void cleanDBs(String... dbs) throws UnknownHostException {
        // Delete Mongo collection
        MongoClient mongoClient = new MongoClient("localhost");

        for (String dbName : dbs) {
            DB db = mongoClient.getDB(dbName);
            db.dropDatabase();
        }
        mongoClient.close();
    }

    public static JobParameters getJobParameters(){
        return new JobParametersBuilder()
                .addLong("time",System.currentTimeMillis()).toJobParameters();
    }

    public static File makeGzipFile(String content) throws IOException {
        File tempFile = createTempFile();
        try(FileOutputStream output = new FileOutputStream(tempFile)) {
            try(Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8")) {
                writer.write(content);
            }
        }
        return tempFile;
    }

    public static void makeGzipFile(String content, String file) throws IOException {
        try(FileOutputStream output = new FileOutputStream(file)) {
            try(Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8")) {
                writer.write(content);
            }
        }
    }

    public static void restoreMongoDbFromDump(String dumpLocation, String databaseName) throws IOException, InterruptedException {
    	assert(dumpLocation != null && !dumpLocation.isEmpty());
    	assert(databaseName != null && !databaseName.isEmpty());
    	
        logger.info("restoring DB from " + dumpLocation + " into database " + databaseName);

        Process exec = Runtime.getRuntime().exec(String.format("mongorestore -d %s %s", databaseName, dumpLocation));
        exec.waitFor();
        String line;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
        while ((line = bufferedReader.readLine()) != null) {
            logger.info("mongorestore output:" + line);
        }
        bufferedReader.close();
        bufferedReader = new BufferedReader(new InputStreamReader(exec.getErrorStream()));
        while ((line = bufferedReader.readLine()) != null) {
            logger.info("mongorestore errorOutput:" + line);
        }
        bufferedReader.close();

        logger.info("mongorestore exit value: " + exec.exitValue());
    }

    public static File createTempFile() throws IOException {
        File tempFile = File.createTempFile(EVA_PIPELINE_TEMP_PREFIX,EVA_PIPELINE_TEMP_POSTFIX);
        tempFile.deleteOnExit();
        return tempFile;
    }

    /**
     * Returns a DBObject obtained by parsing a given string
     * @param variant string in JSON format
     * @return DBObject
     */
    public static DBObject constructDbo(String variant) {
        return (DBObject) JSON.parse(variant);
    }
}
