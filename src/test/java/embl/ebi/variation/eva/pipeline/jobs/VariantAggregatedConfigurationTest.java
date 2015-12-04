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
package embl.ebi.variation.eva.pipeline.jobs;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.countRows;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getLines;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getTransformedOutputPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by jmmut on 2015-10-14.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {VariantAggregatedConfiguration.class})
public class VariantAggregatedConfigurationTest {

    public static final String FILE_AGGREGATED = "/aggregated.vcf.gz";
    public static final String FILE_22 = "/small22.vcf.gz";
    public static final String FILE_WRONG_NO_ALT = "/wrong_no_alt.vcf.gz";

    private static final Logger logger = LoggerFactory.getLogger(VariantAggregatedConfigurationTest.class);

    // iterable doing an enum. Does it worth it?
    private static final String VALID_TRANSFORM = "validAggTransform";
    private static final String INVALID_TRANSFORM = "invalidAggTransform";
    private static final String VALID_LOAD = "validAggLoad";
    private static final String INVALID_LOAD = "invalidAggLoad";



    @Autowired
    VariantAggregatedConfiguration variantConfiguration;

    @Autowired
    private Job job;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private JobExecutionListener listener;


    @Test
    public void validTransform() throws JobExecutionException, IOException {
        String input = VariantAggregatedConfigurationTest.class.getResource(FILE_AGGREGATED).getFile();
        String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        String dbName = VALID_TRANSFORM;
        
        JobParameters parameters = new JobParametersBuilder()
                .addString("input", input)
                .addString("outputDir", "/tmp")
                .addString("dbName", dbName)
                .addString("compressExtension", ".gz")
                .addString("includeSrc", "FIRST_8_COLUMNS")
                .addString("studyType", "COLLECTION")
                .addString("studyName", "studyName")
                .addString("studyId", "1")
                .addString("fileId", "1")
                .addString("opencga.app.home", opencgaHome)
                .addString("aggregated", VariantSource.Aggregation.BASIC.toString())
                .addString("includeStats", "false")
                .addString("skipLoad", "true")
                .toJobParameters();

        JobExecution execution = jobLauncher.run(job, parameters);

        assertEquals(input, execution.getJobParameters().getString("input"));
        assertEquals("COMPLETED", execution.getExitStatus().getExitCode());

        ////////// check transformed file
        String outputFilename = getTransformedOutputPath(Paths.get(FILE_AGGREGATED).getFileName(),
                parameters.getString("compressExtension"), parameters.getString("outputDir"));
        logger.info("reading transformed output from: " + outputFilename);


        BufferedReader file = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(outputFilename))));
        long lines = 0;
        while (file.readLine() != null) {
            lines++;
        }
        file.close();
        assertEquals(156, lines);
    }
//
//    @Test
//    public void invalidTransform() throws JobExecutionException {
//        String input = VariantAggregatedConfigurationTest.class.getResource(FILE_WRONG_NO_ALT).getFile();
//        String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
//        String dbName = INVALID_TRANSFORM;
//
//        JobParameters parameters = new JobParametersBuilder()
//                .addString("input", input)
//                .addString("outputDir", "/tmp")
//                .addString("dbName", dbName)
//                .addString("compressExtension", ".gz")
//                .addString("compressGenotypes", "true")
//                .addString("includeSrc", "FIRST_8_COLUMNS")
//                .addString("aggregated", "NONE")
//                .addString("studyType", "COLLECTION")
//                .addString("studyName", "studyName")
//                .addString("studyId", "2")
//                .addString("fileId", "2")
//                .addString("opencga.app.home", opencgaHome)
//                .addString("skipLoad", "true")
//                .toJobParameters();
//
//        JobExecution execution = jobLauncher.run(job, parameters);
//
//        assertEquals(input, execution.getJobParameters().getString("input"));
//        assertEquals("FAILED", execution.getExitStatus().getExitCode());
//    }

    @Test
    public void validLoad() throws JobExecutionException, IllegalAccessException, ClassNotFoundException, InstantiationException, StorageManagerException, IOException {
        String input = VariantAggregatedConfigurationTest.class.getResource(FILE_AGGREGATED).getFile();
        String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        String dbName = VALID_LOAD;
        
        JobParameters parameters = new JobParametersBuilder()
                .addString("input", input)
                .addString("outputDir", "/tmp")
                .addString("dbName", dbName)
                .addString("compressExtension", ".gz")
                .addString("compressGenotypes", "true")
                .addString("includeSrc", "FIRST_8_COLUMNS")
                .addString("aggregated", "NONE")
                .addString("studyType", "COLLECTION")
                .addString("studyName", "studyName")
                .addString("studyId", "1")
                .addString("fileId", "1")
                .addString("aggregated", VariantSource.Aggregation.BASIC.toString())
                .addString("includeStats", "false")
                .addString("opencga.app.home", opencgaHome)
                .toJobParameters();

        JobExecution execution = jobLauncher.run(job, parameters);

        assertEquals(input, execution.getJobParameters().getString("input"));
        assertEquals("COMPLETED", execution.getExitStatus().getExitCode());

        // check ((documents in DB) == (lines in transformed file))
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_AGGREGATED).getFileName(),
                parameters.getString("compressExtension"), parameters.getString("outputDir"));
        long lines = getLines(new GZIPInputStream(new FileInputStream(outputFilename)));

        assertEquals(countRows(iterator), lines);

        // check stats aren't loaded
        assertTrue(variantDBAdaptor.iterator(new QueryOptions()).next().getSourceEntries().values().iterator().next().getCohortStats().isEmpty());
    }
//
//    @Test
//    public void invalidLoad() throws JobExecutionException {
//        String input = VariantAggregatedConfigurationTest.class.getResource(FILE_20).getFile();
//        String outdir = input;
//        String dbName = INVALID_LOAD;
////        String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";  // TODO make it fail better
//
//        JobParameters parameters = new JobParametersBuilder()
//                .addString("input", input)
//                .addString("outputDir", outdir)
//                .addString("dbName", dbName)
//                .addString("compressExtension", ".gz")
//                .addString("compressGenotypes", "true")
//                .addString("includeSrc", "FIRST_8_COLUMNS")
//                .addString("aggregated", "NONE")
//                .addString("studyType", "COLLECTION")
//                .addString("studyName", "studyName")
//                .addString("studyId", "1")
//                .addString("fileId", "1")
//                .addString("opencga.app.home", null)
//                .toJobParameters();
//
//        Job listenedJob = jobBuilderFactory
//                .get("listenedjob")
//                .incrementer(new RunIdIncrementer())
//                .listener(listener)
//                .start(variantConfiguration.load())
//                .build();
//
//        System.out.println("parameters in load tests" + parameters.toString());
//        JobExecution execution = jobLauncher.run(listenedJob, parameters);
//
//        assertEquals(input, execution.getJobParameters().getString("input"));
//        assertEquals("FAILED", execution.getExitStatus().getExitCode());
//    }

    /*
    @Test
    public void validCreateStats() {

    }

    @Test
    public void invalidCreateStats() {

    }
    */
    @Test
    public void validLoadStats() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, IllegalAccessException, ClassNotFoundException, InstantiationException, StorageManagerException, IOException {String input = VariantAggregatedConfigurationTest.class.getResource(FILE_AGGREGATED).getFile();
        String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        String dbName = VALID_LOAD;

        JobParameters parameters = new JobParametersBuilder()
                .addString("input", input)
                .addString("outputDir", "/tmp")
                .addString("dbName", dbName)
                .addString("compressExtension", ".gz")
                .addString("compressGenotypes", "true")
                .addString("includeSrc", "FIRST_8_COLUMNS")
                .addString("aggregated", "NONE")
                .addString("studyType", "COLLECTION")
                .addString("studyName", "studyName")
                .addString("studyId", "1")
                .addString("fileId", "1")
                .addString("aggregated", VariantSource.Aggregation.BASIC.toString())
                .addString("includeStats", "true")
                .addString("opencga.app.home", opencgaHome)
                .toJobParameters();

        JobExecution execution = jobLauncher.run(job, parameters);

        assertEquals(input, execution.getJobParameters().getString("input"));
        assertEquals("COMPLETED", execution.getExitStatus().getExitCode());

        // check ((documents in DB) == (lines in transformed file))
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_AGGREGATED).getFileName(),
                parameters.getString("compressExtension"), parameters.getString("outputDir"));
        long lines = getLines(new GZIPInputStream(new FileInputStream(outputFilename)));

        assertEquals(countRows(iterator), lines);

        // check stats aren't loaded
        assertFalse(variantDBAdaptor.iterator(new QueryOptions()).next().getSourceEntries().values().iterator().next().getCohortStats().isEmpty());

    }
/*
    @Test
    public void invalidLoadStats() {

    }
    */

    @BeforeClass
    public static void beforeTests() throws UnknownHostException {
        cleanDBs();
    }

    @AfterClass
    public static void afterTests() throws UnknownHostException {
//        cleanDBs();
    }

    private static void cleanDBs() throws UnknownHostException {
        // Delete Mongo collection
        MongoClient mongoClient = new MongoClient("localhost");
        List<String> dbs = Arrays.asList(VALID_TRANSFORM, INVALID_TRANSFORM, VALID_LOAD, INVALID_LOAD);
        for (String dbName : dbs) {
            DB db = mongoClient.getDB(dbName);
            db.dropDatabase();
        }
        mongoClient.close();
    }

    /*
    public JobLauncherTestUtils jobLauncherTestUtils(JobRepository jobRepository, JobLauncher jobLauncher, Job job) {
        JobLauncherTestUtils jobLauncherTestUtils = new JobLauncherTestUtils();
        jobLauncherTestUtils.setJobRepository(jobRepository);
        jobLauncherTestUtils.setJobLauncher(jobLauncher);
        jobLauncherTestUtils.setJob(job);
        return jobLauncherTestUtils;
    }
    */

}
