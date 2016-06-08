/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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

import embl.ebi.variation.eva.pipeline.listeners.VariantJobParametersListener;
import embl.ebi.variation.eva.pipeline.steps.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by jmmut on 2015-10-14.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {VariantStatsConfiguration.class, VariantStatsConfigurationTest.Configs.class})
public class VariantStatsConfigurationTest {

    public static final String FILE_20 = "/small20.vcf.gz";
    public static final String FILE_22 = "/small22.vcf.gz";
    public static final String FILE_WRONG_NO_ALT = "/wrong_no_alt.vcf.gz";

    private static final Logger logger = LoggerFactory.getLogger(VariantStatsConfigurationTest.class);

    // iterable doing an enum. Does it worth it?
//    private static final String VALID_CREATE_STATS = "VariantStatsConfigurationTest_vc";
    private static final String INVALID_CREATE_STATS = "VariantStatsConfigurationTest_ic";
    private static final String VALID_LOAD_STATS = "VariantStatsConfigurationTest_vl";
    private static final String INVALID_LOAD_STATS = "VariantStatsConfigurationTest_il";

    @Autowired
    PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer;

    @Autowired
    private Job job;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    public ObjectMap variantOptions;

    @Autowired
    public ObjectMap pipelineOptions;

    @Configuration
    static class Configs {
        private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

        @Bean
        static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
            PropertySourcesPlaceholderConfigurer configurer = new PropertySourcesPlaceholderConfigurer();

            Properties properties = new Properties();
            properties.put("input", "");
            properties.put("overwriteStats", "false");
            properties.put("calculateStats", "false");
            properties.put("outputDir", "/tmp");
            properties.put("dbName", "");
            properties.put("compressExtension", ".gz");
            properties.put("compressGenotypes", "true");
            properties.put("includeSrc", "FIRST_8_COLUMNS");
            properties.put("pedigree", "FIRST_8_COLUMNS");
            properties.put("annotate", "false");
            properties.put("includeSamples", "false");
            properties.put("includeStats", "false");
            properties.put("aggregated", "NONE");
            properties.put("studyType", "COLLECTION");
            properties.put("studyName", "studyName");
            properties.put("studyId", "1");
            properties.put("fileId", "1");
            properties.put("opencga.app.home", opencgaHome);
            properties.put("skipLoad", "true");
            properties.put("skipStatsCreate", "true");
            properties.put("skipStatsLoad", "true");
            properties.put("skipAnnotGenerateInput", "true");
            properties.put("skipAnnotCreate", "true");
            properties.put("skipAnnotLoad", "true");
            properties.put("vepInput", "");
            properties.put("vepOutput", "");
            properties.put("vepPath", "");
            properties.put("vepCacheDirectory", "");
            properties.put("vepCacheVersion", "");
            properties.put("vepSpecies", "");
            properties.put("vepFasta", "");
            properties.put("vepNumForks", "3");

            configurer.setProperties(properties);

            return configurer;
        }
    }

    /**
     * This test has to fail because it will try to extract variants from a non-existent DB.
     */
    @Test
    public void invalidCreateStats() throws JobExecutionException {
        String input = VariantStatsConfigurationTest.class.getResource(FILE_20).getFile();
        VariantSource source = new VariantSource(input, "1", "1", "studyName");
        String dbName = INVALID_CREATE_STATS;
        String outputDir = "/tmp";

        pipelineOptions.put("input", input);
        pipelineOptions.put("outputDir", outputDir);
        pipelineOptions.put("skipLoad", false);
        pipelineOptions.put("skipStatsCreate", false);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        JobExecution execution = jobLauncher.run(job, new JobParameters());

        assertEquals(input, pipelineOptions.getString("input"));
        assertEquals(ExitStatus.FAILED.getExitCode(), execution.getExitStatus().getExitCode());
    }

    @Test
    public void validLoadStats() throws JobExecutionException, IOException, IllegalAccessException,
            ClassNotFoundException, InstantiationException, StorageManagerException {

        String input = VariantStatsConfigurationTest.class.getResource(FILE_20).getFile();
        VariantSource source = new VariantSource(input, "1", "1", "studyName");
        String dbName = VALID_LOAD_STATS;
        String outputDir = input;

        pipelineOptions.put("input", input);
        pipelineOptions.put("outputDir", outputDir);
        pipelineOptions.put("skipLoad", false);
        pipelineOptions.put("skipStatsLoad", false);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        JobExecution execution = jobLauncher.run(job, new JobParameters());
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());

        // check the DB docs have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());
    }

    /**
     * This test should fail because the variants.stats file is malformed, with an extra `"`.
     */
    @Test
    public void invalidLoadStats() throws JobExecutionException {
        String input = VariantStatsConfigurationTest.class.getResource(FILE_20).getFile();
        VariantSource source = new VariantSource(input, "4", "1", "studyName");
        String dbName = INVALID_LOAD_STATS;
        String outputDir = input;

        pipelineOptions.put("input", input);
        pipelineOptions.put("outputDir", outputDir);
        pipelineOptions.put("skipLoad", false);
        pipelineOptions.put("skipStatsLoad", false);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        JobExecution execution = jobLauncher.run(job, new JobParameters());

        assertEquals(input, pipelineOptions.getString("input"));
        assertEquals(ExitStatus.FAILED.getExitCode(), execution.getExitStatus().getExitCode());
    }

    @BeforeClass
    public static void beforeTests() throws IOException, InterruptedException {
        cleanDBs();
        fillDB();
    }

    @AfterClass
    public static void afterTests() throws UnknownHostException {
        cleanDBs();
    }

    private static void cleanDBs() throws UnknownHostException {
        JobTestUtils.cleanDBs(INVALID_CREATE_STATS, VALID_LOAD_STATS, INVALID_LOAD_STATS);
    }

    public static void fillDB() throws IOException, InterruptedException {
        String dump = VariantStatsConfigurationTest.class.getResource("/dump/").getFile();
        logger.info("restoring DB from " + dump);
        Process exec = Runtime.getRuntime().exec("mongorestore " + dump);
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
}
