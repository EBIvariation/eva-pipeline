package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.pipeline.VariantJobsArgs;
import embl.ebi.variation.eva.pipeline.listeners.VariantJobParametersListener;
import embl.ebi.variation.eva.pipeline.steps.VariantsAnnotLoad;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getLines;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getTransformedOutputPath;
import static embl.ebi.variation.eva.pipeline.jobs.VariantConfigurationTest.FILE_20;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by diego on 23/05/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantConfiguration.class, VariantJobParametersListener.class})
public class VariantConfigurationTestNew {

    static String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();

/*    @Bean
    public VariantJobsArgs getVariantJobsArgs() {
        ObjectMap p  = new ObjectMap();
        p.put("input", input);
        VariantJobsArgs c = new VariantJobsArgs();
        c.setPipelineOptions(p);
        return c;
    }*/

    //@Autowired
    //private JobLauncherTestUtils jobLauncherTestUtils;

    @BeforeClass
    public static void beforeClass() {
        String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        Config.setOpenCGAHome(opencgaHome);

        System.setProperty("input", input);
        System.setProperty("overwriteStats", "false");
        System.setProperty("calculateStats", "false");

        System.setProperty("outputDir", "/tmp");
        System.setProperty("dbName", VALID_TRANSFORM);
        System.setProperty("compressExtension", ".gz");
        System.setProperty("compressGenotypes", "true");
        System.setProperty("includeSrc", "FIRST_8_COLUMNS");
        System.setProperty("pedigree", "FIRST_8_COLUMNS");
        System.setProperty("annotate", "false");
        System.setProperty("includeSamples", "false");
        System.setProperty("includeStats", "false");
        System.setProperty("aggregated", "NONE");
        System.setProperty("studyType", "COLLECTION");
        System.setProperty("studyName", "studyName");
        System.setProperty("studyId", "1");
        System.setProperty("fileId", "1");
        System.setProperty("opencga.app.home", opencgaHome);
        System.setProperty("skipLoad", "true");
        System.setProperty("skipStatsCreate", "true");
        System.setProperty("skipStatsLoad", "true");
        System.setProperty("skipAnnotGenerateInput", "true");
        System.setProperty("skipAnnotCreate", "true");
        System.setProperty("skipAnnotLoad", "true");
        System.setProperty("vepInput", "");
        System.setProperty("vepOutput", "");
        System.setProperty("vepPath", "");
        System.setProperty("vepCacheDirectory", "");
        System.setProperty("vepCacheVersion", "");
        System.setProperty("vepSpecies", "");
        System.setProperty("vepFasta", "");
        System.setProperty("vepNumForks", "");
    }

    @AfterClass
    public static void afterClass() {
        System.clearProperty("some.property");
    }

    private static final String VALID_TRANSFORM = "VariantConfigurationTest_vt";

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @Test
    public void ss() throws Exception {
//        JobLauncherTestUtils jobLauncherTestUtils = new JobLauncherTestUtils();
//        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        String dbName = VALID_TRANSFORM;

/*        JobParameters parameters = new JobParametersBuilder()
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
                .addString("opencga.app.home", opencgaHome)
                .addString("skipLoad", "true")
                .addString("skipStatsCreate", "true")
                .addString("skipStatsLoad", "true")
                .addString("skipAnnotGenerateInput", "true")
                .addString("skipAnnotCreate", "true")
                .addString(VariantsAnnotLoad.SKIP_ANNOT_LOAD, "true")
                .toJobParameters();*/

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_20).getFileName(),
                ".gz", "/tmp");
        //logger.info("transformed output will be at: " + outputFilename);
        File file = new File(outputFilename);
        file.delete();
        assertFalse(file.exists());

        JobExecution execution = jobLauncher.run(job, new JobParameters());

//        assertEquals(input, execution.getJobParameters().getString("input"));
        ObjectMap mm = (ObjectMap) execution.getExecutionContext().get("variantOptions");
        assertEquals(input, mm.getString("input"));
        assertEquals(ExitStatus.COMPLETED.getExitCode(), execution.getExitStatus().getExitCode());

        ////////// check transformed file
        long lines = getLines(new GZIPInputStream(new FileInputStream(outputFilename)));
        assertEquals(300, lines);
    }

/*    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils(){
        return new JobLauncherTestUtils();
    }*/




}