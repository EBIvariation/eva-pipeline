package embl.ebi.variation.eva;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.boot.SpringApplication;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;

/**
 * Created by jmmut on 2015-10-14.
 * //TODO implement TestExecutionListener for database tear down
 *      [at]TestExecutionListener({miImpl.class})
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {VariantConfiguration.class})
public class VariantConfigurationTest extends AbstractTransactionalJUnit4SpringContextTests {

    public static final String FILE_20 = "/small20.vcf.gz";
    public static final String FILE_22 = "/small22.vcf.gz";
    public static final String FILE_WITH_ERROR = "/wrong.vcf.gz";

    private static final Logger logger = LoggerFactory.getLogger(VariantConfigurationTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Autowired
    VariantConfiguration variantConfiguration;

//    @Autowired
//    JobLauncher jobLauncher;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

//    @Bean
//    public JobLauncherTestUtils jobLauncherTestUtils(JobRepository jobRepository, JobLauncher jobLauncher, Job job) {
//    public JobLauncherTestUtils jobLauncherTestUtils() {
//        JobLauncherTestUtils jobLauncherTestUtils = new JobLauncherTestUtils();
//        jobLauncherTestUtils.setJobRepository(jobRepository);
//        jobLauncherTestUtils.setJobLauncher(jobLauncher);
//        jobLauncherTestUtils.setJob(job);
//        return jobLauncherTestUtils;
//    }


    @Test
    public void validTransform() throws Exception {
        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();

        String dbName = "validTransformTest";
        String compressExtension = ".gz";
        String outputDir = "/tmp";

        String[] args = {
                "--input=" + input,
                "--outputDir=" + outputDir,
                "--dbName=" + dbName,
                "--fileId=10",
                "--calculateStats=false",
        };

        Application.main(args);


        String outputFilename = getTransformedOutputPath(Paths.get(FILE_20).getFileName(), compressExtension, outputDir);
        logger.info("reading transformed output from: " + outputFilename);


        BufferedReader file = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(outputFilename))));
        long lines = 0;
        while (file.readLine() != null) {
            lines++;
        }
        file.close();
        assertEquals(300, lines);
    }

    private String getTransformedOutputPath(Path input, String compressExtension, String outputDir) {
        return Paths.get(outputDir).resolve(input) + ".variants.json" + compressExtension;
    }

    @Test
    public void invalidTransform() throws Exception {
        String input = VariantConfigurationTest.class.getResource(FILE_WITH_ERROR).getFile();

        String dbName = "invalidTransformTest";
        String compressExtension = ".gz";
        String outputDir = "/tmp";

        String[] args = {
                "--input=" + input,
                "--outputDir=" + outputDir,
                "--dbName=" + dbName,
                "--fileId=10",
                "--calculateStats=false",
        };

        Application.main(args);

        String outputFilename = getTransformedOutputPath(Paths.get(FILE_WITH_ERROR).getFileName(), compressExtension, outputDir);
        logger.info("reading transformed output from: " + outputFilename);

//        thrown.expect(FileNotFoundException.class);
        GZIPInputStream fileInputStream = new GZIPInputStream(new FileInputStream(outputFilename)); // TODO this does not work
        fileInputStream.skip(Long.MAX_VALUE);
        fileInputStream.close();


        ////////

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals("COMPLETED", jobExecution.getExitStatus().toString());
    }

    @Test
    public void validLoad() {

    }

    @Test
    public void invalidLoad() {

    }

    @Test
    public void validCreateStats() {

    }

    @Test
    public void invalidCreateStats() {

    }
    @Test
    public void validLoadStats() {

    }

    @Test
    public void invalidLoadStats() {

    }
}