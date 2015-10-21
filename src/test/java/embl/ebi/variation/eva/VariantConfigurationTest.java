package embl.ebi.variation.eva;

import embl.ebi.variation.eva.pipeline.configuration.StandaloneInfrastructureConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.util.StringUtils;

/**
 * Created by jmmut on 2015-10-14.
 * //TODO implement TestExecutionListener for database tear down, or H2 datasource
 *      [at]TestExecutionListener({miImpl.class})
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {VariantConfiguration.class, StandaloneInfrastructureConfiguration.class})
public class VariantConfigurationTest {

    public static final String FILE_20 = "/small20.vcf.gz";
    public static final String FILE_22 = "/small22.vcf.gz";
    public static final String FILE_WITH_ERROR = "/wrong.vcf.gz";

    private static final Logger logger = LoggerFactory.getLogger(VariantConfigurationTest.class);

    @Autowired
    VariantConfiguration variantConfiguration;

    @Autowired
    private Job job;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    PipelineConfig pipelineConfig;

    /**
     * Launch a job with given parameters, but always forcing a new execution of the instance.
     * This is approximately how JobLauncherCommandLineRunner works. The main difference is that it only uses a new
     * JobExecution if the previous one was successfully completed. As running an already completed JobExecution raises
     * a "already completed" exception, we force a new JobExecution everytime.
     * @see org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner
     */
    private static JobExecution execute(Job job, String[] args, JobExplorer jobExplorer, JobLauncher jobLauncher)
            throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
        List<JobInstance> lastInstances = jobExplorer.getJobInstances(job.getName(), 0, 1);
        JobParameters nextParameters;

        JobParametersIncrementer incrementer = job.getJobParametersIncrementer();
        if (incrementer == null) {
            throw new RuntimeException("test needs a job with a incrementer");
        }

        if (lastInstances.isEmpty()) {
            nextParameters = incrementer.getNext(new JobParameters()); // Start from a completely clean sheet
        } else {
            List<JobExecution> previousExecutions = jobExplorer.getJobExecutions(lastInstances.get(0));
            JobExecution previousExecution = previousExecutions.get(0);
            nextParameters = incrementer.getNext(removeNonIdentifying(previousExecution.getJobParameters()));   // always getting next run.id
        }

        JobParameters testParameters = new DefaultJobParametersConverter().getJobParameters(
                StringUtils.splitArrayElementsIntoProperties(args, "="));


        JobParameters parameters = merge(nextParameters, testParameters);
        logger.info("using as test parameters: " + parameters.toString());
        return jobLauncher.run(job, parameters);
    }

    /**
     * @see org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner::removeNonIdentifying(Map<String, JobParameter> parameters)
     */
    private static JobParameters removeNonIdentifying(JobParameters parameters) {
        Map<String, JobParameter> parameterMap = parameters.getParameters();
        HashMap<String, JobParameter> copy = new HashMap<>(parameterMap);
        for (Map.Entry<String, JobParameter> parameter : copy.entrySet()) {
            if (!parameter.getValue().isIdentifying()) {
                parameterMap.remove(parameter.getKey());
            }
        }
        return new JobParameters(parameterMap);
    }

    /**
     * @see org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner
     */
    private static JobParameters merge(JobParameters parameters,
                                       JobParameters additionals) {
        Map<String, JobParameter> merged = new HashMap<>();
        merged.putAll(parameters.getParameters());
        merged.putAll(additionals.getParameters());
        parameters = new JobParameters(merged);
        return parameters;
    }

    private String getTransformedOutputPath(Path input, String compressExtension, String outputDir) {
        return Paths.get(outputDir).resolve(input) + ".variants.json" + compressExtension;
    }

    @Test
    public void validTransform() throws Exception {
        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        
        pipelineConfig.input = input;
        pipelineConfig.outputDir = "/tmp";
        pipelineConfig.dbName = "validTransformTest";
        pipelineConfig.fileId = "10";
        pipelineConfig.compressExtension = ".gz";
        pipelineConfig.calculateStats = false;
        pipelineConfig.annotate = false;
        
        JobExecution execution = execute(job, new String[]{}, jobExplorer, jobLauncher);
//        JobExecution execution = jobLauncher.run(job, new JobParameters());

        assertEquals("COMPLETED", execution.getExitStatus().getExitCode());


        ////////// check transformed file
        String outputFilename = getTransformedOutputPath(Paths.get(FILE_20).getFileName(), pipelineConfig.compressExtension, pipelineConfig.outputDir);
        logger.info("reading transformed output from: " + outputFilename);


        BufferedReader file = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(outputFilename))));
        long lines = 0;
        while (file.readLine() != null) {
            lines++;
        }
        file.close();
        assertEquals(300, lines);
    }


    @Test
    public void invalidTransform() throws Exception {
        String input = VariantConfigurationTest.class.getResource(FILE_WITH_ERROR).getFile();

        pipelineConfig.input = input;
        pipelineConfig.outputDir = "/tmp";
        pipelineConfig.dbName = "invalidTransformTest";
        pipelineConfig.fileId = "10";
        pipelineConfig.compressExtension = ".gz";
        pipelineConfig.calculateStats = false;
        pipelineConfig.annotate = false;
        
        JobExecution execution = execute(job, new String[]{}, jobExplorer, jobLauncher);
        
        assertEquals("FAILED", execution.getExitStatus().getExitCode());
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