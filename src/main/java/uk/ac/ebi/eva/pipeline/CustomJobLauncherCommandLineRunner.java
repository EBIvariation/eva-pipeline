package uk.ac.ebi.eva.pipeline;

import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

@Component
public class CustomJobLauncherCommandLineRunner extends JobLauncherCommandLineRunner {

    @Value("${spring.batch.job.names:#{null}}")
    private String springBatchJob;

    @Autowired
    private ParameterFromProperties parameterFromProperties;

    public CustomJobLauncherCommandLineRunner(JobLauncher jobLauncher, JobExplorer jobExplorer) {
        super(jobLauncher, jobExplorer);
    }

    public void run(String... args) throws JobExecutionException {
        if(springBatchJob!=null) {
            setJobNames(springBatchJob);
        }
        launchJobFromProperties(parameterFromProperties.getProperties());
    }

}
