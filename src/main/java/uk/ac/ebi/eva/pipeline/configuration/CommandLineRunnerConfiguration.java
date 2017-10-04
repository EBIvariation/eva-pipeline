package uk.ac.ebi.eva.pipeline.configuration;

import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.pipeline.runner.EvaPipelineJobLauncherCommandLineRunner;

@Configuration
public class CommandLineRunnerConfiguration {

    @Bean
    JobLauncherCommandLineRunner jobLauncherCommandLineRunner(JobLauncher jobLauncher, JobExplorer jobExplorer,
                                                              JobRepository jobRepository){
        return new EvaPipelineJobLauncherCommandLineRunner(jobLauncher, jobExplorer, jobRepository);
    }

}
