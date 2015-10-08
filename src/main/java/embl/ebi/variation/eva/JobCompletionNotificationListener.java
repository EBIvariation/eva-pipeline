package embl.ebi.variation.eva;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JobOperator jobOperator;
    @Autowired
    private JobExplorer jobExplorer;
    @Autowired
    private JobRegistry jobRegistry;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor;

    private Thread hook;


    @Override
    public void beforeJob(final JobExecution jobExecution) {
        log.info("on before job operator: ----------------------------------" + this.jobOperator.getJobNames());
        log.info("on before job explorer: ----------------------------------" + this.jobExplorer.getJobNames());
        log.info("on before job registry: ----------------------------------" + this.jobRegistry.getJobNames());
        log.info("setting stop for job " + jobExecution.getId() + "----------------------------------");

        log.info("on before ----------- job configurations" + jobExecution.getJobConfigurationName());

        // TODO make this work. now it doesn't do anything, but perhaps it is close to work
        hook = new Thread() {
            @Override
            public void run() {
                try {
                    super.run();
                    log.info("waiting for job " + jobExecution.getId() + " to stop...----------------------------------");
                    for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                        stepExecution.setTerminateOnly();
                        stepExecution.setStatus(BatchStatus.FAILED);
                        stepExecution.setExitStatus(ExitStatus.FAILED);
                        log.info("step.setTerminate ------- "+ stepExecution.getStepName());
                    }
                    jobExecution.setExitStatus(ExitStatus.FAILED);
                    jobExecution.setStatus(BatchStatus.FAILED);

                    log.info("----------- job names " + jobOperator.getJobNames());
                    log.info("----------- job status " + jobExecution.getExitStatus());
                    log.info("----------- job configurations " + jobExecution.getJobConfigurationName());
//                    jobOperator.stop(jobExecution.getId());

//                    while (jobExecution.isRunning()) {
//                        log.info("waiting for job to stop...----------------------------------");
//                        try {
//                            Thread.sleep(100);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
                    log.info("job stopped ----------------------------------");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Runtime.getRuntime().addShutdownHook(hook);
    }
    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("on after job operator: ----------------------------------" + this.jobOperator.getJobNames());
        log.info("on after job explorer: ----------------------------------" + this.jobExplorer.getJobNames());
        log.info("on after job registry: ----------------------------------" + this.jobRegistry.getJobNames());
//        log.info("on before job launcher: ----------------------------------" + this.jobLauncher.getJobNames());
        Runtime.getRuntime().removeShutdownHook(hook);
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED! Time to verify the results");

            List<Person> results = jdbcTemplate.query("SELECT first_name, last_name FROM people", new RowMapper<Person>() {
                @Override
                public Person mapRow(ResultSet rs, int row) throws SQLException {
                    return new Person(rs.getString(1), rs.getString(2));
                }
            });

            for (Person person : results) {
                log.info("Found <" + person + "> in the database.");
            }

        }
    }

//    @Autowired
//    public void setJobOperator(JobOperator jobOperator) {
//        this.jobOperator = jobOperator;
//        log.info("setting job operator " + jobOperator + "----------------------------------" + this.jobOperator.getJobNames());
//    }

//    @Autowired
//    public void setJobExplorer(JobExplorer jobExplorer) {
//        this.jobExplorer = jobExplorer;
//        log.info("setting job explorer " + jobExplorer + "----------------------------------" + this.jobExplorer.getJobNames());
//    }
}
