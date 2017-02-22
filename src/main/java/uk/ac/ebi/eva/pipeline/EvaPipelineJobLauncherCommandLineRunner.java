/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.converter.JobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobParametersNotFoundException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.stereotype.Component;
import org.springframework.util.PatternMatchUtils;
import org.springframework.util.StringUtils;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.pipeline.runner.JobExecutionApplicationListener;
import uk.ac.ebi.eva.pipeline.runner.ManageJobsUtils;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NoJobToExecuteException;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NoParametersHaveBeenPassedException;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NoPreviousJobExecutionException;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NotValidParameterFormatException;
import uk.ac.ebi.eva.pipeline.runner.exceptions.UnexpectedErrorReadingFileException;
import uk.ac.ebi.eva.pipeline.runner.exceptions.UnexpectedFileEncodingException;
import uk.ac.ebi.eva.pipeline.runner.exceptions.UnknownJobException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * This class is a modified version of the default JobLauncherCommandLineRunner.
 * It's main differences are:
 * -If no job is specified then the execution stops.
 * -Job parameters can be passed from command line as normal parameters.
 * -Job parameters can be passed from a properties file by the user.
 * -The user can restart a job that has been run previously marking the previous execution as failed.
 */
@Component
public class EvaPipelineJobLauncherCommandLineRunner extends JobLauncherCommandLineRunner implements CommandLineRunner,
        ApplicationEventPublisherAware, ExitCodeGenerator {

    private static final Logger logger = LoggerFactory.getLogger(EvaPipelineJobLauncherCommandLineRunner.class);

    public static final String SPRING_BATCH_JOB_NAME_PROPERTY = "spring.batch.job.names";

    public static final int EXIT_WITHOUT_ERRORS = 0;

    private static final int EXIT_WITH_ERRORS = 1;

    @Value("${" + SPRING_BATCH_JOB_NAME_PROPERTY + ":#{null}}")
    private String jobName;

    @Value("${" + JobParametersNames.PROPERTY_FILE_PROPERTY + ":#{null}}")
    private String propertyFilePath;

    @Value("${" + JobParametersNames.RESTART_PROPERTY + ":false}")
    private boolean restartPreviousExecution;

    private Collection<Job> jobs;

    private JobRepository jobRepository;

    private JobRegistry jobRegistry;

    private JobParametersConverter converter;

    @Autowired
    private JobExecutionApplicationListener jobExecutionApplicationListener;

    public EvaPipelineJobLauncherCommandLineRunner(JobLauncher jobLauncher, JobExplorer jobExplorer,
                                                   JobRepository jobRepository) {
        super(jobLauncher, jobExplorer);
        jobs = Collections.emptySet();
        this.jobRepository = jobRepository;
        converter = new DefaultJobParametersConverter();
    }

    @Autowired(required = false)
    public void setJobRegistry(JobRegistry jobRegistry) {
        this.jobRegistry = jobRegistry;
    }

    @Autowired(required = false)
    public void setJobParametersConverter(JobParametersConverter converter) {
        this.converter = converter;
    }

    @Autowired(required = false)
    public void setJobs(Collection<Job> jobs) {
        this.jobs = jobs;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setPropertyFilePath(String propertyFilePath) {
        this.propertyFilePath = propertyFilePath;
    }

    @Override
    public int getExitCode() {
        if (jobExecutionApplicationListener.isJobExecutionComplete()) {
            return EXIT_WITHOUT_ERRORS;
        } else {
            return EXIT_WITH_ERRORS;
        }
    }

    @Override
    public void run(String... args) throws JobExecutionException {
        try {
            JobParameters jobParameters = getJobParameters(args);
            if (restartPreviousExecution) {
                restartPreviousJobExecution(jobParameters);
            }
            launchJob(jobParameters);
        } catch (NoJobToExecuteException | NoParametersHaveBeenPassedException | UnexpectedFileEncodingException
                | FileNotFoundException | UnexpectedErrorReadingFileException | NoPreviousJobExecutionException
                | NotValidParameterFormatException | UnknownJobException e) {
            logger.error(e.getMessage());
            logger.trace("Error trace", e);
        }
    }

    private void launchJob(JobParameters jobParameters) throws JobExecutionException, UnknownJobException {
        executeLocalJobs(jobParameters);
        executeRegisteredJobs(jobParameters);
    }

    private void executeLocalJobs(JobParameters jobParameters) throws JobExecutionException {
        for (Job job : this.jobs) {
            if (!PatternMatchUtils.simpleMatch(jobName, job.getName())) {
                logger.debug("Skipped job: " + job.getName());
                continue;
            }
            execute(job, jobParameters);
            return;
        }
    }

    private void executeRegisteredJobs(JobParameters jobParameters) throws JobExecutionException {
        if (this.jobRegistry != null) {
            try {
                execute(jobRegistry.getJob(jobName), jobParameters);
                return;
            } catch (NoSuchJobException ex) {
                logger.debug("No job found in registry for job name: " + jobName);
            }
        }
    }

    @Override
    protected void execute(Job job, JobParameters jobParameters) throws JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException,
            JobParametersNotFoundException {
        logger.info("Running job '" + jobName + "' with parameters: " + jobParameters);
        super.execute(job, jobParameters);
    }

    private JobParameters getJobParameters(String[] args) throws NotValidParameterFormatException,
            NoJobToExecuteException, NoParametersHaveBeenPassedException, FileNotFoundException,
            UnexpectedFileEncodingException, UnexpectedErrorReadingFileException {
        checkAllParametersStartByDoubleDash(args);

        String[] processedArgs = removeStartingHypens(args);
        String[] filteredArgs = filterLauncherOnlyParameters(processedArgs);

        checkIfJobNamesHasBeenDefined();
        checkIfPropertiesHaveBeenProvided(filteredArgs);

        // Command line properties have precedence over file defined ones.
        Properties properties = new Properties();
        if (propertyFilePath != null) {
            properties.putAll(getPropertiesFile());
        }
        properties.putAll(StringUtils.splitArrayElementsIntoProperties(filteredArgs, "="));
        return converter.getJobParameters(properties);
    }

    private void restartPreviousJobExecution(JobParameters jobParameters) throws
            NoPreviousJobExecutionException {
        logger.info("Force restartPreviousExecution of job '" + jobName + "' with parameters: " + jobParameters);
        ManageJobsUtils.markLastJobAsFailed(jobRepository, jobName, jobParameters);
    }

    private void checkAllParametersStartByDoubleDash(String[] args) throws NotValidParameterFormatException {
        for (String arg : args) {
            if (!arg.startsWith("--")) {
                throw new NotValidParameterFormatException(arg);
            }
        }
    }

    private String[] removeStartingHypens(String[] args) {
        return Arrays.stream(args).map(arg -> arg.substring(2)).toArray(String[]::new);
    }

    private String[] filterLauncherOnlyParameters(String[] args) {
        return Arrays.stream(args).filter(arg -> isLauncherParameter(arg)).toArray(String[]::new);
    }

    private static boolean isLauncherParameter(String arg) {
        return !(arg.startsWith(JobParametersNames.PROPERTY_FILE_PROPERTY + "=")
                || arg.startsWith(JobParametersNames.RESTART_PROPERTY + "=")
                || arg.startsWith(SPRING_BATCH_JOB_NAME_PROPERTY + "="));
    }

    private Properties getPropertiesFile() throws FileNotFoundException, UnexpectedErrorReadingFileException,
            UnexpectedFileEncodingException {
        Properties propertiesFile = new Properties();
        try {
            propertiesFile.putAll(readPropertiesFromFile(propertyFilePath));
            return propertiesFile;
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw new UnexpectedErrorReadingFileException(propertyFilePath, e);
        } catch (IllegalArgumentException e) {
            throw new UnexpectedFileEncodingException(propertyFilePath);
        }
    }

    private void checkIfPropertiesHaveBeenProvided(String[] args) throws NoParametersHaveBeenPassedException {
        if (args == null || args.length == 0) {
            throw new NoParametersHaveBeenPassedException();
        }
    }

    private void checkIfJobNamesHasBeenDefined() throws NoJobToExecuteException {
        if (!StringUtils.hasText(jobName)) {
            throw new NoJobToExecuteException();
        }
    }

    private Properties readPropertiesFromFile(String propertyFilePath) throws IOException, IllegalArgumentException {
        InputStream input = new FileInputStream(propertyFilePath);
        Properties properties = new Properties();
        properties.load(input);
        return properties;
    }

}
