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
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.converter.JobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import uk.ac.ebi.eva.pipeline.runner.ManageJobsUtils;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NoJobToExecute;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NoParametersHaveBeenPassed;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NoPreviousJobExecution;
import uk.ac.ebi.eva.pipeline.runner.exceptions.UnexpectedErrorReadingFile;
import uk.ac.ebi.eva.pipeline.runner.exceptions.UnexpectedFileCodification;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

@Component
@Profile(Application.EXCLUDE_INTEGRATION_TEST_PROFILE)
public class EvaPipelineJobLauncher extends JobLauncherCommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(EvaPipelineJobLauncher.class);

    public static final String PROPERTY_FILE_PROPERTY = "properties.file";
    private static final String PROPERTY_FORCE_NAME = "force.restart";

    @Value("${spring.batch.job.names:#{null}}")
    private String springBatchJob;

    @Value("${" + PROPERTY_FILE_PROPERTY + ":#{null}}")
    private String propertyFilePath;

    @Value("${" + PROPERTY_FORCE_NAME + ":false}")
    private boolean forceRestart;

    @Autowired
    private JobRepository jobRepository;

    private JobParametersConverter converter = new DefaultJobParametersConverter();

    public EvaPipelineJobLauncher(JobLauncher jobLauncher, JobExplorer jobExplorer) {
        super(jobLauncher, jobExplorer);
    }

    @Autowired(required = false)
    @Override
    public void setJobParametersConverter(JobParametersConverter converter) {
        this.converter = converter;
        super.setJobParametersConverter(converter);
    }

    @Override
    public void run(String... args) throws JobExecutionException {
        try {
            checkIfJobHasBeenDefined();
            checkIfPropertiesHaveBeenProvided(args);

            // Command line properties have precedence over file defined ones.
            Properties properties = new Properties();
            if (propertyFilePath != null) {
                properties.putAll(getPropertiesFile());
            }
            properties.putAll(StringUtils.splitArrayElementsIntoProperties(args, "="));

            setJobNames(springBatchJob);

            if(forceRestart){
                ManageJobsUtils.markLastJobAsFailed(jobRepository, springBatchJob,
                        converter.getJobParameters(properties));
            }

            properties = removeLauncherSpecificProperties(properties);
            logger.info("Running job '" + springBatchJob + "' with properties: " + properties);
            launchJobFromProperties(properties);
        } catch (NoJobToExecute | NoParametersHaveBeenPassed | UnexpectedFileCodification | FileNotFoundException |
                UnexpectedErrorReadingFile | NoPreviousJobExecution e) {
            logger.error(e.getMessage());
        }

    }

    private Properties removeLauncherSpecificProperties(Properties unfilteredProperties) {
        Properties filteredProperties = new Properties();
        filteredProperties.putAll(unfilteredProperties);
        filteredProperties.remove(PROPERTY_FILE_PROPERTY);
        filteredProperties.remove(PROPERTY_FORCE_NAME);
        return filteredProperties;
    }

    private Properties getPropertiesFile() throws FileNotFoundException, UnexpectedErrorReadingFile,
            UnexpectedFileCodification {
        Properties propertiesFile = new Properties();
        try {
            propertiesFile.putAll(readPropertiesFromFile(propertyFilePath));
            return propertiesFile;
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("Property file '" + propertyFilePath + "' is not available.");
        } catch (IOException e) {
            throw new UnexpectedErrorReadingFile(propertyFilePath);
        } catch (IllegalArgumentException e) {
            throw new UnexpectedFileCodification(propertyFilePath);
        }
    }

    private void checkIfPropertiesHaveBeenProvided(String[] args) throws NoParametersHaveBeenPassed {
        if (args == null) {
            throw new NoParametersHaveBeenPassed();
        }
    }

    private void checkIfJobHasBeenDefined() throws NoJobToExecute {
        if (springBatchJob == null) {
            throw new NoJobToExecute();
        }
    }

    private Properties readPropertiesFromFile(String propertyFilePath) throws IOException, IllegalArgumentException {
        InputStream input = new FileInputStream(propertyFilePath);
        Properties properties = new Properties();
        properties.load(input);
        return properties;
    }

}
