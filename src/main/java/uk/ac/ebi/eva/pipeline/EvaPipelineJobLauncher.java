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
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.converter.JobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import uk.ac.ebi.eva.pipeline.runner.ManageJobsUtils;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NoJobToExecute;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NoParametersHaveBeenPassed;
import uk.ac.ebi.eva.pipeline.runner.exceptions.NoPreviousJobExecution;
import uk.ac.ebi.eva.pipeline.runner.exceptions.UnexpectedErrorReadingFile;
import uk.ac.ebi.eva.pipeline.runner.exceptions.UnexpectedFileCodification;
import uk.ac.ebi.eva.pipeline.runner.exceptions.UnrecognizedElement;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

@Component
public class EvaPipelineJobLauncher extends JobLauncherCommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(EvaPipelineJobLauncher.class);

    public static final String SPRING_BATCH_JOB_NAME_PROPERTY = "spring.batch.job.names";
    public static final String PROPERTY_FILE_PROPERTY = "properties.file";
    private static final String PROPERTY_FORCE_NAME = "force.restart";

    @Value("${spring.batch.job.names:#{null}}")
    private String jobNames;

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

            setJobNames(jobNames);

            if (forceRestart) {
                logger.info("Force restart against job '" + jobNames + "' with parameters: " + properties);
                ManageJobsUtils.markLastJobAsFailed(jobRepository, jobNames,
                        converter.getJobParameters(properties));
            }
            logger.info("Running job '" + jobNames + "' with parameters: " + properties);
            launchJobFromProperties(properties);
        } catch (NoJobToExecute | NoParametersHaveBeenPassed | UnexpectedFileCodification | FileNotFoundException |
                UnexpectedErrorReadingFile | NoPreviousJobExecution | UnrecognizedElement e) {
            logger.error(e.getMessage());
        }
    }

    private void checkAllParametersStartByDoubleDash(String[] args) throws UnrecognizedElement {
        for (String arg : args) {
            if (!arg.startsWith("--")) {
                throw new UnrecognizedElement(arg);
            }
        }
    }

    private String[] removeStartingHypens(String[] args) {
        return Arrays.stream(args).map(arg -> arg.substring(2)).toArray(String[]::new);
    }

    private String[] filterLauncherOnlyParameters(String[] args) {
        return Arrays.stream(args).filter(arg -> isLaunchParameter(arg)).toArray(String[]::new);
    }

    private boolean isLaunchParameter(String arg) {
        return !(arg.startsWith(PROPERTY_FILE_PROPERTY + "=") || arg.startsWith(PROPERTY_FORCE_NAME + "=")
                || arg.startsWith(SPRING_BATCH_JOB_NAME_PROPERTY + "="));
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
        if (args == null || args.length == 0) {
            throw new NoParametersHaveBeenPassed();
        }
    }

    private void checkIfJobNamesHasBeenDefined() throws NoJobToExecute {
        if (jobNames == null) {
            throw new NoJobToExecute();
        }
    }

    private Properties readPropertiesFromFile(String propertyFilePath) throws IOException, IllegalArgumentException {
        InputStream input = new FileInputStream(propertyFilePath);
        Properties properties = new Properties();
        properties.load(input);
        return properties;
    }

    @Override
    public void setJobNames(String jobNames) {
        this.jobNames = jobNames;
        super.setJobNames(jobNames);
    }

    public void setPropertyFilePath(String propertyFilePath) {
        this.propertyFilePath = propertyFilePath;
    }
}
