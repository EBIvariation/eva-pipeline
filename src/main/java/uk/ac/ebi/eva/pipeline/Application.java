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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;


/**
 * Main entry point. Spring Boot takes care of autowiring everything with the JobLauncherCommandLineRunner.
 * In order to select which job to run on start up, run a command like the following:
 * <p>
 * java -jar target/eva-pipeline.jar --spring.batch.job.names=load-genotyped-vcf
 * <p>
 * Append any parameter as needed.
 * TODO document all parameters
 */
@SpringBootApplication(exclude = {MongoDataAutoConfiguration.class, JobLauncherCommandLineRunner.class})
public class Application {

    public static final String VARIANT_WRITER_MONGO_PROFILE = "variant-writer-mongo";
    public static final String VARIANT_ANNOTATION_MONGO_PROFILE = "variant-annotation-mongo";

    /**
     * Profile for features that shall run in production only, such as a persistent job repository.
     */
    public static final String PRODUCTION_PROFILE = "production";

    /**
     * Profile for experimental features that shall not be active unless explicitly requested.
     */
    public static final String MONGO_EXPERIMENTAL_PROFILE = "experimental";

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
        System.exit(SpringApplication.exit(context));
    }
}
