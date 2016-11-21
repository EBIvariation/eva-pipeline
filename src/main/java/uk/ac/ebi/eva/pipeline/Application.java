/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
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
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * Main entry point. spring boot takes care of autowiring everything with the JobLauncherCommandLineRunner.
 * By default, no job will be run on startup. Use this to launch any job:
 * <p>
 * java -jar target/eva-pipeline-0.1.jar --spring.batch.job.names=load-genotyped-vcf
 * <p>
 * append any parameter needed.
 * TODO document all parameters
 */
@SpringBootApplication
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);
    public static final String VARIANT_ANNOTATION_MONGO_PROFILE = "variant-annotation-mongo";

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Application.class, args);
    }
}
