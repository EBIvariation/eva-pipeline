package embl.ebi.variation.eva;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * Main entry point. spring boot takes care of autowiring everything with the JobLauncherCommandLineRunner.
 * By default, no job will be run on startup. Use this to launch any job:
 *
 *     java -jar target/gs-batch-processing-0.1.0.jar --spring.batch.job.names=variantJob
 *
 * or, create a file `application.properties` in your working directory setting it.
 *
 *     spring.batch.job.names=variantJob
 *
 */
@SpringBootApplication
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Application.class, args);
    }
}
