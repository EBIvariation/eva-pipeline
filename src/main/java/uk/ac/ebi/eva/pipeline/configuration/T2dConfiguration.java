package uk.ac.ebi.eva.pipeline.configuration;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;

@Configuration
@Profile(Application.T2D_PROFILE)
@ComponentScan("uk.ac.ebi.eva.t2d")
public class T2dConfiguration {
}
